package service

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"trend_recognition/internal/config"
)

const (
	timeLayoutClock = "15:04:05"
	timeLayoutDate  = "2006-01-02"
)

// Tick represents one normalized live price event consumed from NATS.
type Tick struct {
	Symbol        string
	Time          time.Time
	LTP           float64
	DayOpen       float64
	CumulativeVol int64
	Source        string
}

// MinuteBar represents one completed 1-minute candle built from live ticks.
type MinuteBar struct {
	Start   time.Time `json:"start"`
	Open    float64   `json:"open"`
	High    float64   `json:"high"`
	Low     float64   `json:"low"`
	Close   float64   `json:"close"`
	Volume  float64   `json:"volume"`
	TickCnt int       `json:"tick_cnt"`
}

// PullbackState tracks open-price excursion and reclaim sequence for the pull_back strategy.
type PullbackState struct {
	SeenUpMove           bool
	SeenDownMove         bool
	SeenUpOppBeforeDown  bool
	SeenDownOppBeforeUp  bool
	ReclaimedAfterUp     bool
	ReclaimedAfterDown   bool
	ReclaimedAfterUpAt   time.Time
	ReclaimedAfterDownAt time.Time
}

// SymbolState keeps intraday aggregation for one symbol.
type SymbolState struct {
	Symbol          string
	TradingDate     string
	DayOpen         float64
	DayHigh         float64
	DayLow          float64
	LastPrice       float64
	LastTick        time.Time
	LastCumVolume   int64
	VWAPPV          float64
	VWAPVolume      float64
	CurrentBar      *MinuteBar
	CompletedBars   []MinuteBar
	FirstFiveHigh   float64
	FirstFiveLow    float64
	FirstFiveReady  bool
	Pullback        PullbackState
	LastDecisionRaw string
}

// LaneState tracks one index lane and its active positions/pending signals.
type LaneState struct {
	ID                         string
	StrategyName               string
	PullbackStrategyName       string
	Symbols                    map[string]struct{}
	OpenPositions              map[string]string
	PendingEntries             map[string]time.Time
	LastSignalBySymbol         map[string]time.Time
	SignalsToday               int64
	PullbackOpenPositions      map[string]string
	PullbackPendingEntries     map[string]time.Time
	PullbackLastSignalBySymbol map[string]time.Time
	PullbackTriggeredSymbols   map[string]struct{}
	PullbackSignalsToday       int64
	TradingDate                string
	LastEvalAt                 time.Time
	LastSummary                string
	LastSelected               []Decision
}

// Decision captures one ranked entry decision.
type Decision struct {
	Symbol      string  `json:"symbol"`
	Side        string  `json:"side"`
	Score       float64 `json:"score"`
	MoveOpenPct float64 `json:"move_open_pct"`
	RVOL        float64 `json:"rvol"`
	VWAP        float64 `json:"vwap"`
	StopLossPct float64 `json:"stop_loss_pct"`
	TargetPct   float64 `json:"target_pct"`
	Reason      string  `json:"reason"`
}

// dashboardResponse models the detail JSON served at /dashboard.
type dashboardResponse struct {
	StartedAt        time.Time                `json:"started_at"`
	UptimeSeconds    int64                    `json:"uptime_seconds"`
	ConnectionStatus map[string]bool          `json:"connection_status"`
	Metrics          map[string]float64       `json:"metrics"`
	System           map[string]float64       `json:"system"`
	Lanes            []map[string]interface{} `json:"lanes"`
	SignalsTotal     int64                    `json:"signals_total"`
}

// metricsStore owns all prometheus collectors used by this service.
type metricsStore struct {
	upGauge              prometheus.Gauge
	brokerConnectedGauge prometheus.Gauge
	ticksCounter         prometheus.Counter
	signalsCounter       prometheus.Counter
	errorsCounter        prometheus.Counter
	lastTickGauge        prometheus.Gauge
	symbolsGauge         prometheus.Gauge
	laneSignalsCounter   *prometheus.CounterVec
	laneOpenGauge        *prometheus.GaugeVec
	lanePendingGauge     *prometheus.GaugeVec
	laneRejectCounter    *prometheus.CounterVec
}

// Service implements the trend_recognition strategy runtime.
type Service struct {
	cfg       *config.AppConfig
	loc       *time.Location
	startedAt time.Time

	logger  *log.Logger
	logFile *os.File
	debug   bool

	nc        *nats.Conn
	ticksSub  *nats.Subscription
	tradesSub *nats.Subscription
	httpSrv   *http.Server

	mu          sync.RWMutex
	symbols     map[string]*SymbolState
	lanes       map[string]*LaneState
	laneOrder   []string
	lastEvalMin time.Time

	ticksConsumed atomic.Int64
	signalsOut    atomic.Int64
	errorsTotal   atomic.Int64
	lastTickUnix  atomic.Int64
	brokerOK      atomic.Bool

	metrics *metricsStore
}

// New constructs and validates the strategy service instance.
func New(cfg *config.AppConfig) (*Service, error) {
	loc, err := time.LoadLocation(cfg.Strategy.Timezone)
	if err != nil {
		return nil, fmt.Errorf("load timezone %q: %w", cfg.Strategy.Timezone, err)
	}

	logger, file, debug, err := buildLogger(cfg.Logging)
	if err != nil {
		return nil, err
	}

	s := &Service{
		cfg:       cfg,
		loc:       loc,
		startedAt: time.Now().In(loc),
		logger:    logger,
		logFile:   file,
		debug:     debug,
		symbols:   make(map[string]*SymbolState),
		lanes:     make(map[string]*LaneState),
		metrics:   registerMetrics(),
	}

	if err := s.loadLanes(); err != nil {
		return nil, err
	}
	s.metrics.symbolsGauge.Set(float64(len(s.symbolsUniverse())))
	s.logger.Printf("INFO service initialized lanes=%d symbols_union=%d", len(s.lanes), len(s.symbolsUniverse()))
	return s, nil
}

// Start runs subscriptions, evaluation loop, and HTTP endpoints until context cancellation.
func (s *Service) Start(ctx context.Context) error {
	defer s.closeLogger()

	if err := s.connectBroker(); err != nil {
		return err
	}
	defer s.closeBroker()

	if err := s.startHTTP(); err != nil {
		return err
	}
	defer s.stopHTTP()

	s.lastEvalMin = time.Now().In(s.loc).Truncate(time.Minute)
	evalTicker := time.NewTicker(1 * time.Second)
	defer evalTicker.Stop()

	s.logger.Printf("INFO trend_recognition started metrics=%s", s.cfg.Service.MetricsAddress)

	for {
		select {
		case <-ctx.Done():
			s.logger.Printf("INFO shutdown requested: %v", ctx.Err())
			return nil
		case now := <-evalTicker.C:
			s.runMinuteEvaluation(now.In(s.loc))
		}
	}
}

// buildLogger creates an append logger writing to stdout and service log file.
func buildLogger(cfg config.LoggingConfig) (*log.Logger, *os.File, bool, error) {
	level := strings.ToLower(strings.TrimSpace(cfg.Level))
	debug := level == "debug"

	filePath := strings.TrimSpace(cfg.FilePath)
	if filePath == "" {
		filePath = "logs/trend_recognition.log"
	}
	if !filepath.IsAbs(filePath) {
		if cwd, err := os.Getwd(); err == nil {
			filePath = filepath.Join(cwd, filePath)
		}
	}
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, nil, false, fmt.Errorf("create log dir: %w", err)
	}
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, false, fmt.Errorf("open log file: %w", err)
	}
	logger := log.New(ioMultiWriter(os.Stdout, f), "", log.LstdFlags|log.Lmicroseconds)
	return logger, f, debug, nil
}

// ioMultiWriter avoids importing io in every logging call site.
func ioMultiWriter(writers ...*os.File) *multiWriter {
	return &multiWriter{writers: writers}
}

// multiWriter is a minimal stdout+file fanout writer.
type multiWriter struct {
	writers []*os.File
}

// Write forwards bytes to all configured outputs.
func (m *multiWriter) Write(p []byte) (int, error) {
	for _, w := range m.writers {
		if w == nil {
			continue
		}
		if _, err := w.Write(p); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

// closeLogger flushes and closes the service log file handle.
func (s *Service) closeLogger() {
	if s.logFile != nil {
		_ = s.logFile.Close()
	}
}

// registerMetrics registers all prometheus collectors with default registry.
func registerMetrics() *metricsStore {
	m := &metricsStore{
		upGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trend_recognition_up",
			Help: "Service up gauge (1=running)",
		}),
		brokerConnectedGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trend_recognition_broker_connected",
			Help: "Broker connection state (1=connected)",
		}),
		ticksCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trend_recognition_ticks_consumed_total",
			Help: "Total ticks consumed from live subject",
		}),
		signalsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trend_recognition_signals_emitted_total",
			Help: "Total strategy signals emitted",
		}),
		errorsCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trend_recognition_errors_total",
			Help: "Total service-level errors",
		}),
		lastTickGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trend_recognition_last_tick_unixtime",
			Help: "Unix timestamp for latest consumed tick",
		}),
		symbolsGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trend_recognition_symbols_tracking",
			Help: "Number of unique symbols tracked across all lanes",
		}),
		laneSignalsCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "trend_recognition_lane_signals_total",
			Help: "Signals emitted per lane",
		}, []string{"lane"}),
		laneOpenGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "trend_recognition_lane_open_positions",
			Help: "Open positions per lane",
		}, []string{"lane"}),
		lanePendingGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "trend_recognition_lane_pending_entries",
			Help: "Pending entries per lane",
		}, []string{"lane"}),
		laneRejectCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "trend_recognition_lane_rejections_total",
			Help: "Rejected candidates per lane and reason",
		}, []string{"lane", "reason"}),
	}
	prometheus.MustRegister(
		m.upGauge,
		m.brokerConnectedGauge,
		m.ticksCounter,
		m.signalsCounter,
		m.errorsCounter,
		m.lastTickGauge,
		m.symbolsGauge,
		m.laneSignalsCounter,
		m.laneOpenGauge,
		m.lanePendingGauge,
		m.laneRejectCounter,
	)
	m.upGauge.Set(1)
	return m
}

// loadLanes reads configured CSVs and prepares lane symbol universes.
func (s *Service) loadLanes() error {
	for _, laneCfg := range s.cfg.Lanes {
		symbols, err := loadSymbolsCSV(resolvePathCandidates(laneCfg.SymbolsCSV)...)
		if err != nil {
			return fmt.Errorf("lane %s symbols load failed: %w", laneCfg.ID, err)
		}
		lane := &LaneState{
			ID:                         laneCfg.ID,
			StrategyName:               laneCfg.StrategyName,
			PullbackStrategyName:       laneCfg.PullbackStrategyName,
			Symbols:                    symbols,
			OpenPositions:              make(map[string]string),
			PendingEntries:             make(map[string]time.Time),
			LastSignalBySymbol:         make(map[string]time.Time),
			PullbackOpenPositions:      make(map[string]string),
			PullbackPendingEntries:     make(map[string]time.Time),
			PullbackLastSignalBySymbol: make(map[string]time.Time),
			PullbackTriggeredSymbols:   make(map[string]struct{}),
		}
		s.lanes[laneCfg.ID] = lane
		s.laneOrder = append(s.laneOrder, laneCfg.ID)
		for sym := range symbols {
			if _, ok := s.symbols[sym]; !ok {
				s.symbols[sym] = &SymbolState{Symbol: sym}
			}
		}
		s.logger.Printf("INFO lane loaded id=%s strategy=%s pullback_strategy=%s symbols=%d", lane.ID, lane.StrategyName, lane.PullbackStrategyName, len(symbols))
	}
	return nil
}

// resolvePathCandidates returns possible filesystem locations for config-relative files.
func resolvePathCandidates(path string) []string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil
	}
	candidates := []string{trimmed}
	if filepath.IsAbs(trimmed) {
		return candidates
	}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, trimmed))
	}
	candidates = append(candidates,
		filepath.Join("/Users/vikasbhandekar/live_services", trimmed),
		filepath.Join("/Users/vikasbhandekar/Desktop/TradingArchitecture", trimmed),
	)
	return candidates
}

// loadSymbolsCSV loads unique uppercase symbols from a CSV containing a Symbol column.
func loadSymbolsCSV(candidates ...string) (map[string]struct{}, error) {
	var lastErr error
	for _, p := range candidates {
		if strings.TrimSpace(p) == "" {
			continue
		}
		file, err := os.Open(p)
		if err != nil {
			lastErr = err
			continue
		}
		defer file.Close()
		reader := csv.NewReader(file)
		rows, err := reader.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read csv %q: %w", p, err)
		}
		if len(rows) == 0 {
			return nil, fmt.Errorf("empty csv %q", p)
		}
		col := findSymbolColumn(rows[0])
		if col < 0 {
			return nil, fmt.Errorf("symbol column not found in %q", p)
		}
		out := make(map[string]struct{})
		for i := 1; i < len(rows); i++ {
			if col >= len(rows[i]) {
				continue
			}
			sym := strings.ToUpper(strings.TrimSpace(rows[i][col]))
			if sym == "" {
				continue
			}
			out[sym] = struct{}{}
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("no symbols parsed in %q", p)
		}
		return out, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no candidates provided")
	}
	return nil, lastErr
}

// findSymbolColumn identifies the symbol column index in a CSV header row.
func findSymbolColumn(header []string) int {
	for idx, raw := range header {
		h := strings.ToLower(strings.TrimSpace(raw))
		if h == "symbol" || h == "ticker" {
			return idx
		}
	}
	return -1
}

// symbolsUniverse returns deduplicated symbol list across all lanes.
func (s *Service) symbolsUniverse() map[string]struct{} {
	out := make(map[string]struct{})
	for _, lane := range s.lanes {
		for sym := range lane.Symbols {
			out[sym] = struct{}{}
		}
	}
	return out
}

// connectBroker opens NATS connection and starts tick/trade subscriptions.
func (s *Service) connectBroker() error {
	nc, err := nats.Connect(s.cfg.Broker.BootstrapServers,
		nats.Name("trend_recognition_service"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, discErr error) {
			s.brokerOK.Store(false)
			s.metrics.brokerConnectedGauge.Set(0)
			s.logger.Printf("WARN broker disconnected: %v", discErr)
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			s.brokerOK.Store(true)
			s.metrics.brokerConnectedGauge.Set(1)
			s.logger.Printf("INFO broker reconnected")
		}),
	)
	if err != nil {
		return fmt.Errorf("connect broker: %w", err)
	}
	s.nc = nc
	s.brokerOK.Store(true)
	s.metrics.brokerConnectedGauge.Set(1)

	ticksSub, err := s.nc.Subscribe(s.cfg.Broker.TicksTopic, s.onTickMessage)
	if err != nil {
		return fmt.Errorf("subscribe ticks subject %q: %w", s.cfg.Broker.TicksTopic, err)
	}
	tradesSub, err := s.nc.Subscribe(s.cfg.Broker.TradesTopic, s.onTradeMessage)
	if err != nil {
		return fmt.Errorf("subscribe trades subject %q: %w", s.cfg.Broker.TradesTopic, err)
	}
	s.ticksSub = ticksSub
	s.tradesSub = tradesSub
	s.logger.Printf("INFO broker subscriptions active ticks=%s trades=%s", s.cfg.Broker.TicksTopic, s.cfg.Broker.TradesTopic)
	return nil
}

// closeBroker drains active subscriptions and closes NATS connection.
func (s *Service) closeBroker() {
	if s.ticksSub != nil {
		_ = s.ticksSub.Unsubscribe()
	}
	if s.tradesSub != nil {
		_ = s.tradesSub.Unsubscribe()
	}
	if s.nc != nil {
		_ = s.nc.Drain()
		s.nc.Close()
	}
	s.brokerOK.Store(false)
	s.metrics.brokerConnectedGauge.Set(0)
}

// startHTTP starts /metrics and /dashboard handlers.
func (s *Service) startHTTP() error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc(s.cfg.Service.DashboardPath, s.handleDashboard)
	s.httpSrv = &http.Server{
		Addr:              s.cfg.Service.MetricsAddress,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Printf("ERROR metrics server stopped: %v", err)
		}
	}()
	return nil
}

// stopHTTP gracefully shuts down the service HTTP server.
func (s *Service) stopHTTP() {
	if s.httpSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.httpSrv.Shutdown(ctx)
}

// onTickMessage decodes one tick payload and updates in-memory market state.
func (s *Service) onTickMessage(msg *nats.Msg) {
	tick, err := parseTick(msg.Subject, msg.Data)
	if err != nil {
		s.recordError("tick_parse")
		s.logDebugf("DEBUG tick parse failed subject=%s err=%v", msg.Subject, err)
		return
	}
	if tick.Symbol == "" || tick.LTP <= 0 || tick.Time.IsZero() {
		s.recordError("tick_invalid")
		return
	}
	s.mu.Lock()
	s.applyTickLocked(tick)
	s.evaluatePullbackOnTickLocked(tick)
	s.mu.Unlock()
	s.ticksConsumed.Add(1)
	s.lastTickUnix.Store(tick.Time.Unix())
	s.metrics.ticksCounter.Inc()
	s.metrics.lastTickGauge.Set(float64(tick.Time.Unix()))
}

// onTradeMessage consumes ENTRY/EXIT fills to maintain per-lane open trade caps.
func (s *Service) onTradeMessage(msg *nats.Msg) {
	var ev struct {
		Symbol    string `json:"symbol"`
		TradeType string `json:"trade_type"`
		Side      string `json:"side"`
		Strategy  string `json:"strategy"`
		Time      string `json:"time"`
		Reason    string `json:"reason"`
	}
	if err := json.Unmarshal(msg.Data, &ev); err != nil {
		s.recordError("trade_parse")
		return
	}
	if strings.TrimSpace(ev.Strategy) == "" || strings.TrimSpace(ev.Symbol) == "" {
		return
	}
	symbol := strings.ToUpper(strings.TrimSpace(ev.Symbol))
	tradeType := strings.ToUpper(strings.TrimSpace(ev.TradeType))

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, lane := range s.lanes {
		tradingDate := time.Now().In(s.loc).Format(timeLayoutDate)
		s.ensureLaneTradingDateLocked(lane, tradingDate)
		isTrend := lane.StrategyName == ev.Strategy
		isPullback := lane.PullbackStrategyName == ev.Strategy
		if !isTrend && !isPullback {
			continue
		}
		switch tradeType {
		case "ENTRY":
			if isTrend {
				lane.OpenPositions[symbol] = strings.ToUpper(strings.TrimSpace(ev.Side))
				delete(lane.PendingEntries, symbol)
			}
			if isPullback {
				lane.PullbackOpenPositions[symbol] = strings.ToUpper(strings.TrimSpace(ev.Side))
				delete(lane.PullbackPendingEntries, symbol)
			}
		case "EXIT":
			if isTrend {
				delete(lane.OpenPositions, symbol)
				delete(lane.PendingEntries, symbol)
			}
			if isPullback {
				delete(lane.PullbackOpenPositions, symbol)
				delete(lane.PullbackPendingEntries, symbol)
			}
		}
		totalOpen := len(lane.OpenPositions) + len(lane.PullbackOpenPositions)
		totalPending := len(lane.PendingEntries) + len(lane.PullbackPendingEntries)
		s.metrics.laneOpenGauge.WithLabelValues(lane.ID).Set(float64(totalOpen))
		s.metrics.lanePendingGauge.WithLabelValues(lane.ID).Set(float64(totalPending))
		s.logger.Printf("INFO trade_event lane=%s strategy=%s symbol=%s type=%s reason=%s open=%d pending=%d",
			lane.ID, ev.Strategy, symbol, tradeType, ev.Reason, totalOpen, totalPending)
	}
}

// applyTickLocked applies one normalized tick into symbol/day/bar state.
func (s *Service) applyTickLocked(t Tick) {
	st, ok := s.symbols[t.Symbol]
	if !ok {
		return
	}
	tradingDate := t.Time.In(s.loc).Format(timeLayoutDate)
	if st.TradingDate != tradingDate {
		st.TradingDate = tradingDate
		st.DayOpen = 0
		st.DayHigh = 0
		st.DayLow = 0
		st.LastPrice = 0
		st.LastTick = time.Time{}
		st.LastCumVolume = 0
		st.VWAPPV = 0
		st.VWAPVolume = 0
		st.CurrentBar = nil
		st.CompletedBars = st.CompletedBars[:0]
		st.FirstFiveHigh = 0
		st.FirstFiveLow = 0
		st.FirstFiveReady = false
		st.Pullback = PullbackState{}
	}

	price := t.LTP
	if st.DayOpen <= 0 {
		if t.DayOpen > 0 {
			st.DayOpen = t.DayOpen
		} else {
			st.DayOpen = price
		}
	}
	if st.DayHigh <= 0 || price > st.DayHigh {
		st.DayHigh = price
	}
	if st.DayLow <= 0 || price < st.DayLow {
		st.DayLow = price
	}
	st.LastPrice = price
	st.LastTick = t.Time

	volDelta := 0.0
	if t.CumulativeVol > 0 {
		if st.LastCumVolume > 0 && t.CumulativeVol >= st.LastCumVolume {
			volDelta = float64(t.CumulativeVol - st.LastCumVolume)
		}
		st.LastCumVolume = t.CumulativeVol
	}
	if volDelta <= 0 {
		volDelta = 1
	}
	st.VWAPPV += price * volDelta
	st.VWAPVolume += volDelta

	minute := t.Time.In(s.loc).Truncate(time.Minute)
	if st.CurrentBar == nil || !st.CurrentBar.Start.Equal(minute) {
		if st.CurrentBar != nil {
			s.appendCompletedBar(st, *st.CurrentBar)
		}
		st.CurrentBar = &MinuteBar{
			Start:   minute,
			Open:    price,
			High:    price,
			Low:     price,
			Close:   price,
			Volume:  volDelta,
			TickCnt: 1,
		}
		return
	}

	if price > st.CurrentBar.High {
		st.CurrentBar.High = price
	}
	if price < st.CurrentBar.Low {
		st.CurrentBar.Low = price
	}
	st.CurrentBar.Close = price
	st.CurrentBar.Volume += volDelta
	st.CurrentBar.TickCnt++
}

// appendCompletedBar stores a finalized bar and caps memory usage.
func (s *Service) appendCompletedBar(st *SymbolState, bar MinuteBar) {
	st.CompletedBars = append(st.CompletedBars, bar)
	if len(st.CompletedBars) > 450 {
		st.CompletedBars = st.CompletedBars[len(st.CompletedBars)-450:]
	}
}

// evaluatePullbackOnTickLocked evaluates the pull_back setup on every tick and emits lane-specific signals.
func (s *Service) evaluatePullbackOnTickLocked(t Tick) {
	st, ok := s.symbols[t.Symbol]
	if !ok || st.DayOpen <= 0 || st.LastPrice <= 0 {
		return
	}

	ts := t.Time.In(s.loc)
	if !isWeekday(ts) || !s.isWithinPullbackWindow(ts) {
		return
	}

	movePct := pctChange(st.LastPrice, st.DayOpen)
	pb := &st.Pullback

	// Rule 2: if stock first goes up by 0.2% before down move setup, skip BUY setup for the day.
	if !pb.SeenDownMove && movePct >= s.cfg.Strategy.PullbackOppositePct {
		pb.SeenUpOppBeforeDown = true
	}
	// Symmetric guard for SELL noise filtering.
	if !pb.SeenUpMove && movePct <= -s.cfg.Strategy.PullbackOppositePct {
		pb.SeenDownOppBeforeUp = true
	}

	if movePct >= s.cfg.Strategy.PullbackMovePct {
		pb.SeenUpMove = true
	}
	if movePct <= -s.cfg.Strategy.PullbackMovePct {
		pb.SeenDownMove = true
	}

	isNearOpen := math.Abs(movePct) <= s.cfg.Strategy.PullbackReclaimTolerancePct
	if isNearOpen && pb.SeenUpMove && !pb.ReclaimedAfterUp {
		pb.ReclaimedAfterUp = true
		pb.ReclaimedAfterUpAt = ts
	}
	if isNearOpen && pb.SeenDownMove && !pb.ReclaimedAfterDown {
		pb.ReclaimedAfterDown = true
		pb.ReclaimedAfterDownAt = ts
	}

	// Rule 3: allow trigger only between 10 and 40 second in the minute.
	if ts.Second() < s.cfg.Strategy.PullbackSecondStart || ts.Second() > s.cfg.Strategy.PullbackSecondEnd {
		return
	}

	triggerSide := ""
	triggerReason := ""
	if pb.ReclaimedAfterDown && !pb.SeenUpOppBeforeDown {
		triggerSide = "BUY"
		triggerReason = fmt.Sprintf("pullback: moved %.2f%% down from open then reclaimed open within %02d-%02d sec",
			s.cfg.Strategy.PullbackMovePct, s.cfg.Strategy.PullbackSecondStart, s.cfg.Strategy.PullbackSecondEnd)
	}
	if pb.ReclaimedAfterUp {
		useSell := true
		if pb.ReclaimedAfterDown && pb.ReclaimedAfterDownAt.After(pb.ReclaimedAfterUpAt) {
			useSell = false
		}
		if useSell {
			triggerSide = "SELL"
			triggerReason = fmt.Sprintf("pullback: moved %.2f%% up from open then reclaimed open within %02d-%02d sec",
				s.cfg.Strategy.PullbackMovePct, s.cfg.Strategy.PullbackSecondStart, s.cfg.Strategy.PullbackSecondEnd)
		}
	}

	if triggerSide == "" {
		return
	}

	tradingDate := ts.Format(timeLayoutDate)
	for _, laneID := range s.laneOrder {
		lane := s.lanes[laneID]
		if _, inLane := lane.Symbols[t.Symbol]; !inLane {
			continue
		}
		s.ensureLaneTradingDateLocked(lane, tradingDate)
		if lane.PullbackSignalsToday >= int64(s.cfg.Strategy.PullbackMaxSignalsPerLane) {
			continue
		}
		if len(lane.PullbackOpenPositions)+len(lane.PullbackPendingEntries) >= s.cfg.Strategy.MaxOpenTradesPerLane {
			continue
		}
		if _, already := lane.PullbackTriggeredSymbols[t.Symbol]; already {
			continue
		}
		if _, open := lane.PullbackOpenPositions[t.Symbol]; open {
			continue
		}
		if _, pending := lane.PullbackPendingEntries[t.Symbol]; pending {
			continue
		}
		if last, seen := lane.PullbackLastSignalBySymbol[t.Symbol]; seen {
			if ts.Sub(last) < time.Duration(s.cfg.Strategy.SymbolCooldownSec)*time.Second {
				continue
			}
		}

		reason := triggerReason
		if triggerSide == "BUY" && pb.SeenUpOppBeforeDown {
			reason = "pullback rejected: +0.2% move seen before down-reclaim sequence"
			s.logger.Printf("INFO reject lane=%s strategy=%s symbol=%s reason=%s", lane.ID, lane.PullbackStrategyName, t.Symbol, reason)
			s.metrics.laneRejectCounter.WithLabelValues(lane.ID, "pullback_buy_pre_up_0_2").Inc()
			continue
		}

		if err := s.emitPullbackSignalLocked(lane, t.Symbol, triggerSide, reason, ts); err != nil {
			s.recordError("publish_pullback_signal")
			s.logger.Printf("ERROR emit pullback signal lane=%s strategy=%s symbol=%s side=%s err=%v",
				lane.ID, lane.PullbackStrategyName, t.Symbol, triggerSide, err)
			continue
		}
		lane.PullbackTriggeredSymbols[t.Symbol] = struct{}{}
		s.logger.Printf("INFO selected lane=%s strategy=%s symbol=%s side=%s reason=%s pullback_signals_today=%d",
			lane.ID, lane.PullbackStrategyName, t.Symbol, triggerSide, reason, lane.PullbackSignalsToday)
	}
}

// runMinuteEvaluation triggers one full lane scan exactly once per minute.
func (s *Service) runMinuteEvaluation(now time.Time) {
	currentMin := now.Truncate(time.Minute)
	if !currentMin.After(s.lastEvalMin) {
		return
	}
	evalMin := currentMin.Add(-time.Minute)
	s.lastEvalMin = currentMin

	if !isWeekday(evalMin) {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, laneID := range s.laneOrder {
		lane := s.lanes[laneID]
		s.ensureLaneTradingDateLocked(lane, evalMin.Format(timeLayoutDate))
		s.cleanupPendingLocked(lane, evalMin)
		if !s.isWithinTradeSessions(evalMin) {
			lane.LastEvalAt = evalMin
			lane.LastSummary = "outside trade sessions"
			continue
		}
		selected, summary := s.evaluateLaneLocked(lane, evalMin)
		lane.LastEvalAt = evalMin
		lane.LastSelected = selected
		lane.LastSummary = summary
		totalOpen := len(lane.OpenPositions) + len(lane.PullbackOpenPositions)
		totalPending := len(lane.PendingEntries) + len(lane.PullbackPendingEntries)
		s.metrics.laneOpenGauge.WithLabelValues(lane.ID).Set(float64(totalOpen))
		s.metrics.lanePendingGauge.WithLabelValues(lane.ID).Set(float64(totalPending))
	}
}

// cleanupPendingLocked removes stale pending entries that never converted to ENTRY.
func (s *Service) cleanupPendingLocked(lane *LaneState, now time.Time) {
	timeout := time.Duration(s.cfg.Strategy.PendingEntryTimeoutSec) * time.Second
	for symbol, ts := range lane.PendingEntries {
		if now.Sub(ts) > timeout {
			delete(lane.PendingEntries, symbol)
			s.logger.Printf("WARN pending_timeout lane=%s symbol=%s age_sec=%d", lane.ID, symbol, int(now.Sub(ts).Seconds()))
		}
	}
	for symbol, ts := range lane.PullbackPendingEntries {
		if now.Sub(ts) > timeout {
			delete(lane.PullbackPendingEntries, symbol)
			s.logger.Printf("WARN pullback_pending_timeout lane=%s symbol=%s age_sec=%d", lane.ID, symbol, int(now.Sub(ts).Seconds()))
		}
	}
}

// evaluateLaneLocked scores one lane and emits signals up to available slots.
func (s *Service) evaluateLaneLocked(lane *LaneState, evalMin time.Time) ([]Decision, string) {
	availableSlots := s.cfg.Strategy.MaxOpenTradesPerLane - len(lane.OpenPositions) - len(lane.PendingEntries)
	if availableSlots <= 0 {
		s.logger.Printf("INFO eval lane=%s minute=%s skipped=no_slot open=%d pending=%d",
			lane.ID, evalMin.Format(time.RFC3339), len(lane.OpenPositions), len(lane.PendingEntries))
		return nil, "no slot available"
	}

	bias := s.computeLaneBiasLocked(lane)
	candidates := make([]Decision, 0, 64)
	rejected := 0

	for symbol := range lane.Symbols {
		decision, reason := s.evaluateSymbolLocked(lane, symbol, evalMin, bias)
		if reason != "" {
			rejected++
			s.metrics.laneRejectCounter.WithLabelValues(lane.ID, reason).Inc()
			s.logger.Printf("INFO reject lane=%s minute=%s symbol=%s reason=%s",
				lane.ID, evalMin.Format(time.RFC3339), symbol, reason)
			continue
		}
		candidates = append(candidates, decision)
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Score == candidates[j].Score {
			return candidates[i].MoveOpenPct > candidates[j].MoveOpenPct
		}
		return candidates[i].Score > candidates[j].Score
	})

	selected := make([]Decision, 0, availableSlots)
	for _, cand := range candidates {
		if availableSlots <= 0 {
			break
		}
		if _, alreadyOpen := lane.OpenPositions[cand.Symbol]; alreadyOpen {
			continue
		}
		if _, pending := lane.PendingEntries[cand.Symbol]; pending {
			continue
		}
		if ts, seen := lane.LastSignalBySymbol[cand.Symbol]; seen {
			if evalMin.Sub(ts) < time.Duration(s.cfg.Strategy.SymbolCooldownSec)*time.Second {
				continue
			}
		}
		if err := s.emitSignalLocked(lane, cand, evalMin); err != nil {
			s.recordError("publish_signal")
			s.logger.Printf("ERROR emit signal lane=%s symbol=%s err=%v", lane.ID, cand.Symbol, err)
			continue
		}
		selected = append(selected, cand)
		availableSlots--
	}

	for _, sel := range selected {
		s.logger.Printf("INFO selected lane=%s strategy=%s minute=%s symbol=%s side=%s score=%.2f move_open=%.2f rvol=%.2f sl=%.2f target=%.2f reason=%s",
			lane.ID, lane.StrategyName, evalMin.Format(time.RFC3339), sel.Symbol, sel.Side, sel.Score,
			sel.MoveOpenPct, sel.RVOL, sel.StopLossPct, sel.TargetPct, sel.Reason)
	}

	s.logger.Printf("INFO eval lane=%s minute=%s bias=%.3f candidates=%d selected=%d rejected=%d open=%d pending=%d",
		lane.ID, evalMin.Format(time.RFC3339), bias, len(candidates), len(selected), rejected, len(lane.OpenPositions), len(lane.PendingEntries))
	return selected, fmt.Sprintf("candidates=%d selected=%d rejected=%d", len(candidates), len(selected), rejected)
}

// computeLaneBiasLocked calculates average % move from day open in the lane.
func (s *Service) computeLaneBiasLocked(lane *LaneState) float64 {
	sum := 0.0
	count := 0
	for sym := range lane.Symbols {
		st := s.symbols[sym]
		if st == nil || st.DayOpen <= 0 || st.LastPrice <= 0 {
			continue
		}
		sum += pctChange(st.LastPrice, st.DayOpen)
		count++
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// evaluateSymbolLocked produces one candidate decision or a reject reason.
func (s *Service) evaluateSymbolLocked(lane *LaneState, symbol string, evalMin time.Time, laneBias float64) (Decision, string) {
	st := s.symbols[symbol]
	if st == nil {
		return Decision{}, "state_missing"
	}
	if st.TradingDate != evalMin.Format(timeLayoutDate) {
		return Decision{}, "stale_day"
	}
	bar, ok := latestBarForMinute(st.CompletedBars, evalMin)
	if !ok {
		return Decision{}, "no_1m_bar"
	}
	decisionPrice := bar.Close
	if st.DayOpen <= 0 || decisionPrice <= 0 {
		return Decision{}, "no_open_or_price"
	}

	s.ensureFirstFiveRange(st, evalMin)
	if !st.FirstFiveReady {
		return Decision{}, "first5_not_ready"
	}

	avgVol := averageBarVolume(st.CompletedBars, evalMin, 20)
	rvol := 0.0
	if avgVol > 0 {
		rvol = bar.Volume / avgVol
	}
	moveOpenPct := pctChange(decisionPrice, st.DayOpen)
	vwap := decisionPrice
	if st.VWAPVolume > 0 {
		vwap = st.VWAPPV / st.VWAPVolume
	}
	extVWAP := math.Abs(pctChange(decisionPrice, vwap))
	if extVWAP > s.cfg.Strategy.MaxExtensionFromVWAPPct {
		return Decision{}, "overextended_vs_vwap"
	}
	if rvol < s.cfg.Strategy.MinRVOL {
		return Decision{}, "rvol_below_threshold"
	}

	nearHigh := nearExtreme(decisionPrice, st.DayHigh, s.cfg.Strategy.NearExtremePct)
	nearLow := nearExtreme(st.DayLow, decisionPrice, s.cfg.Strategy.NearExtremePct)
	breakout := decisionPrice >= st.FirstFiveHigh*(1+s.cfg.Strategy.BreakoutBufferPct/100)
	breakdown := decisionPrice <= st.FirstFiveLow*(1-s.cfg.Strategy.BreakoutBufferPct/100)
	pullbackLong := pullbackDepthFromHigh(st.CompletedBars, 10)
	pullbackShort := pullbackDepthFromLow(st.CompletedBars, 10)

	longScore := 0.0
	shortScore := 0.0
	if moveOpenPct >= s.cfg.Strategy.MinMoveFromOpenPct {
		longScore += 2
	}
	if moveOpenPct <= -s.cfg.Strategy.MinMoveFromOpenPct {
		shortScore += 2
	}
	if rvol >= 1.5 {
		longScore += 1.5
		shortScore += 1.5
	}
	if rvol >= 2.2 {
		longScore += 0.8
		shortScore += 0.8
	}
	if decisionPrice > vwap {
		longScore += 1.5
	}
	if decisionPrice < vwap {
		shortScore += 1.5
	}
	if decisionPrice > st.DayOpen {
		longScore += 1
	}
	if decisionPrice < st.DayOpen {
		shortScore += 1
	}
	if breakout || nearHigh {
		longScore += 1.5
	}
	if breakdown || nearLow {
		shortScore += 1.5
	}
	if laneBias >= 0 {
		longScore += 1
	}
	if laneBias <= 0 {
		shortScore += 1
	}
	if pullbackLong <= s.cfg.Strategy.PullbackShallowPct {
		longScore += 0.7
	}
	if pullbackShort <= s.cfg.Strategy.PullbackShallowPct {
		shortScore += 0.7
	}

	if longScore < s.cfg.Strategy.MinScoreToTrade && shortScore < s.cfg.Strategy.MinScoreToTrade {
		return Decision{}, "score_below_threshold"
	}

	if longScore >= shortScore {
		if moveOpenPct < s.cfg.Strategy.MinMoveFromOpenPct || decisionPrice <= vwap || decisionPrice <= st.DayOpen || (!breakout && !nearHigh) {
			return Decision{}, "long_structure_fail"
		}
		sl, target := s.riskForScore(longScore)
		return Decision{
			Symbol:      symbol,
			Side:        "BUY",
			Score:       longScore,
			MoveOpenPct: moveOpenPct,
			RVOL:        rvol,
			VWAP:        vwap,
			StopLossPct: sl,
			TargetPct:   target,
			Reason: fmt.Sprintf("LONG trend score=%.2f move_open=%.2f rvol=%.2f breakout=%t near_high=%t lane_bias=%.2f pullback=%.2f",
				longScore, moveOpenPct, rvol, breakout, nearHigh, laneBias, pullbackLong),
		}, ""
	}

	if moveOpenPct > -s.cfg.Strategy.MinMoveFromOpenPct || decisionPrice >= vwap || decisionPrice >= st.DayOpen || (!breakdown && !nearLow) {
		return Decision{}, "short_structure_fail"
	}
	sl, target := s.riskForScore(shortScore)
	return Decision{
		Symbol:      symbol,
		Side:        "SELL",
		Score:       shortScore,
		MoveOpenPct: moveOpenPct,
		RVOL:        rvol,
		VWAP:        vwap,
		StopLossPct: sl,
		TargetPct:   target,
		Reason: fmt.Sprintf("SHORT trend score=%.2f move_open=%.2f rvol=%.2f breakdown=%t near_low=%t lane_bias=%.2f pullback=%.2f",
			shortScore, moveOpenPct, rvol, breakdown, nearLow, laneBias, pullbackShort),
	}, ""
}

// ensureFirstFiveRange calculates first 5-minute opening range once available.
func (s *Service) ensureFirstFiveRange(st *SymbolState, evalMin time.Time) {
	if st.FirstFiveReady {
		return
	}
	day := evalMin.Format(timeLayoutDate)
	openTs, err := parseClock(day, s.cfg.Strategy.MarketOpenTime, s.loc)
	if err != nil {
		return
	}
	endTs := openTs.Add(5 * time.Minute)
	firstBars := make([]MinuteBar, 0, 5)
	for _, bar := range st.CompletedBars {
		if bar.Start.Before(openTs) || !bar.Start.Before(endTs) {
			continue
		}
		firstBars = append(firstBars, bar)
	}
	if len(firstBars) < 5 {
		return
	}
	high := firstBars[0].High
	low := firstBars[0].Low
	for _, b := range firstBars[1:] {
		if b.High > high {
			high = b.High
		}
		if b.Low < low {
			low = b.Low
		}
	}
	st.FirstFiveHigh = high
	st.FirstFiveLow = low
	st.FirstFiveReady = true
}

// riskForScore maps momentum score to adaptive stop-loss and target percentages.
func (s *Service) riskForScore(score float64) (float64, float64) {
	if score >= s.cfg.Risk.StrongScoreThreshold {
		return s.cfg.Risk.StrongStopLossPct, s.cfg.Risk.StrongTargetPct
	}
	if score >= s.cfg.Risk.MediumScoreThreshold {
		return s.cfg.Risk.MediumStopLossPct, s.cfg.Risk.MediumTargetPct
	}
	return s.cfg.Risk.WeakStopLossPct, s.cfg.Risk.WeakTargetPct
}

// emitSignalLocked publishes a strategy signal and marks the symbol as pending.
func (s *Service) emitSignalLocked(lane *LaneState, d Decision, now time.Time) error {
	signalID := buildSignalID(lane.StrategyName, d.Symbol, d.Side, now)
	payload := map[string]interface{}{
		"signal_id":             signalID,
		"strategy":              lane.StrategyName,
		"strategy_version":      "v1",
		"signal_source_service": "trend_recognition",
		"symbol":                d.Symbol,
		"side":                  d.Side,
		"time":                  now.Format(time.RFC3339Nano),
		"timeframe":             "1m",
		"reason":                d.Reason,
		"setup_type":            "TREND_RECOGNITION",
		"trigger_type":          lane.ID,
		"capital_multiplier":    s.cfg.Risk.CapitalMultiplier,
		"stop_loss_pct":         d.StopLossPct,
		"target_pct":            d.TargetPct,
		"entry_context": map[string]interface{}{
			"signal_price":   s.symbols[d.Symbol].LastPrice,
			"day_open":       s.symbols[d.Symbol].DayOpen,
			"last_tick_time": s.symbols[d.Symbol].LastTick.Format(time.RFC3339Nano),
		},
		"market_context": map[string]interface{}{
			"score":         d.Score,
			"move_open_pct": d.MoveOpenPct,
			"rvol":          d.RVOL,
			"vwap":          d.VWAP,
			"lane_id":       lane.ID,
		},
		"risk_context": map[string]interface{}{
			"capital_multiplier":    s.cfg.Risk.CapitalMultiplier,
			"planned_stop_loss_pct": d.StopLossPct,
			"planned_target_pct":    d.TargetPct,
		},
		"filter_context": map[string]interface{}{
			"lane_id":                lane.ID,
			"open_positions":         len(lane.OpenPositions),
			"pending_entries":        len(lane.PendingEntries),
			"signals_today":          lane.SignalsToday,
			"strong_score_threshold": s.cfg.Risk.StrongScoreThreshold,
			"medium_score_threshold": s.cfg.Risk.MediumScoreThreshold,
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := s.nc.Publish(s.cfg.Broker.SignalTopic, raw); err != nil {
		return err
	}
	lane.PendingEntries[d.Symbol] = now
	lane.LastSignalBySymbol[d.Symbol] = now
	lane.SignalsToday++
	s.metrics.signalsCounter.Inc()
	s.metrics.laneSignalsCounter.WithLabelValues(lane.ID).Inc()
	s.metrics.lanePendingGauge.WithLabelValues(lane.ID).Set(float64(len(lane.PendingEntries)))
	s.signalsOut.Add(1)
	return nil
}

// emitPullbackSignalLocked publishes a pull_back strategy signal in the paper-engine format.
func (s *Service) emitPullbackSignalLocked(lane *LaneState, symbol, side, reason string, now time.Time) error {
	signalID := buildSignalID(lane.PullbackStrategyName, symbol, side, now)
	payload := map[string]interface{}{
		"signal_id":             signalID,
		"strategy":              lane.PullbackStrategyName,
		"strategy_version":      "v1",
		"signal_source_service": "trend_recognition",
		"symbol":                symbol,
		"side":                  side,
		"time":                  now.Format(time.RFC3339Nano),
		"timeframe":             "1m",
		"reason":                reason,
		"setup_type":            "TREND_PULLBACK_RECLAIM",
		"trigger_type":          lane.ID + "_pullback",
		"capital_multiplier":    s.cfg.Risk.CapitalMultiplier,
		"stop_loss_pct":         s.cfg.Strategy.PullbackStopLossPct,
		"target_pct":            s.cfg.Strategy.PullbackTargetPct,
		"entry_context": map[string]interface{}{
			"signal_price":   s.symbols[symbol].LastPrice,
			"day_open":       s.symbols[symbol].DayOpen,
			"last_tick_time": s.symbols[symbol].LastTick.Format(time.RFC3339Nano),
		},
		"market_context": map[string]interface{}{
			"lane_id": lane.ID,
		},
		"risk_context": map[string]interface{}{
			"capital_multiplier":    s.cfg.Risk.CapitalMultiplier,
			"planned_stop_loss_pct": s.cfg.Strategy.PullbackStopLossPct,
			"planned_target_pct":    s.cfg.Strategy.PullbackTargetPct,
		},
		"filter_context": map[string]interface{}{
			"lane_id":                  lane.ID,
			"pullback_pending_entries": len(lane.PullbackPendingEntries),
			"pullback_signals_today":   lane.PullbackSignalsToday,
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := s.nc.Publish(s.cfg.Broker.SignalTopic, raw); err != nil {
		return err
	}

	lane.PullbackPendingEntries[symbol] = now
	lane.PullbackLastSignalBySymbol[symbol] = now
	lane.PullbackSignalsToday++
	s.metrics.signalsCounter.Inc()
	s.metrics.laneSignalsCounter.WithLabelValues(lane.ID).Inc()
	s.metrics.lanePendingGauge.WithLabelValues(lane.ID).Set(float64(len(lane.PendingEntries) + len(lane.PullbackPendingEntries)))
	s.signalsOut.Add(1)
	return nil
}

func buildSignalID(strategy, symbol, side string, at time.Time) string {
	return fmt.Sprintf("%s|%s|%s|%d", strategy, strings.ToUpper(strings.TrimSpace(symbol)), strings.ToUpper(strings.TrimSpace(side)), at.UnixNano())
}

// handleDashboard serves JSON detail used by service-detail dashboard page.
func (s *Service) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	resp := dashboardResponse{
		StartedAt:     s.startedAt,
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
		ConnectionStatus: map[string]bool{
			"broker_connected": s.brokerOK.Load(),
			"service_up":       true,
		},
		Metrics: map[string]float64{
			"trend_recognition_up":                    1,
			"trend_recognition_broker_connected":      boolToFloat(s.brokerOK.Load()),
			"trend_recognition_ticks_consumed_total":  float64(s.ticksConsumed.Load()),
			"trend_recognition_signals_emitted_total": float64(s.signalsOut.Load()),
			"trend_recognition_errors_total":          float64(s.errorsTotal.Load()),
			"trend_recognition_last_tick_unixtime":    float64(s.lastTickUnix.Load()),
			"trend_recognition_symbols_tracking":      float64(len(s.symbolsUniverse())),
		},
		System: map[string]float64{
			"cpu_percent":      0,
			"goroutines":       float64(runtime.NumGoroutine()),
			"heap_alloc_bytes": float64(readMemStats().HeapAlloc),
			"sys_bytes":        float64(readMemStats().Sys),
		},
		SignalsTotal: s.signalsOut.Load(),
	}

	for _, laneID := range s.laneOrder {
		l := s.lanes[laneID]
		row := map[string]interface{}{
			"id":                      l.ID,
			"strategy":                l.StrategyName,
			"pullback_strategy":       l.PullbackStrategyName,
			"symbols":                 len(l.Symbols),
			"open_positions":          len(l.OpenPositions),
			"pending":                 len(l.PendingEntries),
			"signals_today":           l.SignalsToday,
			"pullback_open_positions": len(l.PullbackOpenPositions),
			"pullback_pending":        len(l.PullbackPendingEntries),
			"pullback_signals_today":  l.PullbackSignalsToday,
			"last_eval_at":            l.LastEvalAt,
			"last_summary":            l.LastSummary,
			"last_selected":           l.LastSelected,
		}
		resp.Lanes = append(resp.Lanes, row)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// recordError increments error counters with one helper.
func (s *Service) recordError(_ string) {
	s.errorsTotal.Add(1)
	s.metrics.errorsCounter.Inc()
}

// logDebugf writes debug logs only when debug level is enabled.
func (s *Service) logDebugf(format string, args ...interface{}) {
	if s.debug {
		s.logger.Printf(format, args...)
	}
}

// ensureLaneTradingDateLocked resets per-day lane counters/maps when the trading date changes.
func (s *Service) ensureLaneTradingDateLocked(lane *LaneState, tradingDate string) {
	if lane.TradingDate == tradingDate {
		return
	}
	lane.TradingDate = tradingDate
	lane.OpenPositions = make(map[string]string)
	lane.PendingEntries = make(map[string]time.Time)
	lane.LastSignalBySymbol = make(map[string]time.Time)
	lane.SignalsToday = 0
	lane.PullbackOpenPositions = make(map[string]string)
	lane.PullbackPendingEntries = make(map[string]time.Time)
	lane.PullbackLastSignalBySymbol = make(map[string]time.Time)
	lane.PullbackTriggeredSymbols = make(map[string]struct{})
	lane.PullbackSignalsToday = 0
}

// isWithinTradeSessions checks if current time is inside morning/closing trading windows.
func (s *Service) isWithinTradeSessions(ts time.Time) bool {
	day := ts.Format(timeLayoutDate)
	mStart, err1 := parseClock(day, s.cfg.Strategy.MorningWindowStart, s.loc)
	mEnd, err2 := parseClock(day, s.cfg.Strategy.MorningWindowEnd, s.loc)
	cStart, err3 := parseClock(day, s.cfg.Strategy.ClosingWindowStart, s.loc)
	cEnd, err4 := parseClock(day, s.cfg.Strategy.ClosingWindowEnd, s.loc)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		return false
	}
	inMorning := (ts.Equal(mStart) || ts.After(mStart)) && ts.Before(mEnd)
	inClosing := (ts.Equal(cStart) || ts.After(cStart)) && ts.Before(cEnd)
	return inMorning || inClosing
}

// isWithinPullbackWindow checks if tick time is inside pull_back strategy runtime window.
func (s *Service) isWithinPullbackWindow(ts time.Time) bool {
	day := ts.Format(timeLayoutDate)
	start, err1 := parseClock(day, s.cfg.Strategy.PullbackWindowStart, s.loc)
	end, err2 := parseClock(day, s.cfg.Strategy.PullbackWindowEnd, s.loc)
	if err1 != nil || err2 != nil {
		return false
	}
	return (ts.Equal(start) || ts.After(start)) && ts.Before(end)
}

// parseClock converts HH:MM:SS values into local timestamp for a given date.
func parseClock(datePart, clock string, loc *time.Location) (time.Time, error) {
	full := strings.TrimSpace(datePart) + " " + strings.TrimSpace(clock)
	return time.ParseInLocation("2006-01-02 "+timeLayoutClock, full, loc)
}

// latestBarForMinute finds bar with exact minute timestamp.
func latestBarForMinute(bars []MinuteBar, minute time.Time) (MinuteBar, bool) {
	for i := len(bars) - 1; i >= 0; i-- {
		if bars[i].Start.Equal(minute) {
			return bars[i], true
		}
		if bars[i].Start.Before(minute) {
			break
		}
	}
	return MinuteBar{}, false
}

// averageBarVolume computes historical average minute volume before target minute.
func averageBarVolume(bars []MinuteBar, targetMin time.Time, lookback int) float64 {
	sum := 0.0
	count := 0
	for i := len(bars) - 1; i >= 0 && count < lookback; i-- {
		if !bars[i].Start.Before(targetMin) {
			continue
		}
		sum += bars[i].Volume
		count++
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}

// pullbackDepthFromHigh returns depth from highest high in last N bars to latest close.
func pullbackDepthFromHigh(bars []MinuteBar, lookback int) float64 {
	if len(bars) == 0 {
		return 0
	}
	end := len(bars)
	start := end - lookback
	if start < 0 {
		start = 0
	}
	high := bars[start].High
	for i := start + 1; i < end; i++ {
		if bars[i].High > high {
			high = bars[i].High
		}
	}
	last := bars[end-1].Close
	if high <= 0 {
		return 0
	}
	return ((high - last) / high) * 100
}

// pullbackDepthFromLow returns bounce depth from lowest low in last N bars to latest close.
func pullbackDepthFromLow(bars []MinuteBar, lookback int) float64 {
	if len(bars) == 0 {
		return 0
	}
	end := len(bars)
	start := end - lookback
	if start < 0 {
		start = 0
	}
	low := bars[start].Low
	for i := start + 1; i < end; i++ {
		if bars[i].Low < low {
			low = bars[i].Low
		}
	}
	last := bars[end-1].Close
	if low <= 0 {
		return 0
	}
	return ((last - low) / low) * 100
}

// nearExtreme checks whether base and value are close within threshold pct.
func nearExtreme(base, value, thresholdPct float64) bool {
	if base <= 0 || value <= 0 {
		return false
	}
	return math.Abs((base-value)/base*100) <= thresholdPct
}

// pctChange computes percentage move from reference to current.
func pctChange(current, reference float64) float64 {
	if reference == 0 {
		return 0
	}
	return ((current - reference) / reference) * 100
}

// boolToFloat converts bools into numeric gauges.
func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// readMemStats captures runtime memory counters for dashboard rendering.
func readMemStats() runtime.MemStats {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms
}

// isWeekday returns true for Monday-Friday timestamps.
func isWeekday(ts time.Time) bool {
	wd := ts.Weekday()
	return wd >= time.Monday && wd <= time.Friday
}

// parseTick supports GO_FEED payloads and plain-symbol fallback payloads.
func parseTick(subject string, raw []byte) (Tick, error) {
	var envelope struct {
		Timestamp string `json:"timestamp"`
		Payload   *struct {
			Symbol    string  `json:"symbol"`
			Timestamp string  `json:"timestamp"`
			LTP       float64 `json:"ltp"`
			DayOpen   float64 `json:"day_open"`
			Volume    int64   `json:"volume"`
		} `json:"payload"`
		Symbol string  `json:"symbol"`
		Time   string  `json:"time"`
		LTP    float64 `json:"ltp"`
		Volume int64   `json:"volume"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return Tick{}, err
	}

	var symbol string
	var tsText string
	var ltp float64
	var open float64
	var vol int64

	if envelope.Payload != nil {
		symbol = envelope.Payload.Symbol
		tsText = envelope.Payload.Timestamp
		ltp = envelope.Payload.LTP
		open = envelope.Payload.DayOpen
		vol = envelope.Payload.Volume
	} else {
		symbol = envelope.Symbol
		tsText = envelope.Time
		ltp = envelope.LTP
		vol = envelope.Volume
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if tsText == "" {
		tsText = envelope.Timestamp
	}
	tm, err := time.Parse(time.RFC3339Nano, tsText)
	if err != nil {
		return Tick{}, err
	}
	source := parseTickSource(subject)
	return Tick{
		Symbol:        symbol,
		Time:          tm,
		LTP:           ltp,
		DayOpen:       open,
		CumulativeVol: vol,
		Source:        source,
	}, nil
}

// parseTickSource extracts upstream feed name from subject format ticks.raw.<source>.<symbol>.
func parseTickSource(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) >= 3 {
		return strings.ToUpper(strings.TrimSpace(parts[2]))
	}
	return ""
}
