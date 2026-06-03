package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	serviceName      = "ldrb_strategy"
	defaultNATSURL   = "nats://127.0.0.1:4222"
	defaultStockSubj = "candle.raw.>"
	defaultIndexSubj = "indices.raw.>"
	defaultSignalSub = "signals.strategy"
)

type appConfig struct {
	Env   string `json:"env"`
	Kafka struct {
		BootstrapServers      string `json:"bootstrap_servers"`
		GroupID               string `json:"group_id"`
		StockCandlesTopic     string `json:"stock_candles_topic"`
		IndexCandlesTopic     string `json:"index_candles_topic"`
		SignalTopic           string `json:"signal_topic"`
		CommitIntervalMs      int    `json:"commit_interval_ms"`
		StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
	} `json:"kafka"`
	Dependencies struct {
		EventsPath           string `json:"events_path"`
		VolumeProfilePath    string `json:"volume_profile_path"`
		TurnoverRankingPath  string `json:"turnover_ranking_path"`
		EnableSafeMode       bool   `json:"enable_safe_mode"`
		AllowRVOLFallback    bool   `json:"allow_rvol_fallback"`
		SkipAllIfEventsEmpty bool   `json:"skip_all_if_events_missing"`
	} `json:"dependencies"`
	Strategy struct {
		Timezone               string  `json:"timezone"`
		SessionStart           string  `json:"session_start"`
		EntryStart             string  `json:"entry_start"`
		EntryEnd               string  `json:"entry_end"`
		SameDayExitCutoff      string  `json:"same_day_exit_cutoff"`
		Capital                float64 `json:"capital"`
		RiskPerTradePct        float64 `json:"risk_per_trade_pct"`
		MaxTradesPerDay        int     `json:"max_trades_per_day"`
		MinPrice               float64 `json:"min_price"`
		MaxIntradayMovePct     float64 `json:"max_intraday_move_pct"`
		BodyRatioMin           float64 `json:"body_ratio_min"`
		VolumeRatioMin         float64 `json:"volume_ratio_min"`
		RVOLMin                float64 `json:"rvol_min"`
		BreakoutCushionPct     float64 `json:"breakout_cushion_pct"`
		SlippageBufferPct      float64 `json:"slippage_buffer_pct"`
		MinStopDistancePct     float64 `json:"min_stop_distance_pct"`
		MaxStopDistancePct     float64 `json:"max_stop_distance_pct"`
		MarketDrawdownLimitPct float64 `json:"market_drawdown_limit_pct"`
		LiquidityTopN          int     `json:"liquidity_top_n"`
		SwingStart             string  `json:"swing_start"`
		IndexSymbol            string  `json:"index_symbol"`
		EnableMarketFilter     bool    `json:"enable_market_filter"`
		EnableEventFilter      bool    `json:"enable_event_filter"`
	} `json:"strategy"`
}

type candle struct {
	Symbol    string  `json:"symbol"`
	Time      string  `json:"time"`
	Timeframe string  `json:"timeframe"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    int64   `json:"volume"`
	VWAP      float64 `json:"vwap"`
}

type parsedCandle struct {
	candle
	At time.Time
}

type symbolState struct {
	Day         string
	SessionOpen float64
	HighSince   float64
	LowSince    float64
	SwingHigh   float64
	Candles     []parsedCandle
	Signaled    bool
}

type volumeProfile struct {
	AvgDailyVolume10 float64 `json:"avg_daily_volume_10"`
	AvgVolume1M      float64 `json:"avg_volume_1m"`
}

type metricsStore struct {
	up      prometheus.Gauge
	candles prometheus.Counter
	signals prometheus.Counter
	errors  prometheus.Counter
	filters *prometheus.CounterVec
}

type minuteStats struct {
	mu     sync.Mutex
	counts map[string]int
}

type service struct {
	cfg        appConfig
	loc        *time.Location
	logger     *log.Logger
	nc         *nats.Conn
	stockSub   *nats.Subscription
	indexSub   *nats.Subscription
	states     map[string]*symbolState
	indexes    map[string]*symbolState
	events     map[string]bool
	volume     map[string]volumeProfile
	liquidity  map[string]int
	signalsDay string
	signalsOut int
	mu         sync.Mutex
	stats      *minuteStats
	metrics    *metricsStore
	entryStart time.Duration
	entryEnd   time.Duration
	swingStart time.Duration
}

func main() {
	configPath := flag.String("config", "config/ldrb_strategy_config.json", "path to config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}
	logger, closeLog, err := newServiceLogger(serviceName)
	if err != nil {
		log.Fatalf("logger error: %v", err)
	}
	defer closeLog()

	svc, err := newService(cfg, logger)
	if err != nil {
		logger.Fatalf("startup error: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := svc.run(ctx); err != nil {
		logger.Fatalf("runtime error: %v", err)
	}
}

func loadConfig(path string) (appConfig, error) {
	var cfg appConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	if cfg.Kafka.BootstrapServers == "" {
		cfg.Kafka.BootstrapServers = defaultNATSURL
	}
	if cfg.Kafka.StockCandlesTopic == "" {
		cfg.Kafka.StockCandlesTopic = defaultStockSubj
	}
	if cfg.Kafka.IndexCandlesTopic == "" {
		cfg.Kafka.IndexCandlesTopic = defaultIndexSubj
	}
	if cfg.Kafka.SignalTopic == "" {
		cfg.Kafka.SignalTopic = defaultSignalSub
	}
	if cfg.Strategy.Timezone == "" {
		cfg.Strategy.Timezone = "Asia/Kolkata"
	}
	if cfg.Strategy.SessionStart == "" {
		cfg.Strategy.SessionStart = "09:15"
	}
	if cfg.Strategy.EntryStart == "" {
		cfg.Strategy.EntryStart = "14:30"
	}
	if cfg.Strategy.EntryEnd == "" {
		cfg.Strategy.EntryEnd = "15:15"
	}
	if cfg.Strategy.SwingStart == "" {
		cfg.Strategy.SwingStart = "11:00"
	}
	if cfg.Strategy.IndexSymbol == "" {
		cfg.Strategy.IndexSymbol = "NIFTY"
	}
	if cfg.Strategy.MaxTradesPerDay <= 0 {
		cfg.Strategy.MaxTradesPerDay = 2
	}
	if cfg.Strategy.MinPrice <= 0 {
		cfg.Strategy.MinPrice = 100
	}
	if cfg.Strategy.MaxIntradayMovePct <= 0 {
		cfg.Strategy.MaxIntradayMovePct = 0.04
	}
	if cfg.Strategy.BodyRatioMin <= 0 {
		cfg.Strategy.BodyRatioMin = 0.60
	}
	if cfg.Strategy.VolumeRatioMin <= 0 {
		cfg.Strategy.VolumeRatioMin = 2.0
	}
	if cfg.Strategy.RVOLMin <= 0 {
		cfg.Strategy.RVOLMin = 1.5
	}
	if cfg.Strategy.BreakoutCushionPct <= 0 {
		cfg.Strategy.BreakoutCushionPct = 0.0015
	}
	if cfg.Strategy.MinStopDistancePct <= 0 {
		cfg.Strategy.MinStopDistancePct = 0.003
	}
	if cfg.Strategy.MaxStopDistancePct <= 0 {
		cfg.Strategy.MaxStopDistancePct = 0.012
	}
	if cfg.Strategy.LiquidityTopN <= 0 {
		cfg.Strategy.LiquidityTopN = 200
	}
	return cfg, nil
}

func newServiceLogger(name string) (*log.Logger, func(), error) {
	now := time.Now()
	logDir := filepath.Join("logs", now.Format("2006-01-02"))
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, nil, err
	}
	filePath := filepath.Join(logDir, now.Format("150405")+"_"+name+".log")
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, err
	}
	logger := log.New(io.MultiWriter(os.Stdout, f), "", 0)
	logger.Printf(`{"level":"INFO","service":"%s","message":"logger initialized","path":"%s"}`, name, filePath)
	return logger, func() { _ = f.Close() }, nil
}

func newService(cfg appConfig, logger *log.Logger) (*service, error) {
	loc, err := time.LoadLocation(cfg.Strategy.Timezone)
	if err != nil {
		return nil, err
	}
	entryStart, err := parseHHMM(cfg.Strategy.EntryStart)
	if err != nil {
		return nil, err
	}
	entryEnd, err := parseHHMM(cfg.Strategy.EntryEnd)
	if err != nil {
		return nil, err
	}
	swingStart, err := parseHHMM(cfg.Strategy.SwingStart)
	if err != nil {
		return nil, err
	}
	svc := &service{
		cfg:        cfg,
		loc:        loc,
		logger:     logger,
		states:     make(map[string]*symbolState),
		indexes:    make(map[string]*symbolState),
		events:     make(map[string]bool),
		volume:     make(map[string]volumeProfile),
		liquidity:  make(map[string]int),
		stats:      &minuteStats{counts: make(map[string]int)},
		metrics:    registerMetrics(),
		entryStart: entryStart,
		entryEnd:   entryEnd,
		swingStart: swingStart,
	}
	svc.loadDependencies()
	return svc, nil
}

func registerMetrics() *metricsStore {
	m := &metricsStore{
		up:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "ldrb_strategy_up", Help: "Service up gauge (1=running)"}),
		candles: prometheus.NewCounter(prometheus.CounterOpts{Name: "ldrb_strategy_candles_consumed_total", Help: "Total candles consumed by ldrb_strategy"}),
		signals: prometheus.NewCounter(prometheus.CounterOpts{Name: "ldrb_strategy_signals_emitted_total", Help: "Total LDRB signals emitted"}),
		errors:  prometheus.NewCounter(prometheus.CounterOpts{Name: "ldrb_strategy_errors_total", Help: "Total errors in ldrb_strategy"}),
		filters: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "ldrb_strategy_filter_total", Help: "LDRB filter pass/reject counters"}, []string{"stage", "result"}),
	}
	prometheus.MustRegister(m.up, m.candles, m.signals, m.errors, m.filters)
	return m
}

func (s *service) loadDependencies() {
	s.loadEvents()
	s.loadVolumeProfile()
	s.loadLiquidityRanking()
	s.logger.Printf(`{"level":"INFO","service":"%s","message":"dependencies loaded","events":%d,"volume_profiles":%d,"liquidity_symbols":%d,"event_filter_enabled":%t,"rvol_fallback":%t}`, serviceName, len(s.events), len(s.volume), len(s.liquidity), s.cfg.Strategy.EnableEventFilter, s.cfg.Dependencies.AllowRVOLFallback)
}

func (s *service) loadEvents() {
	if strings.TrimSpace(s.cfg.Dependencies.EventsPath) == "" {
		return
	}
	data, err := os.ReadFile(s.cfg.Dependencies.EventsPath)
	if err != nil {
		s.logger.Printf(`{"level":"WARN","service":"%s","message":"events load failed","error":%q}`, serviceName, err.Error())
		return
	}
	var raw struct {
		Events []struct {
			Symbol string `json:"symbol"`
			Date   string `json:"date"`
		} `json:"events"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		s.logger.Printf(`{"level":"WARN","service":"%s","message":"events parse failed","error":%q}`, serviceName, err.Error())
		return
	}
	for _, ev := range raw.Events {
		s.events[strings.ToUpper(strings.TrimSpace(ev.Symbol))] = true
	}
}

func (s *service) loadVolumeProfile() {
	if strings.TrimSpace(s.cfg.Dependencies.VolumeProfilePath) == "" {
		return
	}
	data, err := os.ReadFile(s.cfg.Dependencies.VolumeProfilePath)
	if err != nil {
		s.logger.Printf(`{"level":"WARN","service":"%s","message":"volume profile load failed","error":%q}`, serviceName, err.Error())
		return
	}
	var raw struct {
		Symbols map[string]volumeProfile `json:"symbols"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		s.logger.Printf(`{"level":"WARN","service":"%s","message":"volume profile parse failed","error":%q}`, serviceName, err.Error())
		return
	}
	for sym, vp := range raw.Symbols {
		s.volume[strings.ToUpper(strings.TrimSpace(sym))] = vp
	}
}

func (s *service) loadLiquidityRanking() {
	if strings.TrimSpace(s.cfg.Dependencies.TurnoverRankingPath) == "" {
		return
	}
	f, err := os.Open(s.cfg.Dependencies.TurnoverRankingPath)
	if err != nil {
		s.logger.Printf(`{"level":"WARN","service":"%s","message":"liquidity ranking load failed","error":%q}`, serviceName, err.Error())
		return
	}
	defer f.Close()
	reader := csv.NewReader(f)
	rows, err := reader.ReadAll()
	if err != nil {
		s.logger.Printf(`{"level":"WARN","service":"%s","message":"liquidity ranking parse failed","error":%q}`, serviceName, err.Error())
		return
	}
	rank := 0
	for i, row := range rows {
		if i == 0 || len(row) == 0 {
			continue
		}
		rank++
		s.liquidity[strings.ToUpper(strings.TrimSpace(row[0]))] = rank
	}
}

func (s *service) run(ctx context.Context) error {
	s.metrics.up.Set(1)
	defer s.metrics.up.Set(0)
	go s.serveMetrics()
	if err := s.connect(); err != nil {
		return err
	}
	defer s.close()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	s.logger.Printf(`{"level":"INFO","service":"%s","message":"started","stock_subject":"%s","index_subject":"%s","signal_subject":"%s","entry_start":"%s","entry_end":"%s"}`, serviceName, normalizeSubject(s.cfg.Kafka.StockCandlesTopic, true), normalizeSubject(s.cfg.Kafka.IndexCandlesTopic, true), s.cfg.Kafka.SignalTopic, s.cfg.Strategy.EntryStart, s.cfg.Strategy.EntryEnd)
	for {
		select {
		case <-ctx.Done():
			s.logger.Printf(`{"level":"INFO","service":"%s","message":"shutdown requested"}`, serviceName)
			return nil
		case <-ticker.C:
			s.logMinuteSummary()
		}
	}
}

func (s *service) connect() error {
	nc, err := nats.Connect(s.cfg.Kafka.BootstrapServers, nats.Name(serviceName), nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		return err
	}
	s.nc = nc
	stockSub, err := nc.Subscribe(normalizeSubject(s.cfg.Kafka.StockCandlesTopic, true), func(msg *nats.Msg) { s.onCandle(msg, false) })
	if err != nil {
		return err
	}
	indexSub, err := nc.Subscribe(normalizeSubject(s.cfg.Kafka.IndexCandlesTopic, true), func(msg *nats.Msg) { s.onCandle(msg, true) })
	if err != nil {
		return err
	}
	s.stockSub = stockSub
	s.indexSub = indexSub
	return nil
}

func (s *service) close() {
	if s.stockSub != nil {
		_ = s.stockSub.Unsubscribe()
	}
	if s.indexSub != nil {
		_ = s.indexSub.Unsubscribe()
	}
	if s.nc != nil {
		_ = s.nc.Drain()
		s.nc.Close()
	}
}

func (s *service) serveMetrics() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	if err := http.ListenAndServe(":9106", mux); err != nil && !strings.Contains(err.Error(), "Server closed") {
		s.logger.Printf(`{"level":"ERROR","service":"%s","message":"metrics server failed","error":%q}`, serviceName, err.Error())
	}
}

func (s *service) onCandle(msg *nats.Msg, isIndex bool) {
	pc, err := decodeCandle(msg.Data)
	if err != nil {
		s.metrics.errors.Inc()
		s.addStat("decode_error")
		return
	}
	if pc.Symbol == "" || pc.Close <= 0 || pc.At.IsZero() {
		s.addStat("invalid_candle")
		return
	}
	if pc.Timeframe != "" && pc.Timeframe != "1m" {
		s.addStat("reject_timeframe")
		return
	}
	s.metrics.candles.Inc()
	s.mu.Lock()
	defer s.mu.Unlock()
	if isIndex {
		s.updateState(s.indexes, pc)
		s.addStat("index_candles")
		return
	}
	s.updateState(s.states, pc)
	s.evaluate(pc)
	s.refreshSwingHigh(s.states[pc.Symbol], pc)
}

func decodeCandle(data []byte) (parsedCandle, error) {
	var c candle
	if err := json.Unmarshal(data, &c); err != nil {
		return parsedCandle{}, err
	}
	c.Symbol = strings.ToUpper(strings.TrimSpace(c.Symbol))
	at, err := time.Parse(time.RFC3339Nano, c.Time)
	if err != nil {
		return parsedCandle{}, err
	}
	return parsedCandle{candle: c, At: at}, nil
}

func (s *service) updateState(states map[string]*symbolState, pc parsedCandle) *symbolState {
	day := pc.At.In(s.loc).Format("2006-01-02")
	st := states[pc.Symbol]
	if st == nil || st.Day != day {
		st = &symbolState{Day: day, SessionOpen: pc.Open, HighSince: pc.High, LowSince: pc.Low}
		states[pc.Symbol] = st
	}
	if st.SessionOpen <= 0 {
		st.SessionOpen = pc.Open
	}
	if pc.High > st.HighSince {
		st.HighSince = pc.High
	}
	if st.LowSince == 0 || pc.Low < st.LowSince {
		st.LowSince = pc.Low
	}
	st.Candles = append(st.Candles, pc)
	if len(st.Candles) > 120 {
		st.Candles = st.Candles[len(st.Candles)-120:]
	}
	return st
}

func (s *service) refreshSwingHigh(st *symbolState, pc parsedCandle) {
	if st == nil || timeOffset(pc.At.In(s.loc)) < s.swingStart {
		return
	}
	if pc.High > st.SwingHigh {
		st.SwingHigh = pc.High
	}
}

func (s *service) evaluate(pc parsedCandle) {
	s.addStat("stocks_seen")
	s.passFilter("seen", true)
	day := pc.At.In(s.loc).Format("2006-01-02")
	if s.signalsDay != day {
		s.signalsDay = day
		s.signalsOut = 0
	}
	if !s.inEntryWindow(pc.At) {
		s.reject("time_window")
		return
	}
	st := s.states[pc.Symbol]
	if st == nil || st.SessionOpen <= 0 || len(st.Candles) < 2 {
		s.reject("history")
		return
	}
	if st.Signaled || s.signalsOut >= s.cfg.Strategy.MaxTradesPerDay {
		s.reject("trade_limit")
		return
	}
	if !s.liquidityPass(pc.Symbol) {
		s.reject("liquidity")
		return
	}
	s.passFilter("liquidity", true)
	if pc.Close < s.cfg.Strategy.MinPrice {
		s.reject("price")
		return
	}
	s.passFilter("price", true)
	move := math.Abs(pc.Close-st.SessionOpen) / st.SessionOpen
	if move > s.cfg.Strategy.MaxIntradayMovePct {
		s.reject("overextended")
		return
	}
	s.passFilter("overextended", true)
	if s.cfg.Strategy.EnableMarketFilter && !s.marketPass() {
		s.reject("market")
		return
	}
	s.passFilter("market", true)
	if s.cfg.Strategy.EnableEventFilter && !s.eventPass(pc.Symbol) {
		s.reject("event")
		return
	}
	s.passFilter("event", true)
	if bodyRatio(pc) < s.cfg.Strategy.BodyRatioMin {
		s.reject("body")
		return
	}
	s.passFilter("body", true)
	if !s.volumePass(pc) {
		s.reject("volume")
		return
	}
	s.passFilter("volume", true)
	breakoutHigh, breakoutOK := s.breakoutReference(st, pc)
	if !breakoutOK {
		s.reject("breakout")
		return
	}
	s.passFilter("breakout", true)
	if err := s.emitSignal(pc, st, breakoutHigh); err != nil {
		s.metrics.errors.Inc()
		s.reject("publish_error")
		return
	}
	st.Signaled = true
	s.signalsOut++
	s.metrics.signals.Inc()
	s.addStat("signals")
}

func (s *service) liquidityPass(symbol string) bool {
	if len(s.liquidity) == 0 {
		s.addStat("liquidity_fallback")
		return true
	}
	rank, ok := s.liquidity[symbol]
	return ok && rank <= s.cfg.Strategy.LiquidityTopN
}

func (s *service) eventPass(symbol string) bool {
	if len(s.events) == 0 {
		if s.cfg.Dependencies.SkipAllIfEventsEmpty {
			return false
		}
		s.addStat("event_fallback")
		return true
	}
	return s.events[symbol]
}

func (s *service) volumePass(pc parsedCandle) bool {
	if pc.Volume <= 0 {
		if s.cfg.Dependencies.AllowRVOLFallback {
			s.addStat("volume_fallback")
			return true
		}
		return false
	}
	vp, ok := s.volume[pc.Symbol]
	if !ok || vp.AvgVolume1M <= 0 {
		if s.cfg.Dependencies.AllowRVOLFallback {
			s.addStat("volume_profile_fallback")
			return true
		}
		return false
	}
	return float64(pc.Volume)/vp.AvgVolume1M >= s.cfg.Strategy.RVOLMin
}

func (s *service) marketPass() bool {
	st := s.indexes[strings.ToUpper(s.cfg.Strategy.IndexSymbol)]
	if st == nil || st.SessionOpen <= 0 || len(st.Candles) == 0 {
		s.addStat("market_fallback")
		return true
	}
	last := st.Candles[len(st.Candles)-1]
	drawdown := (last.Close - st.SessionOpen) / st.SessionOpen
	return drawdown >= s.cfg.Strategy.MarketDrawdownLimitPct
}

func (s *service) breakoutReference(st *symbolState, pc parsedCandle) (float64, bool) {
	if st.SwingHigh <= 0 {
		return 0, false
	}
	return st.SwingHigh, pc.Close > st.SwingHigh*(1+s.cfg.Strategy.BreakoutCushionPct)
}

func (s *service) emitSignal(pc parsedCandle, st *symbolState, breakoutHigh float64) error {
	stopPct := s.cfg.Strategy.MinStopDistancePct * 100
	if stopPct <= 0 {
		stopPct = 0.5
	}
	signalID := buildSignalID("BTST_LDRB_V1", pc.Symbol, "BUY", pc.At)
	payload := map[string]any{
		"signal_id":             signalID,
		"strategy":              "BTST_LDRB_V1",
		"strategy_version":      "v1",
		"signal_source_service": serviceName,
		"symbol":                pc.Symbol,
		"side":                  "BUY",
		"time":                  pc.At.Format(time.RFC3339Nano),
		"timeframe":             pc.Timeframe,
		"reason":                "LDRB_LATE_DAY_BREAKOUT_CONFIRMED",
		"setup_type":            "LATE_DAY_RANGE_BREAKOUT",
		"trigger_type":          "SWING_HIGH_BREAKOUT",
		"stop_loss_pct":         stopPct,
		"target_pct":            1.0,
		"entry_context": map[string]any{
			"candle_open":   pc.Open,
			"candle_high":   pc.High,
			"candle_low":    pc.Low,
			"candle_close":  pc.Close,
			"candle_volume": pc.Volume,
			"signal_price":  pc.Close,
		},
		"market_context": map[string]any{
			"session_open":  st.SessionOpen,
			"swing_high":    st.SwingHigh,
			"breakout_high": breakoutHigh,
			"index_symbol":  strings.ToUpper(s.cfg.Strategy.IndexSymbol),
			"day":           st.Day,
		},
		"risk_context": map[string]any{
			"planned_stop_loss_pct": stopPct,
			"planned_target_pct":    1.0,
			"risk_per_trade_pct":    s.cfg.Strategy.RiskPerTradePct,
			"capital":               s.cfg.Strategy.Capital,
		},
		"filter_context": map[string]any{
			"market_filter_enabled": s.cfg.Strategy.EnableMarketFilter,
			"event_filter_enabled":  s.cfg.Strategy.EnableEventFilter,
			"liquidity_rank":        s.liquidity[pc.Symbol],
			"liquidity_top_n":       s.cfg.Strategy.LiquidityTopN,
			"allow_rvol_fallback":   s.cfg.Dependencies.AllowRVOLFallback,
		},
	}
	raw, _ := json.Marshal(payload)
	s.logger.Printf(`{"level":"INFO","service":"%s","message":"signal emitted","signal_id":"%s","symbol":"%s","side":"BUY","close":%.4f,"session_open":%.4f,"breakout_high":%.4f,"signals_today":%d}`, serviceName, signalID, pc.Symbol, pc.Close, st.SessionOpen, breakoutHigh, s.signalsOut+1)
	return s.nc.Publish(s.cfg.Kafka.SignalTopic, raw)
}

func buildSignalID(strategy, symbol, side string, at time.Time) string {
	return fmt.Sprintf("%s|%s|%s|%d", strategy, strings.ToUpper(strings.TrimSpace(symbol)), strings.ToUpper(strings.TrimSpace(side)), at.UnixNano())
}

func (s *service) inEntryWindow(t time.Time) bool {
	offset := timeOffset(t.In(s.loc))
	return offset >= s.entryStart && offset <= s.entryEnd
}

func (s *service) reject(stage string) {
	s.addStat("reject_" + stage)
	s.passFilter(stage, false)
}

func (s *service) passFilter(stage string, pass bool) {
	if pass {
		s.metrics.filters.WithLabelValues(stage, "pass").Inc()
		return
	}
	s.metrics.filters.WithLabelValues(stage, "reject").Inc()
}

func (s *service) addStat(key string) {
	s.stats.mu.Lock()
	s.stats.counts[key]++
	s.stats.mu.Unlock()
}

func (s *service) logMinuteSummary() {
	s.stats.mu.Lock()
	counts := s.stats.counts
	s.stats.counts = make(map[string]int)
	s.stats.mu.Unlock()
	raw, _ := json.Marshal(counts)
	s.logger.Printf(`{"level":"INFO","service":"%s","message":"filter summary","window":"1m","counts":%s,"tracked_stocks":%d,"signals_today":%d}`, serviceName, string(raw), len(s.states), s.signalsOut)
}

func normalizeSubject(topic string, wildcard bool) string {
	topic = strings.TrimSpace(topic)
	if strings.HasSuffix(topic, ".>") || strings.HasSuffix(topic, ".*") {
		return topic
	}
	if wildcard {
		return topic + ".>"
	}
	return topic
}

func parseHHMM(v string) (time.Duration, error) {
	var h, m int
	if _, err := fmt.Sscanf(v, "%d:%d", &h, &m); err != nil {
		return 0, err
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}

func timeOffset(t time.Time) time.Duration {
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute + time.Duration(t.Second())*time.Second
}

func bodyRatio(c parsedCandle) float64 {
	rng := c.High - c.Low
	if rng <= 0 {
		return 0
	}
	return math.Abs(c.Close-c.Open) / rng
}

func strconvParseFloat(v string) float64 {
	f, _ := strconv.ParseFloat(strings.TrimSpace(v), 64)
	return f
}
