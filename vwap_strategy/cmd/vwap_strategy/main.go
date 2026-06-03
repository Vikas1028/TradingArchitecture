package main

import (
	"context"
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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	serviceName      = "vwap_strategy"
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
	Strategy struct {
		Timezone            string  `json:"timezone"`
		EntryStart          string  `json:"entry_start"`
		EntryEnd            string  `json:"entry_end"`
		TrendLookback       int     `json:"trend_lookback"`
		PullbackWindow      int     `json:"pullback_window"`
		MinBodyPct          float64 `json:"min_body_pct"`
		MaxPullbackPct      float64 `json:"max_pullback_pct"`
		EnableIndexBias     bool    `json:"enable_index_bias"`
		IndexBiasLookback   int     `json:"index_bias_lookback"`
		IndexBiasVWAPThresh float64 `json:"index_bias_vwap_threshold"`
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
	Day       string
	Candles   []parsedCandle
	CumTP     float64
	CumPV     float64
	CumVolume float64
	Count     int
	LastVWAP  float64
	UsedProxy bool
	Signaled  bool
}

type minuteStats struct {
	mu     sync.Mutex
	counts map[string]int
}

type metricsStore struct {
	up      prometheus.Gauge
	candles prometheus.Counter
	signals prometheus.Counter
	errors  prometheus.Counter
	filters *prometheus.CounterVec
}

type service struct {
	cfg        appConfig
	loc        *time.Location
	logger     *log.Logger
	nc         *nats.Conn
	stockSub   *nats.Subscription
	indexSub   *nats.Subscription
	states     map[string]*symbolState
	indexState map[string]*symbolState
	mu         sync.Mutex
	stats      *minuteStats
	metrics    *metricsStore
	entryStart time.Duration
	entryEnd   time.Duration
}

func main() {
	configPath := flag.String("config", "config/vwap_strategy_config.json", "path to config file")
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
	if cfg.Strategy.EntryStart == "" {
		cfg.Strategy.EntryStart = "09:20"
	}
	if cfg.Strategy.EntryEnd == "" {
		cfg.Strategy.EntryEnd = "11:30"
	}
	if cfg.Strategy.TrendLookback <= 0 {
		cfg.Strategy.TrendLookback = 10
	}
	if cfg.Strategy.PullbackWindow <= 0 {
		cfg.Strategy.PullbackWindow = 5
	}
	if cfg.Strategy.MinBodyPct <= 0 {
		cfg.Strategy.MinBodyPct = 60
	}
	if cfg.Strategy.MaxPullbackPct <= 0 {
		cfg.Strategy.MaxPullbackPct = 0.3
	}
	if cfg.Strategy.IndexBiasLookback <= 0 {
		cfg.Strategy.IndexBiasLookback = 10
	}
	if cfg.Strategy.IndexBiasVWAPThresh <= 0 {
		cfg.Strategy.IndexBiasVWAPThresh = 0.1
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
	start, err := parseHHMM(cfg.Strategy.EntryStart)
	if err != nil {
		return nil, err
	}
	end, err := parseHHMM(cfg.Strategy.EntryEnd)
	if err != nil {
		return nil, err
	}
	return &service{
		cfg:        cfg,
		loc:        loc,
		logger:     logger,
		states:     make(map[string]*symbolState),
		indexState: make(map[string]*symbolState),
		stats:      &minuteStats{counts: make(map[string]int)},
		metrics:    registerMetrics(),
		entryStart: start,
		entryEnd:   end,
	}, nil
}

func registerMetrics() *metricsStore {
	m := &metricsStore{
		up:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "vwap_strategy_up", Help: "Service up gauge (1=running)"}),
		candles: prometheus.NewCounter(prometheus.CounterOpts{Name: "vwap_strategy_candles_consumed_total", Help: "Total candles consumed"}),
		signals: prometheus.NewCounter(prometheus.CounterOpts{Name: "vwap_strategy_signals_emitted_total", Help: "Total signals emitted to the configured strategy topic"}),
		errors:  prometheus.NewCounter(prometheus.CounterOpts{Name: "vwap_strategy_errors_total", Help: "Total errors in vwap_strategy"}),
		filters: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "vwap_strategy_filter_total", Help: "VWAP filter pass/reject counters"}, []string{"stage", "result"}),
	}
	prometheus.MustRegister(m.up, m.candles, m.signals, m.errors, m.filters)
	return m
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
	stockSubj := normalizeSubject(s.cfg.Kafka.StockCandlesTopic, true)
	indexSubj := normalizeSubject(s.cfg.Kafka.IndexCandlesTopic, true)
	stockSub, err := nc.Subscribe(stockSubj, func(msg *nats.Msg) { s.onCandle(msg, false) })
	if err != nil {
		return err
	}
	indexSub, err := nc.Subscribe(indexSubj, func(msg *nats.Msg) { s.onCandle(msg, true) })
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
	addr := ":9101"
	if err := http.ListenAndServe(addr, mux); err != nil && !strings.Contains(err.Error(), "Server closed") {
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
		s.updateState(s.indexState, pc)
		s.addStat("index_candles")
		return
	}
	s.updateState(s.states, pc)
	s.evaluate(pc)
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
		st = &symbolState{Day: day}
		states[pc.Symbol] = st
	}
	tp := (pc.High + pc.Low + pc.Close) / 3
	if pc.Volume > 0 {
		st.CumPV += tp * float64(pc.Volume)
		st.CumVolume += float64(pc.Volume)
		st.LastVWAP = st.CumPV / st.CumVolume
		st.UsedProxy = false
	} else if pc.VWAP > 0 {
		st.LastVWAP = pc.VWAP
		st.UsedProxy = false
	} else {
		st.CumTP += tp
		st.Count++
		st.LastVWAP = st.CumTP / float64(st.Count)
		st.UsedProxy = true
		s.addStat("vwap_proxy_used")
	}
	st.Candles = append(st.Candles, pc)
	if len(st.Candles) > 80 {
		st.Candles = st.Candles[len(st.Candles)-80:]
	}
	return st
}

func (s *service) evaluate(pc parsedCandle) {
	s.addStat("stocks_seen")
	s.passFilter("seen", true)
	if !s.inEntryWindow(pc.At) {
		s.reject("time_window")
		return
	}
	st := s.states[pc.Symbol]
	if st == nil || len(st.Candles) < maxInt(s.cfg.Strategy.TrendLookback, s.cfg.Strategy.PullbackWindow)+1 {
		s.reject("history")
		return
	}
	if st.Signaled {
		s.reject("already_signaled")
		return
	}
	if s.cfg.Strategy.EnableIndexBias && s.indexBias() != "LONG_ONLY" {
		s.reject("index_bias")
		return
	}
	s.passFilter("index_bias", true)
	if st.LastVWAP <= 0 || pc.Close <= st.LastVWAP {
		s.reject("close_above_vwap")
		return
	}
	s.passFilter("close_above_vwap", true)
	if !s.trendUp(st) {
		s.reject("trend")
		return
	}
	s.passFilter("trend", true)
	if !s.pullbackNearVWAP(st) {
		s.reject("pullback")
		return
	}
	s.passFilter("pullback", true)
	if bodyPct(pc) < s.cfg.Strategy.MinBodyPct {
		s.reject("body")
		return
	}
	s.passFilter("body", true)
	if err := s.emitSignal(pc, st); err != nil {
		s.metrics.errors.Inc()
		s.reject("publish_error")
		return
	}
	st.Signaled = true
	s.addStat("signals")
	s.metrics.signals.Inc()
}

func (s *service) trendUp(st *symbolState) bool {
	n := len(st.Candles)
	lookback := s.cfg.Strategy.TrendLookback
	if n <= lookback {
		return false
	}
	return st.Candles[n-1].Close > st.Candles[n-1-lookback].Close
}

func (s *service) pullbackNearVWAP(st *symbolState) bool {
	n := len(st.Candles)
	win := s.cfg.Strategy.PullbackWindow
	if n < win {
		return false
	}
	threshold := s.cfg.Strategy.MaxPullbackPct / 100
	for _, c := range st.Candles[n-win:] {
		if st.LastVWAP <= 0 {
			continue
		}
		dist := math.Abs(c.Low-st.LastVWAP) / st.LastVWAP
		if dist <= threshold || c.Low <= st.LastVWAP {
			return true
		}
	}
	return false
}

func (s *service) indexBias() string {
	longVotes := 0
	total := 0
	threshold := s.cfg.Strategy.IndexBiasVWAPThresh / 100
	for _, sym := range []string{"NIFTY", "BANKNIFTY"} {
		st := s.indexState[sym]
		if st == nil || len(st.Candles) < s.cfg.Strategy.IndexBiasLookback || st.LastVWAP <= 0 {
			continue
		}
		total++
		last := st.Candles[len(st.Candles)-1]
		if (last.Close-st.LastVWAP)/st.LastVWAP >= threshold {
			longVotes++
		}
	}
	if total == 0 {
		return "NONE"
	}
	if longVotes*2 >= total {
		return "LONG_ONLY"
	}
	return "NONE"
}

func (s *service) emitSignal(pc parsedCandle, st *symbolState) error {
	reason := "VWAP_PULLBACK_CONFIRMED"
	if st.UsedProxy {
		reason = "VWAP_PROXY_PULLBACK_CONFIRMED_VOLUME_MISSING"
	}
	signalID := buildSignalID("VWAP_PULLBACK_V1", pc.Symbol, "BUY", pc.At)
	indexBias := s.indexBias()
	payload := map[string]any{
		"signal_id":             signalID,
		"strategy":              "VWAP_PULLBACK_V1",
		"strategy_version":      "v1",
		"signal_source_service": serviceName,
		"symbol":                pc.Symbol,
		"side":                  "BUY",
		"time":                  pc.At.Format(time.RFC3339Nano),
		"timeframe":             pc.Timeframe,
		"reason":                reason,
		"setup_type":            "VWAP_PULLBACK",
		"trigger_type":          "PULLBACK_RECLAIM",
		"stop_loss_pct":         0.5,
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
			"vwap_at_signal": st.LastVWAP,
			"day":            st.Day,
			"candle_count":   len(st.Candles),
			"index_bias":     indexBias,
		},
		"risk_context": map[string]any{
			"planned_stop_loss_pct": 0.5,
			"planned_target_pct":    1.0,
			"planned_rr":            2.0,
		},
		"filter_context": map[string]any{
			"used_vwap_proxy":      st.UsedProxy,
			"index_bias_required":  s.cfg.Strategy.EnableIndexBias,
			"index_bias_lookback":  s.cfg.Strategy.IndexBiasLookback,
			"index_bias_threshold": s.cfg.Strategy.IndexBiasVWAPThresh,
			"pullback_window":      s.cfg.Strategy.PullbackWindow,
			"trend_lookback":       s.cfg.Strategy.TrendLookback,
		},
	}
	raw, _ := json.Marshal(payload)
	s.logger.Printf(`{"level":"INFO","service":"%s","message":"signal emitted","signal_id":"%s","symbol":"%s","side":"BUY","reason":"%s","close":%.4f,"vwap":%.4f,"used_vwap_proxy":%t}`, serviceName, signalID, pc.Symbol, reason, pc.Close, st.LastVWAP, st.UsedProxy)
	return s.nc.Publish(s.cfg.Kafka.SignalTopic, raw)
}

func buildSignalID(strategy, symbol, side string, at time.Time) string {
	return fmt.Sprintf("%s|%s|%s|%d", strategy, strings.ToUpper(strings.TrimSpace(symbol)), strings.ToUpper(strings.TrimSpace(side)), at.UnixNano())
}

func (s *service) inEntryWindow(t time.Time) bool {
	local := t.In(s.loc)
	offset := time.Duration(local.Hour())*time.Hour + time.Duration(local.Minute())*time.Minute + time.Duration(local.Second())*time.Second
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
	s.logger.Printf(`{"level":"INFO","service":"%s","message":"filter summary","window":"1m","counts":%s,"index_bias":"%s","tracked_stocks":%d}`, serviceName, string(raw), s.indexBias(), len(s.states))
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
	parts := strings.Split(v, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid HH:MM: %s", v)
	}
	var h, m int
	if _, err := fmt.Sscanf(v, "%d:%d", &h, &m); err != nil {
		return 0, err
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}

func bodyPct(c parsedCandle) float64 {
	rng := c.High - c.Low
	if rng <= 0 {
		return 0
	}
	return math.Abs(c.Close-c.Open) / rng * 100
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
