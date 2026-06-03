package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const serviceName = "s2_fallback_selector"

type appConfig struct {
	Env  string `json:"env"`
	NATS struct {
		URL           string `json:"url"`
		InputSubject  string `json:"input_subject"`
		OutputSubject string `json:"output_subject"`
	} `json:"nats"`
	Strategy struct {
		Timezone            string  `json:"timezone"`
		WindowStart         string  `json:"window_start"`
		EvaluateAt          string  `json:"evaluate_at"`
		ThresholdPct        float64 `json:"threshold_pct"`
		MinPositiveMovePct  float64 `json:"min_positive_move_pct"`
		MaxBaselineDelaySec int     `json:"max_baseline_delay_sec"`
		MaxLastTickAgeSec   int     `json:"max_last_tick_age_sec"`
		MaxPublishDelaySec  int     `json:"max_publish_delay_sec"`
		StrategyName        string  `json:"strategy_name"`
		StrategyVersion     string  `json:"strategy_version"`
		SignalReason        string  `json:"signal_reason"`
	} `json:"strategy"`
}

type tick struct {
	Symbol        string
	Time          time.Time
	LTP           float64
	DayOpen       float64
	CumulativeVol int64
	Source        string
}

type symbolState struct {
	BasePrice float64
	BaseAt    time.Time
	LastPrice float64
	LastAt    time.Time
	MaxMove   float64
}

type dayState struct {
	TradingDate  string
	ThresholdHit bool
	Evaluated    bool
	Published    bool
	Symbols      map[string]*symbolState
}

type service struct {
	cfg                *appConfig
	nc                 *nats.Conn
	sub                *nats.Subscription
	loc                *time.Location
	windowStart        time.Duration
	evaluateAt         time.Duration
	thresholdPct       float64
	minPositiveMovePct float64
	maxBaselineDelay   time.Duration
	maxLastTickAge     time.Duration
	maxPublishDelay    time.Duration
	mu                 sync.Mutex
	state              dayState
}

func main() {
	configPath := flag.String("config", "config/s2_fallback_selector_config.json", "path to config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	loc, err := time.LoadLocation(cfg.Strategy.Timezone)
	if err != nil {
		log.Fatalf("timezone error: %v", err)
	}

	windowStart, err := parseClock(cfg.Strategy.WindowStart)
	if err != nil {
		log.Fatalf("window_start parse error: %v", err)
	}
	evaluateAt, err := parseClock(cfg.Strategy.EvaluateAt)
	if err != nil {
		log.Fatalf("evaluate_at parse error: %v", err)
	}
	if evaluateAt < windowStart {
		log.Fatalf("evaluate_at must be >= window_start")
	}

	nc, err := nats.Connect(cfg.NATS.URL, nats.Name(serviceName), nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		log.Fatalf("nats connect error: %v", err)
	}
	defer nc.Close()

	svc := &service{
		cfg:                cfg,
		nc:                 nc,
		loc:                loc,
		windowStart:        windowStart,
		evaluateAt:         evaluateAt,
		thresholdPct:       cfg.Strategy.ThresholdPct,
		minPositiveMovePct: cfg.Strategy.MinPositiveMovePct,
		maxBaselineDelay:   time.Duration(cfg.Strategy.MaxBaselineDelaySec) * time.Second,
		maxLastTickAge:     time.Duration(cfg.Strategy.MaxLastTickAgeSec) * time.Second,
		maxPublishDelay:    time.Duration(cfg.Strategy.MaxPublishDelaySec) * time.Second,
		state: dayState{
			Symbols: make(map[string]*symbolState),
		},
	}

	msgs := make(chan *nats.Msg, 4096)
	sub, err := nc.ChanSubscribe(cfg.NATS.InputSubject, msgs)
	if err != nil {
		log.Fatalf("subscribe error: %v", err)
	}
	defer sub.Unsubscribe()
	svc.sub = sub

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("%s started env=%s input=%s output=%s strategy=%s window_start=%s evaluate_at=%s threshold_pct=%.2f",
		serviceName,
		cfg.Env,
		cfg.NATS.InputSubject,
		cfg.NATS.OutputSubject,
		cfg.Strategy.StrategyName,
		cfg.Strategy.WindowStart,
		cfg.Strategy.EvaluateAt,
		cfg.Strategy.ThresholdPct,
	)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("%s shutting down", serviceName)
			return
		case msg := <-msgs:
			if msg == nil {
				continue
			}
			if err := svc.onTick(msg.Subject, msg.Data); err != nil {
				log.Printf("tick handling failed: %v", err)
			}
		case now := <-ticker.C:
			if err := svc.maybeEvaluate(now.In(loc)); err != nil {
				log.Printf("evaluation failed: %v", err)
			}
		}
	}
}

func loadConfig(path string) (*appConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg appConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.NATS.URL) == "" {
		cfg.NATS.URL = "nats://127.0.0.1:4222"
	}
	if strings.TrimSpace(cfg.NATS.InputSubject) == "" {
		cfg.NATS.InputSubject = "ticks.raw.>"
	}
	if strings.TrimSpace(cfg.NATS.OutputSubject) == "" {
		cfg.NATS.OutputSubject = "signals.strategy"
	}
	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = "Asia/Kolkata"
	}
	if strings.TrimSpace(cfg.Strategy.WindowStart) == "" {
		cfg.Strategy.WindowStart = "09:16:00"
	}
	if strings.TrimSpace(cfg.Strategy.EvaluateAt) == "" {
		cfg.Strategy.EvaluateAt = "09:16:30"
	}
	if cfg.Strategy.ThresholdPct <= 0 {
		cfg.Strategy.ThresholdPct = 1.0
	}
	if cfg.Strategy.MinPositiveMovePct <= 0 {
		cfg.Strategy.MinPositiveMovePct = 0.05
	}
	if cfg.Strategy.MaxBaselineDelaySec <= 0 {
		cfg.Strategy.MaxBaselineDelaySec = 5
	}
	if cfg.Strategy.MaxLastTickAgeSec <= 0 {
		cfg.Strategy.MaxLastTickAgeSec = 5
	}
	if cfg.Strategy.MaxPublishDelaySec <= 0 {
		cfg.Strategy.MaxPublishDelaySec = 15
	}
	if strings.TrimSpace(cfg.Strategy.StrategyName) == "" {
		cfg.Strategy.StrategyName = "openmarketvolatility_s2_top_gainer_fallback"
	}
	if strings.TrimSpace(cfg.Strategy.StrategyVersion) == "" {
		cfg.Strategy.StrategyVersion = "v1"
	}
	if strings.TrimSpace(cfg.Strategy.SignalReason) == "" {
		cfg.Strategy.SignalReason = "S2_FALLBACK_TOP_GAINER_30SEC"
	}
	return &cfg, nil
}

func (s *service) onTick(subject string, raw []byte) error {
	t, err := parseTick(subject, raw)
	if err != nil {
		return err
	}
	if t.Symbol == "" || t.LTP <= 0 || t.Time.IsZero() {
		return nil
	}

	local := t.Time.In(s.loc)
	if !isWeekday(local) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureDayLocked(local)
	if s.state.Evaluated {
		return nil
	}

	clock := durationOfDay(local)
	if clock < s.windowStart {
		return nil
	}
	if clock > s.evaluateAt {
		return nil
	}

	baselineDeadline := s.windowStart + s.maxBaselineDelay
	st, ok := s.state.Symbols[t.Symbol]
	if !ok {
		if clock > baselineDeadline {
			return nil
		}
		st = &symbolState{
			BasePrice: t.LTP,
			BaseAt:    local,
		}
		s.state.Symbols[t.Symbol] = st
	}
	if st.BasePrice <= 0 {
		return nil
	}

	st.LastPrice = t.LTP
	st.LastAt = local
	movePct := pctMove(st.BasePrice, st.LastPrice)
	if movePct > st.MaxMove {
		st.MaxMove = movePct
	}
	if movePct >= s.thresholdPct {
		s.state.ThresholdHit = true
	}
	return nil
}

func (s *service) maybeEvaluate(now time.Time) error {
	if !isWeekday(now) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureDayLocked(now)
	if s.state.Evaluated {
		return nil
	}

	clock := durationOfDay(now)
	if clock < s.evaluateAt {
		return nil
	}

	evalTime := combineClock(now, s.evaluateAt, s.loc)
	if now.Sub(evalTime) > s.maxPublishDelay {
		s.state.Evaluated = true
		log.Printf("skip fallback publish trading_date=%s reason=late_evaluation now=%s eval_time=%s",
			s.state.TradingDate,
			now.Format(time.RFC3339),
			evalTime.Format(time.RFC3339),
		)
		return nil
	}

	s.state.Evaluated = true
	if s.state.ThresholdHit {
		log.Printf("skip fallback publish trading_date=%s reason=threshold_hit symbols_tracked=%d",
			s.state.TradingDate,
			len(s.state.Symbols),
		)
		return nil
	}

	bestSymbol := ""
	bestMove := -1e9
	bestBase := 0.0
	bestLast := 0.0
	candidateCount := 0
	minLastAt := evalTime.Add(-s.maxLastTickAge)

	for symbol, st := range s.state.Symbols {
		if st.BasePrice <= 0 || st.LastPrice <= 0 || st.LastAt.IsZero() {
			continue
		}
		if st.LastAt.Before(minLastAt) || st.LastAt.After(evalTime.Add(2*time.Second)) {
			continue
		}
		movePct := pctMove(st.BasePrice, st.LastPrice)
		if movePct < s.minPositiveMovePct {
			continue
		}
		candidateCount++
		if movePct > bestMove {
			bestMove = movePct
			bestSymbol = symbol
			bestBase = st.BasePrice
			bestLast = st.LastPrice
		}
	}

	if bestSymbol == "" {
		log.Printf("skip fallback publish trading_date=%s reason=no_candidate symbols_tracked=%d",
			s.state.TradingDate,
			len(s.state.Symbols),
		)
		return nil
	}

	payload := map[string]any{
		"signal_id":             fmt.Sprintf("%s|%s|%s|%s", serviceName, s.state.TradingDate, bestSymbol, evalTime.Format("150405")),
		"strategy":              s.cfg.Strategy.StrategyName,
		"symbol":                bestSymbol,
		"side":                  "BUY",
		"time":                  now.Format(time.RFC3339Nano),
		"reason":                s.cfg.Strategy.SignalReason,
		"signal_source_service": serviceName,
		"strategy_version":      s.cfg.Strategy.StrategyVersion,
		"setup_type":            "top_gainer_fallback",
		"trigger_type":          "no_threshold_top_gainer",
		"decision_score":        bestMove,
		"stop_loss_pct":         0.30,
		"target_pct":            0.0,
		"entry_context": map[string]any{
			"window_start":   s.cfg.Strategy.WindowStart,
			"evaluate_at":    s.cfg.Strategy.EvaluateAt,
			"baseline_price": bestBase,
			"entry_price":    bestLast,
			"move_open_pct":  bestMove,
		},
		"market_context": map[string]any{
			"trading_date":      s.state.TradingDate,
			"tracked_symbols":   len(s.state.Symbols),
			"eligible_symbols":  candidateCount,
			"threshold_pct":     s.thresholdPct,
			"threshold_hit":     false,
			"decision_time_ist": now.Format(time.RFC3339Nano),
		},
		"risk_context": map[string]any{
			"stop_loss_pct": 0.30,
			"target_pct":    0.0,
			"quantity":      1,
		},
		"filter_context": map[string]any{
			"fallback_used":          true,
			"max_baseline_delay_sec": s.cfg.Strategy.MaxBaselineDelaySec,
			"max_last_tick_age_sec":  s.cfg.Strategy.MaxLastTickAgeSec,
			"min_positive_move_pct":  s.minPositiveMovePct,
			"max_publish_delay_sec":  s.cfg.Strategy.MaxPublishDelaySec,
			"original_threshold_pct": s.thresholdPct,
		},
	}

	out, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := s.nc.Publish(s.cfg.NATS.OutputSubject, out); err != nil {
		return err
	}
	s.state.Published = true
	log.Printf("published fallback signal strategy=%s symbol=%s side=BUY move_pct=%.4f base=%.4f last=%.4f tracked=%d eligible=%d",
		s.cfg.Strategy.StrategyName,
		bestSymbol,
		bestMove,
		bestBase,
		bestLast,
		len(s.state.Symbols),
		candidateCount,
	)
	return nil
}

func (s *service) ensureDayLocked(ts time.Time) {
	day := ts.In(s.loc).Format("2006-01-02")
	if s.state.TradingDate == day {
		return
	}
	s.state = dayState{
		TradingDate: day,
		Symbols:     make(map[string]*symbolState),
	}
}

func parseTick(subject string, raw []byte) (tick, error) {
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
		return tick{}, err
	}

	var symbol string
	var tsText string
	var ltp float64
	var dayOpen float64
	var vol int64

	if envelope.Payload != nil {
		symbol = envelope.Payload.Symbol
		tsText = envelope.Payload.Timestamp
		ltp = envelope.Payload.LTP
		dayOpen = envelope.Payload.DayOpen
		vol = envelope.Payload.Volume
	} else {
		symbol = envelope.Symbol
		tsText = envelope.Time
		ltp = envelope.LTP
		vol = envelope.Volume
	}
	if tsText == "" {
		tsText = envelope.Timestamp
	}
	tm, err := time.Parse(time.RFC3339Nano, tsText)
	if err != nil {
		return tick{}, err
	}
	return tick{
		Symbol:        strings.ToUpper(strings.TrimSpace(symbol)),
		Time:          tm,
		LTP:           ltp,
		DayOpen:       dayOpen,
		CumulativeVol: vol,
		Source:        parseTickSource(subject),
	}, nil
}

func parseTickSource(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) >= 3 {
		return strings.ToUpper(strings.TrimSpace(parts[2]))
	}
	return ""
}

func parseClock(value string) (time.Duration, error) {
	var hh, mm, ss int
	if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d:%d:%d", &hh, &mm, &ss); err != nil {
		return 0, err
	}
	return time.Duration(hh)*time.Hour + time.Duration(mm)*time.Minute + time.Duration(ss)*time.Second, nil
}

func durationOfDay(ts time.Time) time.Duration {
	local := ts
	return time.Duration(local.Hour())*time.Hour + time.Duration(local.Minute())*time.Minute + time.Duration(local.Second())*time.Second
}

func combineClock(ts time.Time, clock time.Duration, loc *time.Location) time.Time {
	local := ts.In(loc)
	startOfDay := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
	return startOfDay.Add(clock)
}

func pctMove(base, current float64) float64 {
	if base <= 0 {
		return 0
	}
	return ((current - base) / base) * 100
}

func isWeekday(ts time.Time) bool {
	wd := ts.Weekday()
	return wd >= time.Monday && wd <= time.Friday
}
