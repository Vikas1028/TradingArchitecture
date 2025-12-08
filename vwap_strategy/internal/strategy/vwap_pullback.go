package strategy

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// PullbackConfig mirrors strategy config fields needed by the VWAP pullback logic.
type PullbackConfig struct {
	Timezone       string
	EntryStart     string
	EntryEnd       string
	TrendLookback  int
	PullbackWindow int
	MinBodyPct     float64
	MaxPullbackPct float64
	EnableIndexBias bool
}

// SymbolTrendState holds recent candles for a stock used by the VWAP pullback strategy.
// It keeps a rolling buffer to evaluate trend and pullback conditions.
type SymbolTrendState struct {
	Symbol  string
	Candles []Candle
}

// VwapPullbackStrategy implements the VWAP pullback LONG-only strategy.
// Inputs: stock candles and current market bias; Outputs: optional BUY signal.
type VwapPullbackStrategy struct {
	cfg     PullbackConfig
	tz      *time.Location
	logger  *zap.Logger
	symbols map[string]*SymbolTrendState
	entryStart time.Duration
	entryEnd   time.Duration
}

// NewVwapPullbackStrategy creates a new VwapPullbackStrategy instance.
// Inputs: PullbackConfig, timezone location, logger.
// Outputs: initialized strategy or error on bad time window parsing.
func NewVwapPullbackStrategy(cfg PullbackConfig, tz *time.Location, logger *zap.Logger) (*VwapPullbackStrategy, error) {
	startDur, err := parseHHMM(cfg.EntryStart)
	if err != nil {
		return nil, err
	}
	endDur, err := parseHHMM(cfg.EntryEnd)
	if err != nil {
		return nil, err
	}
	return &VwapPullbackStrategy{
		cfg:       cfg,
		tz:        tz,
		logger:    logger,
		symbols:   make(map[string]*SymbolTrendState),
		entryStart: startDur,
		entryEnd:   endDur,
	}, nil
}

// OnStockCandle processes a stock candle and returns a BUY StrategySignal if conditions meet.
// Inputs: Candle and current MarketBias.
// Outputs: *StrategySignal or nil.
// Flow: enforce time window, bias filter, trend/pullback/confirmation checks, then emit signal.
func (s *VwapPullbackStrategy) OnStockCandle(c Candle, bias MarketBias) *StrategySignal {
	symbol := strings.ToUpper(c.Symbol)
	candleTime := c.Time.In(s.tz)

	if !s.withinEntryWindow(candleTime) {
		return nil
	}

	if s.cfg.EnableIndexBias && bias != BiasLong {
		return nil
	}

	state, ok := s.symbols[symbol]
	if !ok {
		state = &SymbolTrendState{Symbol: symbol}
		s.symbols[symbol] = state
	}

	state.Candles = append(state.Candles, c)
	maxKeep := s.cfg.TrendLookback + s.cfg.PullbackWindow + 5
	if len(state.Candles) > maxKeep {
		state.Candles = state.Candles[len(state.Candles)-maxKeep:]
	}

	if len(state.Candles) < s.cfg.TrendLookback || len(state.Candles) < s.cfg.PullbackWindow {
		return nil
	}

	if !s.isUptrend(state.Candles, s.cfg.TrendLookback) {
		return nil
	}

	if !s.hasRecentPullback(state.Candles, s.cfg.PullbackWindow) {
		return nil
	}

	if !s.confirmation(state.Candles) {
		return nil
	}

	return &StrategySignal{
		Strategy: "VWAP_PULLBACK_V1",
		Symbol:   symbol,
		Side:     SideBuy,
		Time:     candleTime,
		Reason:   "INDEX_BIAS_LONG_PULLBACK_CONFIRMED",
	}
}

func (s *VwapPullbackStrategy) withinEntryWindow(t time.Time) bool {
	d := time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute
	return d >= s.entryStart && d <= s.entryEnd
}

func (s *VwapPullbackStrategy) isUptrend(candles []Candle, lookback int) bool {
	if len(candles) < lookback {
		return false
	}
	segment := candles[len(candles)-lookback:]
	above := 0
	for _, c := range segment {
		if c.VWAP == 0 {
			continue
		}
		if c.Close > c.VWAP {
			above++
		}
	}
	return float64(above)/float64(lookback) >= 0.7
}

func (s *VwapPullbackStrategy) hasRecentPullback(candles []Candle, window int) bool {
	if len(candles) < window {
		return false
	}
	segment := candles[len(candles)-window:]
	for _, c := range segment {
		if c.VWAP == 0 {
			continue
		}
		distPct := abs(c.Low-c.VWAP) / c.VWAP * 100
		if distPct <= s.cfg.MaxPullbackPct {
			return true
		}
	}
	return false
}

func (s *VwapPullbackStrategy) confirmation(candles []Candle) bool {
	n := len(candles)
	if n < 2 {
		return false
	}
	current := candles[n-1]
	prev := candles[n-2]

	if current.Close <= current.Open {
		return false
	}
	if current.Close <= current.VWAP {
		return false
	}
	if current.Close <= prev.High {
		return false
	}

	rangeVal := current.High - current.Low
	if rangeVal <= 0 {
		return false
	}
	body := current.Close - current.Open
	bodyPct := body / rangeVal * 100
	return bodyPct >= s.cfg.MinBodyPct
}

func parseHHMM(value string) (time.Duration, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid time format: %s", value)
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	min, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	return time.Duration(hour)*time.Hour + time.Duration(min)*time.Minute, nil
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
