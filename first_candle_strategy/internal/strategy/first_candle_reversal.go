package strategy

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	strategyName        = "FIRST_CANDLE_REVERSAL_V1"
	reasonLongReversal  = "FIRST_CANDLE_DOWN_THEN_RETURN_TO_OPEN"
	firstLegUp          = "up"
	firstLegDown        = "down"
	targetWindowMinutes = 15
)

// EngineConfig defines the parameters required by the first-candle reversal detector.
// SessionStart anchors the trading session, OpeningCandleSlot chooses the 15-minute bucket,
// and MoveThresholdPct defines the trigger distance from the bucket open.
type EngineConfig struct {
	SessionStart      string
	OpeningCandleSlot int
	MoveThresholdPct  float64
}

// SymbolState tracks per-symbol intraday progress while evaluating the selected opening window.
// It resets on date change and emits at most one signal per symbol per day.
type SymbolState struct {
	TradeDate    string
	Started      bool
	Completed    bool
	WindowCount  int
	OpeningPrice float64
	FirstLeg     string
	TriggerIndex int
}

// FirstCandleStrategy evaluates the first-candle reversal setup on a live 1-minute candle stream.
// Inputs: Candle values in time order per symbol.
// Outputs: optional BUY signal when the "down then return to open" pattern completes.
type FirstCandleStrategy struct {
	cfg          EngineConfig
	tz           *time.Location
	logger       *zap.Logger
	sessionStart time.Duration
	symbols      map[string]*SymbolState
}

// NewFirstCandleStrategy creates a new streaming first-candle reversal detector.
// Inputs: EngineConfig, timezone location, logger.
// Outputs: initialized strategy or an error if SessionStart cannot be parsed.
func NewFirstCandleStrategy(cfg EngineConfig, tz *time.Location, logger *zap.Logger) (*FirstCandleStrategy, error) {
	sessionStart, err := parseHHMM(cfg.SessionStart)
	if err != nil {
		return nil, err
	}
	return &FirstCandleStrategy{
		cfg:          cfg,
		tz:           tz,
		logger:       logger,
		sessionStart: sessionStart,
		symbols:      make(map[string]*SymbolState),
	}, nil
}

// OnCandle processes a new stock candle and returns a BUY signal if the setup completes.
// Inputs: one 1-minute Candle.
// Outputs: *StrategySignal or nil.
func (s *FirstCandleStrategy) OnCandle(c Candle) *StrategySignal {
	symbol := strings.ToUpper(strings.TrimSpace(c.Symbol))
	candleTime := c.Time.In(s.tz)
	tradeDate := candleTime.Format("2006-01-02")

	state, ok := s.symbols[symbol]
	if !ok || state.TradeDate != tradeDate {
		state = &SymbolState{TradeDate: tradeDate}
		s.symbols[symbol] = state
	}
	if state.Completed {
		return nil
	}

	windowStart := s.windowStart(candleTime)
	windowEnd := windowStart.Add(targetWindowMinutes * time.Minute)

	if candleTime.Before(windowStart) {
		return nil
	}
	if !candleTime.Before(windowEnd) {
		state.Completed = true
		return nil
	}

	if !state.Started {
		state.Started = true
		state.OpeningPrice = c.Open
		if state.OpeningPrice <= 0 {
			state.Completed = true
			s.logger.Debug("ignoring symbol with non-positive opening price", zap.String("symbol", symbol))
			return nil
		}
	}

	state.WindowCount++

	if state.FirstLeg == "" {
		upTrigger := state.OpeningPrice * (1 + s.cfg.MoveThresholdPct/100.0)
		downTrigger := state.OpeningPrice * (1 - s.cfg.MoveThresholdPct/100.0)
		upHit := c.High >= upTrigger
		downHit := c.Low <= downTrigger

		if upHit && downHit {
			state.Completed = true
			s.logger.Debug("ambiguous first leg; rejecting symbol", zap.String("symbol", symbol), zap.Time("time", candleTime))
			return nil
		}
		if upHit {
			state.FirstLeg = firstLegUp
			state.TriggerIndex = state.WindowCount
		} else if downHit {
			state.FirstLeg = firstLegDown
			state.TriggerIndex = state.WindowCount
		}
	} else if state.WindowCount > state.TriggerIndex && candleTouchesPrice(c, state.OpeningPrice) {
		state.Completed = true
		if state.FirstLeg == firstLegDown {
			return &StrategySignal{
				Strategy: strategyName,
				Symbol:   symbol,
				Side:     SideBuy,
				Time:     candleTime,
				Reason:   reasonLongReversal,
			}
		}
		s.logger.Debug("up-first reversal detected but ignored in long-only pipeline",
			zap.String("symbol", symbol),
			zap.Time("time", candleTime),
		)
		return nil
	}

	if candleTime.Add(time.Minute).Equal(windowEnd) || candleTime.Add(time.Minute).After(windowEnd) {
		state.Completed = true
	}

	return nil
}

func (s *FirstCandleStrategy) windowStart(candleTime time.Time) time.Time {
	slotOffset := time.Duration(s.cfg.OpeningCandleSlot-1) * targetWindowMinutes * time.Minute
	startOfDay := time.Date(
		candleTime.Year(),
		candleTime.Month(),
		candleTime.Day(),
		0,
		0,
		0,
		0,
		s.tz,
	)
	return startOfDay.Add(s.sessionStart + slotOffset)
}

func candleTouchesPrice(c Candle, price float64) bool {
	return c.Low <= price && price <= c.High
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
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	return time.Duration(hour)*time.Hour + time.Duration(minute)*time.Minute, nil
}
