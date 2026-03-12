package engine

import (
	"time"

	"go.uber.org/zap"

	"paper_engine/internal/config"
)

// DailyState holds PnL and risk counters for the current trading day.
type DailyState struct {
	Date          time.Time
	RealizedPnl   float64
	UnrealizedPnl float64
	MaxDrawdown   float64
	PeakPnl       float64
	TradesToday   int
	TradingHalted bool
}

// EngineState holds in-memory positions, daily state, and latest candles.
type EngineState struct {
	Positions      map[string]*Position
	LatestCandles  map[string]*Candle
	PendingSignals map[string]*PendingSignal
	Daily          DailyState

	Risk    config.RiskConfig
	Trading config.TradingConfig
	Tz      *time.Location

	logger *zap.Logger
}

// NewEngineState constructs an EngineState for a new trading day.
// Inputs: risk/trading configs, timezone, logger.
// Outputs: initialized EngineState.
func NewEngineState(risk config.RiskConfig, trading config.TradingConfig, tz *time.Location, logger *zap.Logger) *EngineState {
	now := time.Now().In(tz)
	return &EngineState{
		Positions:      make(map[string]*Position),
		LatestCandles:  make(map[string]*Candle),
		PendingSignals: make(map[string]*PendingSignal),
		Daily: DailyState{
			Date: now,
		},
		Risk:    risk,
		Trading: trading,
		Tz:      tz,
		logger:  logger,
	}
}
