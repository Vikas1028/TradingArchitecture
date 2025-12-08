package strategy

import (
	"strings"
	"time"

	"go.uber.org/zap"
)

// IndexState tracks recent index candles for bias calculation.
// It maintains a rolling window per index symbol.
type IndexState struct {
	Symbol  string
	Candles []Candle
}

// BiasEngine maintains rolling index candle history and computes a directional market bias.
// Inputs: index candles; Outputs: current MarketBias.
// Flow: update state per index, count closes vs VWAP over lookback with threshold, set bias.
type BiasEngine struct {
	cfg        StrategyConfig
	tz         *time.Location
	logger     *zap.Logger
	indexState map[string]*IndexState
	current    MarketBias
}

// StrategyConfig is embedded here to avoid circular import; duplicate minimal fields from config.StrategyConfig.
type StrategyConfig struct {
	IndexBiasLookback   int
	IndexBiasVWAPThresh float64
}

// NewBiasEngine creates a new BiasEngine using the provided strategy config and timezone.
// Inputs: StrategyConfig (index bias fields), timezone, logger.
// Outputs: initialized BiasEngine.
func NewBiasEngine(cfg StrategyConfig, tz *time.Location, logger *zap.Logger) *BiasEngine {
	return &BiasEngine{
		cfg:        cfg,
		tz:         tz,
		logger:     logger,
		indexState: make(map[string]*IndexState),
		current:    BiasNone,
	}
}

// OnIndexCandle ingests a new index candle, updates rolling history, and recalculates market bias.
// Inputs: Candle; Outputs: updated MarketBias.
// Flow: store candle (uppercased symbol), truncate to lookback, compute % distance of close vs VWAP.
//       If more positives than negatives beyond threshold -> BiasLong; vice versa -> BiasShort; else BiasNone.
func (b *BiasEngine) OnIndexCandle(c Candle) MarketBias {
	symbol := strings.ToUpper(c.Symbol)
	state, ok := b.indexState[symbol]
	if !ok {
		state = &IndexState{Symbol: symbol}
		b.indexState[symbol] = state
	}
	state.Candles = append(state.Candles, c)
	if len(state.Candles) > b.cfg.IndexBiasLookback {
		state.Candles = state.Candles[len(state.Candles)-b.cfg.IndexBiasLookback:]
	}

	posCount := 0
	negCount := 0
	for _, candle := range state.Candles {
		if candle.VWAP == 0 {
			continue
		}
		distPct := (candle.Close - candle.VWAP) / candle.VWAP * 100
		if distPct > b.cfg.IndexBiasVWAPThresh {
			posCount++
		} else if distPct < -b.cfg.IndexBiasVWAPThresh {
			negCount++
		}
	}

	if posCount > negCount && posCount > 0 {
		b.current = BiasLong
	} else if negCount > posCount && negCount > 0 {
		b.current = BiasShort
	} else {
		b.current = BiasNone
	}
	return b.current
}

// CurrentBias returns the last computed MarketBias.
// Inputs: none; Outputs: MarketBias value.
func (b *BiasEngine) CurrentBias() MarketBias {
	return b.current
}
