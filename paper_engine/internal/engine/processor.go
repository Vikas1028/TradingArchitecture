package engine

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"paper_engine/internal/metrics"
)

// Engine encapsulates the core logic for handling signals, candles, positions, and risk.
// Inputs: EngineState and logger; Outputs: trade events and PnL snapshots.
type Engine struct {
	state      *EngineState
	logger     *zap.Logger
	entryStart time.Duration
	entryEnd   time.Duration
	eodFlat    time.Duration
}

// NewEngine creates a new Engine instance with the given state and logger.
// Inputs: EngineState pointer, logger.
// Outputs: Engine pointer or error if trading window times are invalid.
func NewEngine(state *EngineState, logger *zap.Logger) (*Engine, error) {
	start, err := parseHHMM(state.Trading.EntryStart)
	if err != nil {
		return nil, fmt.Errorf("parse entry_start: %w", err)
	}
	end, err := parseHHMM(state.Trading.EntryEnd)
	if err != nil {
		return nil, fmt.Errorf("parse entry_end: %w", err)
	}
	flat, err := parseHHMM(state.Trading.EODFlatTime)
	if err != nil {
		return nil, fmt.Errorf("parse eod_flat_time: %w", err)
	}
	return &Engine{
		state:      state,
		logger:     logger,
		entryStart: start,
		entryEnd:   end,
		eodFlat:    flat,
	}, nil
}

// IsWithinEntryWindow returns true if the time is between entry_start and entry_end.
// Inputs: time.Time; Outputs: bool.
func (e *Engine) IsWithinEntryWindow(t time.Time) bool {
	d := durationOfDay(t.In(e.state.Tz))
	return d >= e.entryStart && d <= e.entryEnd
}

// IsAfterEODFlatTime returns true if the time is >= configured EOD flat time.
// Inputs: time.Time; Outputs: bool.
func (e *Engine) IsAfterEODFlatTime(t time.Time) bool {
	return durationOfDay(t.In(e.state.Tz)) >= e.eodFlat
}

// CheckAndResetDayIfNeeded detects date change and resets daily state and positions.
// Inputs: current time; Outputs: none.
func (e *Engine) CheckAndResetDayIfNeeded(now time.Time) {
	nowInTz := now.In(e.state.Tz)
	y1, m1, d1 := e.state.Daily.Date.Date()
	y2, m2, d2 := nowInTz.Date()
	if y1 == y2 && m1 == m2 && d1 == d2 {
		return
	}
	e.logger.Info("new trading day detected; resetting state", zap.Time("prev_date", e.state.Daily.Date), zap.Time("new_date", nowInTz))
	e.state.Positions = make(map[string]*Position)
	e.state.LatestCandles = make(map[string]*Candle)
	e.state.PendingSignals = make(map[string]*PendingSignal)
	e.state.Daily = DailyState{Date: nowInTz}
	metrics.TradingHalted.Set(0)
}

// OnSignal processes a new StrategySignal and may open a new paper position.
// Inputs: StrategySignal, latest candle (may be nil).
// Outputs: *TradeEvent (entry) or nil, and error if validation fails.
func (e *Engine) OnSignal(sig StrategySignal, latest *Candle) (*TradeEvent, error) {
	symbol := strings.ToUpper(sig.Symbol)
	sigTime := sig.Time.In(e.state.Tz)
	e.CheckAndResetDayIfNeeded(sigTime)

	if e.state.Daily.TradingHalted {
		return nil, nil
	}
	if sig.Side != SideBuy {
		return nil, nil
	}
	if !e.IsWithinEntryWindow(sigTime) {
		return nil, nil
	}
	if len(e.state.Positions) >= e.state.Risk.MaxOpenPositions {
		return nil, nil
	}
	if e.state.Daily.TradesToday >= e.state.Risk.MaxTradesPerDay {
		return nil, nil
	}
	if _, exists := e.state.Positions[symbol]; exists {
		return nil, nil
	}
	if latest == nil || latest.Symbol != symbol {
		return nil, errors.New("no latest candle for symbol to price entry")
	}

	entryPrice := latest.Close
	if entryPrice <= 0 {
		return nil, errors.New("invalid entry price")
	}
	qty := int64(math.Floor(e.state.Risk.CapitalPerTrade / entryPrice))
	if qty <= 0 {
		e.logger.Debug("skipping signal: calculated quantity <= 0",
			zap.String("symbol", symbol),
			zap.Float64("entry_price", entryPrice),
			zap.Float64("capital_per_trade", e.state.Risk.CapitalPerTrade),
		)
		return nil, nil
	}

	pos := &Position{
		Symbol:    symbol,
		Side:      SideLong,
		Quantity:  qty,
		AvgEntry:  entryPrice,
		EntryTime: sigTime,
		Strategy:  sig.Strategy,
	}
	e.state.Positions[symbol] = pos
	e.state.Daily.TradesToday++

	trade := &TradeEvent{
		Symbol:     symbol,
		Time:       sigTime,
		Price:      entryPrice,
		Quantity:   qty,
		TradeType:  TradeEntry,
		Side:       string(SideLong),
		Strategy:   sig.Strategy,
		SignalTime: sigTime,
		Reason:     "ENTRY_FROM_SIGNAL",
	}
	return trade, nil
}

// QueueSignal stores the latest signal for a symbol until pricing data catches up.
func (e *Engine) QueueSignal(sig StrategySignal, now time.Time) {
	symbol := strings.ToUpper(sig.Symbol)
	copySig := sig
	copySig.Symbol = symbol
	e.state.PendingSignals[symbol] = &PendingSignal{
		Signal:   copySig,
		QueuedAt: now.In(e.state.Tz),
	}
}

// ConsumePendingSignal returns and removes a queued signal for the symbol if present.
func (e *Engine) ConsumePendingSignal(symbol string) *StrategySignal {
	symbol = strings.ToUpper(symbol)
	pending := e.state.PendingSignals[symbol]
	if pending != nil {
		delete(e.state.PendingSignals, symbol)
		return &pending.Signal
	}
	return nil
}

// DrainExpiredPendingSignals removes and returns signals older than maxAge.
func (e *Engine) DrainExpiredPendingSignals(now time.Time, maxAge time.Duration) []PendingSignal {
	if maxAge <= 0 {
		return nil
	}
	nowInTz := now.In(e.state.Tz)
	expired := make([]PendingSignal, 0)
	for symbol, pending := range e.state.PendingSignals {
		if nowInTz.Sub(pending.QueuedAt) > maxAge {
			expired = append(expired, *pending)
			delete(e.state.PendingSignals, symbol)
		}
	}
	return expired
}

// OnCandle processes a new Candle, updates PnL, and emits exits on SL/TP/EOD.
// Inputs: Candle; Outputs: slice of TradeEvent exits and error if fatal.
func (e *Engine) OnCandle(c Candle) ([]TradeEvent, error) {
	now := c.Time.In(e.state.Tz)
	e.CheckAndResetDayIfNeeded(now)

	c.Symbol = strings.ToUpper(c.Symbol)
	e.state.LatestCandles[c.Symbol] = &c

	events := make([]TradeEvent, 0)
	pos, ok := e.state.Positions[c.Symbol]
	if ok {
		slPrice := pos.AvgEntry * (1 - e.state.Risk.PerTradeSLPct/100)
		targetPrice := pos.AvgEntry * (1 + e.state.Risk.PerTradeTargetPct/100)

		hitSL := c.Low <= slPrice
		hitTP := c.High >= targetPrice

		exitPrice := 0.0
		reason := ""
		if hitSL {
			exitPrice = slPrice
			reason = "EXIT_SL_HIT"
		} else if hitTP {
			exitPrice = targetPrice
			reason = "EXIT_TARGET_HIT"
		} else if e.IsAfterEODFlatTime(now) {
			exitPrice = c.Close
			reason = "EXIT_EOD"
		}

		if exitPrice > 0 {
			realized := (exitPrice - pos.AvgEntry) * float64(pos.Quantity)
			prevPeak := e.state.Daily.PeakPnl
			e.state.Daily.RealizedPnl += realized
			if e.state.Daily.RealizedPnl > e.state.Daily.PeakPnl {
				e.state.Daily.PeakPnl = e.state.Daily.RealizedPnl
			}
			drawdown := e.state.Daily.RealizedPnl - prevPeak
			if drawdown < e.state.Daily.MaxDrawdown {
				e.state.Daily.MaxDrawdown = drawdown
			}
			delete(e.state.Positions, c.Symbol)

			events = append(events, TradeEvent{
				Symbol:      c.Symbol,
				Time:        now,
				Price:       exitPrice,
				Quantity:    pos.Quantity,
				TradeType:   TradeExit,
				Side:        string(pos.Side),
				Strategy:    pos.Strategy,
				SignalTime:  pos.EntryTime,
				Reason:      reason,
				RealizedPnl: realized,
			})
		}
	}

	// Update unrealized PnL across positions.
	unreal := 0.0
	for sym, p := range e.state.Positions {
		last := e.state.LatestCandles[sym]
		if last == nil {
			continue
		}
		unreal += (last.Close - p.AvgEntry) * float64(p.Quantity)
	}
	e.state.Daily.UnrealizedPnl = unreal

	if e.state.Daily.RealizedPnl <= -e.state.Risk.MaxDailyLoss {
		e.state.Daily.TradingHalted = true
		metrics.TradingHalted.Set(1)
	}

	return events, nil
}

// BuildPnlSnapshot builds a PnlSnapshot from current engine state at given time.
// Inputs: time.Time; Outputs: PnlSnapshot.
func (e *Engine) BuildPnlSnapshot(now time.Time) PnlSnapshot {
	return PnlSnapshot{
		Time:              now,
		RealizedPnl:       e.state.Daily.RealizedPnl,
		UnrealizedPnl:     e.state.Daily.UnrealizedPnl,
		OpenPositions:     len(e.state.Positions),
		TradesToday:       e.state.Daily.TradesToday,
		MaxDrawdown:       e.state.Daily.MaxDrawdown,
		MaxDailyLossLimit: e.state.Risk.MaxDailyLoss,
		TradingHalted:     e.state.Daily.TradingHalted,
	}
}

// UpdateLatestCandle stores the latest candle for a symbol for entry pricing.
// Inputs: Candle; Outputs: none.
func (e *Engine) UpdateLatestCandle(c Candle) {
	c.Symbol = strings.ToUpper(c.Symbol)
	e.state.LatestCandles[c.Symbol] = &c
}

// GetLatestCandleForSymbol returns the cached latest candle for a symbol.
// Inputs: symbol string; Outputs: *Candle or nil.
func (e *Engine) GetLatestCandleForSymbol(symbol string) *Candle {
	return e.state.LatestCandles[strings.ToUpper(symbol)]
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

func durationOfDay(t time.Time) time.Duration {
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute
}
