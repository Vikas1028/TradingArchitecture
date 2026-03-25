package engine

import (
	"fmt"
	"math"
	"sort"
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
	currentDay := time.Date(y1, m1, d1, 0, 0, 0, 0, e.state.Tz)
	incomingDay := time.Date(y2, m2, d2, 0, 0, 0, 0, e.state.Tz)
	if incomingDay.Before(currentDay) {
		e.logger.Warn(
			"ignoring out-of-order message from older trading day",
			zap.Time("current_date", e.state.Daily.Date),
			zap.Time("incoming_date", nowInTz),
		)
		return
	}
	e.logger.Info("new trading day detected; resetting state", zap.Time("prev_date", e.state.Daily.Date), zap.Time("new_date", nowInTz))
	e.state.Positions = make(map[string]*Position)
	e.state.LatestCandles = make(map[string]*Candle)
	e.state.LatestPrices = make(map[string]float64)
	e.state.LastPriceTimes = make(map[string]time.Time)
	e.state.PendingSignals = make(map[string]*PendingSignal)
	e.state.Daily = DailyState{Date: nowInTz}
	e.syncMetrics()
}

// OnSignal processes a new StrategySignal and may open a new paper position.
// Inputs: StrategySignal, entry price.
// Outputs: *TradeEvent (entry) or nil, and error if validation fails.
func (e *Engine) OnSignal(sig StrategySignal, entryPrice float64) (*TradeEvent, error) {
	symbol := strings.ToUpper(sig.Symbol)
	sigTime := sig.Time.In(e.state.Tz)
	e.CheckAndResetDayIfNeeded(sigTime)

	if _, exists := e.state.Positions[symbol]; exists {
		return nil, nil
	}
	if entryPrice <= 0 {
		return nil, fmt.Errorf("invalid entry price")
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

	posSide := SideLong
	tradeSide := string(SideLong)
	switch sig.Side {
	case SideBuy:
		posSide = SideLong
		tradeSide = string(SideLong)
	case SideSell:
		posSide = SideShort
		tradeSide = string(SideShort)
	default:
		return nil, nil
	}

	pos := &Position{
		Symbol:    symbol,
		Side:      posSide,
		Quantity:  qty,
		AvgEntry:  entryPrice,
		EntryTime: sigTime,
		Strategy:  sig.Strategy,
	}
	e.state.Positions[symbol] = pos
	e.state.Daily.TradesToday++
	e.syncMetrics()

	trade := &TradeEvent{
		Symbol:     symbol,
		Time:       sigTime,
		Price:      entryPrice,
		Quantity:   qty,
		TradeType:  TradeEntry,
		Side:       tradeSide,
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
	e.syncMetrics()
}

// ConsumePendingSignal returns and removes a queued signal for the symbol if present.
func (e *Engine) ConsumePendingSignal(symbol string) *StrategySignal {
	symbol = strings.ToUpper(symbol)
	pending := e.state.PendingSignals[symbol]
	if pending != nil {
		delete(e.state.PendingSignals, symbol)
		e.syncMetrics()
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
	if len(expired) > 0 {
		e.syncMetrics()
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
		exitPrice := 0.0
		reason := ""

		switch pos.Side {
		case SideLong:
			slPrice := pos.AvgEntry * (1 - e.state.Risk.PerTradeSLPct/100)
			targetPrice := pos.AvgEntry * (1 + e.state.Risk.PerTradeTargetPct/100)
			hitSL := c.Low <= slPrice
			hitTP := c.High >= targetPrice
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
		case SideShort:
			slPrice := pos.AvgEntry * (1 + e.state.Risk.PerTradeSLPct/100)
			targetPrice := pos.AvgEntry * (1 - e.state.Risk.PerTradeTargetPct/100)
			hitSL := c.High >= slPrice
			hitTP := c.Low <= targetPrice
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
		}

		if exitPrice > 0 {
			realized := pnlForSide(pos.Side, pos.AvgEntry, exitPrice, pos.Quantity)
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
			e.syncMetrics()

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
	e.recomputeUnrealized()

	e.syncMetrics()

	return events, nil
}

// BuildPnlSnapshot builds a PnlSnapshot from current engine state at given time.
// Inputs: time.Time; Outputs: PnlSnapshot.
func (e *Engine) BuildPnlSnapshot(now time.Time) PnlSnapshot {
	e.syncMetrics()
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

// OpenSymbols returns the set of currently restored or live open position symbols.
func (e *Engine) OpenSymbols() []string {
	out := make([]string, 0, len(e.state.Positions))
	for symbol := range e.state.Positions {
		out = append(out, symbol)
	}
	sort.Strings(out)
	return out
}

// RestoreTradeHistory rebuilds today's open positions and realized PnL from prior trade events.
func (e *Engine) RestoreTradeHistory(events []TradeEvent, now time.Time) {
	e.CheckAndResetDayIfNeeded(now)
	if len(events) == 0 {
		return
	}
	sort.Slice(events, func(i, j int) bool {
		return events[i].Time.Before(events[j].Time)
	})
	for _, event := range events {
		symbol := strings.ToUpper(strings.TrimSpace(event.Symbol))
		if symbol == "" {
			continue
		}
		switch event.TradeType {
		case TradeEntry:
			side := SideLong
			if strings.EqualFold(event.Side, string(SideShort)) {
				side = SideShort
			}
			e.state.Positions[symbol] = &Position{
				Symbol:    symbol,
				Side:      side,
				Quantity:  event.Quantity,
				AvgEntry:  event.Price,
				EntryTime: event.SignalTime,
				Strategy:  event.Strategy,
			}
			e.state.Daily.TradesToday++
		case TradeExit:
			delete(e.state.Positions, symbol)
			e.state.Daily.RealizedPnl += event.RealizedPnl
			if e.state.Daily.RealizedPnl > e.state.Daily.PeakPnl {
				e.state.Daily.PeakPnl = e.state.Daily.RealizedPnl
			}
			drawdown := e.state.Daily.RealizedPnl - e.state.Daily.PeakPnl
			if drawdown < e.state.Daily.MaxDrawdown {
				e.state.Daily.MaxDrawdown = drawdown
			}
		}
	}
	e.recomputeUnrealized()
	e.syncMetrics()
}

func (e *Engine) syncMetrics() {
	metrics.TradingHalted.Set(boolToFloat(e.state.Daily.TradingHalted))
	metrics.RealizedPnl.Set(e.state.Daily.RealizedPnl)
	metrics.UnrealizedPnl.Set(e.state.Daily.UnrealizedPnl)
	metrics.OpenPositions.Set(float64(len(e.state.Positions)))
	metrics.PendingSignals.Set(float64(len(e.state.PendingSignals)))
	metrics.TradesToday.Set(float64(e.state.Daily.TradesToday))
	metrics.MaxDrawdown.Set(e.state.Daily.MaxDrawdown)
}

func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

// UpdateLatestCandle stores the latest candle for a symbol for entry pricing.
// Inputs: Candle; Outputs: none.
func (e *Engine) UpdateLatestCandle(c Candle) {
	c.Symbol = strings.ToUpper(c.Symbol)
	e.state.LatestCandles[c.Symbol] = &c
}

// OnTick updates the latest live price, mark-to-market PnL, and may release a pending signal.
func (e *Engine) OnTick(t Tick) (*TradeEvent, error) {
	t.Symbol = strings.ToUpper(t.Symbol)
	if t.Symbol == "" || t.LTP <= 0 {
		return nil, nil
	}
	e.CheckAndResetDayIfNeeded(t.Time.In(e.state.Tz))
	e.state.LatestPrices[t.Symbol] = t.LTP
	if !t.Time.IsZero() {
		e.state.LastPriceTimes[t.Symbol] = t.Time
	}
	e.recomputeUnrealized()
	e.syncMetrics()
	if pending := e.ConsumePendingSignal(t.Symbol); pending != nil {
		return e.OnSignal(*pending, t.LTP)
	}
	return nil, nil
}

// GetLatestPriceForSymbol returns the cached last-traded price for a symbol.
func (e *Engine) GetLatestPriceForSymbol(symbol string) (float64, bool) {
	value, ok := e.state.LatestPrices[strings.ToUpper(symbol)]
	return value, ok && value > 0
}

// GetLatestCandleForSymbol returns the cached latest candle for a symbol.
// Inputs: symbol string; Outputs: *Candle or nil.
func (e *Engine) GetLatestCandleForSymbol(symbol string) *Candle {
	return e.state.LatestCandles[strings.ToUpper(symbol)]
}

// BuildRunningTrades returns the current open positions enriched with live MTM.
func (e *Engine) BuildRunningTrades() []RunningTrade {
	rows := make([]RunningTrade, 0, len(e.state.Positions))
	for symbol, pos := range e.state.Positions {
		lastPrice := e.state.LatestPrices[symbol]
		if lastPrice <= 0 {
			if candle := e.state.LatestCandles[symbol]; candle != nil {
				lastPrice = candle.Close
			}
		}
		if lastPrice <= 0 {
			lastPrice = pos.AvgEntry
		}
		rows = append(rows, RunningTrade{
			Symbol:        symbol,
			Strategy:      pos.Strategy,
			Side:          string(pos.Side),
			Quantity:      pos.Quantity,
			EntryPrice:    pos.AvgEntry,
			LastPrice:     lastPrice,
			UnrealizedPnl: pnlForSide(pos.Side, pos.AvgEntry, lastPrice, pos.Quantity),
			EntryTime:     pos.EntryTime,
			LastTickTime:  e.state.LastPriceTimes[symbol],
		})
	}
	return rows
}

func (e *Engine) recomputeUnrealized() {
	unreal := 0.0
	for sym, p := range e.state.Positions {
		lastPrice := e.state.LatestPrices[sym]
		if lastPrice <= 0 {
			last := e.state.LatestCandles[sym]
			if last == nil {
				lastPrice = p.AvgEntry
			} else {
				lastPrice = last.Close
			}
		}
		unreal += pnlForSide(p.Side, p.AvgEntry, lastPrice, p.Quantity)
	}
	e.state.Daily.UnrealizedPnl = unreal
}

func pnlForSide(side PositionSide, entryPrice, lastPrice float64, qty int64) float64 {
	switch side {
	case SideShort:
		return (entryPrice - lastPrice) * float64(qty)
	default:
		return (lastPrice - entryPrice) * float64(qty)
	}
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
