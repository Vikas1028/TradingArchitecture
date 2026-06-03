package engine

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"real_engine/internal/metrics"
)

type Engine struct {
	state      *EngineState
	logger     *zap.Logger
	entryStart time.Duration
	entryEnd   time.Duration
	eodFlat    time.Duration
}

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
	return &Engine{state: state, logger: logger, entryStart: start, entryEnd: end, eodFlat: flat}, nil
}

func (e *Engine) IsAfterEODFlatTime(t time.Time) bool {
	return durationOfDay(t.In(e.state.Tz)) >= e.eodFlat
}

func (e *Engine) CheckAndResetDayIfNeeded(now time.Time) {
	nowInTz := now.In(e.state.Tz)
	y1, m1, d1 := e.state.Daily.Date.Date()
	y2, m2, d2 := nowInTz.Date()
	if y1 == y2 && m1 == m2 && d1 == d2 {
		return
	}
	e.state.Positions = make(map[string]*Position)
	e.state.LatestCandles = make(map[string]*Candle)
	e.state.LatestPrices = make(map[string]float64)
	e.state.LastPriceTimes = make(map[string]time.Time)
	e.state.PendingSignals = make(map[string]*PendingSignal)
	e.state.Daily = DailyState{Date: nowInTz}
	e.syncMetrics()
}

func (e *Engine) OnSignal(sig StrategySignal, entryPrice float64) (*TradeEvent, error) {
	symbol := strings.ToUpper(strings.TrimSpace(sig.Symbol))
	sigTime := sig.Time.In(e.state.Tz)
	e.CheckAndResetDayIfNeeded(sigTime)

	if age := time.Since(sigTime); e.state.Trading.PendingSignalMaxAgeSec > 0 && age > time.Duration(e.state.Trading.PendingSignalMaxAgeSec)*time.Second {
		return nil, nil
	}
	if e.state.Daily.TradingHalted || e.state.Daily.TradesToday >= e.state.Risk.MaxTradesPerDay {
		return nil, nil
	}
	if len(e.state.Positions) >= e.state.Risk.MaxOpenPositions {
		return nil, nil
	}
	if _, exists := e.state.Positions[symbol]; exists {
		return nil, nil
	}
	if entryPrice <= 0 {
		return nil, fmt.Errorf("invalid entry price")
	}

	qty := sig.Quantity
	if qty <= 0 {
		qty = int64(math.Floor(e.state.Risk.CapitalPerTrade / entryPrice))
	}
	if qty <= 0 {
		return nil, nil
	}

	side := entryOrderSide(sig.Side)
	pos := &Position{
		Symbol:                  symbol,
		Side:                    side,
		Quantity:                qty,
		AvgEntry:                entryPrice,
		EntryTime:               sigTime,
		Strategy:                sig.Strategy,
		StopLossPct:             effectiveStopLossPct(sig, e.state.Risk.PerTradeSLPct),
		TargetPct:               effectiveTargetPct(sig, e.state.Risk.PerTradeTargetPct),
		TrailingStopPct:         effectiveTrailingStopPct(sig),
		TrailingFreezeProfitPct: effectiveTrailingFreezePct(sig),
	}
	e.state.Positions[symbol] = pos
	e.state.Daily.TradesToday++
	e.syncMetrics()

	return &TradeEvent{
		EventKey:                buildEventKey(symbol, TradeEntry, sigTime, sig.Strategy),
		Symbol:                  symbol,
		Time:                    sigTime,
		Price:                   entryPrice,
		Quantity:                qty,
		TradeType:               TradeEntry,
		Side:                    string(pos.Side),
		Strategy:                sig.Strategy,
		SignalTime:              sigTime,
		Reason:                  firstNonEmpty(sig.Reason, "ENTRY_FROM_SIGNAL"),
		StopLossPct:             pos.StopLossPct,
		TargetPct:               pos.TargetPct,
		TrailingStopPct:         pos.TrailingStopPct,
		TrailingFreezeProfitPct: pos.TrailingFreezeProfitPct,
	}, nil
}

func (e *Engine) QueueSignal(sig StrategySignal, now time.Time) {
	symbol := strings.ToUpper(strings.TrimSpace(sig.Symbol))
	copySig := sig
	copySig.Symbol = symbol
	e.state.PendingSignals[symbol] = &PendingSignal{Signal: copySig, QueuedAt: now.In(e.state.Tz)}
	e.syncMetrics()
}

func (e *Engine) ConsumePendingSignal(symbol string) *StrategySignal {
	pending := e.state.PendingSignals[strings.ToUpper(symbol)]
	if pending == nil {
		return nil
	}
	delete(e.state.PendingSignals, strings.ToUpper(symbol))
	e.syncMetrics()
	return &pending.Signal
}

func (e *Engine) DrainExpiredPendingSignals(now time.Time, maxAge time.Duration) []PendingSignal {
	if maxAge <= 0 {
		return nil
	}
	expired := make([]PendingSignal, 0)
	nowInTz := now.In(e.state.Tz)
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

func (e *Engine) OnCandle(c Candle) ([]TradeEvent, error) {
	e.CheckAndResetDayIfNeeded(c.Time)
	c.Symbol = strings.ToUpper(strings.TrimSpace(c.Symbol))
	e.state.LatestCandles[c.Symbol] = &c

	events := make([]TradeEvent, 0)
	pos, ok := e.state.Positions[c.Symbol]
	if ok {
		if tr := e.evaluateCandleExit(c, pos); tr != nil {
			events = append(events, *tr)
		}
	}
	e.recomputeUnrealized()
	e.syncMetrics()
	return events, nil
}

func (e *Engine) evaluateCandleExit(c Candle, pos *Position) *TradeEvent {
	exitPrice := 0.0
	reason := ""
	if pos.Side == SideLong {
		if pos.StopLossPct > 0 && c.Low <= pos.AvgEntry*(1-pos.StopLossPct/100) {
			exitPrice = pos.AvgEntry * (1 - pos.StopLossPct/100)
			reason = "EXIT_SL_HIT"
		} else if pos.TargetPct > 0 && c.High >= pos.AvgEntry*(1+pos.TargetPct/100) {
			exitPrice = pos.AvgEntry * (1 + pos.TargetPct/100)
			reason = "EXIT_TARGET_HIT"
		}
	} else {
		if pos.StopLossPct > 0 && c.High >= pos.AvgEntry*(1+pos.StopLossPct/100) {
			exitPrice = pos.AvgEntry * (1 + pos.StopLossPct/100)
			reason = "EXIT_SL_HIT"
		} else if pos.TargetPct > 0 && c.Low <= pos.AvgEntry*(1-pos.TargetPct/100) {
			exitPrice = pos.AvgEntry * (1 - pos.TargetPct/100)
			reason = "EXIT_TARGET_HIT"
		}
	}
	if exitPrice == 0 && e.IsAfterEODFlatTime(c.Time) {
		exitPrice = c.Close
		reason = "EXIT_EOD"
	}
	if exitPrice == 0 {
		return nil
	}
	return e.closePosition(c.Symbol, c.Time, exitPrice, reason)
}

func (e *Engine) OnTick(t Tick) (*TradeEvent, error) {
	t.Symbol = strings.ToUpper(strings.TrimSpace(t.Symbol))
	if t.Symbol == "" || t.LTP <= 0 {
		return nil, nil
	}
	e.CheckAndResetDayIfNeeded(t.Time)
	e.state.LatestPrices[t.Symbol] = t.LTP
	e.state.LastPriceTimes[t.Symbol] = t.Time

	if pos, ok := e.state.Positions[t.Symbol]; ok {
		if event := e.evaluateTickExit(t, pos); event != nil {
			e.recomputeUnrealized()
			e.syncMetrics()
			return event, nil
		}
	}
	e.recomputeUnrealized()
	e.syncMetrics()
	if pending := e.ConsumePendingSignal(t.Symbol); pending != nil {
		return e.OnSignal(*pending, t.LTP)
	}
	return nil, nil
}

func (e *Engine) ForceEODExits(now time.Time) []TradeEvent {
	events := make([]TradeEvent, 0)
	for symbol := range e.state.Positions {
		price := e.state.LatestPrices[symbol]
		if price <= 0 {
			if c := e.state.LatestCandles[symbol]; c != nil {
				price = c.Close
			}
		}
		if price <= 0 {
			price = e.state.Positions[symbol].AvgEntry
		}
		if event := e.closePosition(symbol, now, price, "EXIT_EOD"); event != nil {
			events = append(events, *event)
		}
	}
	e.recomputeUnrealized()
	e.syncMetrics()
	return events
}

func (e *Engine) evaluateTickExit(t Tick, pos *Position) *TradeEvent {
	updateTrailingStop(pos, t.LTP)
	if e.state.Daily.RealizedPnl+e.state.Daily.UnrealizedPnl <= -math.Abs(e.state.Risk.MaxDailyLoss) {
		e.state.Daily.TradingHalted = true
		return e.closePosition(t.Symbol, t.Time, t.LTP, "EXIT_MAX_DAILY_LOSS")
	}
	if e.IsAfterEODFlatTime(t.Time) {
		return e.closePosition(t.Symbol, t.Time, t.LTP, "EXIT_EOD")
	}
	if pos.Side == SideLong && pos.StopLossPct > 0 && t.LTP <= pos.AvgEntry*(1-pos.StopLossPct/100) {
		return e.closePosition(t.Symbol, t.Time, t.LTP, "EXIT_SL_HIT")
	}
	if pos.Side == SideShort && pos.StopLossPct > 0 && t.LTP >= pos.AvgEntry*(1+pos.StopLossPct/100) {
		return e.closePosition(t.Symbol, t.Time, t.LTP, "EXIT_SL_HIT")
	}
	return nil
}

func (e *Engine) closePosition(symbol string, at time.Time, exitPrice float64, reason string) *TradeEvent {
	pos := e.state.Positions[strings.ToUpper(symbol)]
	if pos == nil {
		return nil
	}
	realized := pnlForSide(pos.Side, pos.AvgEntry, exitPrice, pos.Quantity)
	e.state.Daily.RealizedPnl += realized
	if e.state.Daily.RealizedPnl > e.state.Daily.PeakPnl {
		e.state.Daily.PeakPnl = e.state.Daily.RealizedPnl
	}
	drawdown := e.state.Daily.RealizedPnl - e.state.Daily.PeakPnl
	if drawdown < e.state.Daily.MaxDrawdown {
		e.state.Daily.MaxDrawdown = drawdown
	}
	delete(e.state.Positions, strings.ToUpper(symbol))
	return &TradeEvent{
		EventKey:                buildEventKey(symbol, TradeExit, at, pos.Strategy),
		Symbol:                  strings.ToUpper(symbol),
		Time:                    at.In(e.state.Tz),
		Price:                   exitPrice,
		Quantity:                pos.Quantity,
		TradeType:               TradeExit,
		Side:                    string(pos.Side),
		Strategy:                pos.Strategy,
		SignalTime:              pos.EntryTime,
		Reason:                  reason,
		StopLossPct:             pos.StopLossPct,
		TargetPct:               pos.TargetPct,
		TrailingStopPct:         pos.TrailingStopPct,
		TrailingFreezeProfitPct: pos.TrailingFreezeProfitPct,
		RealizedPnl:             realized,
	}
}

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

func (e *Engine) OpenSymbols() []string {
	out := make([]string, 0, len(e.state.Positions))
	for s := range e.state.Positions {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func (e *Engine) RestoreTradeHistory(events []TradeEvent, now time.Time) {
	e.CheckAndResetDayIfNeeded(now)
	sort.Slice(events, func(i, j int) bool { return events[i].Time.Before(events[j].Time) })
	for _, ev := range events {
		symbol := strings.ToUpper(strings.TrimSpace(ev.Symbol))
		switch ev.TradeType {
		case TradeEntry:
			e.state.Positions[symbol] = &Position{
				Symbol:                  symbol,
				Side:                    PositionSide(ev.Side),
				Quantity:                ev.Quantity,
				AvgEntry:                ev.Price,
				EntryTime:               ev.SignalTime,
				Strategy:                ev.Strategy,
				StopLossPct:             ev.StopLossPct,
				TargetPct:               ev.TargetPct,
				TrailingStopPct:         ev.TrailingStopPct,
				TrailingFreezeProfitPct: ev.TrailingFreezeProfitPct,
			}
			e.state.Daily.TradesToday++
		case TradeExit:
			delete(e.state.Positions, symbol)
			e.state.Daily.RealizedPnl += ev.RealizedPnl
			if e.state.Daily.RealizedPnl > e.state.Daily.PeakPnl {
				e.state.Daily.PeakPnl = e.state.Daily.RealizedPnl
			}
		}
	}
	e.recomputeUnrealized()
	e.syncMetrics()
}

func (e *Engine) BuildRunningTrades() []RunningTrade {
	rows := make([]RunningTrade, 0, len(e.state.Positions))
	for symbol, pos := range e.state.Positions {
		lastPrice := e.state.LatestPrices[symbol]
		if lastPrice <= 0 {
			if c := e.state.LatestCandles[symbol]; c != nil {
				lastPrice = c.Close
			}
		}
		if lastPrice <= 0 {
			lastPrice = pos.AvgEntry
		}
		rows = append(rows, RunningTrade{
			Symbol:                  symbol,
			Strategy:                pos.Strategy,
			Side:                    string(pos.Side),
			Quantity:                pos.Quantity,
			EntryPrice:              pos.AvgEntry,
			LastPrice:               lastPrice,
			UnrealizedPnl:           pnlForSide(pos.Side, pos.AvgEntry, lastPrice, pos.Quantity),
			EntryTime:               pos.EntryTime,
			LastTickTime:            e.state.LastPriceTimes[symbol],
			StopLossPct:             pos.StopLossPct,
			TargetPct:               pos.TargetPct,
			TrailingStopPct:         pos.TrailingStopPct,
			TrailingFreezeProfitPct: pos.TrailingFreezeProfitPct,
		})
	}
	return rows
}

func (e *Engine) GetLatestPriceForSymbol(symbol string) (float64, bool) {
	v, ok := e.state.LatestPrices[strings.ToUpper(symbol)]
	return v, ok && v > 0
}

func (e *Engine) GetLastPriceTimeForSymbol(symbol string) time.Time {
	return e.state.LastPriceTimes[strings.ToUpper(symbol)]
}

func (e *Engine) GetLatestCandleForSymbol(symbol string) *Candle {
	return e.state.LatestCandles[strings.ToUpper(symbol)]
}

func (e *Engine) UpdateLatestCandle(c Candle) {
	c.Symbol = strings.ToUpper(strings.TrimSpace(c.Symbol))
	e.state.LatestCandles[c.Symbol] = &c
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

func (e *Engine) recomputeUnrealized() {
	unreal := 0.0
	for sym, p := range e.state.Positions {
		lastPrice := e.state.LatestPrices[sym]
		if lastPrice <= 0 {
			if c := e.state.LatestCandles[sym]; c != nil {
				lastPrice = c.Close
			}
		}
		if lastPrice <= 0 {
			lastPrice = p.AvgEntry
		}
		unreal += pnlForSide(p.Side, p.AvgEntry, lastPrice, p.Quantity)
	}
	e.state.Daily.UnrealizedPnl = unreal
}

func pnlForSide(side PositionSide, entryPrice, lastPrice float64, qty int64) float64 {
	if side == SideShort {
		return (entryPrice - lastPrice) * float64(qty)
	}
	return (lastPrice - entryPrice) * float64(qty)
}

func effectiveStopLossPct(sig StrategySignal, fallback float64) float64 {
	if sig.StopLossPct > 0 {
		return sig.StopLossPct
	}
	return fallback
}

func effectiveTargetPct(sig StrategySignal, fallback float64) float64 {
	if sig.TargetPct >= 0 {
		return sig.TargetPct
	}
	return fallback
}

func effectiveTrailingStopPct(sig StrategySignal) float64 {
	return sig.TrailingStopPct
}

func effectiveTrailingFreezePct(sig StrategySignal) float64 {
	return sig.TrailingFreezeProfitPct
}

func entryOrderSide(side SignalSide) PositionSide {
	if side == SideSell {
		return SideShort
	}
	return SideLong
}

func updateTrailingStop(pos *Position, lastPrice float64) {
	if pos == nil || pos.TrailingStopPct <= 0 {
		return
	}
	profitPct := 0.0
	if pos.Side == SideShort {
		profitPct = ((pos.AvgEntry - lastPrice) / pos.AvgEntry) * 100
	} else {
		profitPct = ((lastPrice - pos.AvgEntry) / pos.AvgEntry) * 100
	}
	if profitPct > pos.PeakProfitPct {
		pos.PeakProfitPct = profitPct
	}
	if pos.TrailingFreezeProfitPct > 0 && pos.PeakProfitPct >= pos.TrailingFreezeProfitPct {
		pos.StopLossPct = pos.PeakProfitPct - pos.TrailingStopPct
		if pos.StopLossPct < 0 {
			pos.StopLossPct = 0
		}
	}
}

func parseHHMM(value string) (time.Duration, error) {
	parts := strings.Split(value, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid time format: %s", value)
	}
	h, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	m, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, err
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute, nil
}

func durationOfDay(t time.Time) time.Duration {
	return time.Duration(t.Hour())*time.Hour + time.Duration(t.Minute())*time.Minute
}

func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func buildEventKey(symbol string, tradeType TradeType, at time.Time, strategy string) string {
	return fmt.Sprintf("%s|%s|%s|%s", strings.ToUpper(symbol), tradeType, strategy, at.UTC().Format(time.RFC3339Nano))
}
