package strategy

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"openmarketvolatility/common"
)

type EngineConfig struct {
	Strategy common.StrategyConfig
}

type minuteState struct {
	Minute           time.Time
	Open             float64
	Close            float64
	High             float64
	Low              float64
	BurstStart       time.Time
	BurstReachedUp   bool
	BurstReachedDown bool
}

type strategy1State struct {
	SeenDipBelowOpen   bool
	SeenRallyAboveOpen bool
}

type strategy4State struct {
	OpeningRangeHigh float64
	OpeningRangeLow  float64
	Ready            bool
}

type symbolState struct {
	OpenPrice        float64
	LastPrice        float64
	LastTickTime     time.Time
	LastSource       string
	CurrentMinute    minuteState
	PreviousMinute   minuteState
	Strategy1        strategy1State
	Strategy4        strategy4State
	TradeDate        string
	LastSignalMinute time.Time
}

type Engine struct {
	cfg            EngineConfig
	loc            *time.Location
	mu             sync.RWMutex
	symbols        map[string]*symbolState
	signalsTotal   uint64
	strategy1Total uint64
	strategy2Total uint64
	strategy3Total uint64
	strategy4Total uint64
	s1Count        int
	s3Count        int
	s4Count        int
	s2MinuteCount  map[string]int
	tradeDate      string
}

func NewEngine(cfg EngineConfig) (*Engine, error) {
	loc, err := time.LoadLocation(cfg.Strategy.Timezone)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:           cfg,
		loc:           loc,
		symbols:       make(map[string]*symbolState),
		s2MinuteCount: make(map[string]int),
	}, nil
}

func (e *Engine) OnTick(tick common.Tick) []common.Signal {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := tick.Time.In(e.loc)
	e.ensureTradeDate(now)
	state := e.stateFor(tick.Symbol)
	e.acceptSource(state, tick)
	e.updateCurrentMinute(state, tick, now)

	var signals []common.Signal
	if signal, ok := e.evaluateStrategy1(tick, state, now); ok {
		signals = append(signals, signal)
	}
	if signal, ok := e.evaluateStrategy2(tick, state, now); ok {
		signals = append(signals, signal)
	}
	if signal, ok := e.evaluateStrategy3(tick, state, now); ok {
		signals = append(signals, signal)
	}
	if signal, ok := e.evaluateStrategy4(tick, state, now); ok {
		signals = append(signals, signal)
	}
	return signals
}

func (e *Engine) ensureTradeDate(now time.Time) {
	date := now.Format("2006-01-02")
	if e.tradeDate == date {
		return
	}
	e.tradeDate = date
	e.s1Count = 0
	e.s3Count = 0
	e.s4Count = 0
	e.s2MinuteCount = make(map[string]int)
	e.symbols = make(map[string]*symbolState)
}

func (e *Engine) stateFor(symbol string) *symbolState {
	state, ok := e.symbols[symbol]
	if ok {
		return state
	}
	state = &symbolState{}
	e.symbols[symbol] = state
	return state
}

func (e *Engine) acceptSource(state *symbolState, tick common.Tick) {
	state.LastPrice = tick.LTP
	state.LastTickTime = tick.Time
	state.LastSource = tick.Source
	if state.OpenPrice == 0 && tick.DayOpen > 0 {
		state.OpenPrice = tick.DayOpen
	}
}

func (e *Engine) updateCurrentMinute(state *symbolState, tick common.Tick, now time.Time) {
	minute := now.Truncate(time.Minute)
	if state.CurrentMinute.Minute.IsZero() {
		state.CurrentMinute = minuteState{
			Minute: minute,
			Open:   tick.LTP,
			Close:  tick.LTP,
			High:   tick.LTP,
			Low:    tick.LTP,
		}
		return
	}
	if minute.After(state.CurrentMinute.Minute) {
		state.PreviousMinute = state.CurrentMinute
		state.CurrentMinute = minuteState{
			Minute: minute,
			Open:   tick.LTP,
			Close:  tick.LTP,
			High:   tick.LTP,
			Low:    tick.LTP,
		}
		return
	}
	if tick.LTP > state.CurrentMinute.High {
		state.CurrentMinute.High = tick.LTP
	}
	if tick.LTP < state.CurrentMinute.Low {
		state.CurrentMinute.Low = tick.LTP
	}
	state.CurrentMinute.Close = tick.LTP

	if state.Strategy4.Ready {
		return
	}
	if minute.Sub(durationOfDay(e.cfg.Strategy.OpenWindowStart, e.loc, now).Truncate(time.Minute)) <= 0 {
		if state.Strategy4.OpeningRangeHigh == 0 || tick.LTP > state.Strategy4.OpeningRangeHigh {
			state.Strategy4.OpeningRangeHigh = tick.LTP
		}
		if state.Strategy4.OpeningRangeLow == 0 || tick.LTP < state.Strategy4.OpeningRangeLow {
			state.Strategy4.OpeningRangeLow = tick.LTP
		}
	}
	if now.After(parseClock(e.cfg.Strategy.OpenWindowEnd, e.loc, now)) {
		state.Strategy4.Ready = true
	}
}

func (e *Engine) evaluateStrategy1(tick common.Tick, state *symbolState, now time.Time) (common.Signal, bool) {
	if !withinWindow(now, e.cfg.Strategy.OpenWindowStart, e.cfg.Strategy.OpenWindowEnd, e.loc) || e.s1Count >= e.cfg.Strategy.S1MaxTradesPerDay || state.OpenPrice == 0 {
		return common.Signal{}, false
	}
	if tick.LTP < state.OpenPrice*(1-e.cfg.Strategy.RetraceThresholdPct/100) {
		state.Strategy1.SeenDipBelowOpen = true
	}
	if tick.LTP > state.OpenPrice*(1+e.cfg.Strategy.RetraceThresholdPct/100) {
		state.Strategy1.SeenRallyAboveOpen = true
	}
	if state.Strategy1.SeenDipBelowOpen && tick.LTP >= state.OpenPrice*(1+e.cfg.Strategy.ReturnTolerancePct/100) {
		e.s1Count++
		return e.emit("openmarketvolatility_s1_open_reclaim", tick.Symbol, "BUY", now, fmt.Sprintf("S1 BUY open=%.2f reclaimed after dip", state.OpenPrice), e.cfg.Strategy.DefaultStopLossPct, e.cfg.Strategy.DefaultTargetPct), true
	}
	if state.Strategy1.SeenRallyAboveOpen && tick.LTP <= state.OpenPrice*(1-e.cfg.Strategy.ReturnTolerancePct/100) {
		e.s1Count++
		return e.emit("openmarketvolatility_s1_open_reclaim", tick.Symbol, "SELL", now, fmt.Sprintf("S1 SELL open=%.2f reversed after rally", state.OpenPrice), e.cfg.Strategy.DefaultStopLossPct, e.cfg.Strategy.DefaultTargetPct), true
	}
	return common.Signal{}, false
}

func (e *Engine) evaluateStrategy2(tick common.Tick, state *symbolState, now time.Time) (common.Signal, bool) {
	if !withinWindow(now, e.cfg.Strategy.BurstWindowStart, e.cfg.Strategy.BurstWindowEnd, e.loc) || state.OpenPrice == 0 {
		return common.Signal{}, false
	}
	minuteKey := now.Format("15:04")
	if e.s2MinuteCount[minuteKey] >= e.cfg.Strategy.S2MaxTradesPerMinute {
		return common.Signal{}, false
	}
	upMove := pctUp(state.OpenPrice, tick.LTP)
	downMove := pctDown(state.OpenPrice, tick.LTP)
	if upMove >= e.cfg.Strategy.BurstTriggerPct {
		if state.CurrentMinute.BurstStart.IsZero() {
			state.CurrentMinute.BurstStart = now
		}
		if now.Sub(state.CurrentMinute.BurstStart) >= time.Duration(e.cfg.Strategy.BurstConfirmationSec)*time.Second {
			e.s2MinuteCount[minuteKey]++
			return e.emit("openmarketvolatility_s2_30sec_burst", tick.Symbol, "BUY", now, fmt.Sprintf("S2 BUY minute=%s open=%.2f moved %.2f%% up within %ds", minuteKey, state.OpenPrice, upMove, e.cfg.Strategy.BurstConfirmationSec), e.cfg.Strategy.DefaultStopLossPct, e.cfg.Strategy.DefaultTargetPct), true
		}
	}
	if downMove >= e.cfg.Strategy.BurstTriggerPct {
		if state.CurrentMinute.BurstStart.IsZero() {
			state.CurrentMinute.BurstStart = now
		}
		if now.Sub(state.CurrentMinute.BurstStart) >= time.Duration(e.cfg.Strategy.BurstConfirmationSec)*time.Second {
			e.s2MinuteCount[minuteKey]++
			return e.emit("openmarketvolatility_s2_30sec_burst", tick.Symbol, "SELL", now, fmt.Sprintf("S2 SELL minute=%s open=%.2f moved %.2f%% down within %ds", minuteKey, state.OpenPrice, downMove, e.cfg.Strategy.BurstConfirmationSec), e.cfg.Strategy.DefaultStopLossPct, e.cfg.Strategy.DefaultTargetPct), true
		}
	}
	return common.Signal{}, false
}

func (e *Engine) evaluateStrategy3(tick common.Tick, state *symbolState, now time.Time) (common.Signal, bool) {
	if e.s3Count >= e.cfg.Strategy.S3MaxTradesPerDay || state.PreviousMinute.Minute.IsZero() {
		return common.Signal{}, false
	}
	prevMove := candleMovePct(state.PreviousMinute.Open, state.PreviousMinute.Close)
	currMove := candleMovePct(state.CurrentMinute.Open, state.CurrentMinute.Close)
	if prevMove >= e.cfg.Strategy.TwoCandleMinMovePct && currMove >= e.cfg.Strategy.TwoCandleMinMovePct {
		e.s3Count++
		return e.emit("openmarketvolatility_s3_two_candle", tick.Symbol, "BUY", now, "S3 BUY first two 1m candles strong up", e.cfg.Strategy.DefaultStopLossPct, e.cfg.Strategy.DefaultTargetPct), true
	}
	if prevMove <= -e.cfg.Strategy.TwoCandleMinMovePct && currMove <= -e.cfg.Strategy.TwoCandleMinMovePct {
		e.s3Count++
		return e.emit("openmarketvolatility_s3_two_candle", tick.Symbol, "SELL", now, "S3 SELL first two 1m candles strong down", e.cfg.Strategy.DefaultStopLossPct, e.cfg.Strategy.DefaultTargetPct), true
	}
	return common.Signal{}, false
}

func (e *Engine) evaluateStrategy4(tick common.Tick, state *symbolState, now time.Time) (common.Signal, bool) {
	if !withinWindow(now, e.cfg.Strategy.S4WindowStart, e.cfg.Strategy.S4WindowEnd, e.loc) || e.s4Count >= e.cfg.Strategy.S4MaxTradesPerDay || !state.Strategy4.Ready {
		return common.Signal{}, false
	}
	if state.Strategy4.OpeningRangeHigh > 0 && pctUp(state.Strategy4.OpeningRangeHigh, tick.LTP) >= e.cfg.Strategy.S4ConfirmPct {
		e.s4Count++
		return e.emit("openmarketvolatility_s4_orb_retest", tick.Symbol, "BUY", now, fmt.Sprintf("S4 BUY ORB-retest ORH=%.2f broke and reconfirmed", state.Strategy4.OpeningRangeHigh), e.cfg.Strategy.S4StopLossPct, e.cfg.Strategy.S4TargetPct), true
	}
	if state.Strategy4.OpeningRangeLow > 0 && pctDown(state.Strategy4.OpeningRangeLow, tick.LTP) >= e.cfg.Strategy.S4ConfirmPct {
		e.s4Count++
		return e.emit("openmarketvolatility_s4_orb_retest", tick.Symbol, "SELL", now, fmt.Sprintf("S4 SELL ORB-retest ORL=%.2f broke and reconfirmed", state.Strategy4.OpeningRangeLow), e.cfg.Strategy.S4StopLossPct, e.cfg.Strategy.S4TargetPct), true
	}
	return common.Signal{}, false
}

func (e *Engine) emit(strategyName, symbol, side string, now time.Time, reason string, stopLoss, target float64) common.Signal {
	e.signalsTotal++
	switch strategyName {
	case "openmarketvolatility_s1_open_reclaim":
		e.strategy1Total++
	case "openmarketvolatility_s2_30sec_burst":
		e.strategy2Total++
	case "openmarketvolatility_s3_two_candle":
		e.strategy3Total++
	case "openmarketvolatility_s4_orb_retest":
		e.strategy4Total++
	}
	return common.Signal{
		Strategy:                strategyName,
		Symbol:                  symbol,
		Side:                    side,
		Time:                    now,
		Reason:                  reason,
		StopLossPct:             stopLoss,
		TargetPct:               target,
		TrailingStopPct:         e.cfg.Strategy.TrailingStopStepPct,
		TrailingFreezeProfitPct: e.cfg.Strategy.TrailingFreezeProfitPct,
	}
}

func (e *Engine) ActiveSymbols() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.symbols)
}

func (e *Engine) LastTickTime() time.Time {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var latest time.Time
	for _, state := range e.symbols {
		if state.LastTickTime.After(latest) {
			latest = state.LastTickTime
		}
	}
	return latest
}

func (e *Engine) SignalsTotal() uint64   { e.mu.RLock(); defer e.mu.RUnlock(); return e.signalsTotal }
func (e *Engine) Strategy1Total() uint64 { e.mu.RLock(); defer e.mu.RUnlock(); return e.strategy1Total }
func (e *Engine) Strategy2Total() uint64 { e.mu.RLock(); defer e.mu.RUnlock(); return e.strategy2Total }
func (e *Engine) Strategy3Total() uint64 { e.mu.RLock(); defer e.mu.RUnlock(); return e.strategy3Total }
func (e *Engine) Strategy4Total() uint64 { e.mu.RLock(); defer e.mu.RUnlock(); return e.strategy4Total }

func withinWindow(now time.Time, startRaw, endRaw string, loc *time.Location) bool {
	start := parseClock(startRaw, loc, now)
	end := parseClock(endRaw, loc, now)
	return !now.Before(start) && !now.After(end)
}

func pctDown(base, price float64) float64 {
	if base <= 0 || price >= base {
		return 0
	}
	return ((base - price) / base) * 100
}

func pctUp(base, price float64) float64 {
	if base <= 0 || price <= base {
		return 0
	}
	return ((price - base) / base) * 100
}

func candleMovePct(open, close float64) float64 {
	if open == 0 {
		return 0
	}
	return ((close - open) / open) * 100
}

func parseClock(raw string, loc *time.Location, now time.Time) time.Time {
	raw = strings.TrimSpace(raw)
	t, err := time.ParseInLocation("15:04:05", raw, loc)
	if err != nil {
		t, _ = time.ParseInLocation("15:04", raw, loc)
	}
	return time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), t.Second(), 0, loc)
}

func durationOfDay(raw string, loc *time.Location, now time.Time) time.Time {
	return parseClock(raw, loc, now)
}
