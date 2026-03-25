package strategy

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"volatile_strategy/common"
)

type windowStat struct {
	Symbol    string
	Open      float64
	Close     float64
	OpenTime  time.Time
	CloseTime time.Time
	UpdatedAt time.Time
}

type watchSide string

const (
	sideBuy  watchSide = "BUY"
	sideSell watchSide = "SELL"
)

type watcher struct {
	Symbol      string
	Side        watchSide
	AnchorClose float64
	WindowStart time.Time
	WindowEnd   time.Time
	WatchStart  time.Time
	WatchEnd    time.Time
	Triggered   bool
	Extreme     float64
	Emitted     bool
}

type Engine struct {
	cfg                common.StrategyConfig
	location           *time.Location
	sessionStartOffset time.Duration
	sessionEndOffset   time.Duration
	activeWindowStart  time.Time
	windowStats        map[string]*windowStat
	watchers           map[string]*watcher
	lastLeaders        map[string][]common.Rank
}

func NewEngine(cfg common.StrategyConfig) (*Engine, error) {
	loc, err := time.LoadLocation(cfg.Timezone)
	if err != nil {
		return nil, err
	}
	start, err := parseHHMM(cfg.SessionStart)
	if err != nil {
		return nil, fmt.Errorf("parse session_start: %w", err)
	}
	end, err := parseHHMM(cfg.SessionEnd)
	if err != nil {
		return nil, fmt.Errorf("parse session_end: %w", err)
	}
	return &Engine{
		cfg:                cfg,
		location:           loc,
		sessionStartOffset: start,
		sessionEndOffset:   end,
		windowStats:        make(map[string]*windowStat),
		watchers:           make(map[string]*watcher),
		lastLeaders:        make(map[string][]common.Rank),
	}, nil
}

// OnTick updates the active ranking window, advances completed windows, and
// lets any active confirmation watcher react to the incoming LTP.
func (e *Engine) OnTick(tick common.LTPTick) (signals []common.Signal, windowsClosed int, leaders map[string][]common.Rank) {
	ts := tick.Timestamp.In(e.location)
	if !e.inSession(ts) {
		return nil, 0, nil
	}

	windowStart := e.windowStart(ts)
	if e.activeWindowStart.IsZero() {
		e.activeWindowStart = windowStart
	}
	for windowStart.After(e.activeWindowStart) {
		leaders = e.finalizeActiveWindow()
		windowsClosed++
		e.activeWindowStart = e.activeWindowStart.Add(time.Duration(e.cfg.WindowMinutes) * time.Minute)
	}

	e.processWatcherTick(tick.Symbol, tick.Price, ts, &signals)

	if windowStart.Equal(e.activeWindowStart) {
		e.updateWindowStat(tick.Symbol, tick.Price, ts)
	}
	return signals, windowsClosed, leaders
}

func (e *Engine) ActiveWatchers() int                   { return len(e.watchers) }
func (e *Engine) LastLeaders() map[string][]common.Rank { return e.lastLeaders }

// updateWindowStat tracks the open/close pair for the current ranking window.
func (e *Engine) updateWindowStat(symbol string, price float64, ts time.Time) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	stat := e.windowStats[symbol]
	if stat == nil {
		e.windowStats[symbol] = &windowStat{
			Symbol:    symbol,
			Open:      price,
			Close:     price,
			OpenTime:  ts,
			CloseTime: ts,
			UpdatedAt: ts,
		}
		return
	}
	if ts.Before(stat.OpenTime) {
		stat.Open = price
		stat.OpenTime = ts
	}
	if !ts.Before(stat.CloseTime) {
		stat.Close = price
		stat.CloseTime = ts
	}
	stat.UpdatedAt = ts
}

// finalizeActiveWindow ranks movers for the completed window and arms the
// next confirmation watchers for the highest and lowest movers.
func (e *Engine) finalizeActiveWindow() map[string][]common.Rank {
	if len(e.windowStats) == 0 {
		e.pruneExpiredWatchers(e.activeWindowStart)
		e.lastLeaders = nil
		return nil
	}

	ranks := make([]common.Rank, 0, len(e.windowStats))
	windowEnd := e.activeWindowStart.Add(time.Duration(e.cfg.WindowMinutes) * time.Minute)
	for _, stat := range e.windowStats {
		if stat.Open <= 0 {
			continue
		}
		changePct := ((stat.Close - stat.Open) / stat.Open) * 100
		ranks = append(ranks, common.Rank{
			Symbol:      stat.Symbol,
			Open:        stat.Open,
			Close:       stat.Close,
			ChangePct:   changePct,
			WindowStart: e.activeWindowStart.Format(time.RFC3339),
			WindowEnd:   windowEnd.Format(time.RFC3339),
		})
	}
	sort.Slice(ranks, func(i, j int) bool {
		if ranks[i].ChangePct == ranks[j].ChangePct {
			return ranks[i].Symbol < ranks[j].Symbol
		}
		return ranks[i].ChangePct > ranks[j].ChangePct
	})

	topN := min(e.cfg.TopCount, len(ranks))
	highest := append([]common.Rank(nil), ranks[:topN]...)
	lowest := make([]common.Rank, 0, topN)
	for i := len(ranks) - 1; i >= 0 && len(lowest) < topN; i-- {
		lowest = append(lowest, ranks[i])
	}
	e.lastLeaders = map[string][]common.Rank{
		"highest": highest,
		"lowest":  lowest,
	}

	watchStart := windowEnd.Add(time.Minute)
	watchEnd := watchStart.Add(time.Duration(e.cfg.ConfirmMinutes) * time.Minute)
	for _, rank := range highest {
		e.watchers[rank.Symbol] = &watcher{
			Symbol:      rank.Symbol,
			Side:        sideBuy,
			AnchorClose: rank.Close,
			WindowStart: e.activeWindowStart,
			WindowEnd:   windowEnd,
			WatchStart:  watchStart,
			WatchEnd:    watchEnd,
		}
	}
	for _, rank := range lowest {
		e.watchers[rank.Symbol] = &watcher{
			Symbol:      rank.Symbol,
			Side:        sideSell,
			AnchorClose: rank.Close,
			WindowStart: e.activeWindowStart,
			WindowEnd:   windowEnd,
			WatchStart:  watchStart,
			WatchEnd:    watchEnd,
		}
	}
	e.pruneExpiredWatchers(windowEnd)
	e.windowStats = make(map[string]*windowStat)
	return e.lastLeaders
}

// processWatcherTick confirms a retrace and recovery sequence before emitting
// a signal for a ranked symbol.
func (e *Engine) processWatcherTick(symbol string, price float64, ts time.Time, signals *[]common.Signal) {
	w := e.watchers[strings.ToUpper(strings.TrimSpace(symbol))]
	if w == nil || w.Emitted {
		return
	}
	if ts.Before(w.WatchStart) {
		return
	}
	if !ts.Before(w.WatchEnd) {
		delete(e.watchers, w.Symbol)
		return
	}

	retraceRatio := e.cfg.RetracePct / 100.0
	switch w.Side {
	case sideBuy:
		triggerPrice := w.AnchorClose * (1 - retraceRatio)
		if !w.Triggered && price <= triggerPrice {
			w.Triggered = true
			w.Extreme = price
			return
		}
		if w.Triggered {
			if w.Extreme == 0 || price < w.Extreme {
				w.Extreme = price
			}
			if price >= w.AnchorClose {
				w.Emitted = true
				*signals = append(*signals, common.Signal{
					Strategy: common.AppName,
					Symbol:   w.Symbol,
					Side:     string(sideBuy),
					Time:     ts,
					Reason: fmt.Sprintf(
						"VOLATILE_LONG_CONFIRM window=%s close=%.2f retrace_pct=%.2f stop_loss=%.2f",
						w.WindowStart.Format("15:04"), w.AnchorClose, e.cfg.RetracePct, w.Extreme,
					),
				})
				delete(e.watchers, w.Symbol)
			}
		}
	case sideSell:
		triggerPrice := w.AnchorClose * (1 + retraceRatio)
		if !w.Triggered && price >= triggerPrice {
			w.Triggered = true
			w.Extreme = price
			return
		}
		if w.Triggered {
			if price > w.Extreme {
				w.Extreme = price
			}
			if price <= w.AnchorClose {
				w.Emitted = true
				*signals = append(*signals, common.Signal{
					Strategy: common.AppName,
					Symbol:   w.Symbol,
					Side:     string(sideSell),
					Time:     ts,
					Reason: fmt.Sprintf(
						"VOLATILE_SHORT_CONFIRM window=%s close=%.2f retrace_pct=%.2f stop_loss=%.2f",
						w.WindowStart.Format("15:04"), w.AnchorClose, e.cfg.RetracePct, w.Extreme,
					),
				})
				delete(e.watchers, w.Symbol)
			}
		}
	}
}

func (e *Engine) pruneExpiredWatchers(now time.Time) {
	for symbol, w := range e.watchers {
		if !now.Before(w.WatchEnd) {
			delete(e.watchers, symbol)
		}
	}
}

func (e *Engine) inSession(ts time.Time) bool {
	dayOffset := time.Duration(ts.Hour())*time.Hour + time.Duration(ts.Minute())*time.Minute + time.Duration(ts.Second())*time.Second
	return dayOffset >= e.sessionStartOffset && dayOffset <= e.sessionEndOffset
}

func (e *Engine) windowStart(ts time.Time) time.Time {
	dayStart := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, e.location)
	sessionStart := dayStart.Add(e.sessionStartOffset)
	if ts.Before(sessionStart) {
		return sessionStart
	}
	elapsed := ts.Sub(sessionStart)
	windowDur := time.Duration(e.cfg.WindowMinutes) * time.Minute
	return sessionStart.Add((elapsed / windowDur) * windowDur)
}

func parseHHMM(value string) (time.Duration, error) {
	parsed, err := time.Parse("15:04", value)
	if err != nil {
		return 0, err
	}
	return time.Duration(parsed.Hour())*time.Hour + time.Duration(parsed.Minute())*time.Minute, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
