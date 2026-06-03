package main

import (
	"strings"
	"time"

	"real_engine/common"
	"real_engine/internal/engine"
)

func normalizeStrategySignal(cfg *common.AppConfig, sig *engine.StrategySignal, now time.Time, loc *time.Location) *engine.StrategySignal {
	if sig == nil {
		return nil
	}
	if cfg.Routing.Enabled {
		if !isAllowedStrategy(cfg.Routing.AllowedStrategies, sig.Strategy) {
			return nil
		}
		sigTime := sig.Time.In(loc)
		if cfg.Routing.MaxSignalAgeSec > 0 && now.In(loc).Sub(sigTime) > time.Duration(cfg.Routing.MaxSignalAgeSec)*time.Second {
			return nil
		}
		if !withinAnyEntryWindow(sigTime, cfg.Routing.EntryWindows, loc) {
			return nil
		}
	}

	normalized := *sig
	if cfg.Routing.FixedQuantity > 0 {
		normalized.Quantity = cfg.Routing.FixedQuantity
	}
	normalized.StopLossPct = cfg.Routing.FixedStopLossPct
	normalized.TargetPct = cfg.Routing.FixedTargetPct
	normalized.TrailingStopPct = 0
	normalized.TrailingFreezeProfitPct = 0
	return &normalized
}

func isAllowedStrategy(allowed []string, strategy string) bool {
	if len(allowed) == 0 {
		return true
	}
	needle := strings.TrimSpace(strategy)
	for _, item := range allowed {
		if strings.EqualFold(strings.TrimSpace(item), needle) {
			return true
		}
	}
	return false
}

func withinAnyEntryWindow(ts time.Time, windows []common.EntryWindow, loc *time.Location) bool {
	if len(windows) == 0 {
		return true
	}
	local := ts.In(loc)
	for _, window := range windows {
		start, ok := parseWindowClock(window.Start, local, loc)
		if !ok {
			continue
		}
		end, ok := parseWindowClock(window.End, local, loc)
		if !ok {
			continue
		}
		if !local.Before(start) && !local.After(end) {
			return true
		}
	}
	return false
}

func parseWindowClock(raw string, now time.Time, loc *time.Location) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	layouts := []string{"15:04:05", "15:04"}
	for _, layout := range layouts {
		if parsed, err := time.ParseInLocation(layout, raw, loc); err == nil {
			return time.Date(now.Year(), now.Month(), now.Day(), parsed.Hour(), parsed.Minute(), parsed.Second(), 0, loc), true
		}
	}
	return time.Time{}, false
}
