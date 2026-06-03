package main

import (
	"testing"
	"time"

	"real_engine/common"
	"real_engine/internal/engine"
)

func TestNormalizeStrategySignalRejectsStaleAndWrongStrategy(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	cfg := &common.AppConfig{
		Routing: common.RoutingConfig{
			Enabled:           true,
			AllowedStrategies: []string{"openmarketvolatility_s2_30sec_burst"},
			EntryWindows:      []common.EntryWindow{{Start: "09:16:00", End: "10:00:00"}},
			MaxSignalAgeSec:   30,
			FixedQuantity:     1,
			FixedStopLossPct:  0.30,
			FixedTargetPct:    0,
		},
	}
	now := time.Date(2026, 6, 3, 9, 20, 0, 0, loc)

	if got := normalizeStrategySignal(cfg, &engine.StrategySignal{
		Strategy: "other_strategy",
		Symbol:   "RELIANCE",
		Time:     now,
	}, now, loc); got != nil {
		t.Fatalf("expected disallowed strategy to be rejected")
	}

	if got := normalizeStrategySignal(cfg, &engine.StrategySignal{
		Strategy: "openmarketvolatility_s2_30sec_burst",
		Symbol:   "RELIANCE",
		Time:     now.Add(-31 * time.Second),
	}, now, loc); got != nil {
		t.Fatalf("expected stale signal to be rejected")
	}
}

func TestNormalizeStrategySignalAppliesExecutionOverrides(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	cfg := &common.AppConfig{
		Routing: common.RoutingConfig{
			Enabled:           true,
			AllowedStrategies: []string{"openmarketvolatility_s2_30sec_burst"},
			EntryWindows:      []common.EntryWindow{{Start: "09:16:00", End: "10:00:00"}},
			MaxSignalAgeSec:   30,
			FixedQuantity:     1,
			FixedStopLossPct:  0.30,
			FixedTargetPct:    0,
		},
	}
	now := time.Date(2026, 6, 3, 9, 20, 0, 0, loc)
	normalized := normalizeStrategySignal(cfg, &engine.StrategySignal{
		Strategy:                "openmarketvolatility_s2_30sec_burst",
		Symbol:                  "TCS",
		Time:                    now,
		Quantity:                25,
		StopLossPct:             0.8,
		TargetPct:               1.2,
		TrailingStopPct:         0.2,
		TrailingFreezeProfitPct: 0.4,
	}, now, loc)
	if normalized == nil {
		t.Fatalf("expected signal to pass")
	}
	if normalized.Quantity != 1 {
		t.Fatalf("expected fixed quantity 1, got %d", normalized.Quantity)
	}
	if normalized.StopLossPct != 0.30 {
		t.Fatalf("expected fixed stop loss 0.30, got %.2f", normalized.StopLossPct)
	}
	if normalized.TargetPct != 0 {
		t.Fatalf("expected target 0, got %.2f", normalized.TargetPct)
	}
	if normalized.TrailingStopPct != 0 || normalized.TrailingFreezeProfitPct != 0 {
		t.Fatalf("expected trailing fields to be cleared")
	}
}
