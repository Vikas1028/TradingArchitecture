package strategy

import (
	"testing"
	"time"

	"openmarketvolatility/common"
)

func testEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := NewEngine(EngineConfig{
		Strategy: common.StrategyConfig{
			Timezone:               "Asia/Kolkata",
			BurstWindowStart:       "09:16:00",
			BurstWindowEnd:         "09:30:00",
			BurstTriggerPct:        1.0,
			BurstConfirmationSec:   30,
			S2MaxTradesPerMinute:   2,
			DefaultStopLossPct:     0.5,
			DefaultTargetPct:       1.0,
			FallbackEnabled:        true,
			FallbackEvaluateAt:     "09:16:30",
			FallbackMinPositivePct: 0.05,
			FallbackMaxBaselineSec: 5,
			FallbackMaxLastTickSec: 5,
			FallbackMaxPublishSec:  15,
			FallbackStrategyName:   "openmarketvolatility_s2_top_gainer_fallback",
		},
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	return engine
}

func TestStrategy2DedupesSameSymbolSameMinute(t *testing.T) {
	engine := testEngine(t)
	loc, _ := time.LoadLocation("Asia/Kolkata")
	base := time.Date(2026, 6, 3, 9, 16, 0, 0, loc)

	_ = engine.OnTick(common.Tick{Symbol: "TCS", Time: base, LTP: 100, DayOpen: 100})
	_ = engine.OnTick(common.Tick{Symbol: "TCS", Time: base.Add(1 * time.Second), LTP: 101.2, DayOpen: 100})
	signals := engine.OnTick(common.Tick{Symbol: "TCS", Time: base.Add(31 * time.Second), LTP: 101.4, DayOpen: 100})
	if len(signals) != 1 {
		t.Fatalf("expected first confirmed S2 signal, got %d", len(signals))
	}
	signals = engine.OnTick(common.Tick{Symbol: "TCS", Time: base.Add(40 * time.Second), LTP: 101.5, DayOpen: 100})
	if len(signals) != 0 {
		t.Fatalf("expected duplicate S2 signal to be suppressed, got %d", len(signals))
	}
}

func TestFallbackPublishesTopGainerWhenThresholdNotHit(t *testing.T) {
	engine := testEngine(t)
	loc, _ := time.LoadLocation("Asia/Kolkata")
	base := time.Date(2026, 6, 3, 9, 16, 1, 0, loc)

	_ = engine.OnTick(common.Tick{Symbol: "AAA", Time: base, LTP: 100, DayOpen: 100})
	_ = engine.OnTick(common.Tick{Symbol: "BBB", Time: base, LTP: 100, DayOpen: 100})
	_ = engine.OnTick(common.Tick{Symbol: "AAA", Time: base.Add(25 * time.Second), LTP: 100.30, DayOpen: 100})
	signals := engine.OnTick(common.Tick{Symbol: "BBB", Time: base.Add(29 * time.Second), LTP: 100.60, DayOpen: 100})
	if len(signals) != 1 {
		t.Fatalf("expected fallback signal, got %d", len(signals))
	}
	if signals[0].Strategy != "openmarketvolatility_s2_top_gainer_fallback" {
		t.Fatalf("unexpected strategy %s", signals[0].Strategy)
	}
	if signals[0].Symbol != "BBB" {
		t.Fatalf("expected top gainer BBB, got %s", signals[0].Symbol)
	}
	if signals[0].TargetPct != 0 {
		t.Fatalf("expected no target on fallback signal, got %.2f", signals[0].TargetPct)
	}
}
