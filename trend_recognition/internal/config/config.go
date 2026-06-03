package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultConfigPath = "config/trend_recognition_config.json"
	defaultNATSURL    = "nats://127.0.0.1:4222"
	defaultTickSubj   = "ticks.raw.GO_FEED.>"
	defaultSignalSubj = "signals.strategy"
	defaultTradesSubj = "trades.paper"
	defaultMetrics    = ":9121"
	defaultDashPath   = "/dashboard"
	defaultTimezone   = "Asia/Kolkata"
)

// BrokerConfig contains broker subject configuration.
type BrokerConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	TicksTopic            string `json:"ticks_topic"`
	SignalTopic           string `json:"signal_topic"`
	TradesTopic           string `json:"trades_topic"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

// StrategyConfig contains lane orchestration and ranking parameters.
type StrategyConfig struct {
	Timezone                    string  `json:"timezone"`
	MarketOpenTime              string  `json:"market_open_time"`
	MarketCloseTime             string  `json:"market_close_time"`
	MorningWindowStart          string  `json:"morning_window_start"`
	MorningWindowEnd            string  `json:"morning_window_end"`
	ClosingWindowStart          string  `json:"closing_window_start"`
	ClosingWindowEnd            string  `json:"closing_window_end"`
	MaxOpenTradesPerLane        int     `json:"max_open_trades_per_lane"`
	MinScoreToTrade             float64 `json:"min_score_to_trade"`
	MinMoveFromOpenPct          float64 `json:"min_move_from_open_pct"`
	MinRVOL                     float64 `json:"min_rvol"`
	BreakoutBufferPct           float64 `json:"breakout_buffer_pct"`
	NearExtremePct              float64 `json:"near_extreme_pct"`
	MaxExtensionFromVWAPPct     float64 `json:"max_extension_from_vwap_pct"`
	PullbackShallowPct          float64 `json:"pullback_shallow_pct"`
	SymbolCooldownSec           int     `json:"symbol_cooldown_sec"`
	PendingEntryTimeoutSec      int     `json:"pending_entry_timeout_sec"`
	PullbackMovePct             float64 `json:"pullback_move_pct"`
	PullbackOppositePct         float64 `json:"pullback_opposite_pct"`
	PullbackReclaimTolerancePct float64 `json:"pullback_reclaim_tolerance_pct"`
	PullbackSecondStart         int     `json:"pullback_second_start"`
	PullbackSecondEnd           int     `json:"pullback_second_end"`
	PullbackMaxSignalsPerLane   int     `json:"pullback_max_signals_per_lane"`
	PullbackStopLossPct         float64 `json:"pullback_stop_loss_pct"`
	PullbackTargetPct           float64 `json:"pullback_target_pct"`
	PullbackWindowStart         string  `json:"pullback_window_start"`
	PullbackWindowEnd           string  `json:"pullback_window_end"`
}

// RiskConfig holds momentum-adaptive stop-loss and target settings.
type RiskConfig struct {
	CapitalMultiplier    float64 `json:"capital_multiplier"`
	StrongScoreThreshold float64 `json:"strong_score_threshold"`
	MediumScoreThreshold float64 `json:"medium_score_threshold"`
	StrongStopLossPct    float64 `json:"strong_stop_loss_pct"`
	MediumStopLossPct    float64 `json:"medium_stop_loss_pct"`
	WeakStopLossPct      float64 `json:"weak_stop_loss_pct"`
	StrongTargetPct      float64 `json:"strong_target_pct"`
	MediumTargetPct      float64 `json:"medium_target_pct"`
	WeakTargetPct        float64 `json:"weak_target_pct"`
}

// ServiceConfig controls service endpoints.
type ServiceConfig struct {
	MetricsAddress string `json:"metrics_address"`
	DashboardPath  string `json:"dashboard_path"`
}

// LoggingConfig controls service log routing.
type LoggingConfig struct {
	FilePath string `json:"file_path"`
	Level    string `json:"level"`
}

// LaneConfig defines one index lane and output strategy id.
type LaneConfig struct {
	ID                   string `json:"id"`
	StrategyName         string `json:"strategy_name"`
	PullbackStrategyName string `json:"pullback_strategy_name"`
	SymbolsCSV           string `json:"symbols_csv"`
}

// AppConfig is the full trend_recognition service config.
type AppConfig struct {
	Env      string         `json:"env"`
	Broker   BrokerConfig   `json:"kafka"`
	Strategy StrategyConfig `json:"strategy"`
	Risk     RiskConfig     `json:"risk"`
	Service  ServiceConfig  `json:"service"`
	Logging  LoggingConfig  `json:"logging"`
	Lanes    []LaneConfig   `json:"lanes"`
}

// LoadConfig loads and validates the service config JSON.
func LoadConfig(path string) (*AppConfig, error) {
	resolved, err := resolveConfigPath(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", resolved, err)
	}
	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", resolved, err)
	}
	applyDefaults(&cfg)
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// resolveConfigPath returns explicit path or best-effort defaults near cwd/executable.
func resolveConfigPath(path string) (string, error) {
	if strings.TrimSpace(path) != "" {
		return path, nil
	}
	candidates := []string{defaultConfigPath}
	if exe, err := os.Executable(); err == nil {
		exeDir := filepath.Dir(exe)
		candidates = append(candidates,
			filepath.Join(exeDir, defaultConfigPath),
			filepath.Join(exeDir, "..", defaultConfigPath),
		)
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, nil
		}
	}
	return "", fmt.Errorf("unable to resolve config path, checked: %s", strings.Join(candidates, ", "))
}

// applyDefaults fills non-critical missing values.
func applyDefaults(cfg *AppConfig) {
	if strings.TrimSpace(cfg.Env) == "" {
		cfg.Env = "prod"
	}
	if strings.TrimSpace(cfg.Broker.BootstrapServers) == "" {
		cfg.Broker.BootstrapServers = defaultNATSURL
	}
	if strings.TrimSpace(cfg.Broker.GroupID) == "" {
		cfg.Broker.GroupID = "trend-recognition-v1"
	}
	if strings.TrimSpace(cfg.Broker.TicksTopic) == "" {
		cfg.Broker.TicksTopic = defaultTickSubj
	}
	if strings.TrimSpace(cfg.Broker.SignalTopic) == "" {
		cfg.Broker.SignalTopic = defaultSignalSubj
	}
	if strings.TrimSpace(cfg.Broker.TradesTopic) == "" {
		cfg.Broker.TradesTopic = defaultTradesSubj
	}
	if cfg.Broker.StartupReplayGraceSec < 0 {
		cfg.Broker.StartupReplayGraceSec = 0
	}

	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = defaultTimezone
	}
	if strings.TrimSpace(cfg.Strategy.MarketOpenTime) == "" {
		cfg.Strategy.MarketOpenTime = "09:15:00"
	}
	if strings.TrimSpace(cfg.Strategy.MarketCloseTime) == "" {
		cfg.Strategy.MarketCloseTime = "15:30:00"
	}
	if strings.TrimSpace(cfg.Strategy.MorningWindowStart) == "" {
		cfg.Strategy.MorningWindowStart = "09:20:00"
	}
	if strings.TrimSpace(cfg.Strategy.MorningWindowEnd) == "" {
		cfg.Strategy.MorningWindowEnd = "10:45:00"
	}
	if strings.TrimSpace(cfg.Strategy.ClosingWindowStart) == "" {
		cfg.Strategy.ClosingWindowStart = "14:45:00"
	}
	if strings.TrimSpace(cfg.Strategy.ClosingWindowEnd) == "" {
		cfg.Strategy.ClosingWindowEnd = "15:20:00"
	}
	if cfg.Strategy.MaxOpenTradesPerLane <= 0 {
		cfg.Strategy.MaxOpenTradesPerLane = 3
	}
	if cfg.Strategy.MinScoreToTrade <= 0 {
		cfg.Strategy.MinScoreToTrade = 6
	}
	if cfg.Strategy.MinMoveFromOpenPct <= 0 {
		cfg.Strategy.MinMoveFromOpenPct = 0.5
	}
	if cfg.Strategy.MinRVOL <= 0 {
		cfg.Strategy.MinRVOL = 1.2
	}
	if cfg.Strategy.BreakoutBufferPct <= 0 {
		cfg.Strategy.BreakoutBufferPct = 0.05
	}
	if cfg.Strategy.NearExtremePct <= 0 {
		cfg.Strategy.NearExtremePct = 0.2
	}
	if cfg.Strategy.MaxExtensionFromVWAPPct <= 0 {
		cfg.Strategy.MaxExtensionFromVWAPPct = 1.8
	}
	if cfg.Strategy.PullbackShallowPct <= 0 {
		cfg.Strategy.PullbackShallowPct = 0.6
	}
	if cfg.Strategy.SymbolCooldownSec <= 0 {
		cfg.Strategy.SymbolCooldownSec = 120
	}
	if cfg.Strategy.PendingEntryTimeoutSec <= 0 {
		cfg.Strategy.PendingEntryTimeoutSec = 120
	}
	if cfg.Strategy.PullbackMovePct <= 0 {
		cfg.Strategy.PullbackMovePct = 0.4
	}
	if cfg.Strategy.PullbackOppositePct <= 0 {
		cfg.Strategy.PullbackOppositePct = 0.2
	}
	if cfg.Strategy.PullbackReclaimTolerancePct <= 0 {
		cfg.Strategy.PullbackReclaimTolerancePct = 0.05
	}
	if cfg.Strategy.PullbackSecondStart < 0 || cfg.Strategy.PullbackSecondStart > 59 {
		cfg.Strategy.PullbackSecondStart = 10
	}
	if cfg.Strategy.PullbackSecondEnd <= 0 || cfg.Strategy.PullbackSecondEnd > 59 {
		cfg.Strategy.PullbackSecondEnd = 40
	}
	if cfg.Strategy.PullbackSecondStart > cfg.Strategy.PullbackSecondEnd {
		cfg.Strategy.PullbackSecondStart = 10
		cfg.Strategy.PullbackSecondEnd = 40
	}
	if cfg.Strategy.PullbackMaxSignalsPerLane <= 0 {
		cfg.Strategy.PullbackMaxSignalsPerLane = 3
	}
	if cfg.Strategy.PullbackStopLossPct <= 0 {
		cfg.Strategy.PullbackStopLossPct = 0.5
	}
	if cfg.Strategy.PullbackTargetPct <= 0 {
		cfg.Strategy.PullbackTargetPct = 1.0
	}
	if strings.TrimSpace(cfg.Strategy.PullbackWindowStart) == "" {
		cfg.Strategy.PullbackWindowStart = "09:15:00"
	}
	if strings.TrimSpace(cfg.Strategy.PullbackWindowEnd) == "" {
		cfg.Strategy.PullbackWindowEnd = "15:20:00"
	}

	if cfg.Risk.CapitalMultiplier <= 0 {
		cfg.Risk.CapitalMultiplier = 1.0
	}
	if cfg.Risk.StrongScoreThreshold <= 0 {
		cfg.Risk.StrongScoreThreshold = 8
	}
	if cfg.Risk.MediumScoreThreshold <= 0 {
		cfg.Risk.MediumScoreThreshold = 6.8
	}
	if cfg.Risk.StrongStopLossPct <= 0 {
		cfg.Risk.StrongStopLossPct = 0.35
	}
	if cfg.Risk.MediumStopLossPct <= 0 {
		cfg.Risk.MediumStopLossPct = 0.45
	}
	if cfg.Risk.WeakStopLossPct <= 0 {
		cfg.Risk.WeakStopLossPct = 0.55
	}
	if cfg.Risk.StrongTargetPct <= 0 {
		cfg.Risk.StrongTargetPct = 1.4
	}
	if cfg.Risk.MediumTargetPct <= 0 {
		cfg.Risk.MediumTargetPct = 1.0
	}
	if cfg.Risk.WeakTargetPct <= 0 {
		cfg.Risk.WeakTargetPct = 0.8
	}

	if strings.TrimSpace(cfg.Service.MetricsAddress) == "" {
		cfg.Service.MetricsAddress = defaultMetrics
	}
	if strings.TrimSpace(cfg.Service.DashboardPath) == "" {
		cfg.Service.DashboardPath = defaultDashPath
	}
	if strings.TrimSpace(cfg.Logging.FilePath) == "" {
		cfg.Logging.FilePath = "logs/trend_recognition.log"
	}
	if strings.TrimSpace(cfg.Logging.Level) == "" {
		cfg.Logging.Level = "info"
	}
}

// validateConfig checks required fields and lane definitions.
func validateConfig(cfg *AppConfig) error {
	missing := make([]string, 0)
	if strings.TrimSpace(cfg.Broker.BootstrapServers) == "" {
		missing = append(missing, "kafka.bootstrap_servers")
	}
	if strings.TrimSpace(cfg.Broker.TicksTopic) == "" {
		missing = append(missing, "kafka.ticks_topic")
	}
	if strings.TrimSpace(cfg.Broker.SignalTopic) == "" {
		missing = append(missing, "kafka.signal_topic")
	}
	if strings.TrimSpace(cfg.Broker.TradesTopic) == "" {
		missing = append(missing, "kafka.trades_topic")
	}
	if strings.TrimSpace(cfg.Service.MetricsAddress) == "" {
		missing = append(missing, "service.metrics_address")
	}
	if len(cfg.Lanes) == 0 {
		missing = append(missing, "lanes[]")
	}
	for i, lane := range cfg.Lanes {
		if strings.TrimSpace(lane.ID) == "" {
			missing = append(missing, fmt.Sprintf("lanes[%d].id", i))
		}
		if strings.TrimSpace(lane.StrategyName) == "" {
			missing = append(missing, fmt.Sprintf("lanes[%d].strategy_name", i))
		}
		if strings.TrimSpace(lane.PullbackStrategyName) == "" {
			cfg.Lanes[i].PullbackStrategyName = fmt.Sprintf("trend_recognition_pull_back_%s", strings.ToLower(strings.TrimSpace(lane.ID)))
		}
		if strings.TrimSpace(lane.SymbolsCSV) == "" {
			missing = append(missing, fmt.Sprintf("lanes[%d].symbols_csv", i))
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
