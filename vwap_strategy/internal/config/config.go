package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"vwap_strategy/common"
)

type KafkaConfig = common.KafkaConfig
type StrategyConfig = common.StrategyConfig
type AppConfig = common.AppConfig

// LoadConfig loads and validates the VWAP strategy configuration from JSON path.
// Inputs: path string to JSON file.
// Outputs: populated AppConfig or error.
// Flow: read file, unmarshal, apply defaults, validate required fields, ensure log directory exists.
func LoadConfig(path string) (*common.AppConfig, error) {
	if strings.TrimSpace(path) == "" {
		resolvedPath, err := common.ResolveConfigPath()
		if err != nil {
			return nil, err
		}
		path = resolvedPath
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg common.AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(cfg *common.AppConfig) {
	if strings.TrimSpace(cfg.Env) == "" {
		cfg.Env = common.DefaultEnv
	}
	if cfg.Kafka.StockCandlesTopic == "" {
		cfg.Kafka.StockCandlesTopic = common.DefaultStockCandlesTopic
	}
	if cfg.Kafka.IndexCandlesTopic == "" {
		cfg.Kafka.IndexCandlesTopic = common.DefaultIndexCandlesTopic
	}
	if cfg.Kafka.SignalTopic == "" {
		cfg.Kafka.SignalTopic = common.DefaultSignalTopic
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = common.DefaultCommitIntervalMs
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = common.DefaultReplayGraceSec
	}
	if cfg.Strategy.Timezone == "" {
		cfg.Strategy.Timezone = common.DefaultTimezone
	}
}

func validate(cfg *common.AppConfig) error {
	missing := make([]string, 0)
	if strings.TrimSpace(cfg.Kafka.BootstrapServers) == "" {
		missing = append(missing, "kafka.bootstrap_servers")
	}
	if strings.TrimSpace(cfg.Kafka.GroupID) == "" {
		missing = append(missing, "kafka.group_id")
	}
	if cfg.Kafka.StartupReplayGraceSec < 0 {
		missing = append(missing, "kafka.startup_replay_grace_sec(>=0)")
	}
	if strings.TrimSpace(cfg.Strategy.EntryStart) == "" {
		missing = append(missing, "strategy.entry_start")
	}
	if strings.TrimSpace(cfg.Strategy.EntryEnd) == "" {
		missing = append(missing, "strategy.entry_end")
	}
	if cfg.Strategy.TrendLookback <= 0 {
		missing = append(missing, "strategy.trend_lookback")
	}
	if cfg.Strategy.PullbackWindow <= 0 {
		missing = append(missing, "strategy.pullback_window")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid required config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
