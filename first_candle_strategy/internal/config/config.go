package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"first_candle_strategy/common"
)

type KafkaConfig = common.KafkaConfig
type StrategyConfig = common.StrategyConfig
type AppConfig = common.AppConfig

// LoadConfig loads and validates the first-candle strategy configuration from JSON path.
// Inputs: path to the JSON config file.
// Outputs: populated AppConfig or an error.
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
	if strings.TrimSpace(cfg.Kafka.CandlesTopic) == "" {
		cfg.Kafka.CandlesTopic = common.DefaultCandlesTopic
	}
	if strings.TrimSpace(cfg.Kafka.SignalTopic) == "" {
		cfg.Kafka.SignalTopic = common.DefaultSignalTopic
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = common.DefaultCommitIntervalMs
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = common.DefaultReplayGraceSec
	}
	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = common.DefaultTimezone
	}
	if strings.TrimSpace(cfg.Strategy.SessionStart) == "" {
		cfg.Strategy.SessionStart = common.DefaultSessionStart
	}
	if cfg.Strategy.OpeningCandleSlot == 0 {
		cfg.Strategy.OpeningCandleSlot = common.DefaultOpeningSlot
	}
	if cfg.Strategy.MoveThresholdPct == 0 {
		cfg.Strategy.MoveThresholdPct = common.DefaultMoveThresholdPct
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
	if strings.TrimSpace(cfg.Strategy.SessionStart) == "" {
		missing = append(missing, "strategy.session_start")
	}
	if cfg.Strategy.OpeningCandleSlot < 1 || cfg.Strategy.OpeningCandleSlot > 3 {
		missing = append(missing, "strategy.opening_candle_slot(1..3)")
	}
	if cfg.Strategy.MoveThresholdPct <= 0 {
		missing = append(missing, "strategy.move_threshold_pct(>0)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid required config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
