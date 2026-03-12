package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// KafkaConfig holds Kafka-related settings for the first-candle strategy service.
// It configures bootstrap servers, consumer group, source topic, signal topic, and commit interval.
type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	CandlesTopic          string `json:"candles_topic"`
	SignalTopic           string `json:"signal_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

// LogConfig contains logging configuration.
// Level controls verbosity and File is the output log path.
type LogConfig struct {
	Level string `json:"level"`
	File  string `json:"file"`
}

// StrategyConfig defines parameters for the first-candle reversal detector.
// SessionStart anchors the 9:15 session start, OpeningCandleSlot chooses which 15-minute block to scan,
// and MoveThresholdPct defines the first-leg trigger threshold.
type StrategyConfig struct {
	Timezone          string  `json:"timezone"`
	SessionStart      string  `json:"session_start"`
	OpeningCandleSlot int     `json:"opening_candle_slot"`
	MoveThresholdPct  float64 `json:"move_threshold_pct"`
}

// AppConfig is the root configuration for the first-candle strategy service.
// It aggregates environment, Kafka, logging, and strategy settings.
type AppConfig struct {
	Env      string         `json:"env"`
	Kafka    KafkaConfig    `json:"kafka"`
	Log      LogConfig      `json:"log"`
	Strategy StrategyConfig `json:"strategy"`
}

// LoadConfig loads and validates the first-candle strategy configuration from JSON path.
// Inputs: path to the JSON config file.
// Outputs: populated AppConfig or an error.
func LoadConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	if err := ensureLogDir(cfg.Log.File); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(cfg *AppConfig) {
	if strings.TrimSpace(cfg.Kafka.CandlesTopic) == "" {
		cfg.Kafka.CandlesTopic = "candles.1m"
	}
	if strings.TrimSpace(cfg.Kafka.SignalTopic) == "" {
		cfg.Kafka.SignalTopic = "signals.strategy"
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = 1000
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = 120
	}
	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = "Asia/Kolkata"
	}
	if strings.TrimSpace(cfg.Strategy.SessionStart) == "" {
		cfg.Strategy.SessionStart = "09:15"
	}
	if cfg.Strategy.OpeningCandleSlot == 0 {
		cfg.Strategy.OpeningCandleSlot = 1
	}
	if cfg.Strategy.MoveThresholdPct == 0 {
		cfg.Strategy.MoveThresholdPct = 0.5
	}
	if strings.TrimSpace(cfg.Log.Level) == "" {
		cfg.Log.Level = "INFO"
	}
	if strings.TrimSpace(cfg.Log.File) == "" {
		cfg.Log.File = "logs/first_candle_strategy.log"
	}
}

func validate(cfg *AppConfig) error {
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
	if strings.TrimSpace(cfg.Log.File) == "" {
		missing = append(missing, "log.file")
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

func ensureLogDir(path string) error {
	if path == "" {
		return nil
	}
	return os.MkdirAll(filepath.Dir(path), 0o755)
}
