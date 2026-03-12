package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// KafkaConfig holds Kafka-related settings for the VWAP strategy service.
// It configures bootstrap servers, group, topics, and commit interval.
type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	StockCandlesTopic     string `json:"stock_candles_topic"`
	IndexCandlesTopic     string `json:"index_candles_topic"`
	SignalTopic           string `json:"signal_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

// LogConfig contains logging configuration.
// Level controls verbosity; File is the log path.
type LogConfig struct {
	Level string `json:"level"`
	File  string `json:"file"`
}

// StrategyConfig defines parameters for the VWAP pullback and index bias logic.
// Timezone controls local time calculations; the other fields drive strategy thresholds.
type StrategyConfig struct {
	Timezone            string  `json:"timezone"`
	EntryStart          string  `json:"entry_start"`
	EntryEnd            string  `json:"entry_end"`
	TrendLookback       int     `json:"trend_lookback"`
	PullbackWindow      int     `json:"pullback_window"`
	MinBodyPct          float64 `json:"min_body_pct"`
	MaxPullbackPct      float64 `json:"max_pullback_pct"`
	EnableIndexBias     bool    `json:"enable_index_bias"`
	IndexBiasLookback   int     `json:"index_bias_lookback"`
	IndexBiasVWAPThresh float64 `json:"index_bias_vwap_threshold"`
}

// AppConfig is the root configuration for the VWAP strategy service.
// It aggregates environment, Kafka, logging, and strategy settings.
type AppConfig struct {
	Env      string         `json:"env"`
	Kafka    KafkaConfig    `json:"kafka"`
	Log      LogConfig      `json:"log"`
	Strategy StrategyConfig `json:"strategy"`
}

// LoadConfig loads and validates the VWAP strategy configuration from JSON path.
// Inputs: path string to JSON file.
// Outputs: populated AppConfig or error.
// Flow: read file, unmarshal, apply defaults, validate required fields, ensure log directory exists.
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
	if cfg.Kafka.StockCandlesTopic == "" {
		cfg.Kafka.StockCandlesTopic = "candles.1m"
	}
	if cfg.Kafka.IndexCandlesTopic == "" {
		cfg.Kafka.IndexCandlesTopic = "indices.1m"
	}
	if cfg.Kafka.SignalTopic == "" {
		cfg.Kafka.SignalTopic = "signals.strategy"
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = 1000
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = 120
	}
	if cfg.Strategy.Timezone == "" {
		cfg.Strategy.Timezone = "Asia/Kolkata"
	}
	if strings.TrimSpace(cfg.Log.Level) == "" {
		cfg.Log.Level = "INFO"
	}
	if strings.TrimSpace(cfg.Log.File) == "" {
		cfg.Log.File = "logs/vwap_strategy.log"
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

func ensureLogDir(path string) error {
	if path == "" {
		return nil
	}
	return os.MkdirAll(filepath.Dir(path), 0o755)
}
