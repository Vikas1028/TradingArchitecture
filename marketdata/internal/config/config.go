package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// KafkaConfig holds Kafka-related settings for the service.
// Fields configure bootstrap, group, topics, and commit interval.
type KafkaConfig struct {
	BootstrapServers  string `json:"bootstrap_servers"`
	GroupID           string `json:"group_id"`
	TicksTopic        string `json:"ticks_topic"`
	StockCandlesTopic string `json:"stock_candles_topic"`
	IndexCandlesTopic string `json:"index_candles_topic"`
	CommitIntervalMs  int    `json:"commit_interval_ms"`
}

// SymbolsConfig describes classification of symbols (which ones are indices).
// It is used to route candles to the correct Kafka topic.
type SymbolsConfig struct {
	IndexSymbols []string `json:"index_symbols"`
}

// LogConfig contains logging configuration.
// Level controls verbosity, File is the log destination.
type LogConfig struct {
	Level string `json:"level"`
	File  string `json:"file"`
}

// AggregationConfig controls time zone and flush behavior.
// Timezone is an IANA name; FlushOnShutdown emits partial candles when stopping.
type AggregationConfig struct {
	Timezone        string `json:"timezone"`
	FlushOnShutdown bool   `json:"flush_on_shutdown"`
}

// AppConfig is the root configuration struct for the market data service.
// It combines environment, Kafka, symbol, logging, and aggregation settings.
type AppConfig struct {
	Env         string            `json:"env"`
	Kafka       KafkaConfig       `json:"kafka"`
	Symbols     SymbolsConfig     `json:"symbols"`
	Log         LogConfig         `json:"log"`
	Aggregation AggregationConfig `json:"aggregation"`
}

// LoadConfig loads and validates the market data service configuration from JSON file path.
// Inputs: path to JSON file on disk.
// Outputs: populated AppConfig or error.
// Flow: read file, unmarshal JSON, apply defaults, validate required fields, ensure log directory exists.
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
	if cfg.Kafka.TicksTopic == "" {
		cfg.Kafka.TicksTopic = "ticks.raw"
	}
	if cfg.Kafka.StockCandlesTopic == "" {
		cfg.Kafka.StockCandlesTopic = "candles.1m"
	}
	if cfg.Kafka.IndexCandlesTopic == "" {
		cfg.Kafka.IndexCandlesTopic = "indices.1m"
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = 1000
	}
	if cfg.Aggregation.Timezone == "" {
		cfg.Aggregation.Timezone = "Asia/Kolkata"
	}
	if strings.TrimSpace(cfg.Log.Level) == "" {
		cfg.Log.Level = "INFO"
	}
	if strings.TrimSpace(cfg.Log.File) == "" {
		cfg.Log.File = "logs/marketdata_service.log"
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
	if strings.TrimSpace(cfg.Log.File) == "" {
		missing = append(missing, "log.file")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

func ensureLogDir(logPath string) error {
	if logPath == "" {
		return nil
	}
	dir := filepath.Dir(logPath)
	return os.MkdirAll(dir, 0o755)
}
