package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// KafkaConfig holds Kafka-related settings for the paper engine.
// It configures bootstrap servers, consumer group, topics, and commit interval.
type KafkaConfig struct {
	BootstrapServers string `json:"bootstrap_servers"`
	GroupID          string `json:"group_id"`

	SignalsTopic string `json:"signals_topic"`
	CandlesTopic string `json:"candles_topic"`
	TradesTopic  string `json:"trades_topic"`
	PnlTopic     string `json:"pnl_topic"`

	CommitIntervalMs int `json:"commit_interval_ms"`
}

// LogConfig contains logging configuration.
// Level controls verbosity; File sets the log destination.
type LogConfig struct {
	Level string `json:"level"`
	File  string `json:"file"`
}

// TradingConfig defines intraday trading settings for the paper engine.
// Includes timezone, trading window, EOD flat time, and MTM snapshot cadence.
type TradingConfig struct {
	Timezone               string `json:"timezone"`
	EntryStart             string `json:"entry_start"`
	EntryEnd               string `json:"entry_end"`
	EODFlatTime            string `json:"eod_flat_time"`
	MtmSnapshotIntervalSec int    `json:"mtm_snapshot_interval_sec"`
}

// RiskConfig defines risk management parameters for the paper engine.
// Includes capital sizing, trade limits, loss limits, and per-trade SL/target percentages.
type RiskConfig struct {
	CapitalPerTrade   float64 `json:"capital_per_trade"`
	MaxTradesPerDay   int     `json:"max_trades_per_day"`
	MaxOpenPositions  int     `json:"max_open_positions"`
	MaxDailyLoss      float64 `json:"max_daily_loss"`
	PerTradeSLPct     float64 `json:"per_trade_sl_pct"`
	PerTradeTargetPct float64 `json:"per_trade_target_pct"`
}

// AppConfig is the root configuration for the paper trading engine.
// It combines environment, Kafka, logging, trading, and risk settings.
type AppConfig struct {
	Env     string        `json:"env"`
	Kafka   KafkaConfig   `json:"kafka"`
	Log     LogConfig     `json:"log"`
	Trading TradingConfig `json:"trading"`
	Risk    RiskConfig    `json:"risk"`
}

// LoadConfig loads and validates the paper engine configuration from a JSON path.
// Inputs: path to JSON config file.
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
	if cfg.Kafka.SignalsTopic == "" {
		cfg.Kafka.SignalsTopic = "signals.vwap"
	}
	if cfg.Kafka.CandlesTopic == "" {
		cfg.Kafka.CandlesTopic = "candles.1m"
	}
	if cfg.Kafka.TradesTopic == "" {
		cfg.Kafka.TradesTopic = "trades.paper"
	}
	if cfg.Kafka.PnlTopic == "" {
		cfg.Kafka.PnlTopic = "pnl.paper"
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = 1000
	}
	if cfg.Trading.Timezone == "" {
		cfg.Trading.Timezone = "Asia/Kolkata"
	}
	if cfg.Trading.MtmSnapshotIntervalSec == 0 {
		cfg.Trading.MtmSnapshotIntervalSec = 60
	}
	if strings.TrimSpace(cfg.Log.Level) == "" {
		cfg.Log.Level = "INFO"
	}
	if strings.TrimSpace(cfg.Log.File) == "" {
		cfg.Log.File = "logs/paper_engine.log"
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
	if strings.TrimSpace(cfg.Trading.EntryStart) == "" {
		missing = append(missing, "trading.entry_start")
	}
	if strings.TrimSpace(cfg.Trading.EntryEnd) == "" {
		missing = append(missing, "trading.entry_end")
	}
	if strings.TrimSpace(cfg.Trading.EODFlatTime) == "" {
		missing = append(missing, "trading.eod_flat_time")
	}
	if cfg.Risk.CapitalPerTrade <= 0 {
		missing = append(missing, "risk.capital_per_trade")
	}
	if cfg.Risk.MaxTradesPerDay <= 0 {
		missing = append(missing, "risk.max_trades_per_day")
	}
	if cfg.Risk.MaxOpenPositions <= 0 {
		missing = append(missing, "risk.max_open_positions")
	}
	if cfg.Risk.PerTradeSLPct <= 0 {
		missing = append(missing, "risk.per_trade_sl_pct")
	}
	if cfg.Risk.PerTradeTargetPct <= 0 {
		missing = append(missing, "risk.per_trade_target_pct")
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
