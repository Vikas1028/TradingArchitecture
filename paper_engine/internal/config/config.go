package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"paper_engine/common"
)

type KafkaConfig = common.KafkaConfig
type TradingConfig = common.TradingConfig
type RiskConfig = common.RiskConfig
type AppConfig = common.AppConfig

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
	if strings.TrimSpace(cfg.Kafka.SignalsTopic) == "" {
		cfg.Kafka.SignalsTopic = common.DefaultSignalsTopic
	}
	if strings.TrimSpace(cfg.Kafka.CandlesTopic) == "" {
		cfg.Kafka.CandlesTopic = common.DefaultCandlesTopic
	}
	if strings.TrimSpace(cfg.Kafka.TicksTopic) == "" {
		cfg.Kafka.TicksTopic = common.DefaultTicksTopic
	}
	if strings.TrimSpace(cfg.Kafka.TradesTopic) == "" {
		cfg.Kafka.TradesTopic = common.DefaultTradesTopic
	}
	if strings.TrimSpace(cfg.Kafka.PnlTopic) == "" {
		cfg.Kafka.PnlTopic = common.DefaultPnlTopic
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = common.DefaultCommitMs
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = common.DefaultReplayGrace
	}
	if cfg.Kafka.PriceScaleDivisor <= 0 {
		cfg.Kafka.PriceScaleDivisor = common.DefaultPriceDivisor
	}
	if strings.TrimSpace(cfg.Kafka.SignalsGroupID) == "" {
		cfg.Kafka.SignalsGroupID = strings.TrimSpace(cfg.Kafka.GroupID)
		if cfg.Kafka.SignalsGroupID != "" {
			cfg.Kafka.SignalsGroupID += "-signals"
		}
	}
	if strings.TrimSpace(cfg.Kafka.CandlesGroupID) == "" {
		cfg.Kafka.CandlesGroupID = strings.TrimSpace(cfg.Kafka.GroupID)
		if cfg.Kafka.CandlesGroupID != "" {
			cfg.Kafka.CandlesGroupID += "-candles"
		}
	}
	if strings.TrimSpace(cfg.Kafka.TicksGroupID) == "" {
		cfg.Kafka.TicksGroupID = strings.TrimSpace(cfg.Kafka.GroupID)
		if cfg.Kafka.TicksGroupID != "" {
			cfg.Kafka.TicksGroupID += "-ticks"
		}
	}
	if strings.TrimSpace(cfg.Trading.Timezone) == "" {
		cfg.Trading.Timezone = common.DefaultTimezone
	}
	if cfg.Trading.MtmSnapshotIntervalSec == 0 {
		cfg.Trading.MtmSnapshotIntervalSec = common.DefaultMTMSnapshot
	}
	if cfg.Trading.PendingSignalMaxAgeSec == 0 {
		cfg.Trading.PendingSignalMaxAgeSec = common.DefaultSignalMaxAge
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
	if strings.TrimSpace(cfg.Kafka.SignalsGroupID) == "" {
		missing = append(missing, "kafka.signals_group_id")
	}
	if strings.TrimSpace(cfg.Kafka.CandlesGroupID) == "" {
		missing = append(missing, "kafka.candles_group_id")
	}
	if strings.TrimSpace(cfg.Kafka.TicksGroupID) == "" {
		missing = append(missing, "kafka.ticks_group_id")
	}
	if cfg.Kafka.StartupReplayGraceSec < -1 {
		missing = append(missing, "kafka.startup_replay_grace_sec(>=0 or -1 to disable)")
	}
	if cfg.Kafka.PriceScaleDivisor <= 0 {
		missing = append(missing, "kafka.price_scale_divisor(>0)")
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
	if cfg.Trading.PendingSignalMaxAgeSec < 0 {
		missing = append(missing, "trading.pending_signal_max_age_sec(>=0)")
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
