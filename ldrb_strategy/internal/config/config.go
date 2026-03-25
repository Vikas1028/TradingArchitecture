package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"ldrb_strategy/common"
)

type KafkaConfig = common.KafkaConfig
type DependenciesConfig = common.DependenciesConfig
type StrategyConfig = common.StrategyConfig
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
	if strings.TrimSpace(cfg.Kafka.StockCandlesTopic) == "" {
		cfg.Kafka.StockCandlesTopic = common.DefaultStockTopic
	}
	if strings.TrimSpace(cfg.Kafka.IndexCandlesTopic) == "" {
		cfg.Kafka.IndexCandlesTopic = common.DefaultIndexTopic
	}
	if strings.TrimSpace(cfg.Kafka.SignalTopic) == "" {
		cfg.Kafka.SignalTopic = common.DefaultSignalTopic
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = common.DefaultCommitMs
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = common.DefaultReplayGrace
	}
	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = common.DefaultTimezone
	}
	if strings.TrimSpace(cfg.Strategy.SessionStart) == "" {
		cfg.Strategy.SessionStart = common.DefaultSessionStart
	}
	if strings.TrimSpace(cfg.Strategy.EntryStart) == "" {
		cfg.Strategy.EntryStart = common.DefaultEntryStart
	}
	if strings.TrimSpace(cfg.Strategy.EntryEnd) == "" {
		cfg.Strategy.EntryEnd = common.DefaultEntryEnd
	}
	if strings.TrimSpace(cfg.Strategy.SameDayExitCutoff) == "" {
		cfg.Strategy.SameDayExitCutoff = common.DefaultSameDayExit
	}
	if strings.TrimSpace(cfg.Strategy.ExitMonitoringEnd) == "" {
		cfg.Strategy.ExitMonitoringEnd = common.DefaultMonitorEnd
	}
	if cfg.Strategy.Capital == 0 {
		cfg.Strategy.Capital = 100000
	}
	if cfg.Strategy.RiskPerTradePct == 0 {
		cfg.Strategy.RiskPerTradePct = 0.005
	}
	if cfg.Strategy.MaxTradesPerDay == 0 {
		cfg.Strategy.MaxTradesPerDay = 2
	}
	if cfg.Strategy.MaxOpenOvernight == 0 {
		cfg.Strategy.MaxOpenOvernight = 1
	}
	if cfg.Strategy.MaxOvernightRiskPct == 0 {
		cfg.Strategy.MaxOvernightRiskPct = 0.008
	}
	if cfg.Strategy.CapitalUsageCapPct == 0 {
		cfg.Strategy.CapitalUsageCapPct = 0.70
	}
	if cfg.Strategy.MinPrice == 0 {
		cfg.Strategy.MinPrice = 100
	}
	if cfg.Strategy.MaxIntradayMovePct == 0 {
		cfg.Strategy.MaxIntradayMovePct = 0.04
	}
	if cfg.Strategy.BodyRatioMin == 0 {
		cfg.Strategy.BodyRatioMin = 0.60
	}
	if cfg.Strategy.VolumeRatioMin == 0 {
		cfg.Strategy.VolumeRatioMin = 2.0
	}
	if cfg.Strategy.RVOLMin == 0 {
		cfg.Strategy.RVOLMin = 1.5
	}
	if cfg.Strategy.BreakoutCushionPct == 0 {
		cfg.Strategy.BreakoutCushionPct = 0.0015
	}
	if cfg.Strategy.SecondTryExtraPct == 0 {
		cfg.Strategy.SecondTryExtraPct = 0.0010
	}
	if cfg.Strategy.SlippageBufferPct == 0 {
		cfg.Strategy.SlippageBufferPct = 0.001
	}
	if cfg.Strategy.MinStopDistancePct == 0 {
		cfg.Strategy.MinStopDistancePct = 0.003
	}
	if cfg.Strategy.MaxStopDistancePct == 0 {
		cfg.Strategy.MaxStopDistancePct = 0.012
	}
	if cfg.Strategy.GapUpPct == 0 {
		cfg.Strategy.GapUpPct = 0.008
	}
	if cfg.Strategy.GapDownPct == 0 {
		cfg.Strategy.GapDownPct = -0.003
	}
	if cfg.Strategy.MarketDrawdownLimitPct == 0 {
		cfg.Strategy.MarketDrawdownLimitPct = -0.004
	}
	if cfg.Strategy.LiquidityTopN == 0 {
		cfg.Strategy.LiquidityTopN = 200
	}
	if cfg.Strategy.MinPartialFillPct == 0 {
		cfg.Strategy.MinPartialFillPct = 0.60
	}
	if cfg.Strategy.MaxSpreadPct == 0 {
		cfg.Strategy.MaxSpreadPct = 0.0015
	}
	if strings.TrimSpace(cfg.Strategy.SwingStart) == "" {
		cfg.Strategy.SwingStart = common.DefaultSwingStart
	}
	if strings.TrimSpace(cfg.Strategy.IndexSymbol) == "" {
		cfg.Strategy.IndexSymbol = common.DefaultIndexSymbol
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
	if cfg.Strategy.RiskPerTradePct <= 0 {
		missing = append(missing, "strategy.risk_per_trade_pct(>0)")
	}
	if cfg.Strategy.MaxTradesPerDay < 1 {
		missing = append(missing, "strategy.max_trades_per_day(>=1)")
	}
	if cfg.Strategy.Capital <= 0 {
		missing = append(missing, "strategy.capital(>0)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid required config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
