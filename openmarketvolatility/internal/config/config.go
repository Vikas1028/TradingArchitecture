package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"openmarketvolatility/common"
)

type KafkaConfig = common.KafkaConfig
type StrategyConfig = common.StrategyConfig
type ServiceConfig = common.ServiceConfig
type AppConfig = common.AppConfig

func LoadConfig(path string) (*common.AppConfig, error) {
	if strings.TrimSpace(path) == "" {
		resolved, err := common.ResolveConfigPath()
		if err != nil {
			return nil, err
		}
		path = resolved
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
	if strings.TrimSpace(cfg.Kafka.TicksTopic) == "" {
		cfg.Kafka.TicksTopic = common.DefaultTicksTopic
	}
	if strings.TrimSpace(cfg.Kafka.SignalTopic) == "" {
		cfg.Kafka.SignalTopic = common.DefaultSignalTopic
	}
	if cfg.Kafka.CommitIntervalMs <= 0 {
		cfg.Kafka.CommitIntervalMs = common.DefaultCommitIntervalMs
	}
	if cfg.Kafka.StartupReplayGraceSec < 0 {
		cfg.Kafka.StartupReplayGraceSec = common.DefaultReplayGraceSec
	}
	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = common.DefaultTimezone
	}
	if strings.TrimSpace(cfg.Strategy.OpenWindowStart) == "" {
		cfg.Strategy.OpenWindowStart = common.DefaultOpenWindowStart
	}
	if strings.TrimSpace(cfg.Strategy.OpenWindowEnd) == "" {
		cfg.Strategy.OpenWindowEnd = common.DefaultOpenWindowEnd
	}
	if cfg.Strategy.RetraceThresholdPct <= 0 {
		cfg.Strategy.RetraceThresholdPct = common.DefaultRetracePct
	}
	if cfg.Strategy.ReturnTolerancePct <= 0 {
		cfg.Strategy.ReturnTolerancePct = common.DefaultReturnTolerancePct
	}
	if cfg.Strategy.ReversalTriggerPct <= 0 {
		cfg.Strategy.ReversalTriggerPct = common.DefaultReversalTriggerPct
	}
	if strings.TrimSpace(cfg.Strategy.BurstWindowStart) == "" {
		cfg.Strategy.BurstWindowStart = common.DefaultBurstWindowStart
	}
	if strings.TrimSpace(cfg.Strategy.BurstWindowEnd) == "" {
		cfg.Strategy.BurstWindowEnd = common.DefaultBurstWindowEnd
	}
	if cfg.Strategy.BurstTriggerPct <= 0 {
		cfg.Strategy.BurstTriggerPct = common.DefaultBurstTriggerPct
	}
	if cfg.Strategy.BurstConfirmationSec <= 0 {
		cfg.Strategy.BurstConfirmationSec = common.DefaultBurstConfirmSec
	}
	if cfg.Strategy.TwoCandleMinMovePct <= 0 {
		cfg.Strategy.TwoCandleMinMovePct = common.DefaultTwoCandleMinMovePct
	}
	if cfg.Strategy.DefaultStopLossPct <= 0 {
		cfg.Strategy.DefaultStopLossPct = common.DefaultStopLossPct
	}
	if cfg.Strategy.DefaultTargetPct < 0 {
		cfg.Strategy.DefaultTargetPct = common.DefaultTargetPct
	}
	if cfg.Strategy.SourcePriorityStaleSec <= 0 {
		cfg.Strategy.SourcePriorityStaleSec = common.DefaultSourceStaleSeconds
	}
	if cfg.Strategy.S1MaxTradesPerDay <= 0 {
		cfg.Strategy.S1MaxTradesPerDay = common.DefaultS1MaxTradesPerDay
	}
	if cfg.Strategy.S2MaxTradesPerMinute <= 0 {
		cfg.Strategy.S2MaxTradesPerMinute = common.DefaultS2MaxTradesPerMin
	}
	if cfg.Strategy.S3MaxTradesPerDay <= 0 {
		cfg.Strategy.S3MaxTradesPerDay = common.DefaultS3MaxTradesPerDay
	}
	if cfg.Strategy.S4MaxTradesPerDay <= 0 {
		cfg.Strategy.S4MaxTradesPerDay = common.DefaultS4MaxTradesPerDay
	}
	if strings.TrimSpace(cfg.Strategy.S4WindowStart) == "" {
		cfg.Strategy.S4WindowStart = common.DefaultS4WindowStart
	}
	if strings.TrimSpace(cfg.Strategy.S4WindowEnd) == "" {
		cfg.Strategy.S4WindowEnd = common.DefaultS4WindowEnd
	}
	if cfg.Strategy.S4BreakoutPct <= 0 {
		cfg.Strategy.S4BreakoutPct = common.DefaultS4BreakoutPct
	}
	if cfg.Strategy.S4RetestPct <= 0 {
		cfg.Strategy.S4RetestPct = common.DefaultS4RetestPct
	}
	if cfg.Strategy.S4ConfirmPct <= 0 {
		cfg.Strategy.S4ConfirmPct = common.DefaultS4ConfirmPct
	}
	if cfg.Strategy.S4StopLossPct <= 0 {
		cfg.Strategy.S4StopLossPct = common.DefaultS4StopLossPct
	}
	if cfg.Strategy.S4TargetPct <= 0 {
		cfg.Strategy.S4TargetPct = common.DefaultS4TargetPct
	}
	if cfg.Strategy.TrailingStopStepPct <= 0 {
		cfg.Strategy.TrailingStopStepPct = common.DefaultTrailingStopStepPct
	}
	if cfg.Strategy.TrailingFreezeProfitPct <= 0 {
		cfg.Strategy.TrailingFreezeProfitPct = common.DefaultTrailingFreezePct
	}
	if strings.TrimSpace(cfg.Service.MetricsAddress) == "" {
		cfg.Service.MetricsAddress = common.DefaultMetricsPort
		if !strings.HasPrefix(cfg.Service.MetricsAddress, ":") {
			cfg.Service.MetricsAddress = ":" + cfg.Service.MetricsAddress
		}
	}
	if strings.TrimSpace(cfg.Service.DashboardPath) == "" {
		cfg.Service.DashboardPath = common.DefaultDashboardPath
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
	if strings.TrimSpace(cfg.Kafka.TicksTopic) == "" {
		missing = append(missing, "kafka.ticks_topic")
	}
	if strings.TrimSpace(cfg.Kafka.SignalTopic) == "" {
		missing = append(missing, "kafka.signal_topic")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing/invalid config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
