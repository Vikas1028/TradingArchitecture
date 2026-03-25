package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"marketdata/common"
)

type KafkaConfig = common.KafkaConfig
type SymbolsConfig = common.SymbolsConfig
type AggregationConfig = common.AggregationConfig
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
	if cfg.Kafka.TicksTopic == "" {
		cfg.Kafka.TicksTopic = common.DefaultTicksTopic
	}
	if cfg.Kafka.StockCandlesTopic == "" {
		cfg.Kafka.StockCandlesTopic = common.DefaultStockTopic
	}
	if cfg.Kafka.IndexCandlesTopic == "" {
		cfg.Kafka.IndexCandlesTopic = common.DefaultIndexTopic
	}
	if cfg.Kafka.CommitIntervalMs == 0 {
		cfg.Kafka.CommitIntervalMs = common.DefaultCommitMs
	}
	if cfg.Kafka.StartupReplayGraceSec == 0 {
		cfg.Kafka.StartupReplayGraceSec = common.DefaultReplayGrace
	}
	if cfg.Aggregation.Timezone == "" {
		cfg.Aggregation.Timezone = common.DefaultTimezone
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
	if strings.TrimSpace(cfg.Kafka.StockCandlesTopic) == "" {
		missing = append(missing, "kafka.stock_candles_topic")
	}
	if strings.TrimSpace(cfg.Kafka.IndexCandlesTopic) == "" {
		missing = append(missing, "kafka.index_candles_topic")
	}
	for _, timeframe := range common.SupportedTimeframes {
		if _, ok := common.TimeframePartitions[timeframe]; !ok {
			missing = append(missing, fmt.Sprintf("timeframe_partition.%s", timeframe))
		}
	}
	if cfg.Kafka.StartupReplayGraceSec < 0 {
		missing = append(missing, "kafka.startup_replay_grace_sec(>=0)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config fields: %s", strings.Join(missing, ", "))
	}
	return nil
}
