package config

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"volatile_strategy/common"
	appLogger "volatile_strategy/logger"
)

var (
	GlobalConfig *common.AppConfig
	once         sync.Once
	initErr      error
)

func InitializeConfig() error {
	once.Do(func() {
		GlobalConfig, initErr = loadConfig()
		if initErr == nil {
			initErr = loadLoggerConfig(&GlobalConfig.Logging)
		}
		if initErr == nil {
			applyDefaults(GlobalConfig)
		}
	})
	return initErr
}

func loadConfig() (*common.AppConfig, error) {
	filePath, err := common.ResolveConfigPath()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	cfg := &common.AppConfig{}
	scanner := bufio.NewScanner(file)
	section := ""
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.ToLower(strings.TrimSpace(line[1 : len(line)-1]))
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid config line: %s", line)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if err := assignConfigValue(cfg, section, key, value); err != nil {
			return nil, err
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func assignConfigValue(cfg *common.AppConfig, section, key, value string) error {
	switch section {
	case common.ConfigSectionKafka:
		return assignKafkaValue(&cfg.Kafka, key, value)
	case common.ConfigSectionStrategy:
		return assignStrategyValue(&cfg.Strategy, key, value)
	case common.ConfigSectionService:
		return assignServiceValue(&cfg.Service, key, value)
	default:
		return fmt.Errorf("unknown config section: %s", section)
	}
}

func assignKafkaValue(cfg *common.KafkaConfig, key, value string) error {
	switch key {
	case "brokers":
		cfg.Brokers = value
	case "input_topic":
		cfg.InputTopic = value
	case "input_group_id":
		cfg.InputGroupID = value
	case "signal_topic":
		cfg.SignalTopic = value
	case "signal_partitions":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.SignalPartitions = v
	case "commit_interval_ms":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.CommitIntervalMs = v
	case "startup_replay_grace_sec":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.StartupReplayGraceSec = v
	default:
		return fmt.Errorf("unknown kafka key: %s", key)
	}
	return nil
}

func assignStrategyValue(cfg *common.StrategyConfig, key, value string) error {
	switch key {
	case "timezone":
		cfg.Timezone = value
	case "session_start":
		cfg.SessionStart = value
	case "session_end":
		cfg.SessionEnd = value
	case "window_minutes":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.WindowMinutes = v
	case "confirm_minutes":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.ConfirmMinutes = v
	case "retrace_pct":
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		cfg.RetracePct = v
	case "top_count":
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		cfg.TopCount = v
	default:
		return fmt.Errorf("unknown strategy key: %s", key)
	}
	return nil
}

func assignServiceValue(cfg *common.ServiceConfig, key, value string) error {
	switch key {
	case "metrics_address":
		cfg.MetricsAddress = value
	case "dashboard_path":
		cfg.DashboardPath = value
	default:
		return fmt.Errorf("unknown service key: %s", key)
	}
	return nil
}

func loadLoggerConfig(cfg *common.LoggingConfig) error {
	filePath, err := common.ResolveLoggerConfigPath()
	if err != nil {
		return err
	}
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewDecoder(file).Decode(cfg)
}

func applyDefaults(cfg *common.AppConfig) {
	if strings.TrimSpace(cfg.Kafka.InputTopic) == "" {
		cfg.Kafka.InputTopic = "go_ltp"
	}
	if strings.TrimSpace(cfg.Kafka.SignalTopic) == "" {
		cfg.Kafka.SignalTopic = "signals.strategy"
	}
	if strings.TrimSpace(cfg.Kafka.InputGroupID) == "" {
		cfg.Kafka.InputGroupID = "volatile_strategy"
	}
	if cfg.Kafka.SignalPartitions <= 0 {
		cfg.Kafka.SignalPartitions = 3
	}
	if cfg.Kafka.CommitIntervalMs <= 0 {
		cfg.Kafka.CommitIntervalMs = 1000
	}
	if cfg.Kafka.StartupReplayGraceSec < 0 {
		cfg.Kafka.StartupReplayGraceSec = common.DefaultStartupReplayGraceSec
	}
	if strings.TrimSpace(cfg.Strategy.Timezone) == "" {
		cfg.Strategy.Timezone = common.DefaultTimezone
	}
	if strings.TrimSpace(cfg.Strategy.SessionStart) == "" {
		cfg.Strategy.SessionStart = common.DefaultSessionStart
	}
	if strings.TrimSpace(cfg.Strategy.SessionEnd) == "" {
		cfg.Strategy.SessionEnd = common.DefaultSessionEnd
	}
	if cfg.Strategy.WindowMinutes <= 0 {
		cfg.Strategy.WindowMinutes = common.DefaultWindowMinutes
	}
	if cfg.Strategy.ConfirmMinutes <= 0 {
		cfg.Strategy.ConfirmMinutes = common.DefaultConfirmMinutes
	}
	if cfg.Strategy.RetracePct <= 0 {
		cfg.Strategy.RetracePct = common.DefaultRetracePct
	}
	if cfg.Strategy.TopCount <= 0 {
		cfg.Strategy.TopCount = common.DefaultTopCount
	}
	if strings.TrimSpace(cfg.Service.MetricsAddress) == "" {
		cfg.Service.MetricsAddress = common.DefaultMetricsAddress
	}
	if strings.TrimSpace(cfg.Service.DashboardPath) == "" {
		cfg.Service.DashboardPath = common.DefaultDashboardPath
	}
}

func MustConfig() *common.AppConfig {
	if GlobalConfig == nil {
		appLogger.Fatalf("config not initialized")
	}
	return GlobalConfig
}
