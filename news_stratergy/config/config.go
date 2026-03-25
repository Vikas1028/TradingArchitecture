package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"news_strategy/common"
)

// Load reads the JSON config and applies safe defaults for the MVP service.
func Load(path string) (*common.AppConfig, error) {
	if path == "" {
		path = filepath.Join("config", "config.json")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg common.AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Env == "" {
		cfg.Env = common.DefaultEnv
	}
	if cfg.Service.BindAddress == "" {
		cfg.Service.BindAddress = common.DefaultBindAddress
	}
	if cfg.Service.MetricsPath == "" {
		cfg.Service.MetricsPath = common.DefaultMetricsPath
	}
	if cfg.Service.DashboardPath == "" {
		cfg.Service.DashboardPath = common.DefaultDashboardPath
	}
	if cfg.Service.HealthPath == "" {
		cfg.Service.HealthPath = common.DefaultHealthPath
	}
	if cfg.Service.EventsPath == "" {
		cfg.Service.EventsPath = common.DefaultEventsPath
	}
	if cfg.Service.SignalsPath == "" {
		cfg.Service.SignalsPath = common.DefaultSignalsPath
	}
	if cfg.Service.ReactionPath == "" {
		cfg.Service.ReactionPath = common.DefaultReactionPath
	}
	if cfg.Service.AdminIngest == "" {
		cfg.Service.AdminIngest = common.DefaultAdminIngest
	}
	if cfg.Broker.URL == "" {
		cfg.Broker.URL = common.DefaultBrokerURL
	}
	if cfg.Broker.RawSubject == "" {
		cfg.Broker.RawSubject = common.DefaultRawSubject
	}
	if cfg.Broker.TicksSubject == "" {
		cfg.Broker.TicksSubject = common.DefaultTicksSubject
	}
	if cfg.Broker.CandlesSubject == "" {
		cfg.Broker.CandlesSubject = common.DefaultCandlesSubject
	}
	if cfg.Broker.NormalizedSubject == "" {
		cfg.Broker.NormalizedSubject = common.DefaultNormalized
	}
	if cfg.Broker.ClassifiedSubject == "" {
		cfg.Broker.ClassifiedSubject = common.DefaultClassified
	}
	if cfg.Broker.EventsSubject == "" {
		cfg.Broker.EventsSubject = common.DefaultEventsSubject
	}
	if cfg.Broker.SignalsSubject == "" {
		cfg.Broker.SignalsSubject = common.DefaultSignalsSubject
	}
	if cfg.Broker.AlertsSubject == "" {
		cfg.Broker.AlertsSubject = common.DefaultAlertsSubject
	}
	if cfg.Rules.WatchThreshold <= 0 {
		cfg.Rules.WatchThreshold = common.DefaultWatchThreshold
	}
	if cfg.Rules.LongThreshold <= 0 {
		cfg.Rules.LongThreshold = common.DefaultLongThreshold
	}
	if cfg.Rules.ShortThreshold <= 0 {
		cfg.Rules.ShortThreshold = common.DefaultShortThreshold
	}
	if cfg.Symbols.Aliases == nil {
		cfg.Symbols.Aliases = map[string][]string{}
	}
	return &cfg, nil
}
