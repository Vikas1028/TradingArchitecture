package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	BindAddress      string            `json:"bind_address"`
	ServiceName      string            `json:"service_name"`
	RequestTimeoutSec int              `json:"request_timeout_sec"`
	Engines          map[string]string `json:"engines"`
	OptionFeedBaseURL string           `json:"option_feed_base_url"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	applyDefaults(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if strings.TrimSpace(cfg.BindAddress) == "" {
		cfg.BindAddress = ":9117"
	}
	if strings.TrimSpace(cfg.ServiceName) == "" {
		cfg.ServiceName = "chart_view"
	}
	if cfg.RequestTimeoutSec <= 0 {
		cfg.RequestTimeoutSec = 8
	}
	if cfg.Engines == nil {
		cfg.Engines = map[string]string{}
	}
	if strings.TrimSpace(cfg.Engines["paper_engine"]) == "" {
		cfg.Engines["paper_engine"] = "http://127.0.0.1:9102"
	}
}

func validate(cfg *Config) error {
	if strings.TrimSpace(cfg.Engines["paper_engine"]) == "" {
		return fmt.Errorf("engines.paper_engine is required")
	}
	return nil
}
