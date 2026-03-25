package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type ServiceConfig struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	SourceType   string `json:"source_type"`
	DashboardURL string `json:"dashboard_url"`
	StartScript  string `json:"start_script,omitempty"`
	StopScript   string `json:"stop_script,omitempty"`
	LaunchdLabel string `json:"launchd_label,omitempty"`
}

type AppConfig struct {
	BindAddress        string          `json:"bind_address"`
	PageTitle          string          `json:"page_title"`
	RefreshIntervalSec int             `json:"refresh_interval_sec"`
	Services           []ServiceConfig `json:"services"`
}

func Load(path string) (*AppConfig, error) {
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

	return &cfg, nil
}

func applyDefaults(cfg *AppConfig) {
	if strings.TrimSpace(cfg.BindAddress) == "" {
		cfg.BindAddress = ":9200"
	}
	if strings.TrimSpace(cfg.PageTitle) == "" {
		cfg.PageTitle = "Trading Dashboard"
	}
	if cfg.RefreshIntervalSec <= 0 {
		cfg.RefreshIntervalSec = 2
	}
}

func validate(cfg *AppConfig) error {
	if len(cfg.Services) == 0 {
		return fmt.Errorf("at least one service is required")
	}

	seen := make(map[string]bool)
	for _, service := range cfg.Services {
		if strings.TrimSpace(service.ID) == "" {
			return fmt.Errorf("service id is required")
		}
		if strings.TrimSpace(service.Name) == "" {
			return fmt.Errorf("service name is required for %s", service.ID)
		}
		if strings.TrimSpace(service.SourceType) == "" {
			service.SourceType = "go_feed_dashboard"
		}
		if strings.TrimSpace(service.DashboardURL) == "" && strings.TrimSpace(service.SourceType) != "control_only" {
			return fmt.Errorf("dashboard_url is required for %s", service.ID)
		}
		if seen[service.ID] {
			return fmt.Errorf("duplicate service id: %s", service.ID)
		}
		seen[service.ID] = true
	}

	return nil
}
