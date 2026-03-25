package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"market_price/common"
)

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
	if cfg.Service.BindAddress == "" {
		cfg.Service.BindAddress = ":9107"
	}
	if cfg.Service.MetricsBindAddress == "" {
		cfg.Service.MetricsBindAddress = ":9207"
	}
	if cfg.Service.MetricsPath == "" {
		cfg.Service.MetricsPath = "/metrics"
	}
	return &cfg, nil
}
