package common

import "time"

type TopicConfig struct {
	Name       string `json:"name"`
	KafkaTopic string `json:"kafka_topic"`
}

type KafkaConfig struct {
	BootstrapServers string        `json:"bootstrap_servers"`
	GroupPrefix      string        `json:"group_prefix"`
	Topics           []TopicConfig `json:"topics"`
}

type ServiceConfig struct {
	BindAddress        string `json:"bind_address"`
	MetricsBindAddress string `json:"metrics_bind_address"`
	MetricsPath        string `json:"metrics_path"`
}

type AppConfig struct {
	Env     string        `json:"env"`
	Kafka   KafkaConfig   `json:"kafka"`
	Service ServiceConfig `json:"service"`
}

type TopicPrice struct {
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

type SymbolPriceRow struct {
	Symbol string                `json:"symbol"`
	Prices map[string]TopicPrice `json:"prices"`
}

type DashboardSnapshot struct {
	StartedAt          time.Time          `json:"started_at"`
	UptimeSeconds      int64              `json:"uptime_seconds"`
	ConnectionStatus   map[string]bool    `json:"connection_status"`
	Subscriptions      []any              `json:"subscriptions"`
	QueueDepth         map[string]int     `json:"queue_depth"`
	Reconnects         map[string]uint64  `json:"reconnects"`
	Metrics            map[string]float64 `json:"metrics,omitempty"`
	System             SystemSummary      `json:"system"`
	MarketPriceSources []string           `json:"market_price_sources"`
	MarketPrices       []SymbolPriceRow   `json:"market_prices"`
}

type SystemSummary struct {
	CPUPercent          float64 `json:"cpu_percent"`
	Goroutines          int     `json:"goroutines"`
	HeapAllocBytes      uint64  `json:"heap_alloc_bytes"`
	SysBytes            uint64  `json:"sys_bytes"`
	ResidentMemoryBytes uint64  `json:"resident_memory_bytes,omitempty"`
}
