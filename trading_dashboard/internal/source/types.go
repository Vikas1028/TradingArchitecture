package source

import "time"

type TokenInfo struct {
	Symbol string `json:"symbol"`
	Token  string `json:"token"`
}

type SymbolCount struct {
	Symbol string `json:"symbol"`
	Count  uint64 `json:"count"`
}

type TickSummary struct {
	TotalReceived         uint64        `json:"total_received"`
	PerSecondReceived     uint64        `json:"per_second_received"`
	PerMinuteReceived     uint64        `json:"per_minute_received"`
	LastTickAt            time.Time     `json:"last_tick_at"`
	KafkaInsertedTotal    uint64        `json:"kafka_inserted_total"`
	PostgresInsertedTotal uint64        `json:"postgres_inserted_total"`
	TopSymbols            []SymbolCount `json:"top_symbols"`
}

type ChartPoint struct {
	At    time.Time `json:"at"`
	Value float64   `json:"value"`
}

type SystemSummary struct {
	CPUPercent          float64 `json:"cpu_percent"`
	Goroutines          int     `json:"goroutines"`
	HeapAllocBytes      uint64  `json:"heap_alloc_bytes"`
	SysBytes            uint64  `json:"sys_bytes"`
	ResidentMemoryBytes uint64  `json:"resident_memory_bytes,omitempty"`
}

type TopicPrice struct {
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
}

type SymbolPriceRow struct {
	Symbol string                `json:"symbol"`
	Prices map[string]TopicPrice `json:"prices"`
}

type RunningTradeRow struct {
	Symbol           string    `json:"symbol"`
	Strategy         string    `json:"strategy"`
	Side             string    `json:"side"`
	Quantity         int64     `json:"quantity"`
	EntryPrice       float64   `json:"entry_price"`
	LastPrice        float64   `json:"last_price"`
	InvestedAmount   float64   `json:"invested_amount"`
	UnrealizedPnl    float64   `json:"unrealized_pnl"`
	UnrealizedPnlPct float64   `json:"unrealized_pnl_pct"`
	EntryTime        time.Time `json:"entry_time"`
	LastTickTime     time.Time `json:"last_tick_time"`
}

type DisplayField struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type DetailSection struct {
	Title  string         `json:"title"`
	Fields []DisplayField `json:"fields"`
}

type Snapshot struct {
	StartedAt          time.Time          `json:"started_at"`
	UptimeSeconds      int64              `json:"uptime_seconds"`
	ConnectionStatus   map[string]bool    `json:"connection_status"`
	Subscriptions      []TokenInfo        `json:"subscriptions"`
	QueueDepth         map[string]int     `json:"queue_depth"`
	Ticks              TickSummary        `json:"ticks"`
	Reconnects         map[string]uint64  `json:"reconnects"`
	Metrics            map[string]float64 `json:"metrics,omitempty"`
	System             SystemSummary      `json:"system"`
	SignalsTotal       uint64             `json:"signals_total,omitempty"`
	WindowsClosed      uint64             `json:"windows_closed,omitempty"`
	ActiveWatchers     uint64             `json:"active_watchers,omitempty"`
	MarketPriceSources []string           `json:"market_price_sources,omitempty"`
	MarketPrices       []SymbolPriceRow   `json:"market_prices,omitempty"`
	RunningTrades      []RunningTradeRow  `json:"running_trades,omitempty"`
}

type ServiceState struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	SourceType     string          `json:"source_type"`
	DashboardURL   string          `json:"dashboard_url"`
	StartAllowed   bool            `json:"start_allowed"`
	StopAllowed    bool            `json:"stop_allowed"`
	Running        bool            `json:"running"`
	Snapshot       Snapshot        `json:"snapshot"`
	LastUpdatedAt  time.Time       `json:"last_updated_at"`
	LastError      string          `json:"last_error,omitempty"`
	Healthy        bool            `json:"healthy"`
	SummaryFields  []DisplayField  `json:"summary_fields,omitempty"`
	DetailSections []DetailSection `json:"detail_sections,omitempty"`
	ChartPoints    []ChartPoint    `json:"chart_points,omitempty"`
}

type AllServicesState struct {
	Services []ServiceState `json:"services"`
}
