package common

import "time"

type KafkaConfig struct {
	BootstrapServers      string  `json:"bootstrap_servers"`
	GroupID               string  `json:"group_id"`
	SignalsGroupID        string  `json:"signals_group_id"`
	CandlesGroupID        string  `json:"candles_group_id"`
	TicksGroupID          string  `json:"ticks_group_id"`
	SignalsTopic          string  `json:"signals_topic"`
	CandlesTopic          string  `json:"candles_topic"`
	TicksTopic            string  `json:"ticks_topic"`
	TradesTopic           string  `json:"trades_topic"`
	PnlTopic              string  `json:"pnl_topic"`
	CommitIntervalMs      int     `json:"commit_interval_ms"`
	StartupReplayGraceSec int     `json:"startup_replay_grace_sec"`
	PriceScaleDivisor     float64 `json:"price_scale_divisor"`
}

type TradingConfig struct {
	Timezone               string `json:"timezone"`
	EntryStart             string `json:"entry_start"`
	EntryEnd               string `json:"entry_end"`
	EODFlatTime            string `json:"eod_flat_time"`
	MtmSnapshotIntervalSec int    `json:"mtm_snapshot_interval_sec"`
	PendingSignalMaxAgeSec int    `json:"pending_signal_max_age_sec"`
}

type RiskConfig struct {
	CapitalPerTrade   float64 `json:"capital_per_trade"`
	MaxTradesPerDay   int     `json:"max_trades_per_day"`
	MaxOpenPositions  int     `json:"max_open_positions"`
	MaxDailyLoss      float64 `json:"max_daily_loss"`
	PerTradeSLPct     float64 `json:"per_trade_sl_pct"`
	PerTradeTargetPct float64 `json:"per_trade_target_pct"`
}

type AppConfig struct {
	Env     string        `json:"env"`
	Kafka   KafkaConfig   `json:"kafka"`
	Trading TradingConfig `json:"trading"`
	Risk    RiskConfig    `json:"risk"`
}

type LoggerConfig struct {
	Enable   bool   `json:"enable"`
	Level    string `json:"level"`
	Format   string `json:"format"`
	LogDir   string `json:"log_dir"`
	FileName string `json:"file_name"`
	Console  bool   `json:"console"`
}

type LoggerPaths struct {
	BaseDir  string
	DateDir  string
	FileName string
	FilePath string
}

type SystemSummary struct {
	CPUPercent          float64 `json:"cpu_percent"`
	Goroutines          int     `json:"goroutines"`
	HeapAllocBytes      uint64  `json:"heap_alloc_bytes"`
	SysBytes            uint64  `json:"sys_bytes"`
	ResidentMemoryBytes uint64  `json:"resident_memory_bytes,omitempty"`
}

type RunningTradeSnapshot struct {
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

type DashboardSnapshot struct {
	StartedAt        time.Time              `json:"started_at"`
	UptimeSeconds    int64                  `json:"uptime_seconds"`
	ConnectionStatus map[string]bool        `json:"connection_status"`
	QueueDepth       map[string]int         `json:"queue_depth"`
	Reconnects       map[string]uint64      `json:"reconnects"`
	Metrics          map[string]float64     `json:"metrics"`
	System           SystemSummary          `json:"system"`
	RunningTrades    []RunningTradeSnapshot `json:"running_trades,omitempty"`
}
