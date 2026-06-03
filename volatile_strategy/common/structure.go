package common

import "time"

type KafkaConfig struct {
	Brokers               string
	InputTopic            string
	InputGroupID          string
	SignalTopic           string
	SignalPartitions      int
	CommitIntervalMs      int
	StartupReplayGraceSec int
}

type StrategyConfig struct {
	Timezone                 string
	SessionStart             string
	SessionEnd               string
	WindowMinutes            int
	ConfirmMinutes           int
	RetracePct               float64
	TopCount                 int
	MaxTradesPerDay          int
	MaxTradesPerSymbolPerDay int
	SymbolCooldownMinutes    int
}

type ServiceConfig struct {
	MetricsAddress string
	DashboardPath  string
}

type LoggingConfig struct {
	Enable   bool   `json:"enable"`
	Level    string `json:"level"`
	Format   string `json:"format"`
	LogDir   string `json:"log_dir"`
	FileName string `json:"file_name"`
	Console  bool   `json:"console"`
}

type AppConfig struct {
	Kafka    KafkaConfig
	Strategy StrategyConfig
	Service  ServiceConfig
	Logging  LoggingConfig
}

type LTPTick struct {
	Symbol    string
	Price     float64
	Timestamp time.Time
}

type Signal struct {
	Strategy string    `json:"strategy"`
	Symbol   string    `json:"symbol"`
	Side     string    `json:"side"`
	Time     time.Time `json:"time"`
	Reason   string    `json:"reason"`
}

type DashboardSnapshot struct {
	StartedAt      time.Time         `json:"started_at"`
	UptimeSeconds  int64             `json:"uptime_seconds"`
	LastTickAt     *time.Time        `json:"last_tick_at,omitempty"`
	SignalsTotal   uint64            `json:"signals_total"`
	WindowsClosed  uint64            `json:"windows_closed"`
	ActiveWatchers int               `json:"active_watchers"`
	LastSignals    []Signal          `json:"last_signals"`
	CurrentLeaders map[string][]Rank `json:"current_leaders"`
}

type Rank struct {
	Symbol      string  `json:"symbol"`
	Open        float64 `json:"open"`
	Close       float64 `json:"close"`
	ChangePct   float64 `json:"change_pct"`
	WindowStart string  `json:"window_start"`
	WindowEnd   string  `json:"window_end"`
}
