package common

type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	StockCandlesTopic     string `json:"stock_candles_topic"`
	IndexCandlesTopic     string `json:"index_candles_topic"`
	SignalTopic           string `json:"signal_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

type StrategyConfig struct {
	Timezone            string  `json:"timezone"`
	EntryStart          string  `json:"entry_start"`
	EntryEnd            string  `json:"entry_end"`
	TrendLookback       int     `json:"trend_lookback"`
	PullbackWindow      int     `json:"pullback_window"`
	MinBodyPct          float64 `json:"min_body_pct"`
	MaxPullbackPct      float64 `json:"max_pullback_pct"`
	EnableIndexBias     bool    `json:"enable_index_bias"`
	IndexBiasLookback   int     `json:"index_bias_lookback"`
	IndexBiasVWAPThresh float64 `json:"index_bias_vwap_threshold"`
}

type AppConfig struct {
	Env      string         `json:"env"`
	Kafka    KafkaConfig    `json:"kafka"`
	Strategy StrategyConfig `json:"strategy"`
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
