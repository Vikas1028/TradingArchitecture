package common

type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	TicksTopic            string `json:"ticks_topic"`
	StockCandlesTopic     string `json:"stock_candles_topic"`
	IndexCandlesTopic     string `json:"index_candles_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

type SymbolsConfig struct {
	IndexSymbols []string `json:"index_symbols"`
}

type AggregationConfig struct {
	Timezone        string `json:"timezone"`
	FlushOnShutdown bool   `json:"flush_on_shutdown"`
}

type AppConfig struct {
	Env         string            `json:"env"`
	Kafka       KafkaConfig       `json:"kafka"`
	Symbols     SymbolsConfig     `json:"symbols"`
	Aggregation AggregationConfig `json:"aggregation"`
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
