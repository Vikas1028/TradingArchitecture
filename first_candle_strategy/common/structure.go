package common

type KafkaConfig struct {
	BootstrapServers      string `json:"bootstrap_servers"`
	GroupID               string `json:"group_id"`
	CandlesTopic          string `json:"candles_topic"`
	SignalTopic           string `json:"signal_topic"`
	CommitIntervalMs      int    `json:"commit_interval_ms"`
	StartupReplayGraceSec int    `json:"startup_replay_grace_sec"`
}

type StrategyConfig struct {
	Timezone          string  `json:"timezone"`
	SessionStart      string  `json:"session_start"`
	OpeningCandleSlot int     `json:"opening_candle_slot"`
	MoveThresholdPct  float64 `json:"move_threshold_pct"`
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
