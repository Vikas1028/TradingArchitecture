package common

const (
	AppName                  = "vwap_strategy"
	AppVersion               = "1.0.0"
	ConfigDirName            = "config"
	ConfigFileName           = "vwap_strategy_config.json"
	LoggerConfigDirName      = "cmd"
	LoggerConfigFileName     = "logger.json"
	LogDirName               = "logs"
	DateFolderLayout         = "2006-01-02"
	LogFileTimeLayout        = "150405"
	LogFileNameSuffix        = "_vwap_strategy.log"
	DefaultEnv               = "prod"
	DefaultStockCandlesTopic = "candle.raw"
	DefaultIndexCandlesTopic = "indices.raw"
	DefaultSignalTopic       = "signals.strategy"
	DefaultCommitIntervalMs  = 1000
	DefaultReplayGraceSec    = 120
	DefaultTimezone          = "Asia/Kolkata"
	DefaultLogLevel          = "INFO"
	DefaultLogFormat         = "json"
	DefaultMetricsPort       = "9101"
)

const (
	DefaultDirPermission  = 0o755
	DefaultFilePermission = 0o644
)
