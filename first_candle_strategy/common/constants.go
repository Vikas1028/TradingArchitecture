package common

const (
	AppName                 = "first_candle_strategy"
	AppVersion              = "1.0.0"
	ConfigDirName           = "config"
	ConfigFileName          = "first_candle_strategy_config.json"
	LoggerConfigDirName     = "cmd"
	LoggerConfigFileName    = "logger.json"
	LogDirName              = "logs"
	DateFolderLayout        = "2006-01-02"
	LogFileTimeLayout       = "150405"
	LogFileNameSuffix       = "_first_candle_strategy.log"
	DefaultEnv              = "prod"
	DefaultCandlesTopic     = "candle.raw"
	DefaultSignalTopic      = "signals.strategy"
	DefaultCommitIntervalMs = 1000
	DefaultReplayGraceSec   = 120
	DefaultTimezone         = "Asia/Kolkata"
	DefaultSessionStart     = "09:15"
	DefaultOpeningSlot      = 1
	DefaultMoveThresholdPct = 0.5
	DefaultLogLevel         = "INFO"
	DefaultLogFormat        = "json"
	DefaultMetricsPort      = "9103"
)

const (
	DefaultDirPermission  = 0o755
	DefaultFilePermission = 0o644
)
