package common

const (
	AppName              = "ldrb_strategy"
	AppVersion           = "1.0.0"
	ConfigDirName        = "config"
	ConfigFileName       = "ldrb_strategy_config.json"
	LoggerConfigDirName  = "cmd"
	LoggerConfigFileName = "logger.json"
	LogDirName           = "logs"
	DateFolderLayout     = "2006-01-02"
	LogFileTimeLayout    = "150405"
	DefaultEnv           = "prod"
	DefaultStockTopic    = "candle.raw"
	DefaultIndexTopic    = "indices.raw"
	DefaultSignalTopic   = "signals.strategy"
	DefaultCommitMs      = 1000
	DefaultReplayGrace   = 120
	DefaultLogLevel      = "INFO"
	DefaultLogFormat     = "json"
	DefaultTimezone      = "Asia/Kolkata"
	DefaultSessionStart  = "09:15"
	DefaultEntryStart    = "14:30"
	DefaultEntryEnd      = "15:15"
	DefaultSameDayExit   = "15:25"
	DefaultMonitorEnd    = "10:30"
	DefaultSwingStart    = "11:00"
    DefaultIndexSymbol   = "NIFTY"
	DefaultMetricsPort   = "9106"
)

const (
	DefaultDirPermission  = 0o755
	DefaultFilePermission = 0o644
)
