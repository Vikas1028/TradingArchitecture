package common

const (
	AppName              = "paper_engine"
	AppVersion           = "1.0.0"
	ConfigDirName        = "config"
	ConfigFileName       = "paper_engine_config.json"
	LoggerConfigDirName  = "cmd"
	LoggerConfigFileName = "logger.json"
	LogDirName           = "logs"
	DateFolderLayout     = "2006-01-02"
	LogFileTimeLayout    = "150405"
	DefaultEnv           = "prod"
	DefaultSignalsTopic  = "signals.strategy"
	DefaultCandlesTopic  = "candle.raw"
	DefaultTicksTopic    = "ticks.raw"
	DefaultTradesTopic   = "trades.paper"
	DefaultPnlTopic      = "pnl.paper"
	DefaultCommitMs      = 1000
	DefaultReplayGrace   = 120
	DefaultPriceDivisor  = 1.0
	DefaultTimezone      = "Asia/Kolkata"
	DefaultMTMSnapshot   = 60
	DefaultSignalMaxAge  = 180
	DefaultLogLevel      = "INFO"
	DefaultLogFormat     = "json"
	DefaultMetricsPort   = "9102"
)

const (
	DefaultDirPermission  = 0o755
	DefaultFilePermission = 0o644
)
