package common

const (
	AppName              = "marketdata_go_feed"
	AppVersion           = "1.0.0"
	ConfigDirName        = "config"
	ConfigFileName       = "marketdata_go_feed_config.json"
	LoggerConfigDirName  = "cmd"
	LoggerConfigFileName = "logger.json"
	LogDirName           = "logs"
	DateFolderLayout     = "2006-01-02"
	LogFileTimeLayout    = "150405"
	DefaultEnv           = "prod"
	DefaultTicksTopic    = "go_feed.raw"
	DefaultStockTopic    = "candles.1m"
	DefaultIndexTopic    = "indices.1m"
	DefaultCommitMs      = 1000
	DefaultReplayGrace   = 120
	DefaultLogLevel      = "INFO"
	DefaultLogFormat     = "json"
	DefaultTimezone      = "Asia/Kolkata"
)

const (
	DefaultDirPermission  = 0o755
	DefaultFilePermission = 0o644
)
