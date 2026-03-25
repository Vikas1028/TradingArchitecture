package common

const (
	AppName              = "marketdata"
	AppVersion           = "1.0.0"
	ConfigDirName        = "config"
	ConfigFileName       = "marketdata_config.json"
	LoggerConfigDirName  = "cmd"
	LoggerConfigFileName = "logger.json"
	LogDirName           = "logs"
	DateFolderLayout     = "2006-01-02"
	LogFileTimeLayout    = "150405"
	DefaultEnv           = "prod"
	DefaultTicksTopic    = "ticks.raw"
	DefaultStockTopic    = "candle.raw"
	DefaultIndexTopic    = "indices.raw"
	DefaultCommitMs      = 1000
	DefaultReplayGrace   = 120
	DefaultLogLevel      = "INFO"
	DefaultLogFormat     = "json"
	DefaultTimezone      = "Asia/Kolkata"
	DefaultMetricsPort   = "9104"
)

var SupportedTimeframes = []string{"1m", "3m", "5m", "10m", "15m", "30m", "1h", "3h", "1d"}

var TimeframePartitions = map[string]int{
	"1m":  0,
	"3m":  1,
	"5m":  2,
	"10m": 3,
	"15m": 4,
	"30m": 5,
	"1h":  6,
	"3h":  7,
	"1d":  8,
}

const (
	DefaultDirPermission  = 0o755
	DefaultFilePermission = 0o644
)
