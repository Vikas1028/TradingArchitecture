package common

const (
	AppName                  = "real_engine"
	AppVersion               = "1.0.0"
	ConfigDirName            = "config"
	ConfigFileName           = "real_engine_config.json"
	LoggerConfigDirName      = "cmd"
	LoggerConfigFileName     = "logger.json"
	LogDirName               = "logs"
	DateFolderLayout         = "2006-01-02"
	LogFileTimeLayout        = "150405"
	DefaultEnv               = "prod"
	DefaultSignalsTopic      = "signals.real"
	DefaultCandlesTopic      = "candle.raw"
	DefaultTicksTopic        = "ticks.raw"
	DefaultTradesTopic       = "trades.real"
	DefaultPnlTopic          = "pnl.real"
	DefaultCommitMs          = 1000
	DefaultReplayGrace       = 300
	DefaultPriceDivisor      = 1.0
	DefaultTimezone          = "Asia/Kolkata"
	DefaultMTMSnapshot       = 60
	DefaultSignalMaxAge      = 180
	DefaultLogLevel          = "INFO"
	DefaultLogFormat         = "json"
	DefaultMetricsPort       = "9103"
	DefaultPGHost            = "localhost"
	DefaultPGPort            = 5432
	DefaultPGSSLMode         = "disable"
	DefaultPGTableName       = "real_engine_trades"
	DefaultPGMaxConns        = 8
	DefaultPGMinConns        = 1
	DefaultDhanBaseURL       = "https://api.dhan.co"
	DefaultExchange          = "NSE_EQ"
	DefaultProductType       = "INTRADAY"
	DefaultOrderType         = "MARKET"
	DefaultOrderValidity     = "DAY"
	DefaultInstrumentsCSV    = "./stocks/nifty500.csv"
	DefaultRequestTimeoutSec = 10
)

const (
	DefaultDirPermission  = 0o755
	DefaultFilePermission = 0o644
)
