package common

const (
	AppName              = "volatile_strategy"
	AppVersion           = "1.0.0"
	ConfigDirName        = "cmd"
	ConfigFileName       = "config.cfg"
	LoggerConfigFileName = "logger.json"
	LogDirName           = "logs"
	DateFolderLayout     = "2006-01-02"
	LogFileTimeLayout    = "150405"
	LogFileNameSuffix    = "_volatile_strategy.log"
	MetricsPath          = "/metrics"
	PanicLogFileName     = "panic_stacktrace.log"

	ConfigSectionKafka    = "kafka"
	ConfigSectionStrategy = "strategy"
	ConfigSectionService  = "service"

	DefaultMetricsAddress        = ":9108"
	DefaultDashboardPath         = "/dashboard"
	DefaultTimezone              = "Asia/Kolkata"
	DefaultSessionStart          = "09:15"
	DefaultSessionEnd            = "15:30"
	DefaultWindowMinutes         = 5
	DefaultConfirmMinutes        = 3
	DefaultRetracePct            = 0.20
	DefaultTopCount              = 3
	DefaultStartupReplayGraceSec = 120
	DefaultDirPermission         = 0o755
	DefaultFilePermission        = 0o644
)
