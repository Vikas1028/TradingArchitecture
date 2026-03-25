package common

const (
	AppName    = "news_strategy"
	AppVersion = "1.0.0"

	DefaultEnv            = "prod"
	DefaultBindAddress    = ":9112"
	DefaultMetricsPath    = "/metrics"
	DefaultDashboardPath  = "/dashboard"
	DefaultHealthPath     = "/health"
	DefaultEventsPath     = "/events/latest"
	DefaultSignalsPath    = "/signals/latest"
	DefaultReactionPath   = "/reaction/"
	DefaultAdminIngest    = "/admin/ingest"
	DefaultBrokerURL      = "nats://127.0.0.1:4222"
	DefaultTicksSubject   = "ticks.raw"
	DefaultCandlesSubject = "candle.raw"
	DefaultRawSubject     = "news.raw"
	DefaultNormalized     = "news.normalized"
	DefaultClassified     = "news.classified"
	DefaultEventsSubject  = "events.stock"
	DefaultSignalsSubject = "signals.news"
	DefaultAlertsSubject  = "alerts.news"
	DefaultWatchThreshold = 55.0
	DefaultLongThreshold  = 75.0
	DefaultShortThreshold = 75.0
	MaxRecentItems        = 100
)
