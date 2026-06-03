package common

import "time"

type ServiceConfig struct {
	BindAddress   string `json:"bind_address"`
	MetricsPath   string `json:"metrics_path"`
	DashboardPath string `json:"dashboard_path"`
	HealthPath    string `json:"health_path"`
	EventsPath    string `json:"events_path"`
	SignalsPath   string `json:"signals_path"`
	ReactionPath  string `json:"reaction_path"`
	AdminIngest   string `json:"admin_ingest_path"`
}

type BrokerConfig struct {
	URL               string `json:"url"`
	TicksSubject      string `json:"ticks_subject"`
	CandlesSubject    string `json:"candles_subject"`
	RawSubject        string `json:"raw_subject"`
	NormalizedSubject string `json:"normalized_subject"`
	ClassifiedSubject string `json:"classified_subject"`
	EventsSubject     string `json:"events_subject"`
	SignalsSubject    string `json:"signals_subject"`
	AlertsSubject     string `json:"alerts_subject"`
}

type CollectorConfig struct {
	Enabled             bool   `json:"enabled"`
	IntervalSeconds     int    `json:"interval_seconds"`
	RequestTimeoutSec   int    `json:"request_timeout_sec"`
	LookbackHours       int    `json:"lookback_hours"`
	MaxItemsPerSymbol   int    `json:"max_items_per_symbol"`
	UserAgent           string `json:"user_agent"`
	SearchQuerySuffix   string `json:"search_query_suffix"`
	StartupFetchEnabled bool   `json:"startup_fetch_enabled"`
}

type RulesConfig struct {
	WatchThreshold float64 `json:"watch_threshold"`
	LongThreshold  float64 `json:"long_threshold"`
	ShortThreshold float64 `json:"short_threshold"`
}

type SymbolsConfig struct {
	Aliases map[string][]string `json:"aliases"`
}

type AppConfig struct {
	Env       string          `json:"env"`
	Service   ServiceConfig   `json:"service"`
	Broker    BrokerConfig    `json:"broker"`
	Collector CollectorConfig `json:"collector"`
	Symbols   SymbolsConfig   `json:"symbols"`
	Rules     RulesConfig     `json:"rules"`
}

type RawNewsItem struct {
	Source      string    `json:"source"`
	SourceType  string    `json:"source_type"`
	Title       string    `json:"title"`
	Summary     string    `json:"summary"`
	Body        string    `json:"body"`
	URL         string    `json:"url"`
	PublishedAt time.Time `json:"published_at"`
	CompanyName string    `json:"company_name"`
	Symbol      string    `json:"symbol,omitempty"`
}

type NormalizedNewsItem struct {
	EventID     string    `json:"event_id"`
	Source      string    `json:"source"`
	SourceType  string    `json:"source_type"`
	Title       string    `json:"title"`
	Summary     string    `json:"summary"`
	Body        string    `json:"body"`
	URL         string    `json:"url"`
	PublishedAt time.Time `json:"published_at"`
	CompanyName string    `json:"company_name"`
	Symbol      string    `json:"symbol,omitempty"`
}

type ClassifiedEvent struct {
	EventID     string            `json:"event_id"`
	Symbol      string            `json:"symbol,omitempty"`
	CompanyName string            `json:"company_name"`
	EventType   string            `json:"event_type"`
	Sentiment   string            `json:"sentiment"`
	Severity    int               `json:"severity"`
	Confidence  float64           `json:"confidence"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Title       string            `json:"title"`
	Summary     string            `json:"summary"`
	Source      string            `json:"source"`
	SourceType  string            `json:"source_type"`
	PublishedAt time.Time         `json:"published_at"`
	URL         string            `json:"url"`
}

type EventScore struct {
	EventID            string    `json:"event_id"`
	Symbol             string    `json:"symbol,omitempty"`
	FinalScore         float64   `json:"final_score"`
	SourceReliability  float64   `json:"source_reliability"`
	SeverityScore      float64   `json:"severity_score"`
	ConfidenceScore    float64   `json:"confidence_score"`
	FreshnessScore     float64   `json:"freshness_score"`
	SentimentStrength  float64   `json:"sentiment_strength_score"`
	PriceConfirmation  float64   `json:"price_confirmation_score"`
	VolumeConfirmation float64   `json:"volume_confirmation_score"`
	ComputedAt         time.Time `json:"computed_at"`
}

type MarketReaction struct {
	Symbol             string    `json:"symbol,omitempty"`
	Source             string    `json:"source,omitempty"`
	LastPrice          float64   `json:"last_price"`
	LastTickAt         time.Time `json:"last_tick_at"`
	LastCandleAt       time.Time `json:"last_candle_at"`
	Return1mPct        float64   `json:"return_1m_pct"`
	Return5mPct        float64   `json:"return_5m_pct"`
	Return15mPct       float64   `json:"return_15m_pct"`
	VWAPDistancePct    float64   `json:"vwap_distance_pct"`
	VolumeRatio        float64   `json:"volume_ratio"`
	CandleStrength     float64   `json:"candle_strength"`
	RangeBreak         string    `json:"range_break,omitempty"`
	TechnicalState     string    `json:"technical_state,omitempty"`
	PriceConfirmation  float64   `json:"price_confirmation_score"`
	VolumeConfirmation float64   `json:"volume_confirmation_score"`
}

type EventSignal struct {
	EventID    string    `json:"event_id"`
	Symbol     string    `json:"symbol,omitempty"`
	SignalType string    `json:"signal_type"`
	Status     string    `json:"status"`
	Score      float64   `json:"score"`
	Reason     string    `json:"reason"`
	CreatedAt  time.Time `json:"created_at"`
}

type Alert struct {
	EventID   string    `json:"event_id"`
	Symbol    string    `json:"symbol,omitempty"`
	Signal    string    `json:"signal"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type HealthResponse struct {
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	UptimeSec int64     `json:"uptime_sec"`
}

type DashboardSnapshot struct {
	StartedAt       time.Time         `json:"started_at"`
	UptimeSeconds   int64             `json:"uptime_seconds"`
	BrokerConnected bool              `json:"broker_connected"`
	RawCount        int               `json:"raw_count"`
	NormalizedCount int               `json:"normalized_count"`
	ClassifiedCount int               `json:"classified_count"`
	MappedCount     int               `json:"mapped_count"`
	SignalsCount    int               `json:"signals_count"`
	LastEvents      []ClassifiedEvent `json:"last_events"`
	LastSignals     []EventSignal     `json:"last_signals"`
	LastAlert       *Alert            `json:"last_alert,omitempty"`
	LastReaction    *MarketReaction   `json:"last_reaction,omitempty"`
}
