package common

import "time"

// DhanClientConfig holds broker client credentials and subscription defaults.
type DhanClientConfig struct {
	ClientID        string
	AccessToken     string
	APIEnv          string
	BatchSize       int
	ExchangeSegment string
}

// DhanWebsocketConfig holds websocket connection and reconnect settings.
type DhanWebsocketConfig struct {
	URL                  string
	Version              int
	AuthType             int
	FeedMode             string
	Reconnect            bool
	ReconnectIntervalSec int
	MaxReconnectAttempts int
	PingIntervalSec      int
	ReadTimeoutSec       int
	WriteTimeoutSec      int
}

// KafkaConfig holds Kafka connectivity settings used by the application.
type KafkaConfig struct {
	Brokers string
	Topic   string
	GroupID string
}

// PostgresConfig holds PostgreSQL connectivity settings used by the application.
type PostgresConfig struct {
	Host      string
	Port      int
	User      string
	Password  string
	DBName    string
	SSLMode   string
	TableName string
	MaxConns  int32
	MinConns  int32
}

// LoggingConfig holds runtime log output settings from logger.json.
type LoggingConfig struct {
	Enable   bool   `json:"enable"`
	Level    string `json:"level"`
	Format   string `json:"format"`
	LogDir   string `json:"log_dir"`
	FileName string `json:"file_name"`
	Console  bool   `json:"console"`
}

// SubscriptionConfig holds requested basket subscription information for startup.
type SubscriptionConfig struct {
	Group       string
	RequestCode int
}

// PipelineConfig holds queue sizing and worker counts for downstream sinks.
type PipelineConfig struct {
	KafkaWorkers            int
	PostgresWorkers         int
	KafkaQueueSize          int
	PostgresQueueSize       int
	ShutdownDrainTimeoutSec int
}

// ServiceConfig holds generic runtime settings like metrics binding.
type ServiceConfig struct {
	MetricsAddress string
	DashboardPath  string
}

// AppConfig stores the complete application configuration loaded from config files.
type AppConfig struct {
	DhanClient    DhanClientConfig
	DhanWebsocket DhanWebsocketConfig
	Kafka         KafkaConfig
	Postgres      PostgresConfig
	Logging       LoggingConfig
	Subscription  SubscriptionConfig
	Pipeline      PipelineConfig
	Service       ServiceConfig
}

// TokenInfo represents one tradable symbol and its broker token identifier.
type TokenInfo struct {
	Symbol string
	Token  string
}

// SubscriptionTokens groups token lists by supported Nifty basket sizes.
type SubscriptionTokens struct {
	Nifty50  []TokenInfo
	Nifty100 []TokenInfo
	Nifty200 []TokenInfo
	Nifty300 []TokenInfo
	Nifty400 []TokenInfo
	Nifty500 []TokenInfo
}

// WebsocketSession stores login output needed to open the broker websocket connection.
type WebsocketSession struct {
	FeedURL     string
	ClientID    string
	AccessToken string
}

// LoggerPaths groups resolved log directory and file paths for one application run.
type LoggerPaths struct {
	BaseDir  string
	DateDir  string
	FileName string
	FilePath string
}

// RuntimeChannels holds application-wide channels used during startup and shutdown flow.
type RuntimeChannels struct {
	SignalQuitChan chan struct{}
	ReadLoopDone   chan struct{}
}

// MarketDepthLevel represents one bid/ask level from the Dhan full packet.
type MarketDepthLevel struct {
	BidQuantity int32   `json:"bid_quantity"`
	AskQuantity int32   `json:"ask_quantity"`
	BidOrders   int16   `json:"bid_orders"`
	AskOrders   int16   `json:"ask_orders"`
	BidPrice    float32 `json:"bid_price"`
	AskPrice    float32 `json:"ask_price"`
}

// DhanTick holds one parsed websocket packet in normalized form.
type DhanTick struct {
	ResponseCode      uint8              `json:"response_code"`
	MessageLength     int16              `json:"message_length"`
	ExchangeSegment   uint8              `json:"exchange_segment"`
	SecurityID        string             `json:"security_id"`
	Symbol            string             `json:"symbol"`
	FeedTimestamp     time.Time          `json:"feed_timestamp"`
	LastTradedPrice   float32            `json:"ltp"`
	LastTradeQuantity int16              `json:"last_trade_quantity"`
	LastTradeTime     int32              `json:"last_trade_time"`
	AverageTradePrice float32            `json:"atp"`
	Volume            int32              `json:"volume"`
	TotalSellQuantity int32              `json:"total_sell_quantity"`
	TotalBuyQuantity  int32              `json:"total_buy_quantity"`
	OpenInterest      int32              `json:"open_interest"`
	HighestOI         int32              `json:"highest_open_interest"`
	LowestOI          int32              `json:"lowest_open_interest"`
	DayOpen           float32            `json:"day_open"`
	DayClose          float32            `json:"day_close"`
	DayHigh           float32            `json:"day_high"`
	DayLow            float32            `json:"day_low"`
	Depth             []MarketDepthLevel `json:"depth,omitempty"`
	RawPayload        []byte             `json:"raw_payload,omitempty"`
}

// KafkaTickMessage holds the small packet persisted to Kafka.
type KafkaTickMessage struct {
	SecurityID        string    `json:"security_id"`
	Symbol            string    `json:"symbol"`
	ExchangeSegment   uint8     `json:"exchange_segment"`
	Timestamp         time.Time `json:"timestamp"`
	LastTradedPrice   float32   `json:"ltp"`
	Volume            int32     `json:"volume"`
	TotalSellQuantity int32     `json:"total_sell_quantity"`
	TotalBuyQuantity  int32     `json:"total_buy_quantity"`
}
