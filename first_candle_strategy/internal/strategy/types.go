package strategy

import "time"

// Candle represents a 1-minute OHLCV bar consumed from Kafka.
// It is the input unit for the first-candle reversal detector.
type Candle struct {
	Symbol string    `json:"symbol"`
	Time   time.Time `json:"time"`
	Open   float64   `json:"open"`
	High   float64   `json:"high"`
	Low    float64   `json:"low"`
	Close  float64   `json:"close"`
	Volume int64     `json:"volume"`
	VWAP   float64   `json:"vwap"`
}

// SignalSide describes the direction of a strategy signal.
type SignalSide string

const (
	// SideBuy represents a long-entry signal.
	SideBuy SignalSide = "BUY"
)

// StrategySignal represents a trading signal emitted by the strategy service.
// It deliberately omits quantity and execution-specific fields.
type StrategySignal struct {
	Strategy string     `json:"strategy"`
	Symbol   string     `json:"symbol"`
	Side     SignalSide `json:"side"`
	Time     time.Time  `json:"time"`
	Reason   string     `json:"reason"`
}
