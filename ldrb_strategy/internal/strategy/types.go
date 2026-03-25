package strategy

import "time"

// Candle is a 1-minute OHLCV + VWAP candle consumed from Kafka.
type Candle struct {
	Symbol    string    `json:"symbol"`
	Time      time.Time `json:"time"`
	Timeframe string    `json:"timeframe"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    int64     `json:"volume"`
	VWAP      float64   `json:"vwap"`
}

// SignalSide describes direction for downstream execution.
type SignalSide string

const (
	SideBuy  SignalSide = "BUY"
	SideSell SignalSide = "SELL"
)

// StrategySignal is published to Kafka for execution/recording.
type StrategySignal struct {
	Strategy string     `json:"strategy"`
	Symbol   string     `json:"symbol"`
	Side     SignalSide `json:"side"`
	Time     time.Time  `json:"time"`
	Reason   string     `json:"reason"`
}
