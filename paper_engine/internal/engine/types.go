package engine

import "time"

// SignalSide describes the direction of a signal (v1 long-only).
type SignalSide string

const (
	SideBuy  SignalSide = "BUY"
	SideSell SignalSide = "SELL"
)

// StrategySignal represents an incoming trading signal from a strategy service.
type StrategySignal struct {
	Strategy string     `json:"strategy"`
	Symbol   string     `json:"symbol"`
	Side     SignalSide `json:"side"`
	Time     time.Time  `json:"time"`
	Reason   string     `json:"reason"`
}

// PendingSignal tracks a queued signal waiting for a usable pricing candle.
type PendingSignal struct {
	Signal   StrategySignal `json:"signal"`
	QueuedAt time.Time      `json:"queued_at"`
}

// Candle represents a 1-minute OHLCV bar used for paper execution and MTM.
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

type Tick struct {
	Symbol string    `json:"symbol"`
	Time   time.Time `json:"time"`
	LTP    float64   `json:"ltp"`
}

// PositionSide describes the side of an open position.
type PositionSide string

const (
	SideLong PositionSide = "LONG"
	SideShort PositionSide = "SHORT"
)

// Position represents a single-symbol intraday position (v1: LONG-only).
type Position struct {
	Symbol     string       `json:"symbol"`
	Side       PositionSide `json:"side"`
	Quantity   int64        `json:"quantity"`
	AvgEntry   float64      `json:"avg_entry"`
	EntryTime  time.Time    `json:"entry_time"`
	Strategy   string       `json:"strategy"`
	LastCandle *Candle      `json:"-"`
}

// TradeType classifies whether the trade is an entry or exit.
type TradeType string

const (
	TradeEntry TradeType = "ENTRY"
	TradeExit  TradeType = "EXIT"
)

// TradeEvent represents a simulated trade execution (paper).
type TradeEvent struct {
	Symbol      string    `json:"symbol"`
	Time        time.Time `json:"time"`
	Price       float64   `json:"price"`
	Quantity    int64     `json:"quantity"`
	TradeType   TradeType `json:"trade_type"`
	Side        string    `json:"side"`
	Strategy    string    `json:"strategy"`
	SignalTime  time.Time `json:"signal_time"`
	Reason      string    `json:"reason"`
	RealizedPnl float64   `json:"realized_pnl"`
}

// PnlSnapshot represents a periodic snapshot of daily PnL and risk state.
type PnlSnapshot struct {
	Time              time.Time `json:"time"`
	RealizedPnl       float64   `json:"realized_pnl"`
	UnrealizedPnl     float64   `json:"unrealized_pnl"`
	OpenPositions     int       `json:"open_positions"`
	TradesToday       int       `json:"trades_today"`
	MaxDrawdown       float64   `json:"max_drawdown"`
	MaxDailyLossLimit float64   `json:"max_daily_loss_limit"`
	TradingHalted     bool      `json:"trading_halted"`
}

type RunningTrade struct {
	Symbol        string    `json:"symbol"`
	Strategy      string    `json:"strategy"`
	Side          string    `json:"side"`
	Quantity      int64     `json:"quantity"`
	EntryPrice    float64   `json:"entry_price"`
	LastPrice     float64   `json:"last_price"`
	UnrealizedPnl float64   `json:"unrealized_pnl"`
	EntryTime     time.Time `json:"entry_time"`
	LastTickTime  time.Time `json:"last_tick_time"`
}
