package engine

import "time"

type SignalSide string

const (
	SideBuy  SignalSide = "BUY"
	SideSell SignalSide = "SELL"
)

type StrategySignal struct {
	SignalID                string     `json:"signal_id"`
	Strategy                string     `json:"strategy"`
	Symbol                  string     `json:"symbol"`
	Side                    SignalSide `json:"side"`
	Time                    time.Time  `json:"time"`
	Reason                  string     `json:"reason"`
	Quantity                int64      `json:"quantity"`
	StopLossPct             float64    `json:"stop_loss_pct"`
	TargetPct               float64    `json:"target_pct"`
	TrailingStopPct         float64    `json:"trailing_stop_pct"`
	TrailingFreezeProfitPct float64    `json:"trailing_freeze_profit_pct"`
}

type PendingSignal struct {
	Signal   StrategySignal `json:"signal"`
	QueuedAt time.Time      `json:"queued_at"`
}

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

type PositionSide string

const (
	SideLong  PositionSide = "LONG"
	SideShort PositionSide = "SHORT"
)

type Position struct {
	Symbol                  string       `json:"symbol"`
	Side                    PositionSide `json:"side"`
	Quantity                int64        `json:"quantity"`
	AvgEntry                float64      `json:"avg_entry"`
	EntryTime               time.Time    `json:"entry_time"`
	Strategy                string       `json:"strategy"`
	StopLossPct             float64      `json:"stop_loss_pct"`
	TargetPct               float64      `json:"target_pct"`
	TrailingStopPct         float64      `json:"trailing_stop_pct"`
	TrailingFreezeProfitPct float64      `json:"trailing_freeze_profit_pct"`
	PeakProfitPct           float64      `json:"peak_profit_pct"`
}

type TradeType string

const (
	TradeEntry TradeType = "ENTRY"
	TradeExit  TradeType = "EXIT"
)

type TradeEvent struct {
	EventKey                string    `json:"event_key,omitempty"`
	Symbol                  string    `json:"symbol"`
	Time                    time.Time `json:"time"`
	Price                   float64   `json:"price"`
	Quantity                int64     `json:"quantity"`
	TradeType               TradeType `json:"trade_type"`
	Side                    string    `json:"side"`
	Strategy                string    `json:"strategy"`
	SignalTime              time.Time `json:"signal_time"`
	Reason                  string    `json:"reason"`
	StopLossPct             float64   `json:"stop_loss_pct,omitempty"`
	TargetPct               float64   `json:"target_pct,omitempty"`
	TrailingStopPct         float64   `json:"trailing_stop_pct,omitempty"`
	TrailingFreezeProfitPct float64   `json:"trailing_freeze_profit_pct,omitempty"`
	RealizedPnl             float64   `json:"realized_pnl,omitempty"`
}

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
	Symbol                  string    `json:"symbol"`
	Strategy                string    `json:"strategy"`
	Side                    string    `json:"side"`
	Quantity                int64     `json:"quantity"`
	EntryPrice              float64   `json:"entry_price"`
	LastPrice               float64   `json:"last_price"`
	UnrealizedPnl           float64   `json:"unrealized_pnl"`
	EntryTime               time.Time `json:"entry_time"`
	LastTickTime            time.Time `json:"last_tick_time"`
	StopLossPct             float64   `json:"stop_loss_pct"`
	TargetPct               float64   `json:"target_pct"`
	TrailingStopPct         float64   `json:"trailing_stop_pct"`
	TrailingFreezeProfitPct float64   `json:"trailing_freeze_profit_pct"`
}
