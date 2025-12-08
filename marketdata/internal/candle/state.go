package candle

import "time"

// SymbolState holds the rolling aggregation state for a single symbol and 1-minute candles,
// including cumulative values for per-session VWAP.
// It is maintained per symbol by CandleBuilder.
type SymbolState struct {
	Symbol      string
	MinuteStart time.Time

	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume int64

	CumPV  float64
	CumVol int64

	Initialized bool
}
