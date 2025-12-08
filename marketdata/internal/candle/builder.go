package candle

import (
	"strings"
	"time"

	"go.uber.org/zap"
)

// Tick represents a single normalized market tick consumed from Kafka "ticks.raw".
// It is parsed upstream from JSON and fed into the CandleBuilder.
type Tick struct {
	Symbol   string    `json:"symbol"`
	Exchange string    `json:"exchange"`
	Time     time.Time `json:"time"`
	LTP      float64   `json:"ltp"`
	Volume   int64     `json:"volume"`
	Bid      float64   `json:"bid"`
	Ask      float64   `json:"ask"`
}

// Candle represents a 1-minute OHLCV bar with session VWAP.
// It is emitted by CandleBuilder and published to Kafka.
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

// CandleBuilder aggregates Tick events into 1-minute Candle bars and maintains per-session VWAP.
// Inputs: ticks in arrival order; outputs: closed candles on minute roll, with day-based VWAP reset.
type CandleBuilder struct {
	states map[string]*SymbolState
	tz     *time.Location
	logger *zap.Logger
}

// NewCandleBuilder constructs a CandleBuilder for the given IANA timezone (e.g. "Asia/Kolkata").
// Inputs: tzName string, logger.
// Outputs: initialized CandleBuilder or error if timezone cannot be loaded.
// Flow: load timezone, create state map.
func NewCandleBuilder(tzName string, logger *zap.Logger) (*CandleBuilder, error) {
	loc, err := time.LoadLocation(tzName)
	if err != nil {
		return nil, err
	}
	return &CandleBuilder{
		states: make(map[string]*SymbolState),
		tz:     loc,
		logger: logger,
	}, nil
}

// OnTick processes a single Tick, updates symbol state, and returns closed candles when a minute rolls.
// Inputs: Tick (assumed valid), normalized to configured timezone inside this method.
// Outputs: slice of Candle that just closed (0 or 1 per call).
// Flow: normalize symbol uppercase; convert time to tz; reset state on day change; roll minute and emit candle.
func (b *CandleBuilder) OnTick(t Tick) []Candle {
	symbol := strings.ToUpper(t.Symbol)
	tickTime := t.Time.In(b.tz)
	minuteStart := tickTime.Truncate(time.Minute)

	state, ok := b.states[symbol]
	if !ok {
		state = &SymbolState{Symbol: symbol}
		b.states[symbol] = state
	}

	// Day change -> reset session VWAP and minute state.
	if state.Initialized {
		y1, m1, d1 := state.MinuteStart.Date()
		y2, m2, d2 := minuteStart.Date()
		if y1 != y2 || m1 != m2 || d1 != d2 {
			state.MinuteStart = minuteStart
			state.Open = t.LTP
			state.High = t.LTP
			state.Low = t.LTP
			state.Close = t.LTP
			state.Volume = t.Volume
			state.CumPV = t.LTP * float64(t.Volume)
			state.CumVol = t.Volume
			state.Initialized = true
			return nil
		}
	}

	if !state.Initialized {
		state.MinuteStart = minuteStart
		state.Open = t.LTP
		state.High = t.LTP
		state.Low = t.LTP
		state.Close = t.LTP
		state.Volume = t.Volume
		state.CumPV = t.LTP * float64(t.Volume)
		state.CumVol = t.Volume
		state.Initialized = true
		return nil
	}

	// Same minute
	if minuteStart.Equal(state.MinuteStart) {
		if t.LTP > state.High {
			state.High = t.LTP
		}
		if t.LTP < state.Low {
			state.Low = t.LTP
		}
		state.Close = t.LTP
		state.Volume += t.Volume
		state.CumPV += t.LTP * float64(t.Volume)
		state.CumVol += t.Volume
		return nil
	}

	// Minute rolled: emit previous candle, start new one.
	var vwap float64
	if state.CumVol > 0 {
		vwap = state.CumPV / float64(state.CumVol)
	}
	closed := Candle{
		Symbol: state.Symbol,
		Time:   state.MinuteStart,
		Open:   state.Open,
		High:   state.High,
		Low:    state.Low,
		Close:  state.Close,
		Volume: state.Volume,
		VWAP:   vwap,
	}

	state.MinuteStart = minuteStart
	state.Open = t.LTP
	state.High = t.LTP
	state.Low = t.LTP
	state.Close = t.LTP
	state.Volume = t.Volume
	state.CumPV += t.LTP * float64(t.Volume)
	state.CumVol += t.Volume

	return []Candle{closed}
}

// FlushAll finalizes and returns candles for all initialized symbols.
// Inputs: none; Outputs: slice of Candle (may be empty).
// Flow: compute VWAP from cumulative values and emit current minute state without advancing time.
func (b *CandleBuilder) FlushAll() []Candle {
	result := make([]Candle, 0, len(b.states))
	for _, state := range b.states {
		if !state.Initialized {
			continue
		}
		var vwap float64
		if state.CumVol > 0 {
			vwap = state.CumPV / float64(state.CumVol)
		}
		result = append(result, Candle{
			Symbol: state.Symbol,
			Time:   state.MinuteStart,
			Open:   state.Open,
			High:   state.High,
			Low:    state.Low,
			Close:  state.Close,
			Volume: state.Volume,
			VWAP:   vwap,
		})
	}
	return result
}
