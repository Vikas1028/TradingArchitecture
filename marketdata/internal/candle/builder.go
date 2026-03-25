package candle

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"marketdata/common"
)

// Tick represents a single normalized market tick consumed from Kafka "ticks.raw".
// It is parsed upstream from JSON and fed into the CandleBuilder.
type Tick struct {
	Symbol   string    `json:"symbol"`
	Exchange string    `json:"exchange"`
	Time     time.Time `json:"time"`
	Source   string    `json:"source,omitempty"`
	LTP      float64   `json:"ltp"`
	Volume   int64     `json:"volume"`
	Bid      float64   `json:"bid"`
	Ask      float64   `json:"ask"`
}

// Candle represents an OHLCV bar with session VWAP at the candle close.
// It is emitted by CandleBuilder and published to Kafka.
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

// CandleBuilder aggregates Tick events into 1-minute Candle bars and fans them into higher timeframes.
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

// OnTick processes a single Tick and returns any candles closed across all supported timeframes.
func (b *CandleBuilder) OnTick(t Tick) []Candle {
	symbol := strings.ToUpper(t.Symbol)
	tickTime := t.Time.In(b.tz)
	minuteStart := tickTime.Truncate(time.Minute)

	state, ok := b.states[symbol]
	if !ok {
		state = &SymbolState{
			Symbol: symbol,
			Frames: make(map[string]*FrameState, len(common.SupportedTimeframes)-1),
		}
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
			state.Frames = make(map[string]*FrameState, len(common.SupportedTimeframes)-1)
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
		Symbol:    state.Symbol,
		Time:      state.MinuteStart,
		Timeframe: "1m",
		Open:      state.Open,
		High:      state.High,
		Low:       state.Low,
		Close:     state.Close,
		Volume:    state.Volume,
		VWAP:      vwap,
	}

	state.MinuteStart = minuteStart
	state.Open = t.LTP
	state.High = t.LTP
	state.Low = t.LTP
	state.Close = t.LTP
	state.Volume = t.Volume
	state.CumPV += t.LTP * float64(t.Volume)
	state.CumVol += t.Volume

	emitted := []Candle{closed}
	for _, timeframe := range common.SupportedTimeframes {
		if timeframe == "1m" {
			continue
		}
		if next := b.advanceFrame(state, timeframe, closed); next != nil {
			emitted = append(emitted, *next)
		}
	}
	return emitted
}

// FlushAll finalizes and returns currently open candles for all initialized symbols.
func (b *CandleBuilder) FlushAll() []Candle {
	result := make([]Candle, 0, len(b.states)*len(common.SupportedTimeframes))
	for _, state := range b.states {
		if !state.Initialized {
			continue
		}
		var vwap float64
		if state.CumVol > 0 {
			vwap = state.CumPV / float64(state.CumVol)
		}
		result = append(result, Candle{
			Symbol:    state.Symbol,
			Time:      state.MinuteStart,
			Timeframe: "1m",
			Open:      state.Open,
			High:      state.High,
			Low:       state.Low,
			Close:     state.Close,
			Volume:    state.Volume,
			VWAP:      vwap,
		})
		for _, timeframe := range common.SupportedTimeframes {
			if timeframe == "1m" {
				continue
			}
			frame := state.Frames[timeframe]
			if frame == nil || !frame.Initialized {
				continue
			}
			result = append(result, Candle{
				Symbol:    state.Symbol,
				Time:      frame.Start,
				Timeframe: timeframe,
				Open:      frame.Open,
				High:      frame.High,
				Low:       frame.Low,
				Close:     frame.Close,
				Volume:    frame.Volume,
				VWAP:      frame.VWAP,
			})
		}
	}
	return result
}

func (b *CandleBuilder) advanceFrame(state *SymbolState, timeframe string, oneMinute Candle) *Candle {
	frame := state.Frames[timeframe]
	if frame == nil {
		frame = &FrameState{Timeframe: timeframe}
		state.Frames[timeframe] = frame
	}

	bucketStart, err := bucketStart(oneMinute.Time.In(b.tz), timeframe, b.tz)
	if err != nil {
		b.logger.Warn("failed to compute timeframe bucket", zap.String("timeframe", timeframe), zap.Error(err))
		return nil
	}

	if !frame.Initialized {
		initializeFrame(frame, bucketStart, oneMinute)
		return nil
	}
	if frame.Start.Equal(bucketStart) {
		updateFrame(frame, oneMinute)
		return nil
	}

	closed := &Candle{
		Symbol:    oneMinute.Symbol,
		Time:      frame.Start,
		Timeframe: timeframe,
		Open:      frame.Open,
		High:      frame.High,
		Low:       frame.Low,
		Close:     frame.Close,
		Volume:    frame.Volume,
		VWAP:      frame.VWAP,
	}
	initializeFrame(frame, bucketStart, oneMinute)
	return closed
}

func initializeFrame(frame *FrameState, bucketStart time.Time, candle Candle) {
	frame.Start = bucketStart
	frame.Open = candle.Open
	frame.High = candle.High
	frame.Low = candle.Low
	frame.Close = candle.Close
	frame.Volume = candle.Volume
	frame.VWAP = candle.VWAP
	frame.Initialized = true
}

func updateFrame(frame *FrameState, candle Candle) {
	if candle.High > frame.High {
		frame.High = candle.High
	}
	if candle.Low < frame.Low {
		frame.Low = candle.Low
	}
	frame.Close = candle.Close
	frame.Volume += candle.Volume
	frame.VWAP = candle.VWAP
}

func bucketStart(t time.Time, timeframe string, tz *time.Location) (time.Time, error) {
	local := t.In(tz)
	dayStart := time.Date(local.Year(), local.Month(), local.Day(), 9, 15, 0, 0, tz)
	if timeframe == "1d" {
		return dayStart, nil
	}

	duration, err := timeframeDuration(timeframe)
	if err != nil {
		return time.Time{}, err
	}
	if local.Before(dayStart) {
		return dayStart, nil
	}
	elapsed := local.Sub(dayStart)
	return dayStart.Add((elapsed / duration) * duration), nil
}

func timeframeDuration(timeframe string) (time.Duration, error) {
	switch timeframe {
	case "1m":
		return time.Minute, nil
	case "3m":
		return 3 * time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "10m":
		return 10 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "3h":
		return 3 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported timeframe %q", timeframe)
	}
}
