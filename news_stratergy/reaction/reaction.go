package reaction

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"news_strategy/common"
)

const (
	ticksStreamName       = "TICKS_RAW"
	candlesStreamName     = "CANDLE_RAW"
	tickConsumerName      = "news-strategy-reaction-ticks-v1"
	candleConsumerName    = "news-strategy-reaction-candles-v2"
	maxTickHistory        = 512
	maxTimeframeHistories = 32
)

type tickRecord struct {
	Price     float64
	Timestamp time.Time
	Source    string
}

type candleRecord struct {
	Symbol    string
	Time      time.Time
	Timeframe string
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	VWAP      float64
}

type Engine struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	tickSub   *nats.Subscription
	candleSub *nats.Subscription

	mu           sync.RWMutex
	tickHistory  map[string][]tickRecord
	candleByTF   map[string]map[string][]candleRecord
	ticksSubject string
}

// New connects to live tick and candle streams so event scoring can use recent market reaction.
func New(brokerURL string, tickSubject string, candleSubject string) (*Engine, error) {
	nc, err := nats.Connect(brokerURL, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}

	tickSub, err := js.PullSubscribe(
		strings.TrimSpace(tickSubject)+".>",
		tickConsumerName,
		nats.BindStream(ticksStreamName),
		nats.DeliverNew(),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("subscribe ticks: %w", err)
	}

	candleSub, err := js.PullSubscribe(
		strings.TrimSpace(candleSubject)+".stock.>",
		candleConsumerName,
		nats.BindStream(candlesStreamName),
		nats.DeliverLastPerSubject(),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		_ = tickSub.Unsubscribe()
		nc.Close()
		return nil, fmt.Errorf("subscribe candles: %w", err)
	}

	return &Engine{
		nc:           nc,
		js:           js,
		tickSub:      tickSub,
		candleSub:    candleSub,
		tickHistory:  make(map[string][]tickRecord),
		candleByTF:   make(map[string]map[string][]candleRecord),
		ticksSubject: strings.TrimSpace(tickSubject),
	}, nil
}

func (e *Engine) Close() {
	if e == nil || e.nc == nil {
		return
	}
	_ = e.nc.Drain()
	e.nc.Close()
}

// Start keeps rolling tick and candle windows per symbol for reaction analysis.
func (e *Engine) Start(ctx context.Context) {
	if e == nil {
		return
	}
	if e.tickSub != nil {
		go e.consumeTicks(ctx)
	}
	if e.candleSub != nil {
		go e.consumeCandles(ctx)
	}
}

// Analyze combines the most recent tick and candle state into a reaction snapshot.
func (e *Engine) Analyze(event common.ClassifiedEvent) *common.MarketReaction {
	if e == nil || strings.TrimSpace(event.Symbol) == "" {
		return nil
	}
	symbol := strings.ToUpper(strings.TrimSpace(event.Symbol))

	e.mu.RLock()
	ticks := append([]tickRecord(nil), e.tickHistory[symbol]...)
	candlesByTF := cloneCandleMap(e.candleByTF[symbol])
	e.mu.RUnlock()

	if len(ticks) == 0 && len(candlesByTF) == 0 {
		return nil
	}

	reaction := &common.MarketReaction{
		Symbol:         symbol,
		TechnicalState: "neutral",
	}

	latestTick := latestTickAfter(ticks, event.PublishedAt)
	if latestTick == nil && len(ticks) != 0 {
		record := ticks[len(ticks)-1]
		latestTick = &record
	}
	if latestTick != nil {
		reaction.Source = latestTick.Source
		reaction.LastPrice = latestTick.Price
		reaction.LastTickAt = latestTick.Timestamp
	}

	baselineTick := latestTick
	if candidate := latestTickAtOrBefore(ticks, event.PublishedAt); candidate != nil {
		baselineTick = candidate
	}
	if latestTick != nil && baselineTick != nil && baselineTick.Price > 0 {
		reaction.Return1mPct = pctChange(baselineTick.Price, latestTick.Price)
		reaction.Return5mPct = reaction.Return1mPct
		reaction.Return15mPct = reaction.Return1mPct
	}

	if latest1m, ok := latestCandle(candlesByTF, "1m"); ok {
		reaction.LastCandleAt = latest1m.Time
		if latest1m.Close > 0 && latest1m.VWAP > 0 {
			reaction.VWAPDistancePct = pctChange(latest1m.VWAP, latest1m.Close)
		}
		reaction.CandleStrength = candleStrength(latest1m)
	}

	if base1m, latest1m, ok := returnsFromCandles(candlesByTF, "1m", event.PublishedAt); ok {
		reaction.Return1mPct = pctChange(base1m.Close, latest1m.Close)
	}
	if base5m, latest5m, ok := returnsFromCandles(candlesByTF, "5m", event.PublishedAt); ok {
		reaction.Return5mPct = pctChange(base5m.Close, latest5m.Close)
	}
	if base15m, latest15m, ok := returnsFromCandles(candlesByTF, "15m", event.PublishedAt); ok {
		reaction.Return15mPct = pctChange(base15m.Close, latest15m.Close)
	}
	reaction.VolumeRatio = volumeRatio(candlesByTF["1m"])
	reaction.RangeBreak = detectRangeBreak(candlesByTF)

	priceScore, technicalState := priceConfirmation(event.Sentiment, reaction)
	reaction.PriceConfirmation = priceScore
	reaction.TechnicalState = technicalState
	reaction.VolumeConfirmation = volumeConfirmation(reaction.VolumeRatio)

	return reaction
}

func (e *Engine) consumeTicks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := e.tickSub.Fetch(64, nats.MaxWait(250*time.Millisecond))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, msg := range msgs {
			record, symbol, ok := decodeTick(msg.Data)
			if ok {
				e.appendTick(symbol, record)
			}
			_ = msg.Ack()
		}
	}
}

func (e *Engine) consumeCandles(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := e.candleSub.Fetch(64, nats.MaxWait(250*time.Millisecond))
		if err != nil {
			if err == nats.ErrTimeout {
				continue
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, msg := range msgs {
			record, ok := decodeCandle(msg.Data)
			if ok {
				e.appendCandle(record)
			}
			_ = msg.Ack()
		}
	}
}

func (e *Engine) appendTick(symbol string, record tickRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()

	records := append(e.tickHistory[symbol], record)
	cutoff := time.Now().Add(-20 * time.Minute)
	trim := 0
	for trim < len(records) && records[trim].Timestamp.Before(cutoff) {
		trim++
	}
	if trim > 0 {
		records = records[trim:]
	}
	if len(records) > maxTickHistory {
		records = records[len(records)-maxTickHistory:]
	}
	e.tickHistory[symbol] = records
}

func (e *Engine) appendCandle(record candleRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.candleByTF[record.Symbol]; !ok {
		e.candleByTF[record.Symbol] = make(map[string][]candleRecord)
	}
	records := append(e.candleByTF[record.Symbol][record.Timeframe], record)
	cutoff := time.Now().Add(-4 * time.Hour)
	trim := 0
	for trim < len(records) && records[trim].Time.Before(cutoff) {
		trim++
	}
	if trim > 0 {
		records = records[trim:]
	}
	if len(records) > maxTimeframeHistories {
		records = records[len(records)-maxTimeframeHistories:]
	}
	e.candleByTF[record.Symbol][record.Timeframe] = records
}

func cloneCandleMap(source map[string][]candleRecord) map[string][]candleRecord {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string][]candleRecord, len(source))
	for timeframe, items := range source {
		cloned[timeframe] = append([]candleRecord(nil), items...)
	}
	return cloned
}

func latestTickAfter(records []tickRecord, publishedAt time.Time) *tickRecord {
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if !record.Timestamp.Before(publishedAt) {
			return &record
		}
	}
	return nil
}

func latestTickAtOrBefore(records []tickRecord, publishedAt time.Time) *tickRecord {
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if !record.Timestamp.After(publishedAt) {
			return &record
		}
	}
	return nil
}

func latestCandle(candlesByTF map[string][]candleRecord, timeframe string) (candleRecord, bool) {
	records := candlesByTF[timeframe]
	if len(records) == 0 {
		return candleRecord{}, false
	}
	return records[len(records)-1], true
}

func returnsFromCandles(candlesByTF map[string][]candleRecord, timeframe string, publishedAt time.Time) (candleRecord, candleRecord, bool) {
	records := candlesByTF[timeframe]
	if len(records) == 0 {
		return candleRecord{}, candleRecord{}, false
	}
	latest := records[len(records)-1]
	var base candleRecord
	foundBase := false
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if !record.Time.After(publishedAt) {
			base = record
			foundBase = true
			break
		}
	}
	if !foundBase {
		base = records[0]
	}
	return base, latest, true
}

func volumeRatio(records []candleRecord) float64 {
	if len(records) < 2 {
		return 0
	}
	latest := records[len(records)-1]
	sample := records
	if len(sample) > 6 {
		sample = sample[len(sample)-6:]
	}
	var total float64
	var count int
	for i := 0; i < len(sample)-1; i++ {
		total += sample[i].Volume
		count++
	}
	if count == 0 || total <= 0 {
		return 0
	}
	return latest.Volume / (total / float64(count))
}

func candleStrength(record candleRecord) float64 {
	rng := record.High - record.Low
	if rng <= 0 {
		return 0
	}
	position := (record.Close - record.Low) / rng
	return math.Round((position*100)*100) / 100
}

func detectRangeBreak(candlesByTF map[string][]candleRecord) string {
	latest1m, ok1 := latestCandle(candlesByTF, "1m")
	latest15m, ok15 := latestCandle(candlesByTF, "15m")
	if !ok1 || !ok15 {
		return ""
	}
	switch {
	case latest1m.Close > latest15m.High:
		return "BREAK_ABOVE_15M_HIGH"
	case latest1m.Close < latest15m.Low:
		return "BREAK_BELOW_15M_LOW"
	default:
		return ""
	}
}

func priceConfirmation(sentiment string, reaction *common.MarketReaction) (float64, string) {
	var score float64
	state := "neutral"

	switch strings.ToLower(sentiment) {
	case "positive":
		if reaction.Return1mPct >= 0.20 {
			score += 25
		}
		if reaction.Return5mPct >= 0.35 {
			score += 20
		}
		if reaction.Return15mPct >= 0.50 {
			score += 15
		}
		if reaction.VWAPDistancePct > 0 {
			score += 10
		}
		if reaction.CandleStrength >= 70 {
			score += 10
		}
		if reaction.RangeBreak == "BREAK_ABOVE_15M_HIGH" {
			score += 10
		}
		if score >= 50 {
			state = "confirmed_positive"
		}
	case "negative":
		if reaction.Return1mPct <= -0.20 {
			score += 25
		}
		if reaction.Return5mPct <= -0.35 {
			score += 20
		}
		if reaction.Return15mPct <= -0.50 {
			score += 15
		}
		if reaction.VWAPDistancePct < 0 {
			score += 10
		}
		if reaction.CandleStrength <= 30 {
			score += 10
		}
		if reaction.RangeBreak == "BREAK_BELOW_15M_LOW" {
			score += 10
		}
		if score >= 50 {
			state = "confirmed_negative"
		}
	}

	return score, state
}

func volumeConfirmation(volumeRatio float64) float64 {
	switch {
	case volumeRatio >= 3:
		return 80
	case volumeRatio >= 2:
		return 60
	case volumeRatio >= 1.5:
		return 40
	default:
		return 0
	}
}

func pctChange(base, current float64) float64 {
	if base == 0 {
		return 0
	}
	return ((current - base) / base) * 100
}

func decodeTick(data []byte) (tickRecord, string, bool) {
	var envelope struct {
		Payload struct {
			Symbol    string  `json:"symbol"`
			Timestamp string  `json:"timestamp"`
			LTP       float64 `json:"ltp"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil && envelope.Payload.Symbol != "" {
		ts, err := time.Parse(time.RFC3339Nano, envelope.Payload.Timestamp)
		if err == nil {
			return tickRecord{Price: envelope.Payload.LTP, Timestamp: ts, Source: "go"}, strings.ToUpper(strings.TrimSpace(envelope.Payload.Symbol)), true
		}
	}

	var pythonTick struct {
		Symbol string  `json:"symbol"`
		Time   string  `json:"time"`
		LTP    float64 `json:"ltp"`
	}
	if err := json.Unmarshal(data, &pythonTick); err == nil && pythonTick.Symbol != "" {
		ts, err := time.Parse(time.RFC3339Nano, pythonTick.Time)
		if err == nil {
			return tickRecord{Price: pythonTick.LTP / 100.0, Timestamp: ts, Source: "python_feed_triplex"}, strings.ToUpper(strings.TrimSpace(pythonTick.Symbol)), true
		}
	}
	return tickRecord{}, "", false
}

func decodeCandle(data []byte) (candleRecord, bool) {
	var record candleRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return candleRecord{}, false
	}
	record.Symbol = strings.ToUpper(strings.TrimSpace(record.Symbol))
	record.Timeframe = strings.TrimSpace(record.Timeframe)
	if record.Symbol == "" || record.Timeframe == "" || record.Time.IsZero() {
		return candleRecord{}, false
	}
	return record, true
}
