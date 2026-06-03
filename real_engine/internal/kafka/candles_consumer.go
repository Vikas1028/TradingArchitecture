package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	cfgpkg "real_engine/internal/config"
	"real_engine/internal/engine"
)

type CandlesConsumer struct {
	conn          *nats.Conn
	sub           *nats.Subscription
	logger        *zap.Logger
	lastMsg       *nats.Msg
	startupCutoff time.Time
	priceDivisor  float64
}

func NewCandlesConsumer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*CandlesConsumer, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.CandlesTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	sub, err := js.PullSubscribe(
		strings.TrimSpace(cfg.CandlesTopic)+".stock.1m.*",
		sanitizeName(cfg.CandlesGroupID),
		nats.BindStream(streamName(cfg.CandlesTopic)),
		nats.DeliverNew(),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		nc.Close()
		return nil, err
	}
	startupCutoff := time.Time{}
	if cfg.StartupReplayGraceSec > 0 {
		startupCutoff = time.Now().Add(-time.Duration(cfg.StartupReplayGraceSec) * time.Second)
	}
	return &CandlesConsumer{conn: nc, sub: sub, logger: logger, startupCutoff: startupCutoff, priceDivisor: cfg.PriceScaleDivisor}, nil
}

func (c *CandlesConsumer) Poll(ctx context.Context) (*engine.Candle, error) {
	msg, err := fetchOne(ctx, c.sub)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded || err == nats.ErrTimeout {
			return nil, nil
		}
		return nil, err
	}
	c.lastMsg = msg
	var raw struct {
		Symbol    string  `json:"symbol"`
		Time      string  `json:"time"`
		Timeframe string  `json:"timeframe"`
		Open      float64 `json:"open"`
		High      float64 `json:"high"`
		Low       float64 `json:"low"`
		Close     float64 `json:"close"`
		Volume    int64   `json:"volume"`
		VWAP      float64 `json:"vwap"`
	}
	if err := json.Unmarshal(msg.Data, &raw); err != nil {
		c.logger.Warn("failed to unmarshal candle", zap.Error(err))
		_ = c.Commit()
		return nil, nil
	}
	parsedTime, err := time.Parse(time.RFC3339Nano, raw.Time)
	if err != nil {
		c.logger.Warn("failed to parse candle time", zap.Error(err))
		_ = c.Commit()
		return nil, nil
	}
	candle := engine.Candle{
		Symbol:    strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		Time:      parsedTime,
		Timeframe: strings.TrimSpace(raw.Timeframe),
		Open:      raw.Open,
		High:      raw.High,
		Low:       raw.Low,
		Close:     raw.Close,
		Volume:    raw.Volume,
		VWAP:      raw.VWAP,
	}
	if candle.Timeframe != "1m" {
		_ = c.Commit()
		return nil, nil
	}
	if !c.startupCutoff.IsZero() && parsedTime.Before(c.startupCutoff) {
		_ = c.Commit()
		return nil, nil
	}
	if c.priceDivisor != 0 && c.priceDivisor != 1 {
		candle.Open /= c.priceDivisor
		candle.High /= c.priceDivisor
		candle.Low /= c.priceDivisor
		candle.Close /= c.priceDivisor
		candle.VWAP /= c.priceDivisor
	}
	return &candle, nil
}

func (c *CandlesConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	msg := c.lastMsg
	c.lastMsg = nil
	return msg.Ack()
}

func (c *CandlesConsumer) Close() error {
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
	return nil
}
