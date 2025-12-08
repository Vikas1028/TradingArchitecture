package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	cfgpkg "vwap_strategy/internal/config"
	"vwap_strategy/internal/strategy"
)

// PolledCandle holds a candle and its topic so callers can route index vs stock streams.
type PolledCandle struct {
	Topic  string
	Candle strategy.Candle
}

// CandleConsumer wraps a Kafka consumer that reads Candle messages from index and stock topics.
// Inputs: KafkaConfig, logger; Outputs: PolledCandle via Poll.
type CandleConsumer struct {
	reader  *kafka.Reader
	logger  *zap.Logger
	lastMsg *kafka.Message
}

// NewCandleConsumer creates and subscribes a consumer to both stock and index candle topics.
// Inputs: KafkaConfig, logger.
// Outputs: CandleConsumer ready to Poll.
func NewCandleConsumer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*CandleConsumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.BootstrapServers},
		GroupID:        cfg.GroupID,
		GroupTopics:    []string{cfg.StockCandlesTopic, cfg.IndexCandlesTopic},
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // manual commits
	})
	return &CandleConsumer{
		reader: reader,
		logger: logger,
	}, nil
}

// Poll reads the next Candle from Kafka or returns nil if there is no message yet.
// Inputs: context.
// Outputs: *PolledCandle or error.
// Flow: fetch message, unmarshal JSON, parse time, uppercase symbol, return.
func (c *CandleConsumer) Poll(ctx context.Context) (*PolledCandle, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, nil
		}
		return nil, err
	}
	c.lastMsg = &msg

	var raw struct {
		Symbol string  `json:"symbol"`
		Time   string  `json:"time"`
		Open   float64 `json:"open"`
		High   float64 `json:"high"`
		Low    float64 `json:"low"`
		Close  float64 `json:"close"`
		Volume int64   `json:"volume"`
		VWAP   float64 `json:"vwap"`
	}
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		c.logger.Warn("failed to unmarshal candle", zap.Error(err), zap.String("topic", msg.Topic))
		_ = c.Commit()
		return nil, nil
	}
	parsedTime, err := time.Parse(time.RFC3339Nano, raw.Time)
	if err != nil {
		c.logger.Warn("failed to parse candle time", zap.Error(err), zap.String("topic", msg.Topic))
		_ = c.Commit()
		return nil, nil
	}

	candle := strategy.Candle{
		Symbol: strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		Time:   parsedTime,
		Open:   raw.Open,
		High:   raw.High,
		Low:    raw.Low,
		Close:  raw.Close,
		Volume: raw.Volume,
		VWAP:   raw.VWAP,
	}

	return &PolledCandle{
		Topic:  msg.Topic,
		Candle: candle,
	}, nil
}

// Commit commits current offsets of the Kafka consumer if manual committing is used.
// Inputs: none; Outputs: error on failure.
func (c *CandleConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	return c.reader.CommitMessages(context.Background(), *c.lastMsg)
}

// Close closes the underlying Kafka reader.
// Inputs: none; Outputs: error from close if any.
func (c *CandleConsumer) Close() error {
	return c.reader.Close()
}
