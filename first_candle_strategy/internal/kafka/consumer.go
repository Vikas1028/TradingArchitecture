package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	cfgpkg "first_candle_strategy/internal/config"
	"first_candle_strategy/internal/strategy"
)

// CandleConsumer wraps a Kafka consumer that reads 1-minute candle messages.
// Inputs: KafkaConfig, logger.
// Outputs: strategy.Candle values via Poll.
type CandleConsumer struct {
	reader        *kafka.Reader
	logger        *zap.Logger
	lastMsg       *kafka.Message
	startupCutoff time.Time
}

// NewCandleConsumer creates a Kafka consumer subscribed to the configured candles topic.
// Inputs: KafkaConfig, logger.
// Outputs: CandleConsumer ready to poll.
func NewCandleConsumer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*CandleConsumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.BootstrapServers},
		GroupID:        cfg.GroupID,
		GroupTopics:    []string{cfg.CandlesTopic},
		MinBytes:       1,
		MaxBytes:       10e6,
		StartOffset:    kafka.LastOffset,
		CommitInterval: 0,
	})
	startupCutoff := time.Time{}
	if cfg.StartupReplayGraceSec > 0 {
		startupCutoff = time.Now().Add(-time.Duration(cfg.StartupReplayGraceSec) * time.Second)
	}
	return &CandleConsumer{
		reader:        reader,
		logger:        logger,
		startupCutoff: startupCutoff,
	}, nil
}

// Poll reads the next candle from Kafka or returns nil if no message is currently available.
// Inputs: context.
// Outputs: parsed strategy.Candle or error.
func (c *CandleConsumer) Poll(ctx context.Context) (*strategy.Candle, error) {
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
	if !c.startupCutoff.IsZero() && parsedTime.Before(c.startupCutoff) {
		c.logger.Debug("skipping stale candle from startup backlog",
			zap.String("symbol", candle.Symbol),
			zap.Time("candle_time", parsedTime),
			zap.Time("startup_cutoff", c.startupCutoff),
		)
		_ = c.Commit()
		return nil, nil
	}
	return &candle, nil
}

// Commit commits the current consumer offsets.
// Inputs: none.
// Outputs: error on commit failure.
func (c *CandleConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	return c.reader.CommitMessages(context.Background(), *c.lastMsg)
}

// Close closes the underlying Kafka reader.
// Inputs: none.
// Outputs: error from the Kafka reader close.
func (c *CandleConsumer) Close() error {
	return c.reader.Close()
}
