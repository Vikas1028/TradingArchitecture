package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	cfgpkg "paper_engine/internal/config"
	"paper_engine/internal/engine"
)

// SignalsConsumer wraps a Kafka consumer that reads StrategySignal messages.
// Inputs: KafkaConfig, logger; Outputs: StrategySignal via Poll.
type SignalsConsumer struct {
	reader  *kafka.Reader
	logger  *zap.Logger
	lastMsg *kafka.Message
}

// NewSignalsConsumer creates a Kafka consumer subscribed to cfg.SignalsTopic.
// Inputs: KafkaConfig, logger.
// Outputs: SignalsConsumer ready to Poll.
func NewSignalsConsumer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*SignalsConsumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.BootstrapServers},
		GroupID:        cfg.GroupID,
		GroupTopics:    []string{cfg.SignalsTopic},
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
	})
	return &SignalsConsumer{
		reader: reader,
		logger: logger,
	}, nil
}

// Poll reads a StrategySignal from Kafka or returns nil if there is no message.
// Inputs: context; Outputs: *StrategySignal or error.
// Flow: fetch message, unmarshal JSON, parse time, uppercase symbol.
func (c *SignalsConsumer) Poll(ctx context.Context) (*engine.StrategySignal, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, nil
		}
		return nil, err
	}
	c.lastMsg = &msg

	var raw struct {
		Strategy string `json:"strategy"`
		Symbol   string `json:"symbol"`
		Side     string `json:"side"`
		Time     string `json:"time"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		c.logger.Warn("failed to unmarshal signal", zap.Error(err))
		_ = c.Commit()
		return nil, nil
	}
	parsedTime, err := time.Parse(time.RFC3339Nano, raw.Time)
	if err != nil {
		c.logger.Warn("failed to parse signal time", zap.Error(err))
		_ = c.Commit()
		return nil, nil
	}

	sig := engine.StrategySignal{
		Strategy: raw.Strategy,
		Symbol:   strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		Side:     engine.SignalSide(raw.Side),
		Time:     parsedTime,
		Reason:   raw.Reason,
	}
	return &sig, nil
}

// Commit commits the current consumer offsets.
// Inputs: none; Outputs: error on failure.
func (c *SignalsConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	return c.reader.CommitMessages(context.Background(), *c.lastMsg)
}

// Close closes the underlying Kafka reader.
// Inputs: none; Outputs: error from close if any.
func (c *SignalsConsumer) Close() error {
	return c.reader.Close()
}
