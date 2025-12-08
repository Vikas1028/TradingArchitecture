package kafka

import (
	"context"
	"encoding/json"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	cfgpkg "paper_engine/internal/config"
	"paper_engine/internal/engine"
)

// EngineProducer wraps a Kafka producer for trade and PnL events.
// Inputs: KafkaConfig, logger; Outputs: publishes trades and pnl snapshots.
type EngineProducer struct {
	writer     *kafkago.Writer
	tradesTopic string
	pnlTopic    string
	logger     *zap.Logger
}

// NewEngineProducer creates a new Kafka producer for trades and PnL topics.
// Inputs: KafkaConfig, logger.
// Outputs: EngineProducer ready to publish.
func NewEngineProducer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*EngineProducer, error) {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.BootstrapServers),
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
	}
	return &EngineProducer{
		writer:      writer,
		tradesTopic: cfg.TradesTopic,
		pnlTopic:    cfg.PnlTopic,
		logger:      logger,
	}, nil
}

// PublishTrade serializes and publishes a TradeEvent to the trades topic.
// Inputs: context, TradeEvent; Outputs: error on failure.
func (p *EngineProducer) PublishTrade(ctx context.Context, t engine.TradeEvent) error {
	return p.publish(ctx, p.tradesTopic, t.Symbol, t)
}

// PublishPnlSnapshot serializes and publishes a PnlSnapshot to the pnl topic.
// Inputs: context, PnlSnapshot; Outputs: error on failure.
func (p *EngineProducer) PublishPnlSnapshot(ctx context.Context, s engine.PnlSnapshot) error {
	return p.publish(ctx, p.pnlTopic, "PNL", s)
}

// Flush flushes all pending messages before shutdown.
// Inputs: timeout duration; Outputs: error from close if any.
func (p *EngineProducer) Flush(timeout time.Duration) error {
	return p.writer.Close()
}

func (p *EngineProducer) publish(ctx context.Context, topic, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("failed to publish", zap.Error(err), zap.String("topic", topic), zap.String("key", key))
		return err
	}
	p.logger.Debug("published", zap.String("topic", topic), zap.String("key", key))
	return nil
}
