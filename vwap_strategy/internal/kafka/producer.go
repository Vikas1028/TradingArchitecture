package kafka

import (
	"context"
	"encoding/json"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	cfgpkg "vwap_strategy/internal/config"
	"vwap_strategy/internal/strategy"
)

// SignalProducer wraps a Kafka producer that publishes StrategySignal messages.
// Inputs: KafkaConfig, logger; Outputs: published signals to configured topic.
type SignalProducer struct {
	writer *kafkago.Writer
	topic  string
	logger *zap.Logger
}

// NewSignalProducer creates a Kafka producer for the configured signal topic.
// Inputs: KafkaConfig, logger.
// Outputs: SignalProducer ready to publish.
func NewSignalProducer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*SignalProducer, error) {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.BootstrapServers),
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
	}
	return &SignalProducer{
		writer: writer,
		topic:  cfg.SignalTopic,
		logger: logger,
	}, nil
}

// PublishSignal serializes and publishes a StrategySignal to the signal topic.
// Inputs: context, StrategySignal.
// Outputs: error on failure.
// Flow: marshal JSON, send with symbol as key.
func (p *SignalProducer) PublishSignal(ctx context.Context, s strategy.StrategySignal) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return err
	}
	msg := kafkago.Message{
		Topic: p.topic,
		Key:   []byte(s.Symbol),
		Value: payload,
		Time:  s.Time,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("failed to publish signal", zap.Error(err), zap.String("symbol", s.Symbol))
		return err
	}
	p.logger.Debug("published signal", zap.String("symbol", s.Symbol), zap.String("reason", s.Reason))
	return nil
}

// Flush flushes pending messages in the Kafka producer before shutdown.
// Inputs: timeout duration.
// Outputs: error from close if any.
func (p *SignalProducer) Flush(timeout time.Duration) error {
	// kafka-go writer Close is blocking; ignore timeout here.
	return p.writer.Close()
}
