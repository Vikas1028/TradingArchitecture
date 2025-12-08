package kafka

import (
	"context"
	"encoding/json"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"marketdata/internal/candle"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
)

// CandleProducer wraps a Kafka producer responsible for sending Candle messages.
// Inputs: KafkaConfig and logger; Outputs: published candles to stock/index topics.
type CandleProducer struct {
	writer     *kafkago.Writer
	stockTopic string
	indexTopic string
	logger     *zap.Logger
}

// NewCandleProducer creates a new Kafka producer for stock and index candle topics.
// Inputs: KafkaConfig, logger.
// Outputs: CandleProducer ready to publish.
// Flow: build kafka-go writer with async disabled for easier error handling.
func NewCandleProducer(cfg config.KafkaConfig, logger *zap.Logger) (*CandleProducer, error) {
	writer := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.BootstrapServers),
		BatchTimeout: 10 * time.Millisecond,
		RequiredAcks: kafkago.RequireOne,
		Async:        false,
	}
	metrics.KafkaOutConnected.Set(1)
	return &CandleProducer{
		writer:     writer,
		stockTopic: cfg.StockCandlesTopic,
		indexTopic: cfg.IndexCandlesTopic,
		logger:     logger,
	}, nil
}

// PublishStockCandle serializes the Candle and publishes it to the stock candle topic.
// Inputs: context, Candle.
// Outputs: error on failure.
// Flow: marshal JSON, send with symbol as key.
func (p *CandleProducer) PublishStockCandle(ctx context.Context, c candle.Candle) error {
	return p.publish(ctx, p.stockTopic, c)
}

// PublishIndexCandle serializes the Candle and publishes it to the index candle topic.
// Inputs: context, Candle.
// Outputs: error on failure.
// Flow: marshal JSON, send with symbol as key.
func (p *CandleProducer) PublishIndexCandle(ctx context.Context, c candle.Candle) error {
	return p.publish(ctx, p.indexTopic, c)
}

// Flush flushes pending messages in the producer before shutdown.
// Inputs: timeout duration.
// Outputs: error on failure.
// Flow: call writer.Close after waiting for in-flight messages.
func (p *CandleProducer) Flush(timeout time.Duration) error {
	// kafka-go writer closes synchronously; we ignore timeout as Close is blocking.
	err := p.writer.Close()
	metrics.KafkaOutConnected.Set(0)
	return err
}

func (p *CandleProducer) publish(ctx context.Context, topic string, c candle.Candle) error {
	payload, err := json.Marshal(c)
	if err != nil {
		metrics.ErrorsTotal.Inc()
		return err
	}
	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(c.Symbol),
		Value: payload,
		Time:  c.Time,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("failed to publish candle", zap.Error(err), zap.String("symbol", c.Symbol), zap.String("topic", topic))
		metrics.KafkaOutConnected.Set(0)
		metrics.ErrorsTotal.Inc()
		return err
	}
	metrics.KafkaOutConnected.Set(1)
	metrics.CandlesEmittedTotal.Inc()
	p.logger.Debug("published candle", zap.String("symbol", c.Symbol), zap.String("topic", topic), zap.Time("time", c.Time))
	return nil
}
