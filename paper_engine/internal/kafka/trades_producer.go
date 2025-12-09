package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
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
	if err := ensureTopics(cfg, logger); err != nil {
		logger.Warn("failed to ensure Kafka topics exist", zap.Error(err))
	}

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

// ensureTopics attempts to create trades/pnl topics if they do not exist.
// It is safe to call even if topics already exist.
func ensureTopics(cfg cfgpkg.KafkaConfig, logger *zap.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := kafkago.DialContext(ctx, "tcp", cfg.BootstrapServers)
	if err != nil {
		return fmt.Errorf("dial bootstrap: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("lookup controller: %w", err)
	}

	ctrlAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafkago.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer ctrlConn.Close()

	topicConfigs := []kafkago.TopicConfig{
		{Topic: cfg.TradesTopic, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: cfg.PnlTopic, NumPartitions: 3, ReplicationFactor: 1},
	}
	if err := ctrlConn.CreateTopics(topicConfigs...); err != nil {
		// If topics already exist, CreateTopics returns an error per topic; log and continue.
		logger.Warn("CreateTopics returned error (may already exist)", zap.Error(err))
	}
	return nil
}
