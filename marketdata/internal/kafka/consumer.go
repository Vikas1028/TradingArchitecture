package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"marketdata/internal/candle"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
)

// TickConsumer wraps a Kafka consumer that reads Tick messages from the ticks topic.
// Inputs: KafkaConfig and logger; Outputs: parsed Tick instances via Poll.
// Flow: builds a kafka-go reader with manual commits; Poll returns a Tick or nil on timeout.
type TickConsumer struct {
	reader        *kafka.Reader
	logger        *zap.Logger
	lastMsg       *kafka.Message
	startupCutoff time.Time
}

// NewTickConsumer constructs a Kafka consumer subscribed to cfg.TicksTopic using cfg.GroupID.
// Inputs: KafkaConfig, logger.
// Outputs: TickConsumer ready to Poll.
// Flow: build reader with commit disabled; subscribe to topic.
func NewTickConsumer(cfg config.KafkaConfig, logger *zap.Logger) (*TickConsumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{cfg.BootstrapServers},
		GroupID:        cfg.GroupID,
		Topic:          cfg.TicksTopic,
		MinBytes:       1,
		MaxBytes:       10e6,
		StartOffset:    kafka.LastOffset,
		CommitInterval: 0, // manual commits
	})

	startupCutoff := time.Time{}
	if cfg.StartupReplayGraceSec > 0 {
		startupCutoff = time.Now().Add(-time.Duration(cfg.StartupReplayGraceSec) * time.Second)
	}

	tc := &TickConsumer{
		reader:        reader,
		logger:        logger,
		startupCutoff: startupCutoff,
	}
	metrics.KafkaInConnected.Set(1)
	return tc, nil
}

// Poll reads the next Tick message from Kafka or returns nil if there is no message yet.
// Inputs: context for cancellation.
// Outputs: *Tick (may be nil) and error if fatal.
// Flow: Fetch message, decode JSON, parse time, uppercase symbol; on parse errors log+commit+skip.
func (c *TickConsumer) Poll(ctx context.Context) (*candle.Tick, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, nil
		}
		metrics.KafkaInConnected.Set(0)
		metrics.ErrorsTotal.Inc()
		return nil, err
	}
	c.lastMsg = &msg

	var raw struct {
		Symbol   string  `json:"symbol"`
		Exchange string  `json:"exchange"`
		Time     string  `json:"time"`
		LTP      float64 `json:"ltp"`
		Volume   int64   `json:"volume"`
		Bid      float64 `json:"bid"`
		Ask      float64 `json:"ask"`
	}
	if err := json.Unmarshal(msg.Value, &raw); err != nil {
		c.logger.Warn("failed to unmarshal tick", zap.Error(err))
		metrics.ErrorsTotal.Inc()
		_ = c.Commit()
		return nil, nil
	}
	parsedTime, err := time.Parse(time.RFC3339Nano, raw.Time)
	if err != nil {
		c.logger.Warn("failed to parse tick time", zap.Error(err))
		metrics.ErrorsTotal.Inc()
		_ = c.Commit()
		return nil, nil
	}
	tick := candle.Tick{
		Symbol:   strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		Exchange: raw.Exchange,
		Time:     parsedTime,
		LTP:      raw.LTP,
		Volume:   raw.Volume,
		Bid:      raw.Bid,
		Ask:      raw.Ask,
	}
	if !c.startupCutoff.IsZero() && parsedTime.Before(c.startupCutoff) {
		c.logger.Debug("skipping stale tick from startup backlog",
			zap.String("symbol", tick.Symbol),
			zap.Time("tick_time", parsedTime),
			zap.Time("startup_cutoff", c.startupCutoff),
		)
		_ = c.Commit()
		return nil, nil
	}

	return &tick, nil
}

// Commit commits the current consumer offsets if manual committing is used.
// Inputs: none; Outputs: error if commit fails.
// Flow: commit last fetched message to Kafka.
func (c *TickConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	return c.reader.CommitMessages(context.Background(), *c.lastMsg)
}

// Close closes the underlying Kafka reader.
// Inputs: none; Outputs: error from close if any.
func (c *TickConsumer) Close() error {
	metrics.KafkaInConnected.Set(0)
	return c.reader.Close()
}

// EnsureTopics creates the required topics if they do not exist.
// Inputs: bootstrap server string, topic names slice, replication factor 1 and partitions 1 by default.
// Outputs: error on failure.
func EnsureTopics(ctx context.Context, bootstrap string, topics []string) error {
	conn, err := kafka.DialContext(ctx, "tcp", bootstrap)
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	existing, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("read partitions: %w", err)
	}
	exists := make(map[string]bool)
	for _, p := range existing {
		exists[p.Topic] = true
	}

	configs := make([]kafka.TopicConfig, 0)
	for _, t := range topics {
		if exists[t] {
			continue
		}
		configs = append(configs, kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}
	if len(configs) == 0 {
		return nil
	}
	return conn.CreateTopics(configs...)
}
