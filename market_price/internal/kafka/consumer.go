package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	"market_price/common"
	"market_price/internal/metrics"
)

type Update struct {
	Source    string
	Symbol    string
	Price     float64
	Timestamp time.Time
}

type Consumer struct {
	source string
	conn   *nats.Conn
	sub    *nats.Subscription
	logger *zap.Logger
}

// NewConsumer creates one JetStream pull consumer per source feed so the
// dashboard can track each publisher independently.
func NewConsumer(cfg common.KafkaConfig, topic common.TopicConfig, logger *zap.Logger) *Consumer {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		logger.Error("failed to connect market price consumer", zap.String("source", topic.Name), zap.Error(err))
		return &Consumer{source: topic.Name, logger: logger}
	}
	prefix := strings.TrimSpace(topic.KafkaTopic)
	_ = ensureStream(js, prefix, 72*time.Hour)
	filter := fmt.Sprintf("%s.%s.>", prefix, sourceSubjectToken(topic.Name))
	sub, err := js.PullSubscribe(filter, sanitizeName(cfg.GroupPrefix+"-"+topic.Name), nats.BindStream(streamName(prefix)), nats.ManualAck(), nats.AckExplicit())
	if err != nil {
		logger.Error("failed to subscribe market price source", zap.String("source", topic.Name), zap.Error(err))
		nc.Close()
		return &Consumer{source: topic.Name, logger: logger}
	}
	return &Consumer{source: topic.Name, conn: nc, sub: sub, logger: logger}
}

func (c *Consumer) Run(ctx context.Context, updates chan<- Update) error {
	if c.sub == nil {
		return fmt.Errorf("jetstream subscription not initialized for source=%s", c.source)
	}
	for {
		msg, err := fetchOne(ctx, c.sub)
		if err != nil {
			if ctx.Err() != nil || err == context.Canceled || err == context.DeadlineExceeded {
				return nil
			}
			if err == nats.ErrTimeout {
				continue
			}
			return err
		}
		update, ok, err := decodeMessage(c.source, msg.Data)
		if err != nil {
			c.logger.Warn("failed to decode market price message", zap.String("source", c.source), zap.Error(err))
			metrics.ErrorsTotal.Inc()
		} else if ok {
			updates <- update
			metrics.KafkaMessagesConsumed.WithLabelValues(c.source).Inc()
			metrics.LastTopicTickUnix.WithLabelValues(c.source).Set(float64(update.Timestamp.Unix()))
		}
		_ = msg.Ack()
	}
}

func (c *Consumer) Close() error {
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
	return nil
}

// decodeMessage accepts both the nested Go envelope and the flat Python tick.
func decodeMessage(source string, value []byte) (Update, bool, error) {
	var envelope struct {
		Payload struct {
			Symbol    string  `json:"symbol"`
			Timestamp string  `json:"timestamp"`
			LTP       float64 `json:"ltp"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(value, &envelope); err == nil && envelope.Payload.Symbol != "" {
		ts, err := time.Parse(time.RFC3339Nano, envelope.Payload.Timestamp)
		if err != nil {
			return Update{}, false, fmt.Errorf("parse nested timestamp: %w", err)
		}
		return Update{Source: source, Symbol: strings.ToUpper(strings.TrimSpace(envelope.Payload.Symbol)), Price: normalizePrice(source, envelope.Payload.LTP), Timestamp: ts}, true, nil
	}

	var pythonTick struct {
		Symbol string  `json:"symbol"`
		Time   string  `json:"time"`
		LTP    float64 `json:"ltp"`
	}
	if err := json.Unmarshal(value, &pythonTick); err != nil {
		return Update{}, false, err
	}
	if pythonTick.Symbol == "" {
		return Update{}, false, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, pythonTick.Time)
	if err != nil {
		return Update{}, false, fmt.Errorf("parse flat timestamp: %w", err)
	}
	return Update{Source: source, Symbol: strings.ToUpper(strings.TrimSpace(pythonTick.Symbol)), Price: normalizePrice(source, pythonTick.LTP), Timestamp: ts}, true, nil
}

func normalizePrice(source string, price float64) float64 {
	switch source {
	case "python_feed_triplex":
		return price / 100.0
	default:
		return price
	}
}

func connect(rawURL string) (*nats.Conn, nats.JetStreamContext, error) {
	url := strings.TrimSpace(rawURL)
	if !strings.HasPrefix(url, "nats://") && !strings.HasPrefix(url, "tls://") {
		url = "nats://" + url
	}
	nc, err := nats.Connect(url, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		return nil, nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	return nc, js, nil
}

func ensureStream(js nats.JetStreamContext, prefix string, maxAge time.Duration) error {
	name := streamName(prefix)
	if info, err := js.StreamInfo(name); err == nil && info != nil {
		return nil
	}
	_, err := js.AddStream(&nats.StreamConfig{Name: name, Subjects: []string{strings.TrimSpace(prefix) + ".>"}, Storage: nats.FileStorage, Retention: nats.LimitsPolicy, Discard: nats.DiscardOld, MaxAge: maxAge, Replicas: 1})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") && !strings.Contains(strings.ToLower(err.Error()), "in use") {
		return err
	}
	return nil
}

func fetchOne(ctx context.Context, sub *nats.Subscription) (*nats.Msg, error) {
	wait := 250 * time.Millisecond
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if remaining < wait {
			wait = remaining
		}
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(wait))
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nats.ErrTimeout
	}
	return msgs[0], nil
}

func streamName(prefix string) string {
	return strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(prefix))
}

func sanitizeName(value string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			return r
		case r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, strings.TrimSpace(value))
}

// sourceSubjectToken maps logical source names to the subject token that each
// producer actually publishes. Go publishers uppercase their source token.
func sourceSubjectToken(value string) string {
	switch strings.TrimSpace(value) {
	case "python_feed_triplex":
		return "python_feed_triplex"
	case "go_feed":
		return "GO_FEED"
	case "go_ltp":
		return "GO_LTP"
	default:
		return sanitizeName(value)
	}
}
