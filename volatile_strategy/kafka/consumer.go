package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"

	"volatile_strategy/common"
)

type LTPConsumer struct {
	conn          *nats.Conn
	sub           *nats.Subscription
	startupCutoff time.Time
}

func NewLTPConsumer(cfg common.KafkaConfig) *LTPConsumer {
	nc, js, err := connect(cfg.Brokers)
	if err != nil {
		return &LTPConsumer{}
	}
	prefix := strings.TrimSpace(cfg.InputTopic)
	if !strings.Contains(prefix, ".") {
		prefix = "ticks.raw." + strings.ToUpper(sanitizeName(prefix))
	}
	_ = ensureStream(js, "ticks.raw", 72*time.Hour)
	consumerName := sanitizeName(cfg.InputGroupID) + "_v2"
	sub, err := js.PullSubscribe(
		prefix+".>",
		consumerName,
		nats.BindStream(streamName("ticks.raw")),
		nats.DeliverNew(),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		nc.Close()
		return &LTPConsumer{}
	}
	cutoff := time.Time{}
	if cfg.StartupReplayGraceSec > 0 {
		cutoff = time.Now().Add(-time.Duration(cfg.StartupReplayGraceSec) * time.Second)
	}
	return &LTPConsumer{conn: nc, sub: sub, startupCutoff: cutoff}
}

func (c *LTPConsumer) Read(ctx context.Context) (*common.LTPTick, error) {
	if c.sub == nil {
		return nil, fmt.Errorf("jetstream subscription is not initialized")
	}
	msg, err := fetchOne(ctx, c.sub)
	if err != nil {
		if err == nats.ErrTimeout || err == context.Canceled || err == context.DeadlineExceeded {
			return nil, nil
		}
		return nil, err
	}
	tick, err := decodeLTP(msg.Data)
	if err != nil {
		_ = msg.Ack()
		return nil, err
	}
	if !c.startupCutoff.IsZero() && tick.Timestamp.Before(c.startupCutoff) {
		_ = msg.Ack()
		return nil, nil
	}
	_ = msg.Ack()
	return tick, nil
}

func (c *LTPConsumer) Close() error {
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
	return nil
}

func decodeLTP(value []byte) (*common.LTPTick, error) {
	var nested struct {
		Payload struct {
			Symbol    string  `json:"symbol"`
			Timestamp string  `json:"timestamp"`
			LTP       float64 `json:"ltp"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(value, &nested); err == nil && nested.Payload.Symbol != "" {
		ts, err := time.Parse(time.RFC3339Nano, nested.Payload.Timestamp)
		if err != nil {
			return nil, fmt.Errorf("parse nested timestamp: %w", err)
		}
		return &common.LTPTick{Symbol: strings.ToUpper(strings.TrimSpace(nested.Payload.Symbol)), Price: nested.Payload.LTP, Timestamp: ts}, nil
	}

	var flat struct {
		Symbol string  `json:"symbol"`
		Time   string  `json:"time"`
		LTP    float64 `json:"ltp"`
	}
	if err := json.Unmarshal(value, &flat); err != nil {
		return nil, err
	}
	ts, err := time.Parse(time.RFC3339Nano, flat.Time)
	if err != nil {
		return nil, fmt.Errorf("parse flat timestamp: %w", err)
	}
	price := flat.LTP
	if price > 10000 {
		price /= 100.0
	}
	return &common.LTPTick{Symbol: strings.ToUpper(strings.TrimSpace(flat.Symbol)), Price: price, Timestamp: ts}, nil
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
