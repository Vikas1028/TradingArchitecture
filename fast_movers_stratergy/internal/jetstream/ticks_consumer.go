package jetstream

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"

	"fast_movers_stratergy/common"
)

type TicksConsumer struct {
	conn          *nats.Conn
	sub           *nats.Subscription
	lastMsg       *nats.Msg
	startupCutoff time.Time
}

func NewTicksConsumer(url, subject, group string, startupReplayGraceSec int) (*TicksConsumer, error) {
	nc, js, err := connect(url)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, subject, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	opts := []nats.SubOpt{
		nats.BindStream(streamName(subject)),
		nats.ManualAck(),
		nats.AckExplicit(),
		nats.DeliverNew(),
	}
	startupCutoff := time.Time{}
	if startupReplayGraceSec > 0 {
		startupCutoff = time.Now().Add(-time.Duration(startupReplayGraceSec) * time.Second)
	}
	sub, err := js.PullSubscribe(strings.TrimSpace(subject)+".>", sanitizeName(group), opts...)
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &TicksConsumer{conn: nc, sub: sub, startupCutoff: startupCutoff}, nil
}

func (c *TicksConsumer) Poll(ctx context.Context) (*common.Tick, error) {
	msg, err := fetchOne(ctx, c.sub)
	if err != nil {
		return nil, err
	}
	c.lastMsg = msg
	tick, err := decodeTick(msg)
	if err != nil {
		return nil, err
	}
	if !c.startupCutoff.IsZero() && tick.Time.Before(c.startupCutoff) {
		return nil, nil
	}
	return tick, nil
}

func (c *TicksConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	msg := c.lastMsg
	c.lastMsg = nil
	return msg.Ack()
}

func (c *TicksConsumer) Close() {
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
}

func decodeTick(msg *nats.Msg) (*common.Tick, error) {
	var raw struct {
		Symbol  string  `json:"symbol"`
		Time    string  `json:"time"`
		LTP     float64 `json:"ltp"`
		Price   float64 `json:"price"`
		Open    float64 `json:"day_open"`
		DayOpen float64 `json:"open"`
		Volume  int64   `json:"volume"`
		Source  string  `json:"source"`
	}
	if err := json.Unmarshal(msg.Data, &raw); err != nil {
		return nil, err
	}
	ts, _ := time.Parse(time.RFC3339Nano, raw.Time)
	price := raw.LTP
	if price == 0 {
		price = raw.Price
	}
	open := raw.Open
	if open == 0 {
		open = raw.DayOpen
	}
	return &common.Tick{
		Symbol:     strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		Source:     subjectSource(msg.Subject, raw.Source),
		Time:       ts,
		LTP:        price,
		DayOpen:    open,
		Volume:     raw.Volume,
		RawSubject: msg.Subject,
	}, nil
}

func subjectSource(subject, fallback string) string {
	if strings.TrimSpace(fallback) != "" {
		return fallback
	}
	parts := strings.Split(subject, ".")
	if len(parts) >= 3 {
		return parts[len(parts)-1]
	}
	return "unknown"
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
	wait := 200 * time.Millisecond
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
	value = strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, value)
	if value == "" {
		return "consumer"
	}
	return value
}
