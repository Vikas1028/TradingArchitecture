package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	cfgpkg "real_engine/internal/config"
	"real_engine/internal/engine"
)

type SignalsConsumer struct {
	conn          *nats.Conn
	sub           *nats.Subscription
	logger        *zap.Logger
	lastMsg       *nats.Msg
	startupCutoff time.Time
}

func NewSignalsConsumer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*SignalsConsumer, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.SignalsTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	sub, err := js.PullSubscribe(
		strings.TrimSpace(cfg.SignalsTopic),
		sanitizeName(cfg.SignalsGroupID),
		nats.BindStream(streamName(cfg.SignalsTopic)),
		nats.DeliverLast(),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		nc.Close()
		return nil, err
	}
	startupCutoff := time.Time{}
	if cfg.StartupReplayGraceSec > 0 {
		startupCutoff = time.Now().Add(-time.Duration(cfg.StartupReplayGraceSec) * time.Second)
	}
	return &SignalsConsumer{conn: nc, sub: sub, logger: logger, startupCutoff: startupCutoff}, nil
}

func (c *SignalsConsumer) Poll(ctx context.Context) (*engine.StrategySignal, error) {
	msg, err := fetchOne(ctx, c.sub)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded || err == nats.ErrTimeout {
			return nil, nil
		}
		return nil, err
	}
	c.lastMsg = msg
	var raw struct {
		SignalID                string  `json:"signal_id"`
		Strategy                string  `json:"strategy"`
		Symbol                  string  `json:"symbol"`
		Side                    string  `json:"side"`
		Time                    string  `json:"time"`
		Reason                  string  `json:"reason"`
		Quantity                int64   `json:"quantity"`
		StopLossPct             float64 `json:"stop_loss_pct"`
		TargetPct               float64 `json:"target_pct"`
		TrailingStopPct         float64 `json:"trailing_stop_pct"`
		TrailingFreezeProfitPct float64 `json:"trailing_freeze_profit_pct"`
	}
	if err := json.Unmarshal(msg.Data, &raw); err != nil {
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
		SignalID:                strings.TrimSpace(raw.SignalID),
		Strategy:                raw.Strategy,
		Symbol:                  strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		Side:                    engine.SignalSide(raw.Side),
		Time:                    parsedTime,
		Reason:                  raw.Reason,
		Quantity:                raw.Quantity,
		StopLossPct:             raw.StopLossPct,
		TargetPct:               raw.TargetPct,
		TrailingStopPct:         raw.TrailingStopPct,
		TrailingFreezeProfitPct: raw.TrailingFreezeProfitPct,
	}
	if !c.startupCutoff.IsZero() && parsedTime.Before(c.startupCutoff) {
		_ = c.Commit()
		return nil, nil
	}
	return &sig, nil
}

func (c *SignalsConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	msg := c.lastMsg
	c.lastMsg = nil
	return msg.Ack()
}

func (c *SignalsConsumer) Close() error {
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
	return nil
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
