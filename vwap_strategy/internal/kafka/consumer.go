package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	cfgpkg "vwap_strategy/internal/config"
	"vwap_strategy/internal/strategy"
)

type PolledCandle struct {
	Topic  string
	Candle strategy.Candle
}

type CandleConsumer struct {
	conn          *nats.Conn
	stockSub      *nats.Subscription
	indexSub      *nats.Subscription
	logger        *zap.Logger
	lastMsg       *nats.Msg
	lastTopic     string
	startupCutoff time.Time
	nextIndex     bool
}

func NewCandleConsumer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*CandleConsumer, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.StockCandlesTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	if err := ensureStream(js, cfg.IndexCandlesTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	stockSub, err := js.PullSubscribe(
		strings.TrimSpace(cfg.StockCandlesTopic)+".stock.1m.*",
		sanitizeName(cfg.GroupID+"-stock"),
		nats.BindStream(streamName(cfg.StockCandlesTopic)),
		nats.DeliverNew(),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		nc.Close()
		return nil, err
	}
	indexSub, err := js.PullSubscribe(
		strings.TrimSpace(cfg.IndexCandlesTopic)+".index.1m.*",
		sanitizeName(cfg.GroupID+"-index"),
		nats.BindStream(streamName(cfg.IndexCandlesTopic)),
		nats.DeliverNew(),
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
	return &CandleConsumer{
		conn:          nc,
		stockSub:      stockSub,
		indexSub:      indexSub,
		logger:        logger,
		startupCutoff: startupCutoff,
	}, nil
}

func (c *CandleConsumer) Poll(ctx context.Context) (*PolledCandle, error) {
	subs := []*nats.Subscription{c.stockSub, c.indexSub}
	topics := []string{"stock", "index"}
	start := 0
	if c.nextIndex {
		start = 1
	}
	for offset := 0; offset < len(subs); offset++ {
		idx := (start + offset) % len(subs)
		msg, err := fetchOne(ctx, subs[idx])
		if err != nil {
			if err == nats.ErrTimeout || err == context.Canceled || err == context.DeadlineExceeded {
				continue
			}
			return nil, err
		}
		c.lastMsg = msg
		c.lastTopic = topics[idx]
		c.nextIndex = idx == 0

		var raw struct {
			Symbol    string  `json:"symbol"`
			Time      string  `json:"time"`
			Timeframe string  `json:"timeframe"`
			Open      float64 `json:"open"`
			High      float64 `json:"high"`
			Low       float64 `json:"low"`
			Close     float64 `json:"close"`
			Volume    int64   `json:"volume"`
			VWAP      float64 `json:"vwap"`
		}
		if err := json.Unmarshal(msg.Data, &raw); err != nil {
			c.logger.Warn("failed to unmarshal candle", zap.Error(err), zap.String("topic", c.lastTopic))
			_ = c.Commit()
			return nil, nil
		}
		parsedTime, err := time.Parse(time.RFC3339Nano, raw.Time)
		if err != nil {
			c.logger.Warn("failed to parse candle time", zap.Error(err), zap.String("topic", c.lastTopic))
			_ = c.Commit()
			return nil, nil
		}
		candle := strategy.Candle{
			Symbol:    strings.ToUpper(strings.TrimSpace(raw.Symbol)),
			Time:      parsedTime,
			Timeframe: strings.TrimSpace(raw.Timeframe),
			Open:      raw.Open,
			High:      raw.High,
			Low:       raw.Low,
			Close:     raw.Close,
			Volume:    raw.Volume,
			VWAP:      raw.VWAP,
		}
		if candle.Timeframe != "1m" {
			_ = c.Commit()
			return nil, nil
		}
		if !c.startupCutoff.IsZero() && parsedTime.Before(c.startupCutoff) {
			_ = c.Commit()
			return nil, nil
		}
		return &PolledCandle{Topic: c.lastTopic, Candle: candle}, nil
	}
	return nil, nil
}

func (c *CandleConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	msg := c.lastMsg
	c.lastMsg = nil
	return msg.Ack()
}

func (c *CandleConsumer) Close() error {
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
	wait := 150 * time.Millisecond
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
