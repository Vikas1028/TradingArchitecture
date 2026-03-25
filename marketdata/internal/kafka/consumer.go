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

	"marketdata/common"
	"marketdata/internal/candle"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
)

type TickConsumer struct {
	conn          *nats.Conn
	js            nats.JetStreamContext
	sub           *nats.Subscription
	logger        *zap.Logger
	lastMsg       *nats.Msg
	startupCutoff time.Time
}

func NewTickConsumer(cfg config.KafkaConfig, logger *zap.Logger) (*TickConsumer, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.TicksTopic, 72*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}

	filter := strings.TrimSpace(cfg.TicksTopic) + ".>"
	sub, err := js.PullSubscribe(
		filter,
		sanitizeConsumerName(cfg.GroupID),
		nats.BindStream(streamName(cfg.TicksTopic)),
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

	metrics.KafkaInConnected.Set(1)
	return &TickConsumer{
		conn:          nc,
		js:            js,
		sub:           sub,
		logger:        logger,
		startupCutoff: startupCutoff,
	}, nil
}

func (c *TickConsumer) Poll(ctx context.Context) (*candle.Tick, error) {
	msg, err := fetchOne(ctx, c.sub)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			return nil, nil
		}
		if err == nats.ErrTimeout {
			return nil, nil
		}
		metrics.KafkaInConnected.Set(0)
		metrics.ErrorsTotal.Inc()
		return nil, err
	}
	c.lastMsg = msg

	var raw struct {
		Symbol    string  `json:"symbol"`
		Exchange  string  `json:"exchange"`
		Time      string  `json:"time"`
		Timestamp string  `json:"timestamp"`
		LTP       float64 `json:"ltp"`
		Volume    int64   `json:"volume"`
		Bid       float64 `json:"bid"`
		Ask       float64 `json:"ask"`
		Payload   struct {
			SecurityID        string  `json:"security_id"`
			Symbol            string  `json:"symbol"`
			ExchangeSegment   uint8   `json:"exchange_segment"`
			Timestamp         string  `json:"timestamp"`
			LTP               float64 `json:"ltp"`
			Volume            int64   `json:"volume"`
			TotalSellQuantity int64   `json:"total_sell_quantity"`
			TotalBuyQuantity  int64   `json:"total_buy_quantity"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(msg.Data, &raw); err != nil {
		c.logger.Warn("failed to unmarshal tick", zap.Error(err))
		metrics.ErrorsTotal.Inc()
		_ = c.Commit()
		return nil, nil
	}
	tick, err := normalizeTick(raw)
	if err != nil {
		c.logger.Warn("failed to normalize tick", zap.Error(err))
		metrics.ErrorsTotal.Inc()
		_ = c.Commit()
		return nil, nil
	}
	if !c.startupCutoff.IsZero() && tick.Time.Before(c.startupCutoff) {
		_ = c.Commit()
		return nil, nil
	}

	return &tick, nil
}

func normalizeTick(raw struct {
	Symbol    string  `json:"symbol"`
	Exchange  string  `json:"exchange"`
	Time      string  `json:"time"`
	Timestamp string  `json:"timestamp"`
	LTP       float64 `json:"ltp"`
	Volume    int64   `json:"volume"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	Payload   struct {
		SecurityID        string  `json:"security_id"`
		Symbol            string  `json:"symbol"`
		ExchangeSegment   uint8   `json:"exchange_segment"`
		Timestamp         string  `json:"timestamp"`
		LTP               float64 `json:"ltp"`
		Volume            int64   `json:"volume"`
		TotalSellQuantity int64   `json:"total_sell_quantity"`
		TotalBuyQuantity  int64   `json:"total_buy_quantity"`
	} `json:"payload"`
}) (candle.Tick, error) {
	if symbol := strings.ToUpper(strings.TrimSpace(raw.Payload.Symbol)); symbol != "" {
		parsedTime, err := parseTickTime(raw.Payload.Timestamp, raw.Timestamp)
		if err != nil {
			return candle.Tick{}, err
		}
		source := "go_ltp"
		if raw.Payload.Volume > 0 || raw.Payload.TotalBuyQuantity > 0 || raw.Payload.TotalSellQuantity > 0 {
			source = "go_feed"
		}
		return candle.Tick{
			Symbol:   symbol,
			Exchange: "NSE_EQ",
			Time:     parsedTime,
			Source:   source,
			LTP:      raw.Payload.LTP,
			Volume:   raw.Payload.Volume,
		}, nil
	}

	symbol := strings.ToUpper(strings.TrimSpace(raw.Symbol))
	if symbol == "" {
		return candle.Tick{}, fmt.Errorf("tick missing symbol")
	}
	parsedTime, err := parseTickTime(raw.Time, raw.Timestamp)
	if err != nil {
		return candle.Tick{}, err
	}
	return candle.Tick{
		Symbol:   symbol,
		Exchange: raw.Exchange,
		Time:     parsedTime,
		Source:   "python_feed_triplex",
		LTP:      raw.LTP / 100.0,
		Volume:   raw.Volume,
		Bid:      raw.Bid / 100.0,
		Ask:      raw.Ask / 100.0,
	}, nil
}

func parseTickTime(primary string, fallback string) (time.Time, error) {
	value := strings.TrimSpace(primary)
	if value == "" {
		value = strings.TrimSpace(fallback)
	}
	if value == "" {
		return time.Time{}, fmt.Errorf("tick missing timestamp")
	}
	return time.Parse(time.RFC3339Nano, value)
}

func (c *TickConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	msg := c.lastMsg
	c.lastMsg = nil
	return msg.Ack()
}

func (c *TickConsumer) Close() error {
	metrics.KafkaInConnected.Set(0)
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
	return nil
}

func EnsureTopics(ctx context.Context, bootstrap string, topics []string) error {
	nc, js, err := connect(bootstrap)
	if err != nil {
		return err
	}
	defer nc.Close()

	for _, topic := range topics {
		maxAge := 720 * time.Hour
		if topic == common.DefaultTicksTopic {
			maxAge = 72 * time.Hour
		}
		if err := ensureStream(js, topic, maxAge); err != nil {
			return err
		}
	}
	return nil
}

func connect(rawURL string) (*nats.Conn, nats.JetStreamContext, error) {
	url := normalizeNATSURL(rawURL)
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
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      name,
		Subjects:  []string{strings.TrimSpace(prefix) + ".>"},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardOld,
		MaxAge:    maxAge,
		Replicas:  1,
	})
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

func normalizeNATSURL(raw string) string {
	value := strings.TrimSpace(strings.Split(raw, ",")[0])
	if strings.HasPrefix(value, "nats://") || strings.HasPrefix(value, "tls://") {
		return value
	}
	return "nats://" + value
}

func streamName(prefix string) string {
	return strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(prefix))
}

func sanitizeConsumerName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "consumer"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			return r
		case r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, value)
}
