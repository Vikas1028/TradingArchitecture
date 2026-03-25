package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	cfgpkg "paper_engine/internal/config"
	"paper_engine/internal/engine"
)

type TicksConsumer struct {
	conn          *nats.Conn
	js            nats.JetStreamContext
	sub           *nats.Subscription
	logger        *zap.Logger
	lastMsg       *nats.Msg
	startupCutoff time.Time
	subjectPrefix string
}

func NewTicksConsumer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*TicksConsumer, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.TicksTopic, 72*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	sub, err := js.PullSubscribe(
		strings.TrimSpace(cfg.TicksTopic)+".>",
		sanitizeName(cfg.TicksGroupID),
		nats.BindStream(streamName(cfg.TicksTopic)),
		nats.DeliverLastPerSubject(),
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
	return &TicksConsumer{
		conn:          nc,
		js:            js,
		sub:           sub,
		logger:        logger,
		startupCutoff: startupCutoff,
		subjectPrefix: strings.TrimSpace(cfg.TicksTopic),
	}, nil
}

func (c *TicksConsumer) Poll(ctx context.Context) (*engine.Tick, error) {
	msg, err := fetchOne(ctx, c.sub)
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded || err == nats.ErrTimeout {
			return nil, nil
		}
		return nil, err
	}
	c.lastMsg = msg
	tick, skip, err := c.parseTickMessage(msg, false)
	if err != nil {
		c.logger.Warn("failed to parse tick", zap.Error(err))
		_ = c.Commit()
		return nil, nil
	}
	if skip {
		_ = c.Commit()
		return nil, nil
	}
	return tick, nil
}

// LookupLatestPrice queries JetStream directly for the latest known tick of a symbol.
func (c *TicksConsumer) LookupLatestPrice(symbol string) (float64, bool) {
	tick, ok := c.LookupLatestTick(symbol)
	if !ok || tick == nil {
		return 0, false
	}
	return tick.LTP, true
}

// LookupLatestTick queries JetStream directly for the latest known tick of a symbol.
func (c *TicksConsumer) LookupLatestTick(symbol string) (*engine.Tick, bool) {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" || c.js == nil {
		return nil, false
	}
	stream := streamName(c.subjectPrefix)
	subjects := []string{
		c.subjectPrefix + ".GO_FEED." + symbol,
		c.subjectPrefix + ".GO_LTP." + symbol,
		c.subjectPrefix + ".python_feed_triplex." + symbol,
	}
	var latest *engine.Tick
	for _, subject := range subjects {
		msg, err := c.js.GetLastMsg(stream, subject)
		if err != nil || msg == nil {
			continue
		}
		tick, skip, parseErr := c.parseTickMessage(&nats.Msg{Data: msg.Data}, true)
		if parseErr != nil || skip || tick == nil || tick.LTP <= 0 {
			continue
		}
		if latest == nil || tick.Time.After(latest.Time) {
			latest = tick
		}
	}
	if latest == nil {
		return nil, false
	}
	return latest, true
}

func (c *TicksConsumer) parseTickMessage(msg *nats.Msg, ignoreStartupCutoff bool) (*engine.Tick, bool, error) {
	var raw struct {
		Symbol   string  `json:"symbol"`
		Time     string  `json:"time"`
		LTP      float64 `json:"ltp"`
		Exchange string  `json:"exchange"`
		Payload  struct {
			Symbol    string  `json:"symbol"`
			Timestamp string  `json:"timestamp"`
			LTP       float64 `json:"ltp"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(msg.Data, &raw); err != nil {
		return nil, false, err
	}

	symbol := strings.ToUpper(strings.TrimSpace(raw.Payload.Symbol))
	atText := raw.Payload.Timestamp
	ltp := raw.Payload.LTP
	if symbol == "" {
		symbol = strings.ToUpper(strings.TrimSpace(raw.Symbol))
		atText = raw.Time
		ltp = raw.LTP
		if strings.TrimSpace(raw.Exchange) != "" && ltp > 0 {
			ltp /= 100.0
		}
	}
	if symbol == "" || ltp <= 0 {
		return nil, true, nil
	}

	at, err := time.Parse(time.RFC3339Nano, atText)
	if err != nil {
		return nil, false, err
	}
	if !ignoreStartupCutoff && !c.startupCutoff.IsZero() && at.Before(c.startupCutoff) {
		return nil, true, nil
	}

	return &engine.Tick{Symbol: symbol, Time: at, LTP: ltp}, false, nil
}

func (c *TicksConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	msg := c.lastMsg
	c.lastMsg = nil
	return msg.Ack()
}

func (c *TicksConsumer) Close() error {
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
	return nil
}
