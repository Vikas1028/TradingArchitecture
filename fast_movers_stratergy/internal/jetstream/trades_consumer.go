package jetstream

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type TradeEvent struct {
	Symbol    string
	TradeType string
}

type TradesConsumer struct {
	conn    *nats.Conn
	sub     *nats.Subscription
	lastMsg *nats.Msg
}

func NewTradesConsumer(url, subject, group string) (*TradesConsumer, error) {
	nc, js, err := connect(url)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, subject, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	sub, err := js.PullSubscribe(strings.TrimSpace(subject), sanitizeName(group), nats.BindStream(streamName(subject)), nats.ManualAck(), nats.AckExplicit(), nats.DeliverLast())
	if err != nil {
		nc.Close()
		return nil, err
	}
	return &TradesConsumer{conn: nc, sub: sub}, nil
}

func (c *TradesConsumer) Poll(ctx context.Context) (*TradeEvent, error) {
	msg, err := fetchOne(ctx, c.sub)
	if err != nil {
		return nil, err
	}
	c.lastMsg = msg
	var raw struct {
		Symbol    string `json:"symbol"`
		TradeType string `json:"trade_type"`
	}
	if err := json.Unmarshal(msg.Data, &raw); err != nil {
		return nil, err
	}
	return &TradeEvent{
		Symbol:    strings.ToUpper(strings.TrimSpace(raw.Symbol)),
		TradeType: strings.ToUpper(strings.TrimSpace(raw.TradeType)),
	}, nil
}

func (c *TradesConsumer) Commit() error {
	if c.lastMsg == nil {
		return nil
	}
	msg := c.lastMsg
	c.lastMsg = nil
	return msg.Ack()
}

func (c *TradesConsumer) Close() {
	if c.conn != nil {
		c.conn.Drain()
		c.conn.Close()
	}
}
