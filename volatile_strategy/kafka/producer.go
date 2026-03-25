package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"volatile_strategy/common"
)

type SignalProducer struct {
	conn    *nats.Conn
	js      nats.JetStreamContext
	subject string
}

func NewSignalProducer(cfg common.KafkaConfig) (*SignalProducer, error) {
	nc, js, err := connect(cfg.Brokers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.SignalTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	return &SignalProducer{conn: nc, js: js, subject: cfg.SignalTopic}, nil
}

func (p *SignalProducer) PublishSignal(ctx context.Context, s common.Signal) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = p.js.PublishMsg(&nats.Msg{Subject: strings.TrimSpace(p.subject), Data: payload}, nats.Context(ctx))
	return err
}

func (p *SignalProducer) Close() error {
	if p.conn != nil {
		p.conn.Flush()
		p.conn.Drain()
		p.conn.Close()
	}
	return nil
}
