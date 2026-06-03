package jetstream

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"openmarketvolatility/common"
)

type SignalProducer struct {
	conn    *nats.Conn
	js      nats.JetStreamContext
	subject string
}

func NewSignalProducer(url, subject string) (*SignalProducer, error) {
	nc, js, err := connect(url)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, subject, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	return &SignalProducer{conn: nc, js: js, subject: strings.TrimSpace(subject)}, nil
}

func (p *SignalProducer) PublishSignal(signal common.Signal) error {
	payload, err := json.Marshal(signal)
	if err != nil {
		return err
	}
	_, err = p.js.Publish(p.subject, payload)
	return err
}

func (p *SignalProducer) Close() {
	if p.conn != nil {
		p.conn.Drain()
		p.conn.Close()
	}
}
