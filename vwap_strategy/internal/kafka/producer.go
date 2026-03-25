package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	cfgpkg "vwap_strategy/internal/config"
	"vwap_strategy/internal/strategy"
)

type SignalProducer struct {
	conn    *nats.Conn
	js      nats.JetStreamContext
	subject string
	logger  *zap.Logger
}

func NewSignalProducer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*SignalProducer, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.SignalTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	return &SignalProducer{conn: nc, js: js, subject: cfg.SignalTopic, logger: logger}, nil
}

func (p *SignalProducer) PublishSignal(ctx context.Context, s strategy.StrategySignal) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if _, err := p.js.PublishMsg(&nats.Msg{Subject: strings.TrimSpace(p.subject), Data: payload}, nats.Context(ctx)); err != nil {
		p.logger.Error("failed to publish signal", zap.Error(err), zap.String("symbol", s.Symbol))
		return err
	}
	p.logger.Debug("published signal", zap.String("symbol", s.Symbol), zap.String("reason", s.Reason))
	return nil
}

func (p *SignalProducer) Flush(timeout time.Duration) error {
	_ = timeout
	if p.conn != nil {
		p.conn.Flush()
		p.conn.Drain()
		p.conn.Close()
	}
	return nil
}
