package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	cfgpkg "real_engine/internal/config"
	"real_engine/internal/engine"
)

type EngineProducer struct {
	conn        *nats.Conn
	js          nats.JetStreamContext
	tradesTopic string
	pnlTopic    string
	logger      *zap.Logger
}

func NewEngineProducer(cfg cfgpkg.KafkaConfig, logger *zap.Logger) (*EngineProducer, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	if err := ensureStream(js, cfg.TradesTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	if err := ensureStream(js, cfg.PnlTopic, 720*time.Hour); err != nil {
		nc.Close()
		return nil, err
	}
	return &EngineProducer{
		conn:        nc,
		js:          js,
		tradesTopic: cfg.TradesTopic,
		pnlTopic:    cfg.PnlTopic,
		logger:      logger,
	}, nil
}

func (p *EngineProducer) PublishTrade(ctx context.Context, t engine.TradeEvent) error {
	return p.publish(ctx, strings.TrimSpace(p.tradesTopic), t.Symbol, t)
}

func (p *EngineProducer) PublishPnlSnapshot(ctx context.Context, s engine.PnlSnapshot) error {
	return p.publish(ctx, strings.TrimSpace(p.pnlTopic), "PNL", s)
}

func (p *EngineProducer) Flush(timeout time.Duration) error {
	_ = timeout
	if p.conn != nil {
		p.conn.Flush()
		p.conn.Drain()
		p.conn.Close()
	}
	return nil
}

func (p *EngineProducer) publish(ctx context.Context, subject, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := p.js.PublishMsg(&nats.Msg{Subject: subject, Data: data}, nats.Context(ctx)); err != nil {
		p.logger.Error("failed to publish", zap.Error(err), zap.String("subject", subject), zap.String("key", key))
		return err
	}
	p.logger.Debug("published", zap.String("subject", subject), zap.String("key", key))
	return nil
}
