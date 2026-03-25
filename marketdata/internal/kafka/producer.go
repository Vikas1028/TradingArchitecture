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

	"marketdata/internal/candle"
	"marketdata/internal/config"
	"marketdata/internal/metrics"
)

type CandleProducer struct {
	conn        *nats.Conn
	js          nats.JetStreamContext
	stockPrefix string
	indexPrefix string
	logger      *zap.Logger
}

func NewCandleProducer(cfg config.KafkaConfig, logger *zap.Logger) (*CandleProducer, error) {
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
	metrics.KafkaOutConnected.Set(1)
	return &CandleProducer{
		conn:        nc,
		js:          js,
		stockPrefix: cfg.StockCandlesTopic,
		indexPrefix: cfg.IndexCandlesTopic,
		logger:      logger,
	}, nil
}

func (p *CandleProducer) PublishCandle(ctx context.Context, c candle.Candle, isIndex bool) error {
	prefix := p.stockPrefix
	if isIndex {
		prefix = p.indexPrefix
	}
	payload, err := json.Marshal(c)
	if err != nil {
		metrics.ErrorsTotal.Inc()
		return err
	}
	subject := candleSubject(prefix, c.Timeframe, c.Symbol)
	_, err = p.js.PublishMsg(&nats.Msg{
		Subject: subject,
		Data:    payload,
		Header: nats.Header{
			"Timeframe": []string{c.Timeframe},
			"Symbol":    []string{c.Symbol},
		},
	}, nats.Context(ctx))
	if err != nil {
		p.logger.Error("failed to publish candle", zap.Error(err), zap.String("symbol", c.Symbol), zap.String("subject", subject))
		metrics.KafkaOutConnected.Set(0)
		metrics.ErrorsTotal.Inc()
		return err
	}
	metrics.KafkaOutConnected.Set(1)
	metrics.CandlesEmittedTotal.Inc()
	p.logger.Debug("published candle", zap.String("symbol", c.Symbol), zap.String("subject", subject), zap.String("timeframe", c.Timeframe), zap.Time("time", c.Time))
	return nil
}

func (p *CandleProducer) Flush(timeout time.Duration) error {
	_ = timeout
	metrics.KafkaOutConnected.Set(0)
	if p.conn != nil {
		p.conn.Flush()
		p.conn.Drain()
		p.conn.Close()
	}
	return nil
}

func candleSubject(prefix, timeframe, symbol string) string {
	kind := "stock"
	if strings.Contains(strings.ToLower(prefix), "indices") {
		kind = "index"
	}
	return fmt.Sprintf("%s.%s.%s.%s", strings.TrimSpace(prefix), kind, sanitizeToken(timeframe), sanitizeToken(symbol))
}

func sanitizeToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "UNKNOWN"
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
