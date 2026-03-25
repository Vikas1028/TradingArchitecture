package kafka

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	cfgpkg "paper_engine/internal/config"
	"paper_engine/internal/engine"
)

// LoadTradeHistory replays historical trade events from JetStream for startup recovery.
func LoadTradeHistory(cfg cfgpkg.KafkaConfig, logger *zap.Logger, since time.Time) ([]engine.TradeEvent, error) {
	nc, js, err := connect(cfg.BootstrapServers)
	if err != nil {
		return nil, err
	}
	defer nc.Drain()
	defer nc.Close()

	if err := ensureStream(js, cfg.TradesTopic, 720*time.Hour); err != nil {
		return nil, err
	}

	sub, err := js.SubscribeSync(
		strings.TrimSpace(cfg.TradesTopic),
		nats.BindStream(streamName(cfg.TradesTopic)),
		nats.DeliverAll(),
		nats.OrderedConsumer(),
	)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()

	events := make([]engine.TradeEvent, 0)
	for {
		msg, nextErr := sub.NextMsg(150 * time.Millisecond)
		if nextErr != nil {
			if nextErr == nats.ErrTimeout {
				break
			}
			return nil, nextErr
		}
		var event engine.TradeEvent
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			logger.Warn("failed to unmarshal historical trade", zap.Error(err))
			continue
		}
		if !since.IsZero() && event.Time.Before(since) {
			continue
		}
		events = append(events, event)
	}
	return events, nil
}
