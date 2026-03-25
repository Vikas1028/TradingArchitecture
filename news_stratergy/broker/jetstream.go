package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"news_strategy/common"
)

type Client struct {
	conn *nats.Conn
	js   nats.JetStreamContext
	cfg  common.BrokerConfig
}

// NewClient connects to JetStream and prepares the stream used by the news pipeline.
func NewClient(cfg common.BrokerConfig) (*Client, error) {
	nc, err := nats.Connect(cfg.URL, nats.MaxReconnects(-1), nats.ReconnectWait(2*time.Second))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}
	client := &Client{conn: nc, js: js, cfg: cfg}
	if err := client.ensureStream(); err != nil {
		nc.Close()
		return nil, err
	}
	return client, nil
}

func (c *Client) Close() {
	if c == nil || c.conn == nil {
		return
	}
	_ = c.conn.Drain()
	c.conn.Close()
}

// PublishJSON writes one structured pipeline event into the matching subject.
func (c *Client) PublishJSON(ctx context.Context, subject string, payload any) error {
	if c == nil || c.js == nil {
		return nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = c.js.Publish(subject, body, nats.Context(ctx))
	return err
}

func (c *Client) ensureStream() error {
	const streamName = "NEWS_PIPELINE"
	subjects := []string{
		c.cfg.RawSubject,
		c.cfg.NormalizedSubject,
		c.cfg.ClassifiedSubject,
		c.cfg.EventsSubject,
		c.cfg.SignalsSubject,
		c.cfg.AlertsSubject,
	}
	if info, err := c.js.StreamInfo(streamName); err == nil && info != nil {
		return nil
	}
	_, err := c.js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  trimSubjects(subjects),
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardOld,
		MaxAge:    7 * 24 * time.Hour,
		Replicas:  1,
	})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") && !strings.Contains(strings.ToLower(err.Error()), "in use") {
		return fmt.Errorf("ensure stream: %w", err)
	}
	return nil
}

func trimSubjects(subjects []string) []string {
	out := make([]string, 0, len(subjects))
	for _, subject := range subjects {
		if value := strings.TrimSpace(subject); value != "" {
			out = append(out, value)
		}
	}
	return out
}
