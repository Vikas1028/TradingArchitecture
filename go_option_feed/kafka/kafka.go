package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
	"go_option_feed/common"
	appConfig "go_option_feed/config"
	appLogger "go_option_feed/logger"
	"go_option_feed/monitor"
)

var (
	GlobalConn *nats.Conn
	GlobalJS   nats.JetStreamContext

	queue       chan outboundMessage
	workerGroup sync.WaitGroup
	closeOnce   sync.Once
)

type FeedMessage struct {
	Key       string                  `json:"key"`
	Timestamp time.Time               `json:"timestamp"`
	Payload   common.KafkaTickMessage `json:"payload"`
}

type outboundMessage struct {
	subject string
	payload []byte
	key     string
}

func Connection(ctx context.Context) error {
	appLogger.Debugf("starting JetStream connection initialization")
	if appConfig.GlobalConfig == nil {
		err := fmt.Errorf("application config is not initialized")
		appLogger.Errorf("jetstream connection initialization failed: %v", err)
		monitor.RecordError("kafka")
		return err
	}

	cfg := appConfig.GlobalConfig.Kafka
	if strings.TrimSpace(cfg.Brokers) == "" {
		err := fmt.Errorf("jetstream url is required")
		appLogger.Errorf("jetstream configuration validation failed: %v", err)
		monitor.RecordError("kafka")
		return err
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		err := fmt.Errorf("tick subject prefix is required")
		appLogger.Errorf("jetstream configuration validation failed: %v", err)
		monitor.RecordError("kafka")
		return err
	}

	if err := connect(cfg.Brokers, cfg.Topic); err != nil {
		appLogger.Fatalf("jetstream connection failed: %v", err)
		return err
	}

	queue = make(chan outboundMessage, appConfig.GlobalConfig.Pipeline.KafkaQueueSize)
	monitor.SetKafkaQueueDepth(0)

	for workerIndex := 0; workerIndex < appConfig.GlobalConfig.Pipeline.KafkaWorkers; workerIndex++ {
		workerGroup.Add(1)
		go startWorker(cfg.Brokers, cfg.Topic)
	}

	appLogger.Infof("jetstream connection initialized successfully for subject_prefix=%s", cfg.Topic)
	return nil
}

// PublishMessage wraps the parsed tick in the shared envelope and enqueues it
// for asynchronous JetStream publishing.
func PublishMessage(ctx context.Context, key string, payload common.KafkaTickMessage) error {
	if queue == nil {
		err := fmt.Errorf("jetstream queue is not initialized")
		appLogger.Errorf("jetstream publish failed: %v", err)
		monitor.RecordError("kafka")
		return err
	}

	body, err := json.Marshal(FeedMessage{
		Key:       key,
		Timestamp: time.Now(),
		Payload:   payload,
	})
	if err != nil {
		appLogger.Warnf("message was not inserted into jetstream because marshal failed for key=%s: %v", key, err)
		monitor.RecordError("kafka")
		return err
	}

	message := outboundMessage{
		subject: tickSubject(appConfig.GlobalConfig.Kafka.Topic, "go_option_feed", payload.Symbol),
		payload: body,
		key:     payload.Symbol,
	}

	select {
	case queue <- message:
		monitor.SetKafkaQueueDepth(len(queue))
		appLogger.Debugf("message queued for jetstream for key=%s", key)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func Close(ctx context.Context) error {
	var closeErr error

	closeOnce.Do(func() {
		if queue != nil {
			close(queue)
		}

		done := make(chan struct{})
		go func() {
			workerGroup.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-ctx.Done():
			closeErr = ctx.Err()
		}

		if GlobalConn != nil {
			GlobalConn.Drain()
			GlobalConn.Close()
			GlobalConn = nil
			GlobalJS = nil
		}
		monitor.SetKafkaConnected(false)
		monitor.SetKafkaQueueDepth(0)
	})

	return closeErr
}

// startWorker drains the outbound queue and retries JetStream publish failures
// until the broker connection is re-established.
func startWorker(url, subjectPrefix string) {
	defer workerGroup.Done()

	for message := range queue {
		monitor.SetKafkaQueueDepth(len(queue))
		for {
			writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := writeMessage(writeCtx, message)
			cancel()
			if err != nil {
				appLogger.Warnf("message was not inserted into jetstream for key=%s: %v", message.key, err)
				monitor.RecordError("kafka")
				monitor.SetKafkaConnected(false)
				if reconnect(url, subjectPrefix) != nil {
					time.Sleep(time.Duration(common.DefaultRetryIntervalSec) * time.Second)
					continue
				}
				time.Sleep(time.Duration(common.DefaultRetryIntervalSec) * time.Second)
				continue
			}
			monitor.RecordKafkaInserted()
			appLogger.Debugf("message inserted into jetstream for key=%s", message.key)
			break
		}
	}
}

func writeMessage(ctx context.Context, message outboundMessage) error {
	if GlobalJS == nil {
		return fmt.Errorf("jetstream context is not initialized")
	}
	_, err := GlobalJS.PublishMsg(&nats.Msg{
		Subject: message.subject,
		Data:    message.payload,
		Header: nats.Header{
			"Symbol": []string{message.key},
		},
	}, nats.Context(ctx))
	return err
}

func connect(url, subjectPrefix string) error {
	natsURL := normalizeNATSURL(url)
	conn, err := nats.Connect(natsURL,
		nats.Name("go_option_feed"),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return err
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return err
	}
	if err := ensureTickStream(js, subjectPrefix); err != nil {
		conn.Close()
		return err
	}
	GlobalConn = conn
	GlobalJS = js
	monitor.SetKafkaConnected(true)
	return nil
}

func reconnect(url, subjectPrefix string) error {
	monitor.RecordKafkaReconnect()
	appLogger.Warnf("attempting jetstream reconnect")
	if GlobalConn != nil {
		GlobalConn.Close()
		GlobalConn = nil
		GlobalJS = nil
	}
	return connect(url, subjectPrefix)
}

// ensureTickStream creates the backing stream once for the configured subject
// prefix so the feed can publish symbol-scoped tick subjects safely.
func ensureTickStream(js nats.JetStreamContext, subjectPrefix string) error {
	streamName := streamName(subjectPrefix)
	subjects := []string{strings.TrimSpace(subjectPrefix) + ".>"}
	if info, err := js.StreamInfo(streamName); err == nil && info != nil {
		return nil
	}
	_, err := js.AddStream(&nats.StreamConfig{
		Name:      streamName,
		Subjects:  subjects,
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		Discard:   nats.DiscardOld,
		MaxAge:    72 * time.Hour,
		Replicas:  1,
	})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "in use") && !strings.Contains(strings.ToLower(err.Error()), "already") {
		return err
	}
	return nil
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

func tickSubject(prefix, source, symbol string) string {
	return fmt.Sprintf("%s.%s.%s", strings.TrimSpace(prefix), sanitizeToken(source), sanitizeToken(symbol))
}

func sanitizeToken(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
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
