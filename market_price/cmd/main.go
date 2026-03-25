package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"market_price/common"
	"market_price/internal/config"
	"market_price/internal/kafka"
	"market_price/internal/logging"
	"market_price/internal/metrics"
	"market_price/internal/service"
	"market_price/internal/web"
)

func main() {
	configPath := flag.String("config", "", "path to market_price config")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("market_price exited with error", zap.Error(err))
	}
}

// run connects to each live price source, serves the dashboard endpoint, and
// keeps the latest per-symbol prices in memory for the UI.
func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	logger, err := logging.NewLogger()
	if err != nil {
		return err
	}
	defer logger.Sync() //nolint:errcheck
	zap.ReplaceGlobals(logger)

	if err := metrics.InitAndServe(cfg.Service.MetricsBindAddress, cfg.Service.MetricsPath); err != nil {
		return err
	}

	sourceNames := make([]string, 0, len(cfg.Kafka.Topics))
	for _, topic := range cfg.Kafka.Topics {
		sourceNames = append(sourceNames, topic.Name)
	}
	store := service.NewStore(sourceNames)

	server := &http.Server{
		Addr:    cfg.Service.BindAddress,
		Handler: web.NewServer(store),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
		_ = server.Shutdown(context.Background())
	}()

	updates := make(chan kafka.Update, 4096)
	consumers := make([]*kafka.Consumer, 0, len(cfg.Kafka.Topics))
	for _, topic := range cfg.Kafka.Topics {
		consumer := kafka.NewConsumer(cfg.Kafka, topic, logger)
		consumers = append(consumers, consumer)
		store.SetConnected(topic.Name, false)
		go func(topic common.TopicConfig, consumer *kafka.Consumer) {
			store.SetConnected(topic.Name, true)
			if err := consumer.Run(ctx, updates); err != nil && ctx.Err() == nil {
				logger.Error("market_price consumer stopped", zap.String("source", topic.Name), zap.Error(err))
				metrics.ErrorsTotal.Inc()
				store.SetConnected(topic.Name, false)
			}
		}(topic, consumer)
	}
	defer func() {
		for _, consumer := range consumers {
			_ = consumer.Close()
		}
	}()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("market_price dashboard server failed", zap.Error(err))
			metrics.ErrorsTotal.Inc()
			cancel()
		}
	}()

	logger.Info("market_price started",
		zap.String("bind", cfg.Service.BindAddress),
		zap.Int("topics", len(cfg.Kafka.Topics)),
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case update := <-updates:
			store.Apply(common.TopicPrice{
				Price:     update.Price,
				Timestamp: update.Timestamp,
			}, update.Source, update.Symbol)
		}
	}
}
