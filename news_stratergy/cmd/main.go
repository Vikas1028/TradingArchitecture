package main

import (
	"context"
	"flag"
	"net/http"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"news_strategy/config"
	"news_strategy/logger"
	"news_strategy/service"
)

// main loads config, initializes the logger, starts the MVP news strategy API,
// and keeps the service alive until shutdown.
func main() {
	configPath := flag.String("config", "", "path to news strategy config")
	flag.Parse()
	if err := run(*configPath); err != nil {
		zap.L().Fatal("news_strategy exited with error", zap.Error(err))
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	log, err := logger.NewLogger()
	if err != nil {
		return err
	}
	defer log.Sync() //nolint:errcheck
	zap.ReplaceGlobals(log)

	service.SeedSampleEvents(cfg)
	app, err := service.New(cfg, log)
	if err != nil {
		return err
	}
	defer app.Close()

	server := &http.Server{
		Addr:    cfg.Service.BindAddress,
		Handler: app.Handler(),
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	app.Start(ctx)

	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	log.Info("news strategy service started",
		zap.String("bind", cfg.Service.BindAddress),
		zap.String("dashboard_path", cfg.Service.DashboardPath),
		zap.String("health_path", cfg.Service.HealthPath),
		zap.String("ingest_path", cfg.Service.AdminIngest),
	)
	return server.ListenAndServe()
}
