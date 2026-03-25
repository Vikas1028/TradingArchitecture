package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"trading_dashboard/internal/config"
	"trading_dashboard/internal/source"
	"trading_dashboard/internal/web"
)

const defaultConfigPath = "cmd/config.json"

func main() {
	cfgPath := defaultConfigPath
	if value := os.Getenv("TRADING_DASHBOARD_CONFIG"); value != "" {
		cfgPath = value
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load dashboard config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	poller := source.NewPoller(cfg.Services, time.Duration(cfg.RefreshIntervalSec)*time.Second)
	go poller.Start(ctx)

	server, err := web.NewServer(cfg.PageTitle, poller, cfg.Services)
	if err != nil {
		log.Fatalf("build dashboard server: %v", err)
	}

	httpServer := &http.Server{
		Addr:    cfg.BindAddress,
		Handler: server.Routes(),
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("trading_dashboard listening on %s and polling %d services", cfg.BindAddress, len(cfg.Services))
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("run dashboard server: %v", err)
	}
}
