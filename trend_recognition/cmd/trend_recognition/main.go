package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"trend_recognition/internal/config"
	"trend_recognition/internal/service"
)

// main loads config, starts the strategy service, and waits for shutdown signals.
func main() {
	configPath := flag.String("config", "", "path to trend_recognition_config.json")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	svc, err := service.New(cfg)
	if err != nil {
		log.Fatalf("service init error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		log.Fatalf("service start error: %v", err)
	}
}
