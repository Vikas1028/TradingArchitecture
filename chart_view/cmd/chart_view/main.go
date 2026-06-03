package main

import (
	"flag"
	"log"
	nethttp "net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"chart_view/internal/config"
	apphttp "chart_view/internal/http"
)

func main() {
	configPath := flag.String("config", "config/chart_view_config.json", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	server := &nethttp.Server{
		Addr:              cfg.BindAddress,
		Handler:           apphttp.New(cfg).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	done := make(chan struct{})
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		_ = server.Close()
		close(done)
	}()

	log.Printf("chart_view listening on %s", cfg.BindAddress)
	if err := server.ListenAndServe(); err != nil && err != nethttp.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	<-done
}
