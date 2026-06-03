package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"fast_movers_stratergy/internal/config"
	"fast_movers_stratergy/internal/dashboard"
	"fast_movers_stratergy/internal/jetstream"
	"fast_movers_stratergy/internal/strategy"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	engine, err := strategy.NewEngine(strategy.EngineConfig{Strategy: cfg.Strategy})
	if err != nil {
		log.Fatalf("engine init error: %v", err)
	}
	consumer, err := jetstream.NewTicksConsumer(cfg.Kafka.BootstrapServers, cfg.Kafka.TicksTopic, cfg.Kafka.GroupID, cfg.Kafka.StartupReplayGraceSec)
	if err != nil {
		log.Fatalf("ticks consumer error: %v", err)
	}
	defer consumer.Close()
	producer, err := jetstream.NewSignalProducer(cfg.Kafka.BootstrapServers, cfg.Kafka.SignalTopic)
	if err != nil {
		log.Fatalf("signal producer error: %v", err)
	}
	defer producer.Close()

	store := dashboard.NewStore()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc(cfg.Service.DashboardPath, store.Handler)
	server := &http.Server{Addr: cfg.Service.MetricsAddress, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server error: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = server.Close()
			return
		case <-ticker.C:
			store.Update(dashboard.Snapshot{
				ActiveSymbols:  engine.ActiveSymbols(),
				LastTickTime:   engine.LastTickTime(),
				SignalsTotal:   engine.SignalsTotal(),
				Strategy1Total: engine.Strategy1Total(),
				Strategy2Total: engine.Strategy2Total(),
				Strategy3Total: engine.Strategy3Total(),
				Strategy4Total: engine.Strategy4Total(),
			})
		default:
			pollCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			tick, err := consumer.Poll(pollCtx)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			if tick == nil {
				continue
			}
			signals := engine.OnTick(*tick)
			_ = consumer.Commit()
			for _, signal := range signals {
				if err := producer.PublishSignal(signal); err != nil {
					log.Printf("publish signal failed: %v", err)
				}
			}
		}
	}
}
