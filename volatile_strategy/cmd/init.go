package main

import (
	"context"

	appConfig "volatile_strategy/config"
	appKafka "volatile_strategy/kafka"
	appLogger "volatile_strategy/logger"
	"volatile_strategy/monitor"
	appStrategy "volatile_strategy/strategy"
)

var (
	AppRuntime = struct {
		Done chan struct{}
		Err  chan error
	}{
		Done: make(chan struct{}),
		Err:  make(chan error, 1),
	}
)

func RunApp(ctx context.Context) error {
	if err := appLogger.InitializeLogger(); err != nil {
		return err
	}
	monitor.Initialize()
	if err := appConfig.InitializeConfig(); err != nil {
		return err
	}

	cfg := appConfig.MustConfig()
	consumer := appKafka.NewLTPConsumer(cfg.Kafka)

	producer, err := appKafka.NewSignalProducer(cfg.Kafka)
	if err != nil {
		return err
	}

	engine, err := appStrategy.NewEngine(cfg.Strategy)
	if err != nil {
		return err
	}

	appLogger.Infof("volatile_strategy started input_topic=%s signal_topic=%s", cfg.Kafka.InputTopic, cfg.Kafka.SignalTopic)
	go func() {
		defer close(AppRuntime.Done)
		defer consumer.Close()
		defer producer.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			tick, err := consumer.Read(ctx)
			if err != nil {
				if ctx.Err() == nil {
					select {
					case AppRuntime.Err <- err:
					default:
					}
				}
				return
			}
			if tick == nil {
				continue
			}
			monitor.RecordTick(tick.Timestamp)
			signals, windowsClosed, leaders := engine.OnTick(*tick)
			if windowsClosed > 0 {
				for i := 0; i < windowsClosed; i++ {
					monitor.RecordWindowClosed()
				}
			}
			if leaders != nil {
				monitor.SetLeaders(leaders)
				for side, ranks := range leaders {
					for _, rank := range ranks {
						appLogger.Infof("window leaders side=%s symbol=%s open=%.2f close=%.2f change_pct=%.4f window_start=%s window_end=%s",
							side, rank.Symbol, rank.Open, rank.Close, rank.ChangePct, rank.WindowStart, rank.WindowEnd)
					}
				}
			}
			monitor.SetWatchers(engine.ActiveWatchers())
			for _, signal := range signals {
				if err := producer.PublishSignal(ctx, signal); err != nil {
					if ctx.Err() == nil {
						select {
						case AppRuntime.Err <- err:
						default:
						}
					}
					return
				}
				monitor.RecordSignal(signal)
				appLogger.Infof("signal emitted symbol=%s side=%s reason=%s", signal.Symbol, signal.Side, signal.Reason)
			}
		}
	}()
	return nil
}
