package main

import (
	"context"
	"go_ltp/common"
	appConfig "go_ltp/config"
	appKafka "go_ltp/kafka"
	appLogger "go_ltp/logger"
	appLTP "go_ltp/ltp"
	"go_ltp/monitor"
	appWebsocket "go_ltp/websocket"
)

var AppRuntime = common.RuntimeChannels{
	SignalQuitChan: make(chan struct{}),
	ReadLoopDone:   make(chan struct{}),
}

// RunApp performs application startup tasks and returns an error if initialization fails.
func RunApp(ctx context.Context) error {
	// Initialize logging first so all later startup failures land in the log file.
	if err := appLogger.InitializeLogger(); err != nil {
		return err
	}
	appLogger.Infof("logger initialization completed")
	monitor.Initialize()

	// Load config before touching external systems so all clients use one shared config snapshot.
	if err := appConfig.InitializeConfig(); err != nil {
		return err
	}
	appLogger.Infof("config initialization completed")

	// Temporary validation-only startup check for Dhan account/feed entitlements.
	// Remove this block once websocket/data-plan troubleshooting is complete.
	dataPlanActive := true
	dataPlanActive, err := appWebsocket.ValidateProfileAccess()
	if err != nil {
		appLogger.Warnf("dhan profile validation could not be completed: %v", err)
		dataPlanActive = true
	} else {
		appLogger.Infof("dhan profile validation completed")
	}

	// Kafka must be ready before polling starts because every LTP snapshot is published immediately.
	if err := appKafka.Connection(ctx); err != nil {
		return err
	}
	appLogger.Infof("kafka connection initialization completed")

	if !dataPlanActive {
		appLogger.Warnf("skipping LTP polling startup because dhan data plan is not active")
		close(AppRuntime.ReadLoopDone)
		return nil
	}

	if err := appLTP.Initialize(); err != nil {
		return err
	}
	appLogger.Infof("LTP polling initialization completed")

	appLogger.Infof("starting LTP polling loop")
	go func() {
		// Close the runtime done channel when the poll loop exits so shutdown can proceed cleanly.
		defer close(AppRuntime.ReadLoopDone)
		appLTP.StartPollLoop(ctx)
	}()
	return nil
}
