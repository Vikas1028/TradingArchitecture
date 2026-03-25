package main

import (
	"context"
	"go_feed/common"
	appConfig "go_feed/config"
	appKafka "go_feed/kafka"
	appLogger "go_feed/logger"
	"go_feed/monitor"
	appPostgres "go_feed/postgres"
	appWebsocket "go_feed/websocket"
)

var AppRuntime = common.RuntimeChannels{
	SignalQuitChan: make(chan struct{}),
	ReadLoopDone:   make(chan struct{}),
}

// RunApp performs application startup tasks and returns an error if initialization fails.
func RunApp(ctx context.Context) error {
	if err := appLogger.InitializeLogger(); err != nil {
		return err
	}
	appLogger.Infof("logger initialization completed")
	monitor.Initialize()

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

	if err := appKafka.Connection(ctx); err != nil {
		return err
	}
	appLogger.Infof("kafka connection initialization completed")

	if err := appPostgres.Connection(ctx); err != nil {
		return err
	}
	appLogger.Infof("postgres connection initialization completed")

	if !dataPlanActive {
		appLogger.Warnf("skipping websocket startup because dhan data plan is not active")
		close(AppRuntime.ReadLoopDone)
		return nil
	}

	if err := appWebsocket.ClientLogin(); err != nil {
		return err
	}
	appLogger.Infof("websocket login initialization completed")

	if err := appWebsocket.Connection(); err != nil {
		return err
	}
	appLogger.Infof("websocket connection initialization completed")

	if err := appWebsocket.Subscribe(); err != nil {
		return err
	}
	appLogger.Infof("subscription initialization completed")

	appLogger.Infof("starting websocket read loop")
	go func() {
		defer close(AppRuntime.ReadLoopDone)
		appWebsocket.StartReadLoop(ctx)
	}()
	return nil
}
