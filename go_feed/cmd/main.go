package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go_feed/common"
	appConfig "go_feed/config"
	appKafka "go_feed/kafka"
	appLogger "go_feed/logger"
	"go_feed/monitor"
	appPostgres "go_feed/postgres"
	appWebsocket "go_feed/websocket"
)

var (
	version = common.AppVersion
)

// main bootstraps metrics, starts the application, and handles panic recovery.
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			stackTrace := fmt.Sprintf(common.PanicStackTraceFormat, string(debug.Stack()), r)
			if appLogger.IsInitialized() {
				appLogger.Errorf(common.PanicRecoverLogFormat, r)
				appLogger.Errorf("%s", stackTrace)
			} else {
				log.Printf(common.PanicRecoverLogFormat, r)
				log.Printf("%s", stackTrace)
			}
			appLogger.WriteStackTrace(stackTrace)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle(common.MetricsPath, promhttp.Handler())

	if err := RunApp(ctx); err != nil {
		if appLogger.IsInitialized() {
			appLogger.Fatalf(common.RunAppFailureLogFormat, common.AppName, err)
		}
		log.Fatalf(common.RunAppFailureLogFormat, common.AppName, err)
	}

	metricsAddress := common.DefaultMetricsAddress
	if appConfig.GlobalConfig != nil && appConfig.GlobalConfig.Service.MetricsAddress != "" {
		metricsAddress = appConfig.GlobalConfig.Service.MetricsAddress
	}
	dashboardPath := common.DefaultDashboardPath
	if appConfig.GlobalConfig != nil && appConfig.GlobalConfig.Service.DashboardPath != "" {
		dashboardPath = appConfig.GlobalConfig.Service.DashboardPath
	}
	mux.HandleFunc(dashboardPath, monitor.DashboardHandler)
	appLogger.Infof("starting metrics server on %s", metricsAddress)
	go func() {
		if err := http.ListenAndServe(metricsAddress, mux); err != nil {
			if appLogger.IsInitialized() {
				appLogger.Warnf("metrics server stopped: %v", err)
				return
			}
			log.Printf("metrics server stopped: %v", err)
		}
	}()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChannel
		appLogger.Warnf("shutdown signal received, closing application resources")
		cancel()
		_ = appWebsocket.Close()
		<-AppRuntime.ReadLoopDone

		drainTimeout := time.Duration(common.DefaultShutdownDrainSec) * time.Second
		if appConfig.GlobalConfig != nil && appConfig.GlobalConfig.Pipeline.ShutdownDrainTimeoutSec > 0 {
			drainTimeout = time.Duration(appConfig.GlobalConfig.Pipeline.ShutdownDrainTimeoutSec) * time.Second
		}
		drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
		defer drainCancel()

		_ = appKafka.Close(drainCtx)
		_ = appPostgres.Close(drainCtx)
		monitor.Shutdown()
		_ = appLogger.CloseLogger()
		close(AppRuntime.SignalQuitChan)
	}()

	<-AppRuntime.SignalQuitChan
}
