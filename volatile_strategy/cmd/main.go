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

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"volatile_strategy/common"
	appConfig "volatile_strategy/config"
	appLogger "volatile_strategy/logger"
	"volatile_strategy/monitor"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			stackTrace := fmt.Sprintf("panic recovered: %v\n%s", r, string(debug.Stack()))
			if appLogger.IsInitialized() {
				appLogger.Errorf("%s", stackTrace)
			} else {
				log.Printf("%s", stackTrace)
			}
			appLogger.WriteStackTrace(stackTrace)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle(common.MetricsPath, promhttp.Handler())

	if err := RunApp(ctx); err != nil {
		if appLogger.IsInitialized() {
			appLogger.Fatalf("failed to start %s: %v", common.AppName, err)
		}
		log.Fatalf("failed to start %s: %v", common.AppName, err)
	}

	metricsAddress := common.DefaultMetricsAddress
	dashboardPath := common.DefaultDashboardPath
	if appConfig.GlobalConfig != nil {
		if appConfig.GlobalConfig.Service.MetricsAddress != "" {
			metricsAddress = appConfig.GlobalConfig.Service.MetricsAddress
		}
		if appConfig.GlobalConfig.Service.DashboardPath != "" {
			dashboardPath = appConfig.GlobalConfig.Service.DashboardPath
		}
	}
	mux.HandleFunc(dashboardPath, monitor.DashboardHandler)
	go func() {
		if err := http.ListenAndServe(metricsAddress, mux); err != nil {
			if appLogger.IsInitialized() {
				appLogger.Warnf("metrics server stopped: %v", err)
			}
		}
	}()

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-signalChannel:
		monitor.Shutdown()
		cancel()
	case err := <-AppRuntime.Err:
		monitor.Shutdown()
		cancel()
		if err != nil {
			appLogger.Errorf("volatile_strategy runtime stopped: %v", err)
		}
	}
	<-AppRuntime.Done
	_ = appLogger.CloseLogger()
}
