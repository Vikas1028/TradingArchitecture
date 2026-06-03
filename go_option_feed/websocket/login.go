package websocket

import (
	"fmt"
	"net/url"
	"strconv"
	"time"

	"go_option_feed/common"
	appConfig "go_option_feed/config"
	appLogger "go_option_feed/logger"
	"go_option_feed/monitor"
)

var (
	// GlobalSession exposes websocket login output to the rest of the application.
	GlobalSession *common.WebsocketSession
)

// ClientLogin validates websocket credentials and prepares the authenticated Dhan feed URL.
func ClientLogin() error {
	appLogger.Debugf("starting websocket client login flow")
	return loginWithRetry()
}

// loginWithRetry retries websocket session preparation for a limited number of attempts.
func loginWithRetry() error {
	var lastErr error
	for attempt := 1; attempt <= common.DefaultRetryAttempts; attempt++ {
		appLogger.Debugf("attempting websocket client session build attempt=%d/%d", attempt, common.DefaultRetryAttempts)
		if err := buildClientSession(); err != nil {
			lastErr = err
			monitor.RecordClientRelogin()
			monitor.SetClientLoggedIn(false)
			monitor.RecordError("client_login")
			appLogger.Errorf("websocket client session build failed attempt=%d/%d: %v", attempt, common.DefaultRetryAttempts, err)
			time.Sleep(time.Duration(common.DefaultRetryIntervalSec) * time.Second)
			continue
		}
		monitor.SetClientLoggedIn(true)
		appLogger.Infof("websocket client session initialized successfully")
		return nil
	}
	appLogger.Fatalf("websocket client login failed after %d attempts: %v", common.DefaultRetryAttempts, lastErr)
	return lastErr
}

// buildClientSession validates websocket credentials and prepares the authenticated Dhan feed URL.
func buildClientSession() error {
	if appConfig.GlobalConfig == nil {
		err := fmt.Errorf("application config is not initialized")
		appLogger.Errorf("websocket client session build failed: %v", err)
		return err
	}

	clientCfg := appConfig.GlobalConfig.DhanClient
	wsCfg := appConfig.GlobalConfig.DhanWebsocket

	if clientCfg.ClientID == "" {
		err := fmt.Errorf("%s is required in %s", common.ConfigClientIDKey, common.ConfigSectionDhan)
		appLogger.Errorf("websocket client session validation failed: %v", err)
		return err
	}
	if clientCfg.AccessToken == "" {
		err := fmt.Errorf("%s is required in %s", common.ConfigAccessTokenKey, common.ConfigSectionDhan)
		appLogger.Errorf("websocket client session validation failed: %v", err)
		return err
	}
	if wsCfg.URL == "" {
		err := fmt.Errorf("url is required in %s", common.ConfigSectionWebsocket)
		appLogger.Errorf("websocket client session validation failed: %v", err)
		return err
	}

	parsedURL, err := url.Parse(wsCfg.URL)
	if err != nil {
		appLogger.Errorf("failed to parse websocket url %s: %v", wsCfg.URL, err)
		return err
	}

	queryValues := parsedURL.Query()
	queryValues.Set(common.WebsocketVersionKey, strconv.Itoa(wsCfg.Version))
	queryValues.Set(common.WebsocketTokenKey, clientCfg.AccessToken)
	queryValues.Set(common.WebsocketClientIDKey, clientCfg.ClientID)
	queryValues.Set(common.WebsocketAuthTypeKey, strconv.Itoa(wsCfg.AuthType))
	parsedURL.RawQuery = queryValues.Encode()

	GlobalSession = &common.WebsocketSession{
		FeedURL:     parsedURL.String(),
		ClientID:    clientCfg.ClientID,
		AccessToken: clientCfg.AccessToken,
	}
	appLogger.Debugf("websocket session prepared for client_id=%s", clientCfg.ClientID)
	return nil
}
