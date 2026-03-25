package websocket

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	appConfig "go_feed/config"
	appLogger "go_feed/logger"
	"go_feed/monitor"
)

const dhanProfileURL = "https://api.dhan.co/v2/profile"

type profileValidationResponse struct {
	DhanClientID  string `json:"dhanClientId"`
	TokenValidity string `json:"tokenValidity"`
	ActiveSegment string `json:"activeSegment"`
	DDPI          string `json:"ddpi"`
	MTF           string `json:"mtf"`
	DataPlan      string `json:"dataPlan"`
	DataValidity  string `json:"dataValidity"`
}

var profileDataPlanActive = true

// ValidateProfileAccess is temporary validation-only logic to inspect Dhan account entitlements.
// Remove this helper once websocket/data-plan verification is no longer needed during startup debugging.
func ValidateProfileAccess() (bool, error) {
	if appConfig.GlobalConfig == nil {
		return false, fmt.Errorf("application config is not initialized")
	}

	req, err := http.NewRequest(http.MethodGet, dhanProfileURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("access-token", appConfig.GlobalConfig.DhanClient.AccessToken)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("profile validation failed with status=%s", resp.Status)
	}

	var profile profileValidationResponse
	if err := json.NewDecoder(resp.Body).Decode(&profile); err != nil {
		return false, fmt.Errorf("failed to decode profile validation response: %w", err)
	}

	profileDataPlanActive = strings.EqualFold(profile.DataPlan, "Active")

	appLogger.Infof(
		"dhan profile validation token_validity=%s data_plan=%s data_validity=%s active_segment=%s",
		profile.TokenValidity,
		profile.DataPlan,
		profile.DataValidity,
		profile.ActiveSegment,
	)

	if profile.DhanClientID != "" && profile.DhanClientID != appConfig.GlobalConfig.DhanClient.ClientID {
		appLogger.Warnf(
			"dhan profile validation returned client_id=%s but config has client_id=%s",
			profile.DhanClientID,
			appConfig.GlobalConfig.DhanClient.ClientID,
		)
	}
	if !profileDataPlanActive {
		appLogger.Warnf(
			"dhan profile validation indicates data plan is not active data_plan=%s data_validity=%s; websocket market feed may disconnect immediately",
			profile.DataPlan,
			profile.DataValidity,
		)
		monitor.RecordError("websocket")
	}

	return profileDataPlanActive, nil
}

// IsProfileDataPlanActive returns the latest validation-only data-plan status.
func IsProfileDataPlanActive() bool {
	return profileDataPlanActive
}
