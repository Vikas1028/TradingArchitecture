APP_NAME := vwap_strategy
BINARY_NAME := vwap_strategy
MAIN_PKG := ./cmd/vwap_strategy
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: release install-service uninstall-service

release:
	go build -o $(BINARY_NAME) $(MAIN_PKG)
	mkdir -p $(RELEASE_DIR)/cmd $(RELEASE_DIR)/config $(RELEASE_DIR)/logs $(RELEASE_DIR)/scripts
	cp $(BINARY_NAME) $(RELEASE_DIR)/cmd/$(BINARY_NAME)
	cp cmd/logger.json $(RELEASE_DIR)/cmd/logger.json
	cp config/vwap_strategy_config.json $(RELEASE_DIR)/config/vwap_strategy_config.json
	cp internal/scripts/vwap_strategy.properties $(RELEASE_DIR)/scripts/vwap_strategy.properties
	cp internal/scripts/start_mac.sh $(RELEASE_DIR)/scripts/start_mac.sh
	cp internal/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	cp internal/scripts/vwap_strategy.plist $(RELEASE_DIR)/scripts/vwap_strategy.plist
	chmod +x $(RELEASE_DIR)/scripts/start_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	@echo "UAT release created at $(RELEASE_DIR)"

install-service:
	@echo "Use launchctl with the plist (start_mac.sh) on macOS; systemd not applicable."

uninstall-service:
	@echo "Use stop_mac.sh to unload the launchd agent on macOS."
