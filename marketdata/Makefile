APP_NAME := marketdata
BINARY_NAME := marketdata
MAIN_PKG := ./cmd/marketdata
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: release install-service uninstall-service

release:
	go build -o $(BINARY_NAME) $(MAIN_PKG)
	mkdir -p $(RELEASE_DIR)/cmd $(RELEASE_DIR)/config $(RELEASE_DIR)/logs $(RELEASE_DIR)/scripts
	cp $(BINARY_NAME) $(RELEASE_DIR)/cmd/$(BINARY_NAME)
	cp cmd/logger.json $(RELEASE_DIR)/cmd/logger.json
	cp config/marketdata_config.json $(RELEASE_DIR)/config/marketdata_config.json
	cp internal/scripts/marketdata.properties $(RELEASE_DIR)/scripts/marketdata.properties
	cp internal/scripts/start_mac.sh $(RELEASE_DIR)/scripts/start_mac.sh
	cp internal/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	cp internal/scripts/marketdata.plist $(RELEASE_DIR)/scripts/marketdata.plist
	chmod +x $(RELEASE_DIR)/scripts/start_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	@echo "UAT release created at $(RELEASE_DIR)"

install-service:
	@echo "Use launchctl with the plist (start_mac.sh) on macOS; systemd not applicable."

uninstall-service:
	@echo "Use stop_mac.sh to unload the launchd agent on macOS."
