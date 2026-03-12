APP_NAME := first_candle_strategy
BINARY_NAME := first_candle_strategy
MAIN_PKG := ./cmd/first_candle_strategy
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: build release deploy-usr-local install-service uninstall-service

build:
	go build -o $(BINARY_NAME) $(MAIN_PKG)

release:
	go build -o $(BINARY_NAME) $(MAIN_PKG)
	mkdir -p $(RELEASE_DIR)/cmd $(RELEASE_DIR)/config $(RELEASE_DIR)/logs $(RELEASE_DIR)/scripts
	cp $(BINARY_NAME) $(RELEASE_DIR)/cmd/$(BINARY_NAME)
	cp config/first_candle_strategy_config.json $(RELEASE_DIR)/config/first_candle_strategy_config.json
	cp internal/scripts/first_candle_strategy.properties $(RELEASE_DIR)/scripts/first_candle_strategy.properties
	cp internal/scripts/start_mac.sh $(RELEASE_DIR)/scripts/start_mac.sh
	cp internal/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	cp internal/scripts/install_to_usr_local.sh $(RELEASE_DIR)/scripts/install_to_usr_local.sh
	cp internal/scripts/first_candle_strategy.plist $(RELEASE_DIR)/scripts/first_candle_strategy.plist
	chmod +x $(RELEASE_DIR)/scripts/start_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/install_to_usr_local.sh
	@echo "UAT release created at $(RELEASE_DIR)"

deploy-usr-local:
	./internal/scripts/install_to_usr_local.sh

install-service:
	@echo "Use launchctl with the plist (start_mac.sh) on macOS; systemd not applicable."

uninstall-service:
	@echo "Use stop_mac.sh to unload the launchd agent on macOS."
