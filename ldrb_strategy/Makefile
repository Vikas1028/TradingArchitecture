APP_NAME := ldrb_strategy
BINARY_NAME := ldrb_strategy
MAIN_PKG := ./cmd/ldrb_strategy
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: build release deploy-usr-local install-service uninstall-service

build:
	go build -o $(BINARY_NAME) $(MAIN_PKG)

release:
	go build -o $(BINARY_NAME) $(MAIN_PKG)
	mkdir -p $(RELEASE_DIR)/cmd $(RELEASE_DIR)/config $(RELEASE_DIR)/logs $(RELEASE_DIR)/scripts $(RELEASE_DIR)/data
	cp $(BINARY_NAME) $(RELEASE_DIR)/cmd/$(BINARY_NAME)
	cp config/ldrb_strategy_config.json $(RELEASE_DIR)/config/ldrb_strategy_config.json
	cp internal/scripts/ldrb_strategy.properties $(RELEASE_DIR)/scripts/ldrb_strategy.properties
	cp internal/scripts/start_mac.sh $(RELEASE_DIR)/scripts/start_mac.sh
	cp internal/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	cp internal/scripts/install_to_usr_local.sh $(RELEASE_DIR)/scripts/install_to_usr_local.sh
	cp internal/scripts/ldrb_strategy.plist $(RELEASE_DIR)/scripts/ldrb_strategy.plist
	cp data/events_next_day.json $(RELEASE_DIR)/data/events_next_day.json
	cp data/volume_profile_10d.json $(RELEASE_DIR)/data/volume_profile_10d.json
	chmod +x $(RELEASE_DIR)/scripts/start_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/install_to_usr_local.sh
	@echo "UAT release created at $(RELEASE_DIR)"

deploy-usr-local:
	./internal/scripts/install_to_usr_local.sh

install-service:
	@echo "Use launchctl with start_mac.sh on macOS."

uninstall-service:
	@echo "Use stop_mac.sh to unload the launchd agent on macOS."

