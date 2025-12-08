APP_NAME := paper_engine
BINARY_NAME := paper_engine
MAIN_PKG := ./cmd/paper_engine
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: release install-service uninstall-service

release:
	go build -o $(BINARY_NAME) $(MAIN_PKG)
	mkdir -p $(RELEASE_DIR)/cmd $(RELEASE_DIR)/config $(RELEASE_DIR)/logs $(RELEASE_DIR)/scripts
	cp $(BINARY_NAME) $(RELEASE_DIR)/cmd/$(BINARY_NAME)
	cp config/paper_engine_config.json $(RELEASE_DIR)/config/paper_engine_config.json
	cp internal/scripts/paper_engine.properties $(RELEASE_DIR)/scripts/paper_engine.properties
	cp internal/scripts/start_mac.sh $(RELEASE_DIR)/scripts/start_mac.sh
	cp internal/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	cp internal/scripts/paper_engine.plist $(RELEASE_DIR)/scripts/paper_engine.plist
	chmod +x $(RELEASE_DIR)/scripts/start_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	@echo "UAT release created at $(RELEASE_DIR)"

install-service:
	@echo "Systemd install not applicable on macOS. Use start_mac.sh to load the launchd plist."

uninstall-service:
	@echo "Systemd uninstall not applicable on macOS. Use stop_mac.sh to unload the launchd plist."
