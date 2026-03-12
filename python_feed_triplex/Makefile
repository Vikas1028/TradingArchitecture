APP_NAME := python_feed_triplex
BINARY_NAME := python_feed_triplex
MAIN_ENTRY := main/main.py
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: release install-service uninstall-service

release:
	mkdir -p $(RELEASE_DIR)
	rsync -a --exclude 'logs' main internal config metrics.py __init__.py $(RELEASE_DIR)/
	mkdir -p $(RELEASE_DIR)/logs $(RELEASE_DIR)/scripts
	cp internal/scripts/python_feed.properties $(RELEASE_DIR)/scripts/python_feed.properties
	cp internal/scripts/start_mac.sh $(RELEASE_DIR)/scripts/start_mac.sh
	cp internal/scripts/stop_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	cp internal/scripts/python_feed.plist $(RELEASE_DIR)/scripts/python_feed.plist
	chmod +x $(RELEASE_DIR)/scripts/start_mac.sh $(RELEASE_DIR)/scripts/stop_mac.sh
	@echo "UAT release created at $(RELEASE_DIR)"

install-service:
	@echo "Use start_mac.sh to load the launchd plist on macOS; systemd not applicable."

uninstall-service:
	@echo "Use stop_mac.sh to unload the launchd agent on macOS."
