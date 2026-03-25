APP_NAME := go_feed
BINARY_NAME := go_feed
MAIN_PKG := ./cmd
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: uat-release

uat-release:
	mkdir -p $(RELEASE_DIR)
	mkdir -p $(RELEASE_DIR)/cmd
	mkdir -p $(RELEASE_DIR)/Token
	mkdir -p $(RELEASE_DIR)/stocks
	go mod tidy
	go build -o $(RELEASE_DIR)/cmd/$(BINARY_NAME) $(MAIN_PKG)
	cp cmd/config.cfg $(RELEASE_DIR)/cmd/config.cfg
	cp cmd/logger.json $(RELEASE_DIR)/cmd/logger.json
	cp ../Token/ind_nifty50list.csv $(RELEASE_DIR)/Token/ind_nifty50list.csv
	cp ../stocks/nifty50.csv $(RELEASE_DIR)/stocks/nifty50.csv
	cp ../stocks/nifty100.csv $(RELEASE_DIR)/stocks/nifty100.csv
	cp ../stocks/nifty200.csv $(RELEASE_DIR)/stocks/nifty200.csv
	cp ../stocks/nifty300.csv $(RELEASE_DIR)/stocks/nifty300.csv
	if [ -f ../python_feed/config/nifty500.csv ]; then cp ../python_feed/config/nifty500.csv $(RELEASE_DIR)/stocks/nifty500.csv; else cp ../stocks/nifty500.csv $(RELEASE_DIR)/stocks/nifty500.csv; fi
	@echo "UAT release created at $(RELEASE_DIR)"
