APP_NAME := trading_dashboard
BINARY_NAME := trading_dashboard
MAIN_PKG := ./cmd
RELEASE_DIR := ../uat-release/$(APP_NAME)

.PHONY: build run uat-release

build:
	go build -o ./cmd/$(BINARY_NAME) $(MAIN_PKG)

run:
	go run $(MAIN_PKG)

uat-release:
	mkdir -p $(RELEASE_DIR)
	mkdir -p $(RELEASE_DIR)/cmd
	go mod tidy
	go build -o $(RELEASE_DIR)/cmd/$(BINARY_NAME) $(MAIN_PKG)
	cp cmd/config.json $(RELEASE_DIR)/cmd/config.json
	@echo "UAT release created at $(RELEASE_DIR)"
