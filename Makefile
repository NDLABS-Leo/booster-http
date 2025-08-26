# Makefile for retrieval-http service

APP_NAME = index-http
GO_FILES = $(wildcard *.go)
VERSION = 1.0.0

.PHONY: all build clean run

all: build

build:
	@echo "Building $(APP_NAME)..."
	@go build -o $(APP_NAME) $(GO_FILES)
	@echo "Build complete."

clean:
	@echo "Cleaning up..."
	@rm -f $(APP_NAME)
	@echo "Clean complete."

run: build
	@echo "Running $(APP_NAME)..."
	./$(APP_NAME)