GO ?= go
APP_PKG := ./cmd/sales_annotator
BINARY := bin/sales_annotator

INPUT_DIR ?= /Users/ablackman/data/sales-transcripts/data/chunked_transcripts
OUT_JSONL ?= out/annotations.jsonl
MODEL ?= gpt-5.2-2025-12-11
LIMIT ?= 0
FILTER_PREFIX ?=

RUN_ARGS := --input_dir $(INPUT_DIR) --out_jsonl $(OUT_JSONL) --model $(MODEL) --limit $(LIMIT)
ifneq ($(strip $(FILTER_PREFIX)),)
RUN_ARGS += --filter_prefix $(FILTER_PREFIX)
endif

.PHONY: help fmt vet test build run run-one check clean

help: ## Show available targets
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*## "}; /^[a-zA-Z0-9_-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Runtime variables:"
	@echo "  INPUT_DIR      default: $(INPUT_DIR)"
	@echo "  OUT_JSONL      default: $(OUT_JSONL)"
	@echo "  MODEL          default: $(MODEL)"
	@echo "  LIMIT          default: $(LIMIT)"
	@echo "  FILTER_PREFIX  default: $(FILTER_PREFIX)"
	@echo ""
	@echo "Note: OPENAI_API_KEY must be set for 'run' targets."

fmt: ## Format Go files
	@files="$$(rg --files -g '*.go' 2>/dev/null || find . -name '*.go')"; \
	if [ -n "$$files" ]; then gofmt -w $$files; fi

vet: ## Run go vet
	$(GO) vet ./...

test: ## Run unit tests
	$(GO) test ./...

build: ## Build binary into ./bin
	@mkdir -p bin
	$(GO) build -o $(BINARY) $(APP_PKG)

run: ## Run annotator with current variables
	$(GO) run $(APP_PKG) $(RUN_ARGS)

run-one: ## Run annotator for one conversation (LIMIT=1)
	$(MAKE) run LIMIT=1

check: fmt vet test ## Run local quality checks

clean: ## Remove build artifacts
	rm -rf bin
