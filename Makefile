JSONL ?= out/annotations.jsonl
DB ?= out/annotations.db

.PHONY: help migrate report test

help:
	@echo "Targets:"
	@echo "  make migrate JSONL=out/annotations.jsonl DB=out/annotations.db"
	@echo "  make report DB=out/annotations.db"
	@echo "  make test"

migrate:
	go run . migrate --in_jsonl "$(JSONL)" --out_db "$(DB)"

report:
	go run . report --db "$(DB)"

test:
	go test ./...
