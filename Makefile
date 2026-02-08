DB ?= out/annotations.db
INPUT_DIR ?= /Users/ablackman/data/sales-transcripts/data/chunked_transcripts
FROM ?= 1
TO ?= 20
MODEL ?= gpt-4.1-mini

.PHONY: help setup annotate analytics debug-release report release-check test

help:
	@echo "Targets:"
	@echo "  make setup DB=out/annotations.db"
	@echo "  OPENAI_API_KEY=... make annotate DB=out/annotations.db INPUT_DIR=/path FROM=1 TO=20 MODEL=gpt-4.1-mini"
	@echo "  make analytics DB=out/annotations.db"
	@echo "  make debug-release DB=out/annotations.db"
	@echo "  make report DB=out/annotations.db"
	@echo "  make release-check"
	@echo "  make test"

setup:
	go run . setup --db "$(DB)"

annotate:
	go run . annotate --db "$(DB)" --input_dir "$(INPUT_DIR)" --from_idx "$(FROM)" --to_idx "$(TO)" --model "$(MODEL)"

analytics:
	go run . analytics --db "$(DB)" --out "out/analytics_latest.md"

debug-release:
	go run . debug-release --db "$(DB)" --out "out/release_debug_latest.md"

report:
	go run . report --db "$(DB)"

release-check: annotate analytics debug-release report

test:
	go test ./...
