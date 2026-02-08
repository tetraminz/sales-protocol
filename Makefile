DB ?= out/annotations.db
INPUT_DIR ?= /Users/ablackman/data/sales-transcripts/data/chunked_transcripts
FROM ?= 1
TO ?= 20
RELEASE_TAG ?= manual
RUN_ID ?= latest
MODEL ?= gpt-4.1-mini
MAX_RETRIES ?= 2

.PHONY: help setup annotate analytics debug-release report release-check test

help:
	@echo "Targets:"
	@echo "  make setup DB=out/annotations.db"
	@echo "  OPENAI_API_KEY=... make annotate DB=out/annotations.db INPUT_DIR=/path FROM=1 TO=20 RELEASE_TAG=manual MODEL=gpt-4.1-mini"
	@echo "  make analytics DB=out/annotations.db RUN_ID=latest"
	@echo "  make debug-release DB=out/annotations.db RUN_ID=latest"
	@echo "  make report DB=out/annotations.db RUN_ID=latest"
	@echo "  make release-check"
	@echo "  make test"

setup:
	go run . setup --db "$(DB)"

annotate:
	go run . annotate --db "$(DB)" --input_dir "$(INPUT_DIR)" --from_idx "$(FROM)" --to_idx "$(TO)" --release_tag "$(RELEASE_TAG)" --model "$(MODEL)" --max_retries "$(MAX_RETRIES)"

analytics:
	go run . analytics --db "$(DB)" --run_id "$(RUN_ID)" --out "out/analytics_$(RUN_ID).md"

debug-release:
	go run . debug-release --db "$(DB)" --run_id "$(RUN_ID)" --out "out/release_debug_$(RUN_ID).md"

report:
	go run . report --db "$(DB)" --run_id "$(RUN_ID)"

release-check: annotate analytics debug-release report

test:
	go test ./...
