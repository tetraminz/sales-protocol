DB ?= dialogs.db
CSV_DIR ?= csv
PROMPT_VERSION ?= v1
SGR_VERSION ?= v1
MODEL ?= gpt-4.1-mini
PY ?= PYTHONPATH=src python3

.PHONY: init ingest stats baseline sgr diff demo test

init:
	$(PY) -m dialogs.main db init --db "$(DB)"

ingest:
	$(PY) -m dialogs.main data ingest-csv --csv-dir "$(CSV_DIR)" --db "$(DB)" --replace

stats:
	$(PY) -m dialogs.main db stats --db "$(DB)"

baseline:
	$(PY) -m dialogs.main run eval --mode baseline --db "$(DB)" --rule-set default --prompt-version "$(PROMPT_VERSION)" --sgr-version "$(SGR_VERSION)" --model "$(MODEL)"

sgr:
	$(PY) -m dialogs.main run eval --mode sgr --db "$(DB)" --rule-set default --prompt-version "$(PROMPT_VERSION)" --sgr-version "$(SGR_VERSION)" --model "$(MODEL)"

diff:
	@echo "Usage: make diff RUN_A=<exp_id> RUN_B=<exp_id>"
	$(PY) -m dialogs.main run diff --run-a "$(RUN_A)" --run-b "$(RUN_B)" --db "$(DB)" --png artifacts/accuracy_diff.png --md artifacts/metrics.md

demo:
	$(PY) -m dialogs.demo

test:
	PYTHONPATH=src pytest -q
