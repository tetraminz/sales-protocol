DB ?= dialogs.db
CSV_DIR ?= csv
MODEL ?= gpt-4.1-mini
CONVERSATION_FROM ?= 0
CONVERSATION_TO ?= 4
PY ?= PYTHONPATH=src python3

.PHONY: init-fresh reset-runs scan report demo stats test

init-fresh:
	rm -f "$(DB)"
	$(PY) -m dialogs.main db init --db "$(DB)"
	$(PY) -m dialogs.main data ingest-csv --db "$(DB)" --csv-dir "$(CSV_DIR)" --replace

reset-runs:
	$(PY) -m dialogs.main db reset-runs --db "$(DB)"

scan:
	$(PY) -m dialogs.main run scan --db "$(DB)" --model "$(MODEL)" --conversation-from "$(CONVERSATION_FROM)" --conversation-to "$(CONVERSATION_TO)"

report:
	$(PY) -m dialogs.main run report --db "$(DB)" --md artifacts/metrics.md --png artifacts/accuracy_diff.png

stats:
	$(PY) -m dialogs.main db stats --db "$(DB)"

demo:
	$(PY) sgr_demo.py

test:
	PYTHONPATH=src .venv/bin/pytest -q
