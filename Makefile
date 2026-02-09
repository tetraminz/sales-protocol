DB ?= dialogs.db
CSV_DIR ?= csv
MODEL ?= gpt-4.1-mini
CONVERSATION_FROM ?= 0
CONVERSATION_TO ?= 4
VENV_PY ?= .venv/bin/python
PY ?= $(VENV_PY)
PYPATH ?= PYTHONPATH=src

.PHONY: setup init-fresh reset-runs scan report demo stats test notebook docs

setup:
	[ -x "$(VENV_PY)" ] || python3 -m venv .venv
	$(VENV_PY) -m pip install --upgrade pip
	$(VENV_PY) -m pip install -e '.[dev]'
	$(VENV_PY) -m ipykernel install --user --name dialogs-sgr --display-name "Python (dialogs-sgr)"

init-fresh:
	rm -f "$(DB)"
	$(PYPATH) $(PY) -m dialogs.main db init --db "$(DB)"
	$(PYPATH) $(PY) -m dialogs.main data ingest-csv --db "$(DB)" --csv-dir "$(CSV_DIR)" --replace

reset-runs:
	$(PYPATH) $(PY) -m dialogs.main db reset-runs --db "$(DB)"

scan:
	$(PYPATH) $(PY) -m dialogs.main run scan \
		--db "$(DB)" \
		--model "$(MODEL)" \
		--conversation-from "$(CONVERSATION_FROM)" \
		--conversation-to "$(CONVERSATION_TO)"

report:
	$(PYPATH) $(PY) -m dialogs.main run report --db "$(DB)" --md artifacts/metrics.md --png artifacts/accuracy_diff.png

stats:
	$(PYPATH) $(PY) -m dialogs.main db stats --db "$(DB)"

demo:
	$(PYPATH) $(PY) sgr_demo.py

test:
	$(PYPATH) $(PY) -m pytest -q

notebook:
	$(PY) -m jupyter lab

docs:
	$(PYPATH) $(PY) -m dialogs.docs_refresh refresh --db "$(DB)" --csv-dir "$(CSV_DIR)" --conversation-from "$(CONVERSATION_FROM)" --conversation-to "$(CONVERSATION_TO)" --md artifacts/metrics.md --png artifacts/accuracy_diff.png
	$(PYPATH) $(PY) -m dialogs.docs_refresh check --db "$(DB)" --csv-dir "$(CSV_DIR)" --conversation-from "$(CONVERSATION_FROM)" --conversation-to "$(CONVERSATION_TO)" --md artifacts/metrics.md --png artifacts/accuracy_diff.png
