# Post-Install Checklist (Top Python Tooling)

Use this after installing dependencies (`openai`, `pydantic`, `pytest`, `jupyter`, `ipykernel`).

## 1) Environment
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev]'
```

## 2) Core smoke
```bash
PYTHONPATH=src python3 -m dialogs.main db init --db dialogs.db
PYTHONPATH=src python3 -m dialogs.main data ingest-csv --csv-dir csv --db dialogs.db --replace
PYTHONPATH=src python3 -m dialogs.main db stats --db dialogs.db
```

Expected minimum:
- `conversations=50`
- `messages=989`
- `rules>=3`

## 3) Scan + report artifacts
```bash
PYTHONPATH=src python3 -m dialogs.main run scan --db dialogs.db --model gpt-4.1-mini --conversation-from 0 --conversation-to 4
PYTHONPATH=src python3 -m dialogs.main run report --db dialogs.db --png artifacts/accuracy_diff.png --md artifacts/metrics.md
```

Validate files:
- `artifacts/accuracy_diff.png` exists
- `artifacts/accuracy_diff.png` is a heatmap table `conversation x rule` with green/yellow/red zones
- `artifacts/metrics.md` contains run metadata, per-rule delta table, zone summary, and worst cells

## 4) Full LLM debug
```bash
PYTHONPATH=src python3 -m dialogs.main llm logs --run-id <SCAN_RUN_ID> --db dialogs.db
PYTHONPATH=src python3 -m dialogs.main llm logs --run-id <SCAN_RUN_ID> --db dialogs.db --failed-only
```

Check:
- request/response payloads saved in `llm_calls`
- parse/validation flags populated

## 5) Dataset-style tests
```bash
PYTHONPATH=src pytest -q
```

## 6) Notebook run
Open and run top-to-bottom:
- `notebooks/sgr_quality_demo.ipynb`

Check notebook cells:
- DB stats
- scan run visibility
- LLM call samples
- visual heatmap rendering from `artifacts/accuracy_diff.png`

## 7) Optional live OpenAI validation
Set key and repeat scan:
```bash
export OPENAI_API_KEY=...
PYTHONPATH=src python3 -m dialogs.main run scan --db dialogs.db --model gpt-4.1-mini --conversation-from 0 --conversation-to 4
PYTHONPATH=src python3 -m dialogs.main run report --db dialogs.db --png artifacts/accuracy_diff.png --md artifacts/metrics.md
```
