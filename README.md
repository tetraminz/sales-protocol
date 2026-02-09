# dialogs-sgr

Минимальный SGR-сканер для 3 hardcoded правил (`greeting`, `upsell`, `empathy`) с SQLite, evaluator и `llm as judge`.

## Quickstart (с нуля, единый Python setup)
```bash
make setup
make init-fresh
export OPENAI_API_KEY=...
make scan
make report
make test
```

Для ноутбука:
```bash
make notebook
```
и в Jupyter выбрать kernel `Python (dialogs-sgr)`.

Если в ноутбуке `ModuleNotFoundError` (`pydantic`, `pandas`, `openai`), значит выбран не kernel `Python (dialogs-sgr)`.

## Что важно
- Все `make`-команды работают через `.venv/bin/python` (без `source .venv/bin/activate`).
- Базовая метрика качества: `judge_correctness` (`judge_label=1` означает корректность evaluator).
- Пороги качества централизованы в `src/dialogs/sgr_core.py` (Balanced): `green>=0.90`, `yellow>=0.80`, `red<0.80`.
