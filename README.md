# dialogs-sgr

Минимальный SGR-сканер для 3 hardcoded правил:
- `greeting`
- `upsell`
- `empathy`

Evaluator и `llm as judge` запускаются последовательно в одном `run_id`.

## Один основной запуск
```bash
PYTHONPATH=src python3 sgr_demo.py
```

## Главные файлы
- Core бизнес-логики: `src/dialogs/sgr_core.py`
- Отдельный judge-pass: `src/dialogs/llm_as_judge.py`
- Оркестрация run/report: `src/dialogs/pipeline.py`
- Тесты-документация: `tests/test_platform_dataset_style.py`
- JSON schema regression: `tests/test_json_schema_regression.py`
- SQL аналитика: `docs/analytics_sql.md`

## Быстрые команды
```bash
make init-fresh
make scan
make report
make test
```
