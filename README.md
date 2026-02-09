# dialogs-sgr (Python-only)

Минимальная демо-платформа SGR для проверки бизнес-правил в диалогах.

## Один способ запуска
```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .[dev]
PYTHONPATH=src python3 sgr_demo.py
```

Это единственный основной сценарий запуска demo в проекте.

## Где основная логика и документация
- Кор бизнес-логики SGR: `src/dialogs/sgr_core.py`
- Основные тесты-документация кейсов: `tests/test_platform_dataset_style.py`
- Regression тест strict JSON schema: `tests/test_json_schema_regression.py`

## Проверка
```bash
PYTHONPATH=src .venv/bin/pytest -q
```
