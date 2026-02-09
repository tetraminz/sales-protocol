# dialogs-sgr

SGR-платформа для оценки качества диалогов продаж с прозрачной логикой:
- `evaluator` проверяет правила качества по каждому сообщению продавца;
- `judge` независимо подтверждает, корректна ли оценка evaluator;
- SQLite хранит полный аудит: результаты, метрики и трассировку вызовов LLM.

## Для C-Level за 2 минуты

Что измеряем:
- `greeting`: продавец начал с явного приветствия.
- `upsell`: есть уместное предложение следующего платного шага.
- `empathy`: в контексте есть явное признание состояния клиента.

Зачем это бизнесу:
- стабильное качество коммуникации в масштабировании команды продаж;
- рост конверсии и среднего чека без потери доверия клиента;
- раннее выявление слабых сценариев и точечные рекомендации команде.

Как читать зоны качества:
- `green` (`>= 0.90`): системно хорошо, можно тиражировать паттерн.
- `yellow` (`>= 0.80` и `< 0.90`): приемлемо, но есть риск деградации.
- `red` (`< 0.80`): приоритет на корректирующие действия.

## Как открыть executive-ноутбук

1. Подготовить окружение и данные:
```bash
make setup
make init-fresh
export OPENAI_API_KEY=...
make scan
```
2. Запустить Jupyter:
```bash
make notebook
```
3. Открыть `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/notebooks/sgr_quality_demo.ipynb`.
4. Выбрать kernel `Python (dialogs-sgr)`.

Важно:
- если видите `ModuleNotFoundError` (`pydantic`, `pandas`, `openai`), выбран неверный kernel;
- executive-блоки ноутбука показывают бизнес-поля без перегруза техническими ID;
- негативные кейсы строятся только по последнему успешному run (`latest_run`).

## Как интерпретировать хорошие и негативные кейсы

Хорошие кейсы:
- фильтр: `final_score_for_message >= 0.90` и `judge_label = 1`;
- смысл: сообщение стабильно проходит правила и подтверждено judge.

Негативные кейсы:
- фильтр: `judge_label = 0` в последнем успешном run;
- смысл: evaluator ошибся относительно независимого ожидания judge;
- действие: используйте поле рекомендаций в ноутбуке для понятных бизнес-улучшений.

## Надежность и аудит

- Structured outputs: evaluator/judge отвечают по строгим pydantic-схемам.
- Evidence integrity: каждая положительная оценка привязана к дословной цитате и span.
- Full trace: таблица `llm_calls` хранит `request_json`, `response_json`, parse/validation flags, latency.
- Soft flags: для judge есть маркеры противоречивой rationale без остановки пайплайна.

Ключевая логика и пороги централизованы в:
- `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/src/dialogs/sgr_core.py`

## Короткий Technical Quickstart

```bash
make setup
make init-fresh
export OPENAI_API_KEY=...
make scan
make report
make test
```

Полезные артефакты:
- Markdown-отчет: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/artifacts/metrics.md`
- Визуальный diff: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/artifacts/accuracy_diff.png`
- Executive notebook: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/notebooks/sgr_quality_demo.ipynb`

## Ограничения и корректная интерпретация

- Текущая версия намеренно ограничена тремя правилами: `greeting`, `upsell`, `empathy`.
- Метрика `judge_correctness` оценивает корректность evaluator, а не напрямую бизнес-результат сделки.
- Малое число негативов в latest run означает стабильность на выбранной выборке, но не отменяет мониторинг.
- Любое изменение порогов/правил в `sgr_core.py` является продуктовым решением и должно фиксироваться явно.

## Глоссарий RU + EN

- `judge_correctness`: доля корректных решений evaluator (Evaluator correctness rate).
- `judge_label`: judge подтвердил evaluator (`1`) или опроверг (`0`) (Judge verdict).
- `judge_expected_hit`: каким judge считает правильный hit (Judge expected hit).
- `eval_hit`: бинарное решение evaluator по правилу (Evaluator hit decision).
- `eval_reason`: объяснение evaluator на русском (Evaluator rationale).
- `judge_rationale`: объяснение judge на русском (Judge rationale).
- `evidence_quote`: дословная цитата-основание (Evidence quote).
- `final_score_for_message`: средняя доля подтвержденных правил по сообщению (Final message quality score).
