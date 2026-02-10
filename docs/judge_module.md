# Judge Module: независимый слой оценки

## Зачем нужен отдельный judge

`judge` отделен от `sgr_core`, чтобы:
- не смешивать бизнес-описание Rule и техническую логику judge-проверки;
- иметь стабильный, хорошо тестируемый слой в долгую;
- поддерживать добавление/удаление Rule через один rule-driven механизм schema/prompt/result mapping.

## Границы ответственности

`judge` (пакет `src/dialogs/judge/`):
- строит dynamic strict bundled schema по `rule_keys`;
- формирует базовый judge prompt;
- извлекает judge/evaluator verdicts по `rule_key` из bundled payload.

`judge` не делает:
- SQL/работу с БД;
- оркестрацию scan/report;
- бизнес-решения о том, какие Rule активны.

`sgr_core`:
- источник истины по `RuleCard`, порогам и бизнес-контексту правил;
- источник fixed scan-policy (`fixed_scan_policy`);
- формирует evaluator prompt и контекст для judge.

`infrastructure` + `interfaces`:
- `src/dialogs/infrastructure/` исполняет scan/report orchestration и работу с артефактами;
- `src/dialogs/interfaces/` предоставляет стабильные точки входа `run_scan/build_report`;
- `src/dialogs/pipeline.py` остается совместимым фасадом для legacy-импортов.

## Judge Contract (v6)

Инварианты не меняются:
- bundled evaluator + bundled judge на каждый диалог;
- `judge_coverage=1.0`;
- `context=full`, `trace=full`;
- поля `scan_results`/`scan_metrics` и интерпретация метрик неизменны.

## Checklist: как добавить новое Rule

1. Добавить `RuleCard` в `src/dialogs/sgr_core.py` (`RULES`).
2. Добавить новый код в `ReasonCode` (`src/dialogs/models.py`).
3. Добавить reason codes в `RULE_REASON_CODES` и антипаттерны в `RULE_ANTI_PATTERNS`.
4. Проверить, что новый key автоматически попал в dynamic bundled schema (`judge.schema_factory`).
5. Обновить deterministic/fake контуры в тестах и `docs_refresh` (они должны брать ключи из `all_rules()`).
6. Если изменился публичный бизнес-термин/ключ, сделать version bump `METRICS_VERSION` в `sgr_core`.
7. Прогнать `make test && make docs`.
8. Сверить, что `make scan` и `make report` сохранили контракт и метрики.

## Мини-примеры

Пример rule-driven judge schema:
- вход: `rule_keys=("greeting", "next_step", "empathy")`
- выход: strict bundled model с обязательными полями `greeting/next_step/empathy`.

Пример расширения без ручной правки judge-кода:
- вход: `rule_keys=("greeting", "next_step", "empathy", "follow_up")`
- выход: bundled schema автоматически включает поле `follow_up`.
