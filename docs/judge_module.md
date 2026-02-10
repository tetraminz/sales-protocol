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
- формирует evaluator prompt и контекст для judge.

## Judge Contract (v5)

Инварианты не меняются:
- bundled evaluator + bundled judge на каждый диалог;
- `judge_coverage=1.0`;
- `context=full`, `trace=full`;
- поля `scan_results`/`scan_metrics` и интерпретация метрик неизменны.

## Checklist: как добавить новое Rule

1. Добавить `RuleCard` в `src/dialogs/sgr_core.py` (`RULES`).
2. Добавить reason codes в `RULE_REASON_CODES` и антипаттерны в `RULE_ANTI_PATTERNS`.
3. Проверить, что новый key автоматически попал в dynamic bundled schema (`judge.schema_factory`).
4. Проверить фикстуры/fake LLM в тестах и `docs_refresh` (они должны брать ключи из `all_rules()`).
5. Прогнать `make test && make docs`.
6. Сверить, что `make scan` и `make report` сохранили прежний контракт и метрики.

## Мини-примеры

Пример rule-driven judge schema:
- вход: `rule_keys=("greeting", "upsell", "empathy")`
- выход: strict bundled model с обязательными полями `greeting/upsell/empathy`.

Пример расширения без ручной правки judge-кода:
- вход: `rule_keys=("greeting", "upsell", "empathy", "next_step")`
- выход: bundled schema автоматически включает поле `next_step`.
