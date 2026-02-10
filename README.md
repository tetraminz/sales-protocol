# Платформа для контроля качества продажных переписок.

Основной бизнес-эффект: рост `retention` за счет стабильного качества ответов продавца в каждом клиентском диалоге.

## Про Value за 2 минуты

Что система проверяет в каждом диалоге:
- `greeting`: есть ли корректное приветствие в первых 3 сообщениях продавца;
- `upsell`: был ли в диалоге предложен уместный следующий платный шаг;
- `empathy`: была ли в диалоге признана ситуация клиента перед решением.

Что гарантирует надежность:

- каждый оцениваемый диалог проходит через две системы оценки `evaluator` и `judge`;
- по каждому правилу мы можем посчитать вероятность его срабатывания;
- сохраняется полный аудит LLM-вызовов. Мы можем оценивать и удачные и ошибочные шаги.
- для каждого правила сохраняется evidence-anchor: `evidence_message_id` + `evidence_message_order`.

## Порядок исполнения

Сканирование работает в одном фиксированном режиме:
- bundled-оценка: один вызов evaluator и один вызов judge на диалог;
- full context: оценка опирается на полный контекст диалога;
- full judge coverage: judge обязателен для каждого оцениваемого диалога;
- full LLM audit trace: полный payload и ответ сохраняются всегда.

Этот порядок не настраивается.

## Архитектурные Слои

- `src/dialogs/sgr_core.py`: бизнес-ядро правил (`Rule`), бизнес-порогов, evaluator prompt и fixed scan-policy (`fixed_scan_policy()`).
- `src/dialogs/judge/`: независимый judge-слой (schema factory, prompt builder, rule-key mapping).
- `src/dialogs/infrastructure/`: scan/report orchestration, SQL-агрегации и запись артефактов.
- `src/dialogs/interfaces/`: тонкие публичные интерфейсы `run_scan/build_report`.
- `src/dialogs/pipeline.py`: backward-compatible фасад для legacy-импортов.
- `docs/judge_module.md`: практическая документация judge-слоя и checklist добавления нового Rule.

## Быстрый Запуск

```bash
make setup
make init-fresh
export OPENAI_API_KEY=...
make scan
make report
```

Открыть executive-ноутбук:

```bash
make notebook
```

Kernel: `Python (dialogs-sgr)`.

## Ключевые Артефакты

- Executive notebook: [`notebooks/sgr_quality_demo.ipynb`](notebooks/sgr_quality_demo.ipynb)
- Метрики: [`artifacts/metrics.md`](artifacts/metrics.md)
- Heatmap: [`artifacts/accuracy_diff.png`](artifacts/accuracy_diff.png)

## Точки Входа Документации (Doc-Contract)

После любого продуктового изменения синхронно обновляются:
1. [`README.md`](README.md)
2. [`src/dialogs/sgr_core.py`](src/dialogs/sgr_core.py)
3. [`tests/test_platform_dataset_style.py`](tests/test_platform_dataset_style.py)
4. [`notebooks/sgr_quality_demo.ipynb`](notebooks/sgr_quality_demo.ipynb)
5. [`artifacts/metrics.md`](artifacts/metrics.md)
6. [`artifacts/accuracy_diff.png`](artifacts/accuracy_diff.png)
7. [`Makefile`](Makefile)
8. [`docs/stability_case_review.md`](docs/stability_case_review.md)
9. [`docs/judge_module.md`](docs/judge_module.md)

## Глоссарий Метрик

- `evaluator_hit_rate`: доля `eval_hit=1`.
- `judge_correctness`: доля `judge_label=1` среди проверенных judge кейсов.
- `judge_coverage`: доля проверенных judge кейсов среди всех eval-кейсов.
