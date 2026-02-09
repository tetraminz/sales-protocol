# Платформа для контроля качества продажных переписок.

Основной бизнес-эффект: рост `retention` за счет стабильного качества ответов продавца в каждом клиентском диалоге.

## Про Value за 2 минуты

Что система проверяет в каждой реплике продавца:
- `greeting`: есть ли корректное приветствие;
- `upsell`: предложен ли уместный следующий платный шаг;
- `empathy`: признана ли ситуация клиента перед решением.

Что гарантирует надежность:

- каждая оцениваемая реплика проходит через две системы оценки `evaluator` и `judge`;
- по каждому правилу мы можем посчитать вероятность его срабатывания;
- Cохраняется полный аудит LLM-вызовов. Мы можем оценивать и удачные и ошибочные шаги. 

## Порядок исполнения

Сканирование работает в одном фиксированном режиме:
- bundled-оценка: один вызов evaluator и один вызов judge на реплику продавца;
- full context: оценка опирается на полный контекст диалога до текущего шага;
- full judge coverage: judge обязателен для каждой оцениваемой реплики;
- full LLM audit trace: полный payload и ответ сохраняются всегда.

Этот порядок не настраивается.

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

## Глоссарий Метрик

- `evaluator_hit_rate`: доля `eval_hit=1`.
- `judge_correctness`: доля `judge_label=1` среди проверенных judge кейсов.
- `judge_coverage`: доля проверенных judge кейсов среди всех eval-кейсов.
