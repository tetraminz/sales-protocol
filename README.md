# sales_protocol

Минимальный Go CLI для наглядной демонстрации Schema-Guided Reasoning (SGR) на датасете `gwenshap/sales-transcripts`.

## Что делает

1. Читает `*.csv` из `--input_dir`.
2. Нормализует `Speaker` (включая артефакты вроде `**Sales Rep`).
3. Склеивает последовательные сообщения одного спикера в `Replica`.
4. Запускает два SGR-юнита с strict JSON:
   - `speaker_attribution` (по контексту previous/current/next)
   - `empathy_detection` (только для `Sales Rep`)
5. Пишет JSONL, где 1 строка = 1 реплика.

## Запуск

```bash
go run . \
  --input_dir data/chunked_transcripts \
  --out_jsonl out/annotations.jsonl \
  --limit_conversations 20 \
  --model gpt-4.1-mini \
  --max_retries 2
```

Dry-run без OpenAI API:

```bash
go run . \
  --input_dir data/chunked_transcripts \
  --out_jsonl out/annotations_dry.jsonl \
  --dry_run
```

## Переменные окружения

- `OPENAI_API_KEY` (обязателен, если `--dry_run=false`)
- `OPENAI_BASE_URL` (опционально, по умолчанию `https://api.openai.com`)

## SGR-паттерны в этом проекте

- CASCADE: каждый юнит возвращает структуру фиксированной формы (`strict JSON schema`).
- CYCLE: retry-цикл на ошибки формата/валидации.
- ROUTING: `empathy_detection` вызывается только если `speaker_true == "Sales Rep"`.

## Проверки после ответа LLM

### Unit A: speaker_attribution

- `confidence` приводится к диапазону `[0..1]`
- `evidence.quote` должен быть точной подстрокой `replica_text`
- если `predicted_speaker != speaker_true`, это quality-ошибка:
  `validation_errors` включает `quality:speaker_mismatch`
- при mismatch выполняется дополнительная попытка с quality-подсказкой

### Unit B: empathy_detection

- `confidence` приводится к `[0..1]`
- если `empathy_present=false`:
  - `empathy_type` обязан быть `none`
  - `evidence` обязан быть `[]`
- если `empathy_present=true`:
  - `evidence` должен содержать минимум 1 элемент
  - `evidence[0].quote` должен быть подстрокой `replica_text`

## Тесты

```bash
go test ./...
```

Покрыты:
- нормализация спикеров
- сборка реплик
- retry/quality-ветка unit A
- routing/retry unit B
- dry-run сквозной сценарий с JSONL

## Jupyter-анализ качества

Простой ноутбук с русскими комментариями и бизнес-выводами:
- `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/notebooks/llm_quality_analysis_ru.ipynb`
- входные данные: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/out/annotations.jsonl`

Быстрый запуск:

```bash
bash notebooks/run_notebook.sh lab
```

Headless-проверка выполнения ноутбука:

```bash
bash notebooks/run_notebook.sh check
```

По умолчанию скрипт использует `conda` (более стабильный fallback), но можно принудительно выбрать `micromamba`:

```bash
PREFER_MICROMAMBA=1 bash notebooks/run_notebook.sh lab
```
