# sales_protocol

CLI-инструмент для разметки сейлз-диалогов из датасета `gwenshap/sales-transcripts` в формат JSONL, удобный для дебага, QA и последующей аналитики.

Подробное описание продуктового сценария находится в `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/docs/CASE.md`.

## Задача, которую решает проект

Проект автоматизирует базовый аудит разговоров "Sales Rep ↔ Customer":

- преобразует CSV-реплики в единый машинный формат;
- считает детерминированные метрики, которые легко тестировать;
- вызывает LLM в режиме strict JSON Schema для семантической оценки;
- сохраняет единый JSONL-рекорд на каждый диалог.

Итог: на выходе получается воспроизводимый, дебажный pipeline, где можно отдельно анализировать "сигналы по правилам" и "семантику по LLM".

## Dataset и входные данные

Ожидается директория с файлами:

- `data/chunked_transcripts/<company>__<n>_transcript.csv`

Используемые колонки CSV:

- `Conversation`
- `Chunk_id`
- `Speaker`
- `Text`

Колонка `Embedding` сознательно игнорируется.

Каждый CSV-файл считается одним conversation.

## Что делает пайплайн

Для каждого CSV-файла:

1. Парсит реплики в `turns`:
   - `turn_id = Chunk_id`
   - `speaker = Speaker`
   - `text = Text`
2. Сортирует реплики по `turn_id`.
3. Строит `raw_transcript` как строки вида `Speaker: Text`.
4. Считает детерминированные метрики:
   - `turn_count_total`
   - `turn_count_sales_rep`
   - `turn_count_customer`
   - `question_marks_sales_rep`
   - `question_marks_customer`
   - `mentions_discount`
   - `mentions_return_policy`
   - `mentions_shipping`
   - `mentions_reviews`
   - `mentions_sizing`
5. Вызывает OpenAI Chat Completions в режиме structured outputs:
   - `response_format.type = json_schema`
   - `response_format.json_schema.strict = true`
6. Собирает финальный `record_v1` и пишет одну JSONL-строку.

## Формат выхода

Каждая строка в `.jsonl` содержит:

- `schema_version`
- `dataset`
- `conversation`
- `input`
- `computed`
- `llm`

`llm` заполняется ответом модели строго по схеме `sales_transcript_llm_annotation_v1`.

## Ограничения и допущения

- Нужен `OPENAI_API_KEY`.
- Текущая версия CLI останавливается на первой ошибке аннотации.
- Локальная JSON Schema-валидация итогового `record_v1` пока не добавлена.
- Сетевые retry/backoff пока не реализованы.

## Структура проекта

```text
cmd/sales_annotator/main.go      # CLI, флаги, orchestration, запись JSONL
internal/dataset/loader.go       # загрузка и парсинг CSV
internal/compute/metrics.go      # детерминированные метрики
internal/openai/client.go        # вызов OpenAI structured outputs
internal/openai/schema.go        # strict schema для llm annotation
```

## Требования

- Go `1.22+`
- `OPENAI_API_KEY` в окружении

## Быстрый старт

```bash
make help
make test
make build
OPENAI_API_KEY=... make run INPUT_DIR=/path/to/chunked_transcripts OUT_JSONL=out/annotations.jsonl
```

## Конфигурация запуска

Флаги CLI:

- `--input_dir` (обязательно)
- `--out_jsonl` (обязательно)
- `--model` (по умолчанию `gpt-4.1-mini`)
- `--limit` (по умолчанию `0`, то есть все)
- `--filter_prefix` (опционально, например `modamart__`)

Пример:

```bash
OPENAI_API_KEY=... go run ./cmd/sales_annotator \
  --input_dir data/chunked_transcripts \
  --out_jsonl out/annotations.jsonl \
  --model gpt-4.1-mini \
  --limit 10 \
  --filter_prefix modamart__
```

## Makefile цели

См. `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/Makefile`.

Ключевые цели:

- `make fmt` — форматирование Go-кода
- `make vet` — статическая проверка `go vet`
- `make test` — запуск unit-тестов
- `make build` — сборка бинарника в `bin/sales_annotator`
- `make run` — запуск CLI через `go run`
- `make check` — полный локальный check (`fmt + vet + test`)

## Тесты

Покрытые базовые сценарии:

- корректная сортировка реплик по `Chunk_id`;
- корректный подсчет детерминированных метрик;
- корректная форма OpenAI-запроса (`strict=true`, `json_schema`) через fake HTTP-клиент.

Запуск:

```bash
make test
```

## Отладка

Рекомендации:

- начинайте с `--limit 1`, чтобы проверить контракт полей;
- используйте `--filter_prefix`, чтобы локально гонять отдельную компанию;
- проверяйте `computed` и `llm` вместе: это быстрый способ ловить рассинхрон между правилами и семантикой.
