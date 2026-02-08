# sales_protocol

SQLite-only KISS процесс:
- верхнеуровневая бизнес-документация SGR в `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/business_process_sgr.go`
- разметка по диапазону CSV прямо в SQLite с `run_id` и OpenAI-классификацией спикера
- empathy не классифицируется автоматически: только manual-review
- аналитика и release-debug в markdown (`green/red`, сломанные диалоги, delta к предыдущему запуску)

## Быстрый запуск

```bash
go run . setup --db out/annotations.db
OPENAI_API_KEY=... go run . annotate --db out/annotations.db --input_dir /Users/ablackman/data/sales-transcripts/data/chunked_transcripts --from_idx 1 --to_idx 20 --release_tag manual --model gpt-4.1-mini --max_retries 2
go run . analytics --db out/annotations.db --run_id latest --out out/analytics_latest.md
go run . debug-release --db out/annotations.db --run_id latest --out out/release_debug_latest.md
go run . report --db out/annotations.db --run_id latest
```

Через `make`:

```bash
make setup
OPENAI_API_KEY=... make annotate
make analytics
make debug-release
make report
make release-check
make test
```

## Логи annotate

Во время `annotate` печатаются строки прогресса:
- `annotate_start` — run metadata (run_id, модель, диапазон, db)
- `annotate_file` / `annotate_file_done` — начало/итог по одному CSV-диалогу
- `annotate_row` — каждая записанная реплика (`conversation`, `replica`, `speaker_true/predicted`, `match`, `speaker_ok`)
- `annotate_progress` — агрегированный прогресс каждые 25 реплик
- `annotate_done` — финальные totals по запуску

## Что делает release debug

- доля green/red реплик
- доля green/red диалогов
- список красных диалогов и причины
- delta относительно предыдущего run (`speaker_accuracy`, green/red, mismatch)

## Переменные окружения

- `OPENAI_API_KEY` — обязателен для `annotate`
- `OPENAI_BASE_URL` — опционально, по умолчанию `https://api.openai.com`

## Ключевые файлы

- `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/business_process_sgr.go`  
  Верхнеуровневая бизнес-документация процесса SGR без SQL/IO.
- `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/sqlite_migration.go`  
  SQLite setup, annotate по диапазону, analytics markdown, debug markdown, report.

## Референсы

- Референс SGR: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/docs/SKILL.md`
