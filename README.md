# sales_protocol

KISS-проект для финального процесса:
- бизнес-документация SGR в одном Go-файле (2 кейса: `speaker` и `empathy`)
- миграция `out/annotations.jsonl` в SQLite
- отчеты по точности, дебаг-ошибкам и эмпатии

## Быстрый запуск

```bash
go run . migrate --in_jsonl out/annotations.jsonl --out_db out/annotations.db
go run . report --db out/annotations.db
go test ./...
```

Через `make`:

```bash
make migrate
make report
make test
```

## Что печатает report

- `total_rows`
- `total_conversations`
- `speaker_accuracy_percent` и `speaker_match/total`
- `speaker_ok_false_count`
- `quality_speaker_mismatch_count`
- `empathy_ran_count`
- `empathy_present_count` и процент от `empathy_ran`
- распределение `empathy_type`
- top validation errors для speaker/empathy

## Ключевые файлы

- `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/business_process_sgr.go`  
  Верхнеуровневая бизнес-документация процесса SGR без SQL/IO.
- `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/sqlite_migration.go`  
  JSONL -> SQLite + метрики отчета.

## Референсы

- Основной ТЗ: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/docs/CODEX_TZ_guided_reasoning_minimal.md`
- Референс SGR: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/docs/SKILL.md`
