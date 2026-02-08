# sales_protocol

KISS-процесс разметки:
- бизнес-оркестрация SGR в `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/business_process_sgr.go` (без SQL/IO)
- аннотация CSV напрямую в SQLite (один актуальный срез, без `run_id`)
- speaker и empathy confidence считаются через OpenAI
- финальная проверка empathy делается вручную в SQLite (`pending|ok|not_ok`)
- analytics/debug/report строятся из текущего состояния базы

## Команды

```bash
go run . setup --db out/annotations.db
go run . annotate --db out/annotations.db --input_dir /Users/ablackman/data/sales-transcripts/data/chunked_transcripts --from_idx 1 --to_idx 5 --model gpt-4.1-mini
go run . analytics --db out/annotations.db --out out/analytics_latest.md
go run . debug-release --db out/annotations.db --out out/release_debug_latest.md
go run . report --db out/annotations.db
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

## Минимальная схема

Таблица `annotations`:
- `conversation_id`, `replica_id` (PK)
- `speaker_true`, `speaker_predicted`, `speaker_confidence`, `speaker_match`
- `empathy_confidence`, `empathy_review_status`, `empathy_reviewer_note`
- `replica_text`, `model`, `annotated_at_utc`

`setup` делает hard reset схемы, `annotate` очищает `annotations` и записывает новый срез.
Если база от старой версии, сначала обязательно выполнить `setup`.

## Логи annotate

Во время `annotate` печатаются:
- `annotate_start`
- `annotate_file`
- `annotate_row`
- `annotate_progress` (каждые 25 реплик)
- `annotate_done`
- `annotate_log` (speaker/empathy/pipeline info+error для дебага LLM)

Также эти логи пишутся в таблицу `annotate_logs`.
Если LLM дал невалидный ответ, `annotate` продолжает run, пишет error в `annotate_logs` и сохраняет реплику в `annotations`.

Все фейлы:

```sql
SELECT created_at_utc, conversation_id, replica_id, stage, message
FROM annotate_logs
WHERE status='error'
ORDER BY id;
```

Последний ответ модели для конкретной реплики:

```sql
SELECT stage, status, message, raw_json
FROM annotate_logs
WHERE conversation_id='...' AND replica_id=...
ORDER BY id DESC;
```

## Manual empathy review (SQL)

Pending:

```sql
SELECT conversation_id, replica_id, replica_text, empathy_confidence
FROM annotations
WHERE empathy_review_status = 'pending'
ORDER BY empathy_confidence DESC;
```

Mark reviewed:

```sql
UPDATE annotations
SET empathy_review_status = 'ok', empathy_reviewer_note = 'looks good'
WHERE conversation_id = 'conv_id' AND replica_id = 10;
```

## Ссылки

- ТЗ: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/docs/CODEX_TZ_guided_reasoning_minimal.md`
- SGR reference: `/Users/ablackman/go/src/github.com/tetraminz/sales_protocol/docs/SKILL.md`
