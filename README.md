# sales_protocol

KISS-демо Schema-Guided Reasoning (SGR) для разметки sales-transcripts:
- вход: CSV из `sales-transcripts/data/chunked_transcripts`
- выход: SQLite (`annotations`, `llm_events`) + markdown отчеты
- центральная бизнес-спецификация: `business_process_sgr.go`

## Команды

```bash
go run . setup --db out/annotations.db
OPENAI_API_KEY=... go run . annotate --db out/annotations.db --input_dir sales-transcripts/data/chunked_transcripts --from_idx 1 --to_idx 20 --model gpt-4.1-mini
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

## Архитектура

- `business_process_sgr.go` — бизнес-процесс и SGR инварианты (raw/final, farewell override)
- `pipeline_annotate.go` — orchestration run
- `input_sales_transcripts.go` — чтение CSV и сбор utterance blocks
- `llm_openai_client.go` — strict JSON вызов OpenAI
- `llm_cases_speaker.go` — speaker unit (farewell + speaker)
- `llm_cases_empathy.go` — empathy unit
- `store_sqlite.go` — schema + запись `annotations` и `llm_events`
- `reporting.go` — analytics/debug/report

## SQLite: ключевые поля

`annotations`:
- `conversation_id`, `utterance_index` (PK)
- `ground_truth_speaker`, `predicted_speaker`, `predicted_speaker_confidence`
- `speaker_is_correct_raw`, `speaker_is_correct_final`, `speaker_quality_decision`
- `farewell_is_current_utterance`, `farewell_is_conversation_closing`, `farewell_context_source`
- `speaker_evidence_quote`, `speaker_evidence_is_valid`
- `empathy_applicable`, `empathy_present`, `empathy_confidence`, `empathy_evidence_quote`
- `empathy_review_status` (`pending|ok|not_ok|not_applicable`), `empathy_reviewer_note`
- `utterance_text`, `model`, `annotated_at_utc`

`llm_events` (аудит каждой попытки):
- `unit_name`, `attempt`, `request_json`, `response_http_status`, `response_json`
- `extracted_content_json`, `parse_ok`, `validation_ok`, `error_message`

## SQL гайд

Готовые SQL для аналитики/дебага/manual review:
- `docs/sql_analytics_debug_review.md`

## Важно про миграцию

Это hard-refactor по схеме. Перед annotate выполняйте:

```bash
go run . setup --db out/annotations.db
```
