# Analytics: run_1770551881224374000_manual

## Run Metadata
- run_id: `run_1770551881224374000_manual`
- release_tag: `manual`
- created_at_utc: `2026-02-08T11:58:01Z`
- input_dir: `/Users/ablackman/data/sales-transcripts/data/chunked_transcripts`
- range: `1..20`
- model: `gpt-4.1-mini`

## Totals
- replicas: `396`
- conversations: `20`
- green replicas: `346` (87.37%)
- red replicas: `50` (12.63%)

## Speaker Accuracy
- accuracy: `87.63%` (`347/396`)
- speaker_ok_false_count: `0`
- quality_speaker_mismatch_count: `49`

## Empathy
- empathy_ran_count: `0`
- empathy_present_count: `0` (0.00% of empathy_ran)
- empathy_type_distribution:
  - `none`: `396`

## Top Validation Errors
- speaker:
  - `quality:speaker_mismatch`: `49`
  - `attempt 1: format:evidence_quote_not_substring`: `2`
  - `attempt 2: format:evidence_quote_not_substring`: `1`
- empathy:
  - none

## Short Conclusion
- Разметка стабильна на `87.63%` speaker accuracy.
- Красных реплик: `50` из `396`.
- Красных диалогов: `10` из `20`.
