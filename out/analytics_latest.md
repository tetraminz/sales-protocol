# Analytics: run_1770553770088216000_manual

## Run Metadata
- run_id: `run_1770553770088216000_manual`
- release_tag: `manual`
- created_at_utc: `2026-02-08T12:29:30Z`
- input_dir: `/Users/ablackman/data/sales-transcripts/data/chunked_transcripts`
- range: `1..20`
- model: `gpt-4.1-mini`

## Totals
- replicas: `396`
- conversations: `20`
- green replicas: `379` (95.71%)
- red replicas: `17` (4.29%)

## Speaker Accuracy
- accuracy: `97.22%` (`385/396`)
- speaker_ok_false_count: `15`
- quality_speaker_mismatch_count: `11`

## Empathy
- empathy_ran_count: `0`
- empathy_present_count: `0` (0.00% of empathy_ran)
- empathy_type_distribution:
  - `none`: `396`

## Top Validation Errors
- speaker:
  - `attempt 3: format:evidence_quote_not_substring`: `15`
  - `format:evidence_quote_not_substring`: `15`
  - `quality:speaker_mismatch`: `11`
- empathy:
  - none

## Root Cause Breakdown
- label_format_mismatch_false_red_count: `0`
- real_model_mismatch_count: `11`
- transient_retry_error_count: `0`
- top_short_utterance_mismatches:
  - `nexiv-solutions__4_transcript` / replica `21` / len `4`: `Bye.`
  - `modamart__3_transcript` / replica `17` / len `8`: `Goodbye!`
  - `modamart__5_transcript` / replica `17` / len `8`: `Goodbye!`
  - `modamart__6_transcript` / replica `19` / len `8`: `Goodbye!`
  - `nexiv-solutions__0_transcript` / replica `25` / len `8`: `Goodbye.`
  - `nexiv-solutions__3_transcript` / replica `21` / len `8`: `Goodbye!`
  - `nexiv-solutions__7_transcript` / replica `25` / len `8`: `Goodbye!`
  - `nexiv-solutions__8_transcript` / replica `19` / len `8`: `Goodbye.`
  - `nexiv-solutions__8_transcript` / replica `17` / len `28`: `Thank you! Have a great day.`
  - `nexiv-solutions__9_transcript` / replica `19` / len `28`: `Thank you, have a great day!`

## Short Conclusion
- Разметка стабильна на `97.22%` speaker accuracy.
- Красных реплик: `17` из `396`.
- Красных диалогов: `12` из `20`.
