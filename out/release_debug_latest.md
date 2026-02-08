# Release Debug: run_1770553770088216000_manual

## Summary
- green replicas: `379` (95.71%)
- red replicas: `17` (4.29%)
- green conversations: `8` (40.00%)
- red conversations: `12` (60.00%)

## Red Conversations
| conversation_id | red_replicas | total_replicas | top_reasons |
| --- | ---: | ---: | --- |
| `nexiv-solutions__8_transcript` | `3` | `19` | speaker:attempt 3: format:evidence_quote_not_substring (2); speaker:format:evidence_quote_not_substring (2); speaker_mismatch (2) |
| `nexiv-solutions__0_transcript` | `2` | `25` | speaker:attempt 3: format:evidence_quote_not_substring (2); speaker:format:evidence_quote_not_substring (2); speaker_ok_false (2) |
| `nexiv-solutions__3_transcript` | `2` | `21` | speaker:attempt 3: format:evidence_quote_not_substring (2); speaker:format:evidence_quote_not_substring (2); speaker_ok_false (2) |
| `nexiv-solutions__7_transcript` | `2` | `26` | speaker_mismatch (2); speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1) |
| `modamart__0_transcript` | `1` | `19` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_ok_false (1) |
| `modamart__3_transcript` | `1` | `17` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_mismatch (1) |
| `modamart__5_transcript` | `1` | `17` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_mismatch (1) |
| `modamart__6_transcript` | `1` | `19` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_mismatch (1) |
| `nexiv-solutions__1_transcript` | `1` | `19` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_ok_false (1) |
| `nexiv-solutions__4_transcript` | `1` | `21` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_mismatch (1) |
| `nexiv-solutions__6_transcript` | `1` | `16` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_ok_false (1) |
| `nexiv-solutions__9_transcript` | `1` | `19` | speaker:attempt 3: format:evidence_quote_not_substring (1); speaker:format:evidence_quote_not_substring (1); speaker_mismatch (1) |

## Root Cause Breakdown
- label_format_mismatch_false_red_count: `0`
- real_model_mismatch_count: `11`
- transient_retry_error_count: `0`

## Top Short-Utterance Mismatches
| conversation_id | replica_id | text_length | replica_text |
| --- | ---: | ---: | --- |
| `nexiv-solutions__4_transcript` | `21` | `4` | `Bye.` |
| `modamart__3_transcript` | `17` | `8` | `Goodbye!` |
| `modamart__5_transcript` | `17` | `8` | `Goodbye!` |
| `modamart__6_transcript` | `19` | `8` | `Goodbye!` |
| `nexiv-solutions__0_transcript` | `25` | `8` | `Goodbye.` |
| `nexiv-solutions__3_transcript` | `21` | `8` | `Goodbye!` |
| `nexiv-solutions__7_transcript` | `25` | `8` | `Goodbye!` |
| `nexiv-solutions__8_transcript` | `19` | `8` | `Goodbye.` |
| `nexiv-solutions__8_transcript` | `17` | `28` | `Thank you! Have a great day.` |
| `nexiv-solutions__9_transcript` | `19` | `28` | `Thank you, have a great day!` |

## Delta vs previous run
- previous_run_id: `run_1770551881224374000_manual`
- speaker_accuracy_delta_pp: `+9.60`
- green_replica_delta_pp: `+8.33`
- red_replica_delta_pp: `-8.33`
- green_conversation_delta_pp: `-10.00`
- red_conversation_delta_pp: `+10.00`
- quality_mismatch_delta: `-38`
