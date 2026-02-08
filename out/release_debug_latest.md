# Release Debug

## Summary
- total_rows: `96`
- raw_mismatch_count: `1`
- final_mismatch_count: `0`
- farewell_override_count: `1`
- speaker_evidence_invalid_count: `0`

## Red Conversations (Raw)
| conversation_id | raw_red_rows | total_rows | top_reason |
| --- | ---: | ---: | --- |
| `modamart__3_transcript` | `1` | `17` | raw_speaker_mismatch (1) |

## Red Conversations (Final)
- none

## Top Raw Mismatches
| conversation_id | utterance_index | text_length | quality_decision | utterance_text |
| --- | ---: | ---: | --- | --- |
| `modamart__3_transcript` | `17` | `8` | `farewell_context_override` | `Goodbye!` |

## Top Final Mismatches
- none

## Top Evidence Invalid
- none

## Top Short-Utterance Raw Mismatches
| conversation_id | utterance_index | text_length | quality_decision | utterance_text |
| --- | ---: | ---: | --- | --- |
| `modamart__3_transcript` | `17` | `8` | `farewell_context_override` | `Goodbye!` |

## LLM Event Failures
- parse_failed: `0`
- validation_failed: `3`
