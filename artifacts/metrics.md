# SGR Scan Metrics

- metrics_version: `v6_dialog_level_bundle`
- scan_policy: `bundled=true, judge=full, context=full, llm_trace=full`
- llm_audit_trace: `full request_json + response_json + extracted_json`
- canonical_run_id: `scan_2d539f48a014`
- current_run_id: `scan_2d539f48a014`
- inserted_results: `15`
- judged_results: `15`
- judge_coverage: `1.0000`
- judge_coverage_target: `1.00`

## Rule Metrics

| rule | eval_total | eval_true | evaluator_hit_rate | judge_correctness | judge_coverage |
|---|---:|---:|---:|---:|---:|
| `empathy` | 5 | 5 | 1.0000 | 1.0000 | 1.0000 |
| `greeting` | 5 | 5 | 1.0000 | 1.0000 | 1.0000 |
| `next_step` | 5 | 5 | 1.0000 | 1.0000 | 1.0000 |

## Rule Quality Delta (judge_correctness)

| rule | canonical | current | delta |
|---|---:|---:|---:|
| `greeting` | 1.0000 | 1.0000 | 0.0000 |
| `next_step` | 1.0000 | 1.0000 | 0.0000 |
| `empathy` | 1.0000 | 1.0000 | 0.0000 |

## LLM Calls

| phase | calls | errors | prompt_chars | response_chars |
|---|---:|---:|---:|---:|
| `evaluator` | 5 | 0 | 35154 | 5271 |
| `judge` | 5 | 0 | 39537 | 2641 |

## Judge-Aligned Heatmap

- thresholds: `green >= 0.90, yellow >= 0.80, red < 0.80, na = no_judged`
- conversations: `5`
- rules: `3`

| zone | cells |
|---|---:|
| `green` | 15 |
| `yellow` | 0 |
| `red` | 0 |
| `na` | 0 |

### Worst conversation x rule cells

| conversation_id | rule | score | judged_total |
|---|---|---:|---:|
| `modamart__0_transcript` | `empathy` | 1.0000 | 1 |
| `modamart__0_transcript` | `greeting` | 1.0000 | 1 |
| `modamart__0_transcript` | `next_step` | 1.0000 | 1 |
| `modamart__1_transcript` | `empathy` | 1.0000 | 1 |
| `modamart__1_transcript` | `greeting` | 1.0000 | 1 |
| `modamart__1_transcript` | `next_step` | 1.0000 | 1 |
| `modamart__2_transcript` | `empathy` | 1.0000 | 1 |
| `modamart__2_transcript` | `greeting` | 1.0000 | 1 |
| `modamart__2_transcript` | `next_step` | 1.0000 | 1 |
| `modamart__3_transcript` | `empathy` | 1.0000 | 1 |

## Judge-Confirmed Bad Cases (judge_label=0)

| conversation_id | evidence_message_id | evidence_message_order | rule | eval_hit | expected_hit | reason_code | evidence_quote |
|---|---:|---:|---|---:|---:|---|---|
| `-` | 0 | 0 | `-` | 0 | 0 | `-` | `-` |

## Run Summary JSON

- summary_json: `{"conversation_from":0,"conversation_to":4,"selected_conversations":5,"messages":96,"seller_messages":49,"customer_messages_context_only":47,"rules":3,"metrics_version":"v6_dialog_level_bundle","bundle_rules":true,"judge_mode":"full","context_mode":"full","llm_trace":"full","evaluated_conversations":5,"skipped_conversations_without_seller":0,"processed":15,"inserted":15,"judged":15,"schema_errors":0,"non_schema_errors":0,"judge_coverage":1.0,"canonical_run_id":"scan_2d539f48a014"}`
