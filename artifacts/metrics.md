# SGR Scan Metrics

- metrics_version: `v3_minimal_judge_correctness`
- canonical_run_id: `scan_ec749138d655`
- current_run_id: `scan_ec749138d655`
- inserted_results: `147`
- judged_results: `147`
- judge_coverage: `1.0000`

## Rule Quality (judge_correctness)

| rule | canonical | current | delta |
|---|---:|---:|---:|
| `greeting` | 1.0000 | 1.0000 | 0.0000 |
| `upsell` | 1.0000 | 1.0000 | 0.0000 |
| `empathy` | 0.9796 | 0.9796 | 0.0000 |

## LLM Calls (full trace persisted in `llm_calls`)

| phase | calls | errors |
|---|---:|---:|
| `evaluator` | 148 | 0 |
| `judge` | 147 | 0 |

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
| `modamart__1_transcript` | `empathy` | 0.9000 | 10 |
| `modamart__0_transcript` | `empathy` | 1.0000 | 10 |
| `modamart__0_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__0_transcript` | `upsell` | 1.0000 | 10 |
| `modamart__1_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__1_transcript` | `upsell` | 1.0000 | 10 |
| `modamart__2_transcript` | `empathy` | 1.0000 | 10 |
| `modamart__2_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__2_transcript` | `upsell` | 1.0000 | 10 |
| `modamart__4_transcript` | `empathy` | 1.0000 | 10 |

## Judge-Confirmed Bad Cases (judge_label=0)

| conversation_id | message_id | rule | eval_hit | expected_hit | reason_code | evidence_quote |
|---|---:|---|---:|---:|---|---|
| `modamart__1_transcript` | 22 | `empathy` | 0 | 1 | `courtesy_without_empathy` | `Hi John! Thanks for reaching out.` |

### Bad Case Details

1. `modamart__1_transcript` msg=22 rule=`empathy` eval_hit=0 expected_hit=1
   text: ** Hi John! Thanks for reaching out. I’d be happy to help you find the perfect jacket. Are you looking for something specific, like a certain material or style?
   evaluator_reason: В реплике есть вежливость, но нет явного выражения эмпатии по отношению к затруднениям клиента.
   judge_rationale: В реплике продавца явно выражена эмпатия: "I’d be happy to help you find the perfect jacket." Это соответствует критерию, что эмпатия выражена явно по контексту диалога.
   evidence_quote: Hi John! Thanks for reaching out.

## Run Summary JSON

- summary_json: `{"conversation_from":0,"conversation_to":4,"selected_conversations":5,"messages":96,"seller_messages":49,"customer_messages_context_only":47,"rules":3,"metrics_version":"v3_minimal_judge_correctness","processed":147,"inserted":147,"judged":147,"skipped_due_to_errors":0,"evidence_mismatch_skipped":0,"schema_errors":0,"non_schema_errors":0,"judge_inconsistency_soft_flags":0,"canonical_run_id":"scan_ec749138d655"}`
