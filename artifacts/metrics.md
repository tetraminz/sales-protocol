# SGR Scan Metrics

- metrics_version: `v2_judge_correctness`
- canonical_run_id: `scan_caeeb81ec480`
- current_run_id: `scan_caeeb81ec480`

| rule | canonical_judge_correctness | current_judge_correctness | delta |
|---|---:|---:|---:|
| `greeting` | 1.0000 | 1.0000 | 0.0000 |
| `upsell` | 0.9796 | 0.9796 | 0.0000 |
| `empathy` | 0.9583 | 0.9583 | 0.0000 |

## Judge-Aligned Heatmap

- thresholds: `green >= 0.90, yellow >= 0.80, red < 0.80, na = no_judged`
- conversations: `5`
- rules: `3`
- judged_cells: `15/15`

| zone | cells |
|---|---:|
| `green` | 13 |
| `yellow` | 2 |
| `red` | 0 |
| `na` | 0 |

### Worst conversation x rule cells

| conversation_id | rule | score | judged_total |
|---|---|---:|---:|
| `modamart__1_transcript` | `empathy` | 0.8000 | 10 |
| `modamart__3_transcript` | `upsell` | 0.8889 | 9 |
| `modamart__0_transcript` | `empathy` | 1.0000 | 10 |
| `modamart__0_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__0_transcript` | `upsell` | 1.0000 | 10 |
| `modamart__1_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__1_transcript` | `upsell` | 1.0000 | 10 |
| `modamart__2_transcript` | `empathy` | 1.0000 | 10 |
| `modamart__2_transcript` | `greeting` | 1.0000 | 10 |
| `modamart__2_transcript` | `upsell` | 1.0000 | 10 |
