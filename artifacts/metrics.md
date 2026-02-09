# SGR Scan Metrics

- canonical_run_id: `scan_488bdc081429`
- current_run_id: `scan_488bdc081429`

| rule | canonical_accuracy | current_accuracy | delta |
|---|---:|---:|---:|
| `greeting` | 0.1224 | 0.1224 | 0.0000 |
| `upsell` | 0.1875 | 0.1875 | 0.0000 |
| `empathy` | 0.5306 | 0.5306 | 0.0000 |

## Judge-Aligned Heatmap

- thresholds: `green >= 0.90`, `yellow >= 0.70`, `red < 0.70`, `na = no_judged`
- conversations: `5`
- rules: `3`
- judged_cells: `15/15`

| zone | cells |
|---|---:|
| `green` | 0 |
| `yellow` | 1 |
| `red` | 14 |
| `na` | 0 |

### Worst conversation x rule cells

| conversation_id | rule | score | judged_total |
|---|---|---:|---:|
| `modamart__0_transcript` | `greeting` | 0.1000 | 10 |
| `modamart__0_transcript` | `upsell` | 0.1000 | 10 |
| `modamart__2_transcript` | `greeting` | 0.1000 | 10 |
| `modamart__2_transcript` | `upsell` | 0.1000 | 10 |
| `modamart__4_transcript` | `greeting` | 0.1000 | 10 |
| `modamart__4_transcript` | `upsell` | 0.1000 | 10 |
| `modamart__3_transcript` | `greeting` | 0.1111 | 9 |
| `modamart__1_transcript` | `greeting` | 0.2000 | 10 |
| `modamart__1_transcript` | `upsell` | 0.3000 | 10 |
| `modamart__3_transcript` | `empathy` | 0.3333 | 9 |
