# SQL аналитика scan-run

## 1) Последний run и summary по фазам
```sql
SELECT
  run_id,
  status,
  conversation_from,
  conversation_to,
  selected_conversations,
  messages_count,
  started_at_utc,
  finished_at_utc,
  summary_json
FROM scan_runs
ORDER BY started_at_utc DESC
LIMIT 1;
```

## 2) Метрики по правилам для последнего run
```sql
WITH last_run AS (
  SELECT run_id
  FROM scan_runs
  WHERE status='success'
  ORDER BY started_at_utc DESC
  LIMIT 1
)
SELECT
  m.rule_key,
  MAX(CASE WHEN m.metric_name='accuracy'  THEN m.metric_value END) AS accuracy,
  MAX(CASE WHEN m.metric_name='precision' THEN m.metric_value END) AS precision,
  MAX(CASE WHEN m.metric_name='recall'    THEN m.metric_value END) AS recall,
  MAX(CASE WHEN m.metric_name='f1'        THEN m.metric_value END) AS f1,
  MAX(CASE WHEN m.metric_name='coverage'  THEN m.metric_value END) AS coverage,
  MAX(CASE WHEN m.metric_name='tp'        THEN m.metric_value END) AS tp,
  MAX(CASE WHEN m.metric_name='fp'        THEN m.metric_value END) AS fp,
  MAX(CASE WHEN m.metric_name='tn'        THEN m.metric_value END) AS tn,
  MAX(CASE WHEN m.metric_name='fn'        THEN m.metric_value END) AS fn,
  MAX(CASE WHEN m.metric_name='total'     THEN m.metric_value END) AS total
FROM scan_metrics m
JOIN last_run lr ON lr.run_id = m.run_id
GROUP BY m.rule_key
ORDER BY m.rule_key;
```

## 3) Delta последнего run против canonical_run_id
```sql
WITH state AS (
  SELECT value AS canonical_run_id
  FROM app_state
  WHERE key='canonical_run_id'
),
last_run AS (
  SELECT run_id AS current_run_id
  FROM scan_runs
  WHERE status='success'
  ORDER BY started_at_utc DESC
  LIMIT 1
),
cur AS (
  SELECT rule_key, metric_value AS current_accuracy
  FROM scan_metrics
  WHERE metric_name='accuracy'
    AND run_id=(SELECT current_run_id FROM last_run)
),
can AS (
  SELECT rule_key, metric_value AS canonical_accuracy
  FROM scan_metrics
  WHERE metric_name='accuracy'
    AND run_id=(SELECT canonical_run_id FROM state)
),
keys AS (
  SELECT rule_key FROM cur
  UNION
  SELECT rule_key FROM can
)
SELECT
  keys.rule_key,
  COALESCE(can.canonical_accuracy, 0) AS canonical_accuracy,
  COALESCE(cur.current_accuracy, 0) AS current_accuracy,
  COALESCE(cur.current_accuracy, 0) - COALESCE(can.canonical_accuracy, 0) AS delta
FROM keys
LEFT JOIN cur ON cur.rule_key = keys.rule_key
LEFT JOIN can ON can.rule_key = keys.rule_key
ORDER BY keys.rule_key;
```

## 4) Ошибки llm_calls по фазам evaluator/judge
```sql
WITH last_run AS (
  SELECT run_id
  FROM scan_runs
  ORDER BY started_at_utc DESC
  LIMIT 1
)
SELECT
  phase,
  COUNT(*) AS calls,
  SUM(CASE WHEN error_message<>'' THEN 1 ELSE 0 END) AS errors,
  SUM(CASE WHEN error_message LIKE '%invalid_json_schema%' THEN 1 ELSE 0 END) AS invalid_json_schema_errors,
  SUM(CASE WHEN parse_ok=0 THEN 1 ELSE 0 END) AS parse_fail,
  SUM(CASE WHEN validation_ok=0 THEN 1 ELSE 0 END) AS validation_fail
FROM llm_calls
WHERE run_id=(SELECT run_id FROM last_run)
GROUP BY phase
ORDER BY phase;
```

## 5) Топ расхождений evaluator vs judge
```sql
WITH last_run AS (
  SELECT run_id
  FROM scan_runs
  WHERE status='success'
  ORDER BY started_at_utc DESC
  LIMIT 1
)
SELECT
  r.rule_key,
  r.conversation_id,
  r.message_id,
  m.speaker_label,
  m.text,
  r.eval_hit,
  r.eval_confidence,
  r.judge_label,
  r.judge_confidence,
  r.eval_reason,
  r.judge_rationale
FROM scan_results r
JOIN messages m ON m.message_id = r.message_id
WHERE r.run_id=(SELECT run_id FROM last_run)
  AND r.judge_label IS NOT NULL
  AND r.eval_hit <> r.judge_label
ORDER BY ABS(r.eval_confidence - COALESCE(r.judge_confidence, 0)) DESC,
         r.rule_key,
         r.message_id
LIMIT 50;
```

## 6) Heatmap-срез conversation x rule (judge-aligned)
```sql
WITH last_run AS (
  SELECT run_id
  FROM scan_runs
  WHERE status='success'
  ORDER BY started_at_utc DESC
  LIMIT 1
)
SELECT
  r.conversation_id,
  r.rule_key,
  SUM(CASE WHEN r.judge_label IS NOT NULL THEN 1 ELSE 0 END) AS judged_total,
  SUM(CASE WHEN r.judge_label IS NOT NULL AND r.eval_hit = r.judge_label THEN 1 ELSE 0 END) AS agree_total,
  SUM(CASE WHEN r.judge_label IS NOT NULL AND r.eval_hit <> r.judge_label THEN 1 ELSE 0 END) AS disagree_total,
  CASE
    WHEN SUM(CASE WHEN r.judge_label IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN NULL
    ELSE
      1.0 * SUM(CASE WHEN r.judge_label IS NOT NULL AND r.eval_hit = r.judge_label THEN 1 ELSE 0 END)
      / SUM(CASE WHEN r.judge_label IS NOT NULL THEN 1 ELSE 0 END)
  END AS agreement_rate
FROM scan_results r
WHERE r.run_id=(SELECT run_id FROM last_run)
GROUP BY r.conversation_id, r.rule_key
ORDER BY r.conversation_id, r.rule_key;
```

## 7) Проверка seller-only покрытия
```sql
WITH last_run AS (
  SELECT run_id
  FROM scan_runs
  ORDER BY started_at_utc DESC
  LIMIT 1
)
SELECT
  COUNT(*) AS total_results,
  SUM(CASE WHEN m.speaker_label='Sales Rep' THEN 1 ELSE 0 END) AS seller_results,
  SUM(CASE WHEN m.speaker_label<>'Sales Rep' THEN 1 ELSE 0 END) AS non_seller_results
FROM scan_results r
JOIN messages m ON m.message_id = r.message_id
WHERE r.run_id=(SELECT run_id FROM last_run);
```

## 8) Проверка контекстности empathy (без локального gating)
```sql
WITH last_run AS (
  SELECT run_id
  FROM scan_runs
  ORDER BY started_at_utc DESC
  LIMIT 1
)
SELECT
  COUNT(*) AS empathy_evaluator_calls,
  SUM(CASE WHEN request_json LIKE '%Контекст чата%' THEN 1 ELSE 0 END) AS with_chat_context,
  SUM(CASE WHEN request_json LIKE '%Customer:%' THEN 1 ELSE 0 END) AS with_customer_context
FROM llm_calls
WHERE run_id=(SELECT run_id FROM last_run)
  AND phase='evaluator'
  AND rule_key='empathy';
```

## 9) Как интерпретировать
- `coverage` растет: evaluator чаще срабатывает (`eval_hit=1`) на judged-кейсах.
- `precision` падает: становится больше ложных срабатываний (`fp`).
- `recall` растет: меньше пропусков (`fn`).
- `delta` (current - canonical): `>0` улучшение, `<0` регресс относительно канона.
- Для `empathy` всегда учитывайте, что оценка делается через chat context, а не по isolated message.
