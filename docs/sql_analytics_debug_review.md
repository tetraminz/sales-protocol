# SQL Analytics / Debug / Review

## 1) Проверка схемы

```sql
PRAGMA table_info(annotations);
PRAGMA table_info(llm_events);
```

```sql
SELECT name, sql
FROM sqlite_master
WHERE type IN ('table', 'index')
  AND name IN ('annotations', 'llm_events',
               'idx_annotations_speaker_is_correct_raw',
               'idx_annotations_speaker_is_correct_final',
               'idx_annotations_speaker_quality_decision',
               'idx_annotations_empathy_review_status',
               'idx_llm_events_lookup',
               'idx_llm_events_parse_validation');
```

## 2) Основные метрики

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT conversation_id) AS total_conversations
FROM annotations;
```

```sql
SELECT
  ROUND(100.0 * AVG(speaker_is_correct_raw), 2) AS speaker_accuracy_raw_percent,
  ROUND(100.0 * AVG(speaker_is_correct_final), 2) AS speaker_accuracy_final_percent,
  SUM(CASE WHEN speaker_quality_decision='farewell_context_override' THEN 1 ELSE 0 END) AS farewell_override_count,
  SUM(CASE WHEN speaker_evidence_is_valid=0 THEN 1 ELSE 0 END) AS speaker_evidence_invalid_count
FROM annotations;
```

```sql
SELECT
  SUM(CASE WHEN empathy_applicable=1 THEN 1 ELSE 0 END) AS empathy_applicable_rows,
  ROUND(AVG(CASE WHEN empathy_applicable=1 THEN empathy_confidence END), 4) AS empathy_confidence_avg,
  MIN(CASE WHEN empathy_applicable=1 THEN empathy_confidence END) AS empathy_confidence_min,
  MAX(CASE WHEN empathy_applicable=1 THEN empathy_confidence END) AS empathy_confidence_max
FROM annotations;
```

## 3) Дебаг Speaker (raw/final/short)

```sql
SELECT
  conversation_id,
  utterance_index,
  ground_truth_speaker,
  predicted_speaker,
  speaker_quality_decision,
  utterance_text
FROM annotations
WHERE speaker_is_correct_raw=0
ORDER BY conversation_id, utterance_index;
```

```sql
SELECT
  conversation_id,
  utterance_index,
  ground_truth_speaker,
  predicted_speaker,
  speaker_quality_decision,
  utterance_text
FROM annotations
WHERE speaker_is_correct_final=0
ORDER BY conversation_id, utterance_index;
```

```sql
SELECT
  conversation_id,
  utterance_index,
  LENGTH(utterance_text) AS text_len,
  speaker_quality_decision,
  utterance_text
FROM annotations
WHERE speaker_is_correct_raw=0
  AND LENGTH(TRIM(utterance_text)) <= 40
ORDER BY text_len, conversation_id, utterance_index
LIMIT 20;
```

```sql
SELECT
  conversation_id,
  utterance_index,
  speaker_quality_decision,
  speaker_evidence_quote,
  utterance_text
FROM annotations
WHERE speaker_evidence_is_valid=0
ORDER BY conversation_id, utterance_index;
```

## 4) Дебаг LLM Events

```sql
SELECT
  COUNT(*) AS llm_event_rows,
  SUM(CASE WHEN parse_ok=0 THEN 1 ELSE 0 END) AS parse_failed_count,
  SUM(CASE WHEN validation_ok=0 THEN 1 ELSE 0 END) AS validation_failed_count
FROM llm_events;
```

```sql
SELECT
  created_at_utc,
  conversation_id,
  utterance_index,
  unit_name,
  attempt,
  response_http_status,
  parse_ok,
  validation_ok,
  error_message
FROM llm_events
WHERE parse_ok=0 OR validation_ok=0
ORDER BY created_at_utc DESC;
```

```sql
SELECT
  created_at_utc,
  unit_name,
  attempt,
  request_json,
  response_http_status,
  response_json,
  extracted_content_json,
  parse_ok,
  validation_ok,
  error_message
FROM llm_events
WHERE conversation_id = :conversation_id
  AND utterance_index = :utterance_index
ORDER BY unit_name, attempt;
```

## 5) Manual Review (applicable only)

```sql
SELECT
  conversation_id,
  utterance_index,
  utterance_text,
  empathy_present,
  empathy_confidence,
  empathy_evidence_quote
FROM annotations
WHERE empathy_applicable=1
  AND empathy_review_status='pending'
ORDER BY empathy_confidence DESC;
```

```sql
UPDATE annotations
SET empathy_review_status='ok',
    empathy_reviewer_note='looks good'
WHERE conversation_id=:conversation_id
  AND utterance_index=:utterance_index
  AND empathy_applicable=1;
```

```sql
UPDATE annotations
SET empathy_review_status='not_ok',
    empathy_reviewer_note='reason here'
WHERE conversation_id=:conversation_id
  AND utterance_index=:utterance_index
  AND empathy_applicable=1;
```

```sql
SELECT
  conversation_id,
  utterance_index,
  empathy_confidence,
  empathy_reviewer_note,
  utterance_text
FROM annotations
WHERE empathy_applicable=1
  AND empathy_review_status='not_ok'
ORDER BY empathy_confidence DESC;
```

## 6) Release Quick Checklist

```sql
SELECT
  COUNT(*) AS raw_red_rows
FROM annotations
WHERE speaker_is_correct_raw=0;
```

```sql
SELECT
  COUNT(*) AS final_red_rows
FROM annotations
WHERE speaker_is_correct_final=0;
```

```sql
SELECT
  COUNT(*) AS farewell_overrides
FROM annotations
WHERE speaker_quality_decision='farewell_context_override';
```

```sql
SELECT
  COUNT(*) AS pending_empathy_reviews_applicable
FROM annotations
WHERE empathy_applicable=1
  AND empathy_review_status='pending';
```
