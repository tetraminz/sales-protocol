# CODEX ТЗ: полный рефакторинг `sales_protocol` в KISS-стиле (SGR demo)

Цель: переработать репозиторий так, чтобы он выглядел как **чистая, прозрачная документация по SGR**, начиная с `business_process_sgr.go` (верхний уровень), и далее — до SQLite и OpenAI клиента.  
Критерий читаемости: **контракты и поля в бизнес-модуле должны быть понятны человеку без контекста**.

Важно: **JSONL больше не используем**. Источник данных — CSV из `sales-transcripts`, выход — SQLite + Markdown отчёты.

---

## 0) Термины (обязательная часть для комментариев в коде)

- **Conversation** — один диалог (файл CSV).
- **Turn** — строка CSV (оригинальный чанк из датасета, `Chunk_id`).
- **Utterance Block (Replica)** — несколько подряд идущих `Turn` от одного и того же спикера, склеенные `\n`.
- **Ground Truth Speaker** — истинная метка говорящего из датасета (`Sales Rep` / `Customer`).
- **Predicted Speaker** — предсказание LLM.
- **SGR Quality Decision** — бизнес-решение «считать ли эту реплику ошибкой» (строгая проверка + override по farewell-контексту).
- **LLM Event Log** — полный аудит одной попытки вызова LLM: *что отправили* и *что получили*, + статусы парсинга/валидации.

---

## 1) Что НЕ делаем (чётко)

- Не интегрируем Google ADK / агенты / инструменты. Проект остаётся **простым скриптом**.
- Не добавляем сложную архитектуру (DI-контейнеры, многослойные абстракции, «enterprise»).
- Не добавляем новые признаки, кроме уже имеющихся 2 юнитов (Speaker + Empathy), кроме минимальных полей для аудита/отладки.
- Не используем JSONL.

---

## 2) Аудит текущего состояния (на основе `out/annotations.db` из архива)

### 2.1 Спикер: «100%» — это **final**, но не **raw**
- Всего реплик: `98` (диалогов: `5`)
- **Final match** (после SGR override): `98/98 = 100%`
- **Raw match** (чистое сравнение ground truth vs predicted): `96/98 = 97.96%`
- Есть `2` случая `farewell_context_override`, где LLM уверенно ошибся на коротком farewell:
  - `terravista-properties__1_transcript` replica `23` — текст: `Bye.` (predicted: Customer, true: Sales Rep)
  - `terravista-properties__3_transcript` replica `23` — текст: `Goodbye!` (predicted: Customer, true: Sales Rep)

### 2.2 Валидация evidence.quote деградирует часто
- В `annotate_logs` найдено `13` ошибок в `speaker`-кейсе (quote_empty / quote_not_substring)
- Пайплайн продолжает run, но пишет `pipeline error: speaker_case_degraded`
- Это нужно отражать в отчётах как отдельный индикатор качества (не смешивать с speaker accuracy).

### 2.3 Manual review сейчас считает customer-строки как backlog
- `pending` в отчётах = `98`, но реально empathy применим только к `Sales Rep` строкам (`51`)
- Нужно сделать `empathy_review_status = not_applicable` для Customer, либо вести backlog только по `empathy_applicable=1`.

---

## 3) Главная цель рефакторинга

Сделать код так, чтобы:

1) `business_process_sgr.go` был **главной документацией в коде**: термины, шаги, инварианты, решения.  
2) Остальные компоненты — простые, минимальные реализации этих контрактов:
   - чтение CSV → сбор utterance blocks  
   - два LLM-юнита (speaker, empathy) → строгий JSON (OpenAI)  
   - SGR-решение (strict match + farewell override)  
   - запись в SQLite (аннотации + full LLM event logs)  
   - простые отчёты `analytics` / `debug-release` / `report`

---

## 4) Требования к архитектуре (KISS)

### 4.1 Разделение по ответственности (без «овердизайна»)
Разбить текущий монолит `annotate_pipeline.go` на несколько файлов в `package main` (без внутренних пакетов, чтобы не усложнять):
- `business_process_sgr.go` — верхний уровень SGR (остается центральным)
- `pipeline_annotate.go` — оркестрация run: файлы → blocks → процесс → SQLite
- `input_sales_transcripts.go` — чтение CSV + сбор utterance blocks
- `llm_openai_client.go` — HTTP клиент OpenAI + Strict JSON Schema output
- `llm_cases_speaker.go` — speaker unit: промпт + schema + парсинг + валидация + retries
- `llm_cases_empathy.go` — empathy unit: промпт + schema + парсинг + валидация + retries
- `store_sqlite.go` — схема SQLite + запись/чтение (аннотации, логи)
- `reporting.go` — отчёты (обновить метрики)
- `main.go` — CLI (оставить текущие команды)

Допускается 1 дополнительный файл `types.go` для общих типов/констант, если нужно.

### 4.2 Жёсткое правило «LLM видит только текст»
Speaker unit должен принимать **только**:
- `previous_text` (string)
- `current_text` (string)
- `next_text` (string)

Empathy unit должен принимать **только**:
- `current_text` (string)

Никаких `conversation_id`, `replica_id`, `speaker_true`, метаданных датасета, названий колонок — в prompt/запрос не передавать.

---

## 5) Контракты верхнего уровня (переписать/переименовать для «понятно всем»)

### 5.1 Переименования (в коде и БД)
Текущие имена типа `speaker_true`, `replica_id` заменить на максимально очевидные:

- `speaker_true` → `ground_truth_speaker`
- `speaker_predicted` → `predicted_speaker`
- `speaker_match` → `speaker_is_correct_final` (это именно final, после SGR)
- добавить `speaker_is_correct_raw` (raw сравнение без override)
- `replica_id` → `utterance_index` (или `block_index`; выбрать одно и использовать везде)

Пояснение прямо в комментариях:
- raw = «как сработал LLM»
- final = «как бизнес оценивает качество с учетом farewell-контекста»

### 5.2 Бизнес-результат speaker unit (что сохраняем)
Нужно сохранять в `annotations` минимум:
- predicted speaker + confidence
- farewell флаги + источник
- evidence quote (или явно `""` + причина почему отсутствует)
- SGR decision (строка), raw/final correctness

---

## 6) SQLite схема (простая, но достаточная для дебага)

### 6.1 Таблица `annotations` (1 строка = 1 utterance block)
Обязательные поля (и почему они нужны):

- `conversation_id` TEXT — группировка по диалогу
- `utterance_index` INTEGER — позиция utterance block в диалоге (после склейки)
- `utterance_text` TEXT — исходный текст блока (для анализа/ручной проверки)
- `ground_truth_speaker` TEXT — метка датасета (для unit-тестов/оценки)
- `predicted_speaker` TEXT — результат LLM (чистый вывод)
- `predicted_speaker_confidence` REAL — диагностика уверенности
- `speaker_is_correct_raw` INTEGER — метрика качества LLM
- `speaker_is_correct_final` INTEGER — бизнес-метрика качества после SGR
- `speaker_quality_decision` TEXT — `strict_match | strict_mismatch | farewell_context_override | no_ground_truth`
- `farewell_is_current_utterance` INTEGER — сигнал прощания в current
- `farewell_is_conversation_closing` INTEGER — сигнал closing-обмена (current/neighbor)
- `farewell_context_source` TEXT — `current|previous|next|mixed|none`
- `speaker_evidence_quote` TEXT — кусок из current, который объясняет решение (substring)
- `speaker_evidence_is_valid` INTEGER — прошло ли правило «substring» (чтобы не прятать деградации)
- `empathy_applicable` INTEGER — 1 только если ground truth speaker = Sales Rep, иначе 0
- `empathy_present` INTEGER — факт (boolean), можно вычислять порогом по confidence
- `empathy_confidence` REAL — основная метрика эмпатии
- `empathy_evidence_quote` TEXT — substring из utterance_text (если делаем)
- `empathy_review_status` TEXT — `pending|ok|not_ok|not_applicable`
- `empathy_reviewer_note` TEXT — заметка ревьюера
- `model` TEXT — какой model использовался
- `annotated_at_utc` TEXT — время записи

PRIMARY KEY:
- (`conversation_id`, `utterance_index`)

### 6.2 Таблица `llm_events` (full event log; 1 строка = 1 попытка LLM вызова)
Цель: чтобы можно было дебажить **без догадок**.

Обязательные поля:
- `id` INTEGER PK
- `created_at_utc` TEXT
- `conversation_id` TEXT
- `utterance_index` INTEGER
- `unit_name` TEXT — `speaker` или `empathy`
- `attempt` INTEGER — 1..N (для retry cycle)
- `model` TEXT
- `request_json` TEXT — **полный JSON тела HTTP запроса** (без API key)
- `response_http_status` INTEGER
- `response_json` TEXT — **полный JSON тела HTTP ответа** (или `{}` если сетевой фейл)
- `extracted_content_json` TEXT — `choices[0].message.content` как строка (ожидается JSON)
- `parse_ok` INTEGER
- `validation_ok` INTEGER
- `error_message` TEXT — пусто при успехе, иначе причина (parse/validation/http)

Важно:
- Никаких truncate по умолчанию. Если вводим лимит, он должен быть явным: `was_truncated=1` + `truncated_bytes`.
- `llm_events` должны писаться **сразу** (не копить в памяти до конца run).

---

## 7) LLM: OpenAI Strict JSON Output (обязательное)

### 7.1 Единая функция вызова (в клиенте)
Сделать одну функцию типа:

- `CallStrictJSONSchema(ctx, model, messages, schemaName, schema) -> (LLMCallResult, error)`

Где `LLMCallResult` возвращает:
- `RequestJSON` (string)
- `ResponseJSON` (string)
- `HTTPStatus` (int)
- `Content` (string) // extracted choices[0].message.content
- `Err` (error) // сетевой/HTTP/empty content/refusal

И всегда (даже при ошибках) формировать запись для `llm_events`.

### 7.2 Speaker schema (демо CASCADE)
Схему переписать так, чтобы она визуально отражала «шаги»:

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["farewell", "speaker"],
  "properties": {
    "farewell": {
      "type": "object",
      "additionalProperties": false,
      "required": ["is_current_farewell", "is_closing_context", "context_source"],
      "properties": {
        "is_current_farewell": {"type":"boolean"},
        "is_closing_context": {"type":"boolean"},
        "context_source": {"enum":["current","previous","next","mixed","none"]}
      }
    },
    "speaker": {
      "type": "object",
      "additionalProperties": false,
      "required": ["predicted_speaker", "confidence", "evidence_quote"],
      "properties": {
        "predicted_speaker": {"enum":["Sales Rep","Customer"]},
        "confidence": {"type":"number"},
        "evidence_quote": {"type":"string"}
      }
    }
  }
}
```

Валидации в коде:
- predicted_speaker ∈ {Sales Rep, Customer}
- confidence clamp 0..1
- evidence_quote должен быть substring текущей utterance (иначе `speaker_evidence_is_valid=0`)

### 7.3 Empathy schema (минимально, но «факт» + confidence)
```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["empathy_present", "confidence", "evidence_quote"],
  "properties": {
    "empathy_present": {"type":"boolean"},
    "confidence": {"type":"number"},
    "evidence_quote": {"type":"string"}
  }
}
```

Валидации в коде:
- confidence clamp 0..1
- evidence_quote substring текущей utterance (если не проходит — фиксируем `validation_ok=0`, но не падаем)

---

## 8) SGR (business_process_sgr.go) — обновить документацию и инварианты

Обязательные вещи в `business_process_sgr.go`:

1) В начале файла: краткая «спецификация процесса» (CASCADE / ROUTING / CYCLE) простыми словами.
2) Явные шаги процесса:
   - Step A: Speaker unit (LLM)  
   - Step B: SGR decision (raw vs final; farewell override)  
   - Step C: Routing empathy (только если ground_truth_speaker == Sales Rep)  
3) Инвариант: predicted_speaker не переписывать в SGR, только quality decision.
4) Указать, что farewell override — сознательное бизнес-решение (чтобы short goodbye не считался ошибкой качества).

---

## 9) Retry cycle (демо CYCLE, но минимально)

В `speaker` и `empathy` кейсах:
- maxAttempts = 2 (константа)
- retry делаем только если:
  - JSON не парсится, или
  - evidence_quote пустой/не substring, или
  - enum/поля невалидны
- каждая попытка пишет `llm_events` с `attempt=1..N`

Fallback поведение после исчерпания попыток:
- Speaker: вернуть predicted_speaker = `Customer`, confidence=0, evidence_quote="" и выставить флаги «деградация» в `annotations`
- Empathy: `empathy_present=false`, confidence=0, evidence_quote=""

---

## 10) Обновить отчёты (analytics/debug/report) без усложнений

### 10.1 Analytics должны показывать:
- total_rows, total_conversations
- speaker_accuracy_raw_percent (по `speaker_is_correct_raw`)
- speaker_accuracy_final_percent (по `speaker_is_correct_final`)
- count_farewell_overrides
- speaker_evidence_invalid_count (где `speaker_evidence_is_valid=0`)
- empathy applicable rows count (Sales Rep)
- empathy review backlog: pending только для `empathy_review_status='pending'` и `empathy_applicable=1`

### 10.2 Debug-release должен подсвечивать:
- «red conversations» по raw mismatch (и отдельно по final mismatch)
- топ проблемных реплик:
  - raw mismatch
  - evidence invalid
  - short utterances (len <= 40)

---

## 11) CLI / Makefile / defaults (чтобы запускалось «из коробки»)

### 11.1 Default input_dir
Убрать абсолютный путь `/Users/...`.  
Сделать дефолт:
- `sales-transcripts/data/chunked_transcripts`

(или другой относительный путь, но один, согласованный в README и Makefile)

### 11.2 Env vars
- `OPENAI_API_KEY` (обязателен для annotate)
- `OPENAI_BASE_URL` (опционально; по умолчанию `https://api.openai.com`)
- `MODEL` / `--model` как сейчас

---

## 12) Тесты (обновить и добавить минимальные)

Обязательные тесты:
1) `SetupSQLite` создаёт новые таблицы `annotations` и `llm_events` с ожидаемыми колонками.
2) `AnnotateToSQLite`:
   - пишет `annotations` строки
   - пишет `llm_events` на каждый LLM вызов
3) Тест «LLM видит только текст»:
   - поднимаем mock server
   - проверяем, что `request_json` **не содержит** `conversation_id`, `utterance_index`, `ground_truth_speaker` и т.п. (кроме слов в самих текстах)
4) Тест retry cycle:
   - mock возвращает 1-й раз invalid evidence_quote
   - 2-й раз valid evidence_quote
   - проверяем, что в `llm_events` две попытки, а в `annotations` evidence валиден
5) Тест SGR override:
   - raw mismatch + farewell_context=true → final correct

---

## 13) Acceptance Criteria (что значит «готово»)

- `go test ./...` проходит.
- `go run . setup` + `go run . annotate` работает на локальном датасете.
- В SQLite:
  - `annotations` содержит новые прозрачные поля (raw/final, decision, applicability)
  - `llm_events` содержит **полные request/response** без truncate
- `analytics_latest.md` показывает и raw, и final метрики + количество overrides + количество evidence деградаций.
- README/Makefile больше не содержат абсолютных путей и не упоминают JSONL.
- Код читается сверху вниз: от `business_process_sgr.go` к реализациям, без «магии».

---
