# ТЗ для Codex: минимальный Go‑скрипт “Schema Guided Reasoning” (2 LLM‑юнита) на датасете `gwenshap/sales-transcripts`

## 0) Цель
Сделать **максимально простой локальный Go‑CLI** (по сути “скрипт”), который:

1) читает локальные переписки из датасета `gwenshap/sales-transcripts` (CSV из `chunked_transcripts`)  
2) группирует сообщения в **реплики** (несколько последовательных сообщений одного спикера)  
3) прогоняет **2 независимых LLM‑юнита** с **OpenAI Structured Outputs (STRICT JSON)**:
   - **Unit A: speaker_attribution** — предсказывает, кто говорит (Sales Rep или Customer) по тексту реплики (без подсказки истинного спикера)
   - **Unit B: empathy_detection** — определяет факт эмпатии в реплике продавца
4) делает **валидацию/ретраи/ветвление** (как демонстрацию “Guided Reasoning” в коде)
5) сохраняет результат в **JSONL** (дебаг + дальнейшая аналитика)

---

## 1) Входные данные (локально)
Скрипт принимает папку с CSV файлами из датасета:

- путь: `data/chunked_transcripts/*.csv`
- каждая CSV = один диалог
- ожидаемые колонки:  
  `Conversation, Chunk_id, Speaker, Text, Embedding`
- `Embedding` **не парсить и не использовать** (игнорировать как строку)

Требование: скрипт должен корректно работать, если `Speaker` содержит артефакты вроде `**Sales Rep` (нужна нормализация).

---

## 2) Термины и структуры данных (в коде)

### 2.1 Turn (сообщение из CSV)
```go
type Turn struct {
    ConversationID string // из Conversation
    TurnID         int    // из Chunk_id
    Speaker        string // "Sales Rep" | "Customer" (после нормализации)
    Text           string // из Text
}
```

### 2.2 Replica (реплика = несколько последовательных Turn одного Speaker)
Правило: идти по Turn в порядке `TurnID` и склеивать подряд идущие сообщения одного speaker.

```go
type Replica struct {
    ConversationID string
    ReplicaID      int
    TurnIDs        []int
    SpeakerTrue    string // истинный speaker (из CSV после нормализации)
    Text           string // склейка Turn.Text через "\n"
    Turns          []Turn // исходные Turn реплики (для дебага)
}
```

---

## 3) CLI (минимально)
Команда: `go run .` (или `go build` → бинарь)

Флаги:
- `--input_dir` (string, required): папка с CSV
- `--out_jsonl` (string, required): путь выходного jsonl
- `--limit_conversations` (int, default 20): сколько диалогов обработать
- `--model` (string, default `gpt-4.1-mini`): модель
- `--max_retries` (int, default 2): сколько ретраев на каждый юнит при ошибке/невалидности
- `--dry_run` (bool, default false): если true — **не вызывать OpenAI**, а писать только реплики + “пустые” юниты (для проверки пайплайна)

Конфиг OpenAI:
- `OPENAI_API_KEY` (env, required если не `--dry_run`)
- (опционально) `OPENAI_BASE_URL` (env, default `https://api.openai.com`)

---

## 4) Output: JSONL (1 строка = 1 Replica)
Скрипт пишет **JSON Lines**, каждая строка — отдельная реплика.

### 4.1 Структура JSONL записи
```json
{
  "schema_version": "replica_annotation_v1",
  "dataset": "gwenshap/sales-transcripts",
  "conversation_id": "modamart__0_transcript",
  "replica_id": 3,
  "turn_ids": [5,6],
  "speaker_true": "Sales Rep",
  "replica_text": "....",
  "replica_turns": [
    {"turn_id": 5, "speaker": "Sales Rep", "text": "..."},
    {"turn_id": 6, "speaker": "Sales Rep", "text": "..."}
  ],
  "guided": {
    "unit_speaker": {
      "ok": true,
      "attempts": 1,
      "validation_errors": [],
      "output": { }
    },
    "unit_empathy": {
      "ran": true,
      "ok": true,
      "attempts": 1,
      "validation_errors": [],
      "output": { }
    }
  },
  "meta": {
    "model": "gpt-4.1-mini",
    "timestamp_utc": "2026-02-08T12:34:56Z",
    "openai_request_ids": ["..."]
  }
}
```

Минимальные требования:
- `replica_turns` обязателен (чтобы дебажить, что именно анализировалось)
- `guided.unit_*.*` обязателен даже при ошибке (тогда `ok=false`, `validation_errors` заполнить)
- `meta.timestamp_utc` всегда заполнять
- `meta.openai_request_ids` может быть пустым массивом при `--dry_run` или если ID не возвращён

---

## 5) LLM Unit A: speaker_attribution (STRICT JSON)

### 5.1 Назначение
Определить, кто автор реплики: `"Sales Rep"` или `"Customer"`, основываясь **только на тексте переписки**.

Важно:
- в промпт **не передавать** `speaker_true`
- не подписывать реплики “Sales/Customer” (контекст только текстом)

### 5.2 OpenAI Structured Output schema (strict)
```json
{
  "name": "unit_speaker_attribution_v1",
  "strict": true,
  "schema": {
    "type": "object",
    "additionalProperties": false,
    "required": ["predicted_speaker", "confidence", "evidence"],
    "properties": {
      "predicted_speaker": { "enum": ["Sales Rep", "Customer"] },
      "confidence": { "type": "number" },
      "evidence": {
        "type": "object",
        "additionalProperties": false,
        "required": ["quote"],
        "properties": {
          "quote": { "type": "string" }
        }
      }
    }
  }
}
```

### 5.3 Prompt (минимально)
System:
- Return only JSON matching schema (strict).
- Evidence quote must be an exact substring of the provided text.

User:
```
previous: "<...>"
current: "<replica_text>"
next: "<...>"
Task: predict who wrote "current": Sales Rep or Customer.
Return JSON only.
```

### 5.4 Валидация (в коде)
После парсинга JSON:
- `confidence` clamp в диапазон `[0..1]`
- `evidence.quote` должен быть **подстрокой** `replica_text`, иначе ошибка формата/валидации
- Сравнить `predicted_speaker` с `speaker_true`:
  - mismatch — это **ошибка качества**, не формата

### 5.5 Guided loop / retry / branching (демонстрация)
- Если JSON не распарсился / невалиден / quote не подстрока → retry до `--max_retries`
- Если `predicted_speaker != speaker_true` → сделать **1 retry** с дополнительной подсказкой:
  - “Your previous prediction mismatched the ground truth in our evaluation. Re-check the text and decide again. Return JSON only.”
- Если после ретраев всё равно mismatch → `ok=true` (формат ок), но `validation_errors` включает `quality:speaker_mismatch`

---

## 6) LLM Unit B: empathy_detection (STRICT JSON)

### 6.1 Назначение
Определить, есть ли в реплике эмпатия (сопереживание/валидация/поддержка).

Юнит анализирует **только текущую реплику** (без необходимости в контексте).

### 6.2 Ветвление (демонстрация)
- Юнит запускается **только если `speaker_true == "Sales Rep"`**
- Если `speaker_true != "Sales Rep"` → не вызывать OpenAI, записать:
  - `ran=false`, `ok=true`, `attempts=0`
  - `output.empathy_present=false`, `output.empathy_type="none"`, `output.evidence=[]`, `output.confidence=0`

### 6.3 OpenAI Structured Output schema (strict)
```json
{
  "name": "unit_empathy_detection_v1",
  "strict": true,
  "schema": {
    "type": "object",
    "additionalProperties": false,
    "required": ["empathy_present", "empathy_type", "confidence", "evidence"],
    "properties": {
      "empathy_present": { "type": "boolean" },
      "empathy_type": {
        "enum": ["none", "validation", "reassurance", "apology", "support", "other"]
      },
      "confidence": { "type": "number" },
      "evidence": {
        "type": "array",
        "items": {
          "type": "object",
          "additionalProperties": false,
          "required": ["quote"],
          "properties": {
            "quote": { "type": "string" }
          }
        }
      }
    }
  }
}
```

### 6.4 Правила валидации (в коде)
- `confidence` clamp `[0..1]`
- если `empathy_present == false`:
  - `empathy_type` должен быть `"none"`
  - `evidence` должен быть `[]` (иначе ошибка)
- если `empathy_present == true`:
  - `evidence` должен иметь хотя бы 1 элемент
  - `evidence[0].quote` должен быть **подстрокой** `replica_text`

### 6.5 Guided loop / retry
- Если нарушение правил evidence/подстроки → retry до `--max_retries`, добавляя:
  - “The quote MUST be copied exactly from the text. Pick an exact substring.”

---

## 7) OpenAI API вызов (требования к реализации)
Использовать `POST {OPENAI_BASE_URL}/v1/chat/completions`:

- `model`: из флага
- `messages`: system + user
- `temperature`: 0 (стабильность)
- `response_format`: `json_schema` со `strict:true` (для каждого юнита своя схема)

Сохранить `request id` (если есть в ответе) в `meta.openai_request_ids`.

Режим `--dry_run` не вызывает OpenAI и всё равно пишет JSONL.

---

## 8) Минимальная “архитектура” (без раздутия)
Ограничиться ~5–6 файлами в корне проекта (без `internal/`, без внешних библиотек):

- `main.go` — CLI, цикл по диалогам/репликам, запись JSONL
- `dataset.go` — чтение CSV, нормализация speaker, сбор Turn[], сбор Replica[]
- `openai.go` — минимальный HTTP‑клиент OpenAI + функция `CallStructured(...)`
- `units.go` — `RunSpeakerUnit`, `RunEmpathyUnit`
- `validate.go` — проверки, clamp, substring
- `main_test.go` — unit tests

Только стандартная библиотека: `net/http`, `encoding/csv`, `encoding/json`, `flag`, `os`, `filepath`, `context`, `time`, `strings`, `regexp`, `testing`.

---

## 9) Тесты (обязательные, простые, детерминированные)
Тесты **не должны зависеть от реального OpenAI**.

### 9.1 Fake LLM
Сделать интерфейс:
```go
type LLM interface {
    Call(ctx context.Context, system string, user string, schemaJSON string) (rawJSON []byte, requestID string, err error)
}
```
И `fakeLLM` в тестах, который возвращает заранее заданные JSON ответы по очереди (для проверки ретраев).

### 9.2 Unit tests
1) `TestNormalizeSpeaker()`  
   - вход: `"**Sales Rep"`, `"Sales Rep"`, `" **Customer"`  
   - ожидаемо: `"Sales Rep"`, `"Customer"`

2) `TestGroupIntoReplicas()`  
   - Turn sequence: Sales, Sales, Customer, Customer, Sales  
   - ожидаемо: 3 реплики с правильными `TurnIDs` и `Text`

3) `TestValidateSpeakerUnitEvidenceSubstring()`  
   - evidence.quote не подстрока → ошибка валидации

4) `TestRetryOnInvalidEvidence()`  
   - fakeLLM: 1‑й ответ с плохой quote, 2‑й с корректной  
   - ожидаемо: `attempts==2`, `ok==true`

5) `TestEmpathyUnitBranching()`  
   - для Customer реплики: `ran=false`, `attempts==0`, `empathy_present=false`, `empathy_type="none"`, `evidence=[]`

Опционально: интеграционный тест (только если `OPENAI_API_KEY` задан) — пометить как `//go:build integration`.

---

## 10) Acceptance Criteria (готово, если)
- `go test ./...` проходит локально без ключа
- `go run . --input_dir ... --out_jsonl out.jsonl --limit_conversations 20` создаёт JSONL
- В JSONL по каждой реплике есть:
  - `replica_turns`, `replica_text`, `speaker_true`
  - результат UnitSpeaker + ретраи/валидация
  - результат UnitEmpathy или корректный skip‑branch
- В коде явно видны:
  - **цикл** по репликам
  - **валидация** (подстроки/правила)
  - **ретраи**
  - **ветвление** (skip empathy для Customer + retry на mismatch)

---

## 11) Ничего не упущено (чек‑лист)
- [x] Работа с локальным датасетом CSV (chunked_transcripts), игнор Embedding  
- [x] Реплика = несколько последовательных сообщений одного спикера  
- [x] 2 LLM‑юнита, независимые, каждый со strict JSON schema  
- [x] Валидация quote как подстроки + clamp confidence  
- [x] Демонстрация циклов/валидации/ретраев/ветвления в одном простом скрипте  
- [x] JSONL output с сырьём (replica_turns) для дебага  
- [x] Работа с OpenAI ключом через env, dry_run режим  
- [x] Юнит‑тесты без реального OpenAI через fakeLLM  
- [x] Минимальная архитектура и только стандартная библиотека  
