from __future__ import annotations

import sqlite3
from pathlib import Path

from .utils import now_utc

SCHEMA_DICTIONARY_RU: dict[str, dict[str, str]] = {
    "conversations": {
        "__table__": "Справочник диалогов: одна запись = один исходный разговор.",
        "conversation_id": "Уникальный идентификатор диалога.",
        "source_file_name": "Имя исходного CSV-файла, из которого загружен диалог.",
        "message_count": "Количество сообщений в диалоге после загрузки.",
        "created_at_utc": "Время первого создания записи диалога (UTC).",
        "updated_at_utc": "Время последнего обновления записи диалога (UTC).",
    },
    "messages": {
        "__table__": "Сообщения диалога в порядке появления; используются evaluator и judge.",
        "message_id": "Уникальный числовой идентификатор сообщения.",
        "conversation_id": "Идентификатор диалога, к которому относится сообщение.",
        "source_chunk_id": "Порядковый номер чанка в исходном CSV.",
        "message_order": "Порядок сообщения внутри диалога после сортировки.",
        "speaker_label": "Нормализованная роль автора сообщения (Sales Rep/Customer/Unknown).",
        "text": "Текст сообщения без изменений смысла.",
        "created_at_utc": "Время первого создания записи сообщения (UTC).",
        "updated_at_utc": "Время последнего обновления записи сообщения (UTC).",
    },
    "scan_runs": {
        "__table__": "Реестр запусков сканирования качества.",
        "run_id": "Уникальный идентификатор запуска scan.",
        "model": "Имя модели LLM, использованной в запуске.",
        "conversation_from": "Начальный индекс выбранного диапазона диалогов.",
        "conversation_to": "Конечный индекс выбранного диапазона диалогов.",
        "selected_conversations": "Фактическое число диалогов, попавших в запуск.",
        "messages_count": "Число сообщений в выбранном диапазоне до фильтрации по роли.",
        "status": "Статус запуска (running/success/failed).",
        "started_at_utc": "Время старта запуска (UTC).",
        "finished_at_utc": "Время завершения запуска (UTC).",
        "summary_json": "Сводка counters и конфигурации запуска в JSON.",
    },
    "scan_results": {
        "__table__": "Результаты проверки правила для seller-turn (одна строка = одно правило).",
        "result_id": "Уникальный идентификатор результата.",
        "run_id": "Идентификатор запуска scan.",
        "conversation_id": "Идентификатор диалога результата.",
        "seller_message_id": "Идентификатор реплики продавца, которую оценивали.",
        "customer_message_id": "Идентификатор последней релевантной реплики покупателя (может быть NULL).",
        "rule_key": "Ключ проверяемого правила (greeting/upsell/empathy).",
        "eval_hit": "Решение evaluator: найдено ли соблюдение правила.",
        "eval_confidence": "Уверенность evaluator в решении [0..1].",
        "eval_reason_code": "Код причины решения evaluator из фиксированного списка.",
        "eval_reason": "Текстовое объяснение evaluator на русском языке.",
        "evidence_quote": "Дословная цитата из реплики продавца как evidence.",
        "judge_expected_hit": "Ожидание judge о корректном hit для кейса.",
        "judge_label": "Вердикт judge о корректности evaluator (1/0).",
        "judge_confidence": "Уверенность judge в вердикте [0..1].",
        "judge_rationale": "Краткое объяснение judge на русском языке.",
        "created_at_utc": "Время создания результата (UTC).",
        "updated_at_utc": "Время последнего обновления результата (UTC).",
    },
    "scan_metrics": {
        "__table__": "Агрегаты качества по правилам для отчетов.",
        "run_id": "Идентификатор запуска scan.",
        "rule_key": "Ключ правила.",
        "eval_total": "Число оцененных seller-turn по правилу.",
        "eval_true": "Число eval_hit=1 по правилу.",
        "evaluator_hit_rate": "Доля eval_hit=1 от eval_total.",
        "judge_correctness": "Доля кейсов с judge_label=1 среди judged кейсов.",
        "judge_coverage": "Доля judged кейсов от eval_total.",
        "judged_total": "Количество кейсов по правилу, где есть judge_label.",
        "judge_true": "Количество кейсов judge_label=1 по правилу.",
        "judge_false": "Количество кейсов judge_label=0 по правилу.",
        "created_at_utc": "Время записи агрегата (UTC).",
    },
    "llm_calls": {
        "__table__": "Трасс вызовов LLM с метриками объема prompt/response.",
        "call_id": "Уникальный идентификатор вызова LLM.",
        "run_id": "Идентификатор запуска scan.",
        "phase": "Фаза вызова (evaluator/judge).",
        "rule_key": "Ключ правила или bundle.",
        "conversation_id": "Идентификатор диалога вызова.",
        "message_id": "Идентификатор seller_message_id вызова.",
        "attempt": "Номер попытки вызова.",
        "context_mode": "Политика контекста в продукте (в v3 всегда full).",
        "judge_policy": "Политика judge в продукте (в v3 всегда full coverage).",
        "trace_mode": "Политика трассировки payload (в v3 всегда full).",
        "prompt_chars": "Объем system+user prompt в символах.",
        "response_chars": "Объем extracted response в символах.",
        "request_json": "Полный JSON-запрос к LLM.",
        "response_http_status": "HTTP-статус ответа провайдера.",
        "response_json": "Полный JSON-ответ провайдера.",
        "extracted_json": "JSON-фрагмент, извлеченный из ответа модели.",
        "parse_ok": "Флаг успешного JSON parsing extracted_json.",
        "validation_ok": "Флаг успешной валидации against pydantic schema.",
        "error_message": "Текст ошибки вызова/парсинга/валидации.",
        "latency_ms": "Длительность вызова в миллисекундах.",
        "created_at_utc": "Время создания записи трасса (UTC).",
    },
    "app_state": {
        "__table__": "Небольшое key-value хранилище служебного состояния приложения.",
        "key": "Ключ служебного состояния.",
        "value": "Значение служебного состояния.",
        "updated_at_utc": "Время последнего изменения значения (UTC).",
    },
}


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS conversations (
  conversation_id TEXT PRIMARY KEY,
  source_file_name TEXT NOT NULL UNIQUE,
  message_count INTEGER NOT NULL,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
  message_id INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id TEXT NOT NULL,
  source_chunk_id INTEGER NOT NULL,
  message_order INTEGER NOT NULL,
  speaker_label TEXT NOT NULL,
  text TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  UNIQUE(conversation_id, source_chunk_id),
  FOREIGN KEY(conversation_id) REFERENCES conversations(conversation_id)
);

CREATE TABLE IF NOT EXISTS scan_runs (
  run_id TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  conversation_from INTEGER NOT NULL,
  conversation_to INTEGER NOT NULL,
  selected_conversations INTEGER NOT NULL,
  messages_count INTEGER NOT NULL,
  status TEXT NOT NULL,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT NOT NULL DEFAULT '',
  summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS scan_results (
  result_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  seller_message_id INTEGER NOT NULL,
  customer_message_id INTEGER,
  rule_key TEXT NOT NULL,
  eval_hit INTEGER NOT NULL,
  eval_confidence REAL NOT NULL,
  eval_reason_code TEXT NOT NULL,
  eval_reason TEXT NOT NULL,
  evidence_quote TEXT NOT NULL,
  judge_expected_hit INTEGER,
  judge_label INTEGER,
  judge_confidence REAL,
  judge_rationale TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  UNIQUE(run_id, seller_message_id, rule_key),
  FOREIGN KEY(run_id) REFERENCES scan_runs(run_id),
  FOREIGN KEY(seller_message_id) REFERENCES messages(message_id),
  FOREIGN KEY(customer_message_id) REFERENCES messages(message_id)
);

CREATE TABLE IF NOT EXISTS scan_metrics (
  run_id TEXT NOT NULL,
  rule_key TEXT NOT NULL,
  eval_total INTEGER NOT NULL,
  eval_true INTEGER NOT NULL,
  evaluator_hit_rate REAL NOT NULL,
  judge_correctness REAL NOT NULL,
  judge_coverage REAL NOT NULL,
  judged_total INTEGER NOT NULL,
  judge_true INTEGER NOT NULL,
  judge_false INTEGER NOT NULL,
  created_at_utc TEXT NOT NULL,
  PRIMARY KEY(run_id, rule_key),
  FOREIGN KEY(run_id) REFERENCES scan_runs(run_id)
);

CREATE TABLE IF NOT EXISTS llm_calls (
  call_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  phase TEXT NOT NULL,
  rule_key TEXT NOT NULL DEFAULT '',
  conversation_id TEXT NOT NULL DEFAULT '',
  message_id INTEGER NOT NULL DEFAULT 0,
  attempt INTEGER NOT NULL,
  context_mode TEXT NOT NULL DEFAULT 'full',
  judge_policy TEXT NOT NULL DEFAULT 'full',
  trace_mode TEXT NOT NULL DEFAULT 'full',
  prompt_chars INTEGER NOT NULL DEFAULT 0,
  response_chars INTEGER NOT NULL DEFAULT 0,
  request_json TEXT NOT NULL,
  response_http_status INTEGER NOT NULL,
  response_json TEXT NOT NULL,
  extracted_json TEXT NOT NULL,
  parse_ok INTEGER NOT NULL,
  validation_ok INTEGER NOT NULL,
  error_message TEXT NOT NULL DEFAULT '',
  latency_ms INTEGER NOT NULL DEFAULT 0,
  created_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS app_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_order ON messages(conversation_id, message_order);
CREATE INDEX IF NOT EXISTS idx_scan_results_run_rule ON scan_results(run_id, rule_key);
CREATE INDEX IF NOT EXISTS idx_scan_results_run_seller ON scan_results(run_id, seller_message_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_run_phase ON llm_calls(run_id, phase);
"""


def connect(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def replace_all_data(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM scan_results;
        DELETE FROM scan_metrics;
        DELETE FROM llm_calls;
        DELETE FROM scan_runs;
        DELETE FROM app_state;
        DELETE FROM messages;
        DELETE FROM conversations;
        """
    )


def reset_run_data(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM scan_results;
        DELETE FROM scan_metrics;
        DELETE FROM llm_calls;
        DELETE FROM scan_runs;
        DELETE FROM app_state WHERE key='canonical_run_id';
        """
    )
    conn.commit()


def db_stats(conn: sqlite3.Connection) -> dict[str, int]:
    keys = [
        "conversations",
        "messages",
        "scan_runs",
        "scan_results",
        "scan_metrics",
        "llm_calls",
    ]
    return {key: int(conn.execute(f"SELECT COUNT(*) FROM {key}").fetchone()[0]) for key in keys}


def touch_conversation_counts(conn: sqlite3.Connection) -> None:
    now = now_utc()
    conn.execute(
        """
        UPDATE conversations
        SET message_count = (
          SELECT COUNT(*) FROM messages m WHERE m.conversation_id = conversations.conversation_id
        ),
        updated_at_utc = ?
        """,
        (now,),
    )


def get_state(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM app_state WHERE key=?", (key,)).fetchone()
    return None if row is None else str(row["value"])


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO app_state(key, value, updated_at_utc)
        VALUES(?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at_utc=excluded.updated_at_utc
        """,
        (key, value, now_utc()),
    )
    conn.commit()


def schema_dictionary_missing_entries(conn: sqlite3.Connection) -> list[str]:
    missing: list[str] = []
    tables = [
        str(row["name"])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
    ]
    for table in tables:
        table_dict = SCHEMA_DICTIONARY_RU.get(table)
        if not table_dict or "__table__" not in table_dict:
            missing.append(f"table:{table}")
            table_dict = {}
        columns = [
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        for column in columns:
            if column not in table_dict:
                missing.append(f"column:{table}.{column}")
    return missing
