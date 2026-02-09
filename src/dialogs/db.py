from __future__ import annotations

import sqlite3
from pathlib import Path

from .utils import now_utc

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS review_items;
DROP TABLE IF EXISTS review_cases;
DROP TABLE IF EXISTS rule_results;
DROP TABLE IF EXISTS experiment_metrics;
DROP TABLE IF EXISTS experiment_runs;
DROP TABLE IF EXISTS rule_versions;
DROP TABLE IF EXISTS rules;
DROP TABLE IF EXISTS llm_runs;
DROP TABLE IF EXISTS llm_calls;

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
  speaker_raw TEXT NOT NULL,
  speaker_label TEXT NOT NULL,
  text TEXT NOT NULL,
  embedding_json TEXT NOT NULL,
  extra_json TEXT NOT NULL DEFAULT '{}',
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
  message_id INTEGER NOT NULL,
  rule_key TEXT NOT NULL,
  eval_hit INTEGER NOT NULL,
  eval_confidence REAL NOT NULL,
  evidence_quote TEXT NOT NULL,
  evidence_message_id INTEGER NOT NULL,
  eval_reason TEXT NOT NULL,
  judge_label INTEGER,
  judge_confidence REAL,
  judge_rationale TEXT,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  UNIQUE(run_id, message_id, rule_key),
  FOREIGN KEY(run_id) REFERENCES scan_runs(run_id),
  FOREIGN KEY(message_id) REFERENCES messages(message_id)
);

CREATE TABLE IF NOT EXISTS scan_metrics (
  metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  rule_key TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  metric_value REAL NOT NULL,
  created_at_utc TEXT NOT NULL,
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
CREATE INDEX IF NOT EXISTS idx_scan_metrics_run_rule ON scan_metrics(run_id, rule_key, metric_name);
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
