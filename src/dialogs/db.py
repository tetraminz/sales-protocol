from __future__ import annotations

import sqlite3
from pathlib import Path

from .utils import now_utc

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

CREATE TABLE IF NOT EXISTS rules (
  rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_key TEXT NOT NULL UNIQUE,
  natural_language TEXT NOT NULL,
  language TEXT NOT NULL,
  status TEXT NOT NULL,
  compile_error TEXT NOT NULL DEFAULT '',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rule_versions (
  version_id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id INTEGER NOT NULL,
  version_no INTEGER NOT NULL,
  compiled_spec_json TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  UNIQUE(rule_id, version_no),
  FOREIGN KEY(rule_id) REFERENCES rules(rule_id)
);

CREATE TABLE IF NOT EXISTS experiment_runs (
  run_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  rule_set TEXT NOT NULL,
  model TEXT NOT NULL,
  git_commit TEXT NOT NULL,
  git_branch TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  sgr_version TEXT NOT NULL,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT NOT NULL,
  status TEXT NOT NULL,
  summary_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS experiment_metrics (
  metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  rule_key TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  metric_value REAL NOT NULL,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES experiment_runs(run_id)
);

CREATE TABLE IF NOT EXISTS rule_results (
  result_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  rule_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  message_id INTEGER NOT NULL,
  mode TEXT NOT NULL,
  hit INTEGER NOT NULL,
  confidence REAL NOT NULL,
  evidence TEXT NOT NULL,
  reason TEXT NOT NULL,
  judge_label INTEGER,
  judge_confidence REAL,
  judge_rationale TEXT,
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES experiment_runs(run_id),
  FOREIGN KEY(rule_id) REFERENCES rules(rule_id),
  FOREIGN KEY(version_id) REFERENCES rule_versions(version_id),
  FOREIGN KEY(message_id) REFERENCES messages(message_id)
);

CREATE TABLE IF NOT EXISTS llm_runs (
  run_id TEXT PRIMARY KEY,
  run_kind TEXT NOT NULL,
  mode TEXT NOT NULL,
  model TEXT NOT NULL,
  git_commit TEXT NOT NULL,
  git_branch TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  sgr_version TEXT NOT NULL,
  started_at_utc TEXT NOT NULL,
  finished_at_utc TEXT NOT NULL,
  status TEXT NOT NULL,
  meta_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS llm_calls (
  call_id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  rule_id INTEGER,
  conversation_id TEXT NOT NULL DEFAULT '',
  message_id INTEGER NOT NULL DEFAULT 0,
  phase TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  request_json TEXT NOT NULL,
  response_http_status INTEGER NOT NULL,
  response_json TEXT NOT NULL,
  extracted_json TEXT NOT NULL,
  parse_ok INTEGER NOT NULL,
  validation_ok INTEGER NOT NULL,
  error_message TEXT NOT NULL DEFAULT '',
  latency_ms INTEGER NOT NULL DEFAULT 0,
  tokens_json TEXT NOT NULL DEFAULT '{}',
  created_at_utc TEXT NOT NULL,
  FOREIGN KEY(run_id) REFERENCES llm_runs(run_id)
);

CREATE TABLE IF NOT EXISTS review_cases (
  case_id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  business_area TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_items (
  item_id INTEGER PRIMARY KEY AUTOINCREMENT,
  case_id INTEGER NOT NULL,
  conversation_id TEXT NOT NULL,
  message_id INTEGER NOT NULL,
  decision TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  created_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  UNIQUE(case_id, message_id),
  FOREIGN KEY(case_id) REFERENCES review_cases(case_id),
  FOREIGN KEY(message_id) REFERENCES messages(message_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation_order ON messages(conversation_id, message_order);
CREATE INDEX IF NOT EXISTS idx_rules_status ON rules(status);
CREATE INDEX IF NOT EXISTS idx_rule_results_run_rule ON rule_results(run_id, rule_id);
CREATE INDEX IF NOT EXISTS idx_llm_calls_run_phase ON llm_calls(run_id, phase);
CREATE INDEX IF NOT EXISTS idx_experiment_metrics_run ON experiment_metrics(run_id, rule_key, metric_name);
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
        DELETE FROM rule_results;
        DELETE FROM experiment_metrics;
        DELETE FROM experiment_runs;
        DELETE FROM llm_calls;
        DELETE FROM llm_runs;
        DELETE FROM review_items;
        DELETE FROM review_cases;
        DELETE FROM messages;
        DELETE FROM conversations;
        """
    )


def db_stats(conn: sqlite3.Connection) -> dict[str, int]:
    keys = [
        "conversations",
        "messages",
        "rules",
        "rule_versions",
        "rule_results",
        "llm_runs",
        "llm_calls",
        "review_cases",
        "review_items",
        "experiment_runs",
        "experiment_metrics",
    ]
    out: dict[str, int] = {}
    for key in keys:
        out[key] = conn.execute(f"SELECT COUNT(*) FROM {key}").fetchone()[0]
    return out


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
