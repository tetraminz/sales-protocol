from __future__ import annotations

import sqlite3
from typing import Any

from .db import touch_conversation_counts
from .utils import normalize_speaker, now_utc


def list_messages(conn: sqlite3.Connection, conversation_id: str, limit: int = 200, offset: int = 0) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT message_id, conversation_id, source_chunk_id, message_order, speaker_raw, speaker_label, text
        FROM messages
        WHERE conversation_id=?
        ORDER BY message_order
        LIMIT ? OFFSET ?
        """,
        (conversation_id, int(limit), int(offset)),
    ).fetchall()


def add_message(
    conn: sqlite3.Connection,
    *,
    conversation_id: str,
    chunk_id: int,
    speaker: str,
    text: str,
    embedding_json: str = "[]",
) -> int:
    conv = conn.execute(
        "SELECT 1 FROM conversations WHERE conversation_id=?",
        (conversation_id,),
    ).fetchone()
    now = now_utc()
    if not conv:
        conn.execute(
            "INSERT INTO conversations(conversation_id, source_file_name, message_count, created_at_utc, updated_at_utc) VALUES(?, ?, 0, ?, ?)",
            (conversation_id, f"{conversation_id}.csv", now, now),
        )

    max_order = conn.execute(
        "SELECT COALESCE(MAX(message_order), 0) FROM messages WHERE conversation_id=?",
        (conversation_id,),
    ).fetchone()[0]
    cur = conn.execute(
        """
        INSERT INTO messages(
          conversation_id, source_chunk_id, message_order, speaker_raw, speaker_label,
          text, embedding_json, extra_json, created_at_utc, updated_at_utc
        ) VALUES(?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
        """,
        (
            conversation_id,
            int(chunk_id),
            int(max_order) + 1,
            speaker,
            normalize_speaker(speaker),
            text.strip(),
            embedding_json,
            now,
            now,
        ),
    )
    touch_conversation_counts(conn)
    conn.commit()
    return int(cur.lastrowid)


def update_message(conn: sqlite3.Connection, message_id: int, *, speaker: str | None = None, text: str | None = None) -> dict[str, Any]:
    row = conn.execute(
        "SELECT message_id, conversation_id, speaker_raw, speaker_label, text FROM messages WHERE message_id=?",
        (int(message_id),),
    ).fetchone()
    if not row:
        raise ValueError(f"message_id={message_id} not found")

    next_speaker_raw = row["speaker_raw"] if speaker is None else speaker
    next_speaker_label = row["speaker_label"] if speaker is None else normalize_speaker(speaker)
    next_text = row["text"] if text is None else text.strip()

    conn.execute(
        """
        UPDATE messages
        SET speaker_raw=?, speaker_label=?, text=?, updated_at_utc=?
        WHERE message_id=?
        """,
        (next_speaker_raw, next_speaker_label, next_text, now_utc(), int(message_id)),
    )
    conn.commit()
    return {
        "message_id": int(message_id),
        "speaker_raw": next_speaker_raw,
        "speaker_label": next_speaker_label,
        "text": next_text,
    }
