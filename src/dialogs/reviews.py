from __future__ import annotations

import sqlite3

from .utils import now_utc


def case_create(conn: sqlite3.Connection, *, title: str, business_area: str) -> int:
    now = now_utc()
    cur = conn.execute(
        "INSERT INTO review_cases(title, business_area, status, created_at_utc, updated_at_utc) VALUES(?, ?, 'open', ?, ?)",
        (title.strip(), business_area.strip(), now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def case_update(conn: sqlite3.Connection, *, case_id: int, status: str) -> None:
    if status not in {"open", "closed"}:
        raise ValueError("status must be open or closed")
    conn.execute(
        "UPDATE review_cases SET status=?, updated_at_utc=? WHERE case_id=?",
        (status, now_utc(), int(case_id)),
    )
    conn.commit()


def item_add(
    conn: sqlite3.Connection,
    *,
    case_id: int,
    conversation_id: str,
    message_id: int,
    decision: str,
    note: str,
) -> int:
    if decision not in {"pending", "ok", "not_ok"}:
        raise ValueError("decision must be pending|ok|not_ok")
    now = now_utc()
    cur = conn.execute(
        """
        INSERT INTO review_items(case_id, conversation_id, message_id, decision, note, created_at_utc, updated_at_utc)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        """,
        (int(case_id), conversation_id, int(message_id), decision, note.strip(), now, now),
    )
    conn.commit()
    return int(cur.lastrowid)


def item_update(conn: sqlite3.Connection, *, item_id: int, decision: str | None, note: str | None) -> None:
    row = conn.execute(
        "SELECT decision, note FROM review_items WHERE item_id=?",
        (int(item_id),),
    ).fetchone()
    if not row:
        raise ValueError("item not found")
    new_decision = row["decision"] if decision is None else decision
    if new_decision not in {"pending", "ok", "not_ok"}:
        raise ValueError("decision must be pending|ok|not_ok")
    new_note = row["note"] if note is None else note
    conn.execute(
        "UPDATE review_items SET decision=?, note=?, updated_at_utc=? WHERE item_id=?",
        (new_decision, new_note, now_utc(), int(item_id)),
    )
    conn.commit()


def list_cases(conn: sqlite3.Connection, status: str | None) -> list[sqlite3.Row]:
    if status:
        return conn.execute("SELECT * FROM review_cases WHERE status=? ORDER BY case_id", (status,)).fetchall()
    return conn.execute("SELECT * FROM review_cases ORDER BY case_id").fetchall()


def list_items(conn: sqlite3.Connection, case_id: int) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM review_items WHERE case_id=? ORDER BY item_id", (int(case_id),)).fetchall()
