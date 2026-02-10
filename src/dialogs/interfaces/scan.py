from __future__ import annotations

import sqlite3

from ..infrastructure.scan_runner import run_scan as _run_scan
from ..llm import LLMClient


def run_scan(
    conn: sqlite3.Connection,
    *,
    llm: LLMClient,
    conversation_from: int = 0,
    conversation_to: int = 4,
    run_id_override: str | None = None,
) -> str:
    return _run_scan(
        conn,
        llm=llm,
        conversation_from=conversation_from,
        conversation_to=conversation_to,
        run_id_override=run_id_override,
    )
