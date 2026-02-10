from __future__ import annotations

import sqlite3
from typing import Any

from ..infrastructure.reporting import build_report as _build_report


def build_report(
    conn: sqlite3.Connection,
    *,
    run_id: str | None = None,
    md_path: str = "artifacts/metrics.md",
    png_path: str = "artifacts/accuracy_diff.png",
) -> dict[str, Any]:
    return _build_report(conn, run_id=run_id, md_path=md_path, png_path=png_path)
