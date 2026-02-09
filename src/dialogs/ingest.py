from __future__ import annotations

import csv
from pathlib import Path
import sqlite3

from .db import replace_all_data, touch_conversation_counts
from .utils import normalize_speaker, now_utc

REQUIRED_COLUMNS = ["Conversation", "Chunk_id", "Speaker", "Text", "Embedding"]


def _csv_files(csv_dir: str) -> list[Path]:
    files = sorted(Path(csv_dir).glob("*.csv"))
    if not files:
        raise ValueError(f"no csv files found in {csv_dir}")
    return files


def ingest_csv_dir(conn: sqlite3.Connection, csv_dir: str, replace: bool) -> dict[str, int]:
    files = _csv_files(csv_dir)
    if replace:
        replace_all_data(conn)

    total_rows = 0
    now = now_utc()

    for path in files:
        with path.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None:
                raise ValueError(f"empty csv file: {path}")
            cols = [c.lstrip("\ufeff") for c in reader.fieldnames]
            if cols != REQUIRED_COLUMNS:
                raise ValueError(f"invalid header in {path.name}: got={cols}, want={REQUIRED_COLUMNS}")

            rows: list[dict[str, object]] = []
            for row_num, row in enumerate(reader, start=2):
                conversation_id = (row.get("Conversation") or "").strip() or path.stem
                chunk_raw = (row.get("Chunk_id") or "").strip()
                if not chunk_raw.isdigit():
                    raise ValueError(f"invalid Chunk_id in {path.name}:{row_num} -> {chunk_raw!r}")
                chunk_id = int(chunk_raw)
                speaker = normalize_speaker((row.get("Speaker") or "").strip())
                text = (row.get("Text") or "").strip()
                if not text:
                    raise ValueError(f"empty Text in {path.name}:{row_num}")

                rows.append(
                    {
                        "conversation_id": conversation_id,
                        "chunk_id": chunk_id,
                        "speaker_label": speaker,
                        "text": text,
                    }
                )

            if not rows:
                raise ValueError(f"no data rows in {path.name}")

            conversation_id = str(rows[0]["conversation_id"])
            conn.execute(
                """
                INSERT INTO conversations(conversation_id, source_file_name, message_count, created_at_utc, updated_at_utc)
                VALUES(?, ?, 0, ?, ?)
                ON CONFLICT(conversation_id) DO UPDATE SET
                  source_file_name=excluded.source_file_name,
                  updated_at_utc=excluded.updated_at_utc
                """,
                (conversation_id, path.name, now, now),
            )

            rows.sort(key=lambda r: int(r["chunk_id"]))
            for order, row in enumerate(rows, start=1):
                conn.execute(
                    """
                    INSERT OR REPLACE INTO messages(
                      message_id, conversation_id, source_chunk_id, message_order,
                      speaker_label, text, created_at_utc, updated_at_utc
                    ) VALUES(
                      (SELECT message_id FROM messages WHERE conversation_id=? AND source_chunk_id=?),
                      ?, ?, ?, ?, ?,
                      COALESCE((SELECT created_at_utc FROM messages WHERE conversation_id=? AND source_chunk_id=?), ?),
                      ?
                    )
                    """,
                    (
                        row["conversation_id"],
                        row["chunk_id"],
                        row["conversation_id"],
                        row["chunk_id"],
                        order,
                        row["speaker_label"],
                        row["text"],
                        row["conversation_id"],
                        row["chunk_id"],
                        now,
                        now,
                    ),
                )
            total_rows += len(rows)

    touch_conversation_counts(conn)
    conn.commit()
    return {"files": len(files), "rows": total_rows}
