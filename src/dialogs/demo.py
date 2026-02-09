from __future__ import annotations

import argparse
import os

from .db import connect, init_db
from .ingest import ingest_csv_dir
from .llm import LLMClient
from .pipeline import build_report, run_scan


def run_demo(
    db_path: str = "dialogs.db",
    csv_dir: str = "csv",
    conversation_from: int = 0,
    conversation_to: int = 4,
) -> dict[str, str]:
    init_db(db_path)
    llm = LLMClient(model="gpt-4.1-mini", api_key=os.getenv("OPENAI_API_KEY", ""))

    with connect(db_path) as conn:
        ingest_csv_dir(conn, csv_dir=csv_dir, replace=True)
        run_id = run_scan(
            conn,
            llm=llm,
            conversation_from=conversation_from,
            conversation_to=conversation_to,
        )
        report = build_report(conn, run_id=run_id)

    return {
        "run_id": run_id,
        "canonical_run_id": str(report["canonical_run_id"]),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="dialogs.demo")
    parser.add_argument("--db", default="dialogs.db")
    parser.add_argument("--csv-dir", default="csv")
    parser.add_argument("--conversation-from", type=int, default=0)
    parser.add_argument("--conversation-to", type=int, default=4)
    args = parser.parse_args()
    result = run_demo(
        db_path=args.db,
        csv_dir=args.csv_dir,
        conversation_from=args.conversation_from,
        conversation_to=args.conversation_to,
    )
    print(f"demo_ok run={result['run_id']} canonical={result['canonical_run_id']}")
