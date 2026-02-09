from __future__ import annotations

import argparse
import os

from .db import connect, init_db
from .eval import run_diff, run_eval
from .ingest import ingest_csv_dir
from .llm import LLMClient
from .rules import seed_default_rules


def run_demo(
    db_path: str = "dialogs.db",
    csv_dir: str = "csv",
    conversation_from: int = 0,
    conversation_to: int = 4,
) -> dict[str, str]:
    """Minimal executive demo: ingest -> baseline -> sgr -> diff."""
    init_db(db_path)
    llm = LLMClient(model="gpt-4.1-mini", api_key=os.getenv("OPENAI_API_KEY", ""))

    with connect(db_path) as conn:
        seed_default_rules(conn)
        ingest_csv_dir(conn, csv_dir=csv_dir, replace=True)
        baseline_id = run_eval(
            conn,
            llm=llm,
            mode="baseline",
            rule_set="default",
            prompt_version="v1",
            sgr_version="v1",
            conversation_from=conversation_from,
            conversation_to=conversation_to,
        )
        sgr_id = run_eval(
            conn,
            llm=llm,
            mode="sgr",
            rule_set="default",
            prompt_version="v1",
            sgr_version="v1",
            conversation_from=conversation_from,
            conversation_to=conversation_to,
        )
        run_diff(conn, run_a=baseline_id, run_b=sgr_id, png_path="artifacts/accuracy_diff.png", md_path="artifacts/metrics.md")

    return {"baseline_run": baseline_id, "sgr_run": sgr_id}


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
    print(f"demo_ok baseline={result['baseline_run']} sgr={result['sgr_run']}")
