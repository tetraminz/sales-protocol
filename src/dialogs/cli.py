from __future__ import annotations

import argparse
import os
import sqlite3
import sys

from .db import connect, db_stats, init_db, reset_run_data
from .ingest import ingest_csv_dir
from .llm import LLMClient
from .pipeline import build_report, run_scan

def _conn(db_path: str) -> sqlite3.Connection:
    return connect(db_path)


def _cmd_db_init(args: argparse.Namespace) -> int:
    init_db(args.db)
    print(f"db_initialized db={args.db}")
    return 0


def _cmd_db_stats(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        stats = db_stats(conn)
    for key, value in stats.items():
        print(f"{key}={value}")
    return 0


def _cmd_db_reset_runs(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        reset_run_data(conn)
    print(f"run_data_reset db={args.db}")
    return 0


def _cmd_data_ingest(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        out = ingest_csv_dir(conn, csv_dir=args.csv_dir, replace=args.replace)
    print(f"ingest_ok files={out['files']} rows={out['rows']}")
    return 0


def _cmd_run_scan(args: argparse.Namespace) -> int:
    llm = LLMClient(model=args.model, api_key=os.getenv("OPENAI_API_KEY", ""))
    with _conn(args.db) as conn:
        run_id = run_scan(
            conn,
            llm=llm,
            conversation_from=args.conversation_from,
            conversation_to=args.conversation_to,
        )
    print(f"scan_ok run_id={run_id}")
    return 0


def _cmd_run_report(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        out = build_report(conn, run_id=args.run_id, md_path=args.md, png_path=args.png)
    print(
        f"report_ok run_id={out['run_id']} canonical={out['canonical_run_id']} "
        f"md={out['md_path']} png={out['png_path']}"
    )
    return 0


def _cmd_llm_logs(args: argparse.Namespace) -> int:
    where = "WHERE run_id=?"
    params: tuple[object, ...] = (args.run_id,)
    if args.phase:
        where += " AND phase=?"
        params = (args.run_id, args.phase)
    if args.failed_only:
        where += " AND (parse_ok=0 OR validation_ok=0 OR error_message<>'')"

    with _conn(args.db) as conn:
        rows = conn.execute(
            f"""
            SELECT call_id, phase, rule_key, conversation_id, message_id,
                   response_http_status, parse_ok, validation_ok, error_message
            FROM llm_calls
            {where}
            ORDER BY call_id
            """,
            params,
        ).fetchall()

    for row in rows:
        print(
            f"{row['call_id']}|{row['phase']}|{row['rule_key']}|"
            f"{row['conversation_id']}|msg={row['message_id']}|"
            f"http={row['response_http_status']}|parse={row['parse_ok']}|"
            f"valid={row['validation_ok']}|err={row['error_message']}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dialogs", description="Minimal SGR scanner")
    sub = parser.add_subparsers(dest="group")

    db = sub.add_parser("db")
    db_sub = db.add_subparsers(dest="cmd")

    db_init = db_sub.add_parser("init")
    db_init.add_argument("--db", default="dialogs.db")
    db_init.set_defaults(func=_cmd_db_init)

    db_stats_cmd = db_sub.add_parser("stats")
    db_stats_cmd.add_argument("--db", default="dialogs.db")
    db_stats_cmd.set_defaults(func=_cmd_db_stats)

    db_reset = db_sub.add_parser("reset-runs")
    db_reset.add_argument("--db", default="dialogs.db")
    db_reset.set_defaults(func=_cmd_db_reset_runs)

    data = sub.add_parser("data")
    data_sub = data.add_subparsers(dest="cmd")

    ingest = data_sub.add_parser("ingest-csv")
    ingest.add_argument("--db", default="dialogs.db")
    ingest.add_argument("--csv-dir", default="csv")
    ingest.add_argument("--replace", action="store_true")
    ingest.set_defaults(func=_cmd_data_ingest)

    run = sub.add_parser("run")
    run_sub = run.add_subparsers(dest="cmd")

    scan = run_sub.add_parser("scan")
    scan.add_argument("--db", default="dialogs.db")
    scan.add_argument("--model", default="gpt-4.1-mini")
    scan.add_argument("--conversation-from", type=int, default=0)
    scan.add_argument("--conversation-to", type=int, default=4)
    scan.set_defaults(func=_cmd_run_scan)

    report = run_sub.add_parser("report")
    report.add_argument("--db", default="dialogs.db")
    report.add_argument("--run-id")
    report.add_argument("--md", default="artifacts/metrics.md")
    report.add_argument("--png", default="artifacts/accuracy_diff.png")
    report.set_defaults(func=_cmd_run_report)

    llm = sub.add_parser("llm")
    llm_sub = llm.add_subparsers(dest="cmd")
    logs = llm_sub.add_parser("logs")
    logs.add_argument("--db", default="dialogs.db")
    logs.add_argument("--run-id", required=True)
    logs.add_argument("--phase", choices=["evaluator", "judge"])
    logs.add_argument("--failed-only", action="store_true")
    logs.set_defaults(func=_cmd_llm_logs)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 1

    try:
        return int(args.func(args))
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
