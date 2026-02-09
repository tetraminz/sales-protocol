from __future__ import annotations

import argparse
import os
import sqlite3
import sys

from .db import connect, db_stats, init_db
from .eval import run_diff, run_eval
from .ingest import ingest_csv_dir
from .llm import LLMClient
from .messages import add_message, list_messages, update_message
from .reviews import case_create, case_update, item_add, item_update, list_cases, list_items
from .rules import add_rule, approve_rule, list_rules, seed_default_rules


def _conn(db: str) -> sqlite3.Connection:
    return connect(db)


def _cmd_db_init(args: argparse.Namespace) -> int:
    init_db(args.db)
    with _conn(args.db) as conn:
        seed_default_rules(conn)
    print(f"db_initialized db={args.db}")
    return 0


def _cmd_db_stats(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        stats = db_stats(conn)
    for k, v in stats.items():
        print(f"{k}={v}")
    return 0


def _cmd_data_ingest(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        res = ingest_csv_dir(conn, csv_dir=args.csv_dir, replace=args.replace)
    print(f"ingest_ok files={res['files']} rows={res['rows']}")
    return 0


def _cmd_conversations_list(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        rows = conn.execute(
            """
            SELECT conversation_id, source_file_name, message_count
            FROM conversations
            ORDER BY conversation_id
            LIMIT ? OFFSET ?
            """,
            (args.limit, args.offset),
        ).fetchall()
    for r in rows:
        print(f"{r['conversation_id']}|{r['source_file_name']}|messages={r['message_count']}")
    return 0


def _cmd_conversations_show(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        row = conn.execute(
            "SELECT conversation_id, source_file_name, message_count FROM conversations WHERE conversation_id=?",
            (args.id,),
        ).fetchone()
    if not row:
        raise ValueError(f"conversation_id={args.id} not found")
    print(f"{row['conversation_id']}|{row['source_file_name']}|messages={row['message_count']}")
    return 0


def _cmd_messages_list(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        rows = list_messages(conn, args.conversation_id, limit=args.limit, offset=args.offset)
    for r in rows:
        print(f"{r['message_id']}|{r['source_chunk_id']}|{r['speaker_label']}|{r['text']}")
    return 0


def _cmd_messages_add(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        message_id = add_message(
            conn,
            conversation_id=args.conversation_id,
            chunk_id=args.chunk_id,
            speaker=args.speaker,
            text=args.text,
        )
    print(f"message_added message_id={message_id}")
    return 0


def _cmd_messages_update(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        out = update_message(conn, args.message_id, speaker=args.speaker, text=args.text)
    print(f"message_updated message_id={out['message_id']} speaker={out['speaker_label']}")
    return 0


def _cmd_rules_add(args: argparse.Namespace) -> int:
    llm = LLMClient(model=args.model, api_key=os.getenv("OPENAI_API_KEY", ""))
    with _conn(args.db) as conn:
        out = add_rule(
            conn,
            llm,
            natural_language=args.nl,
            language=args.lang,
            prompt_version=args.prompt_version,
        )
    print(
        "rule_added "
        f"rule_id={out['rule_id']} key={out['rule_key']} "
        f"status={out['status']} compile_error={out['compile_error']}"
    )
    return 0


def _cmd_rules_list(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        rows = list_rules(conn, status=args.status)
    for r in rows:
        print(f"{r['rule_id']}|{r['rule_key']}|{r['status']}|{r['compile_error']}")
    return 0


def _cmd_rules_approve(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        approve_rule(conn, args.rule_id)
    print(f"rule_approved rule_id={args.rule_id}")
    return 0


def _cmd_run_eval(args: argparse.Namespace) -> int:
    llm = LLMClient(model=args.model, api_key=os.getenv("OPENAI_API_KEY", ""))
    with _conn(args.db) as conn:
        run_id = run_eval(
            conn,
            llm=llm,
            mode=args.mode,
            rule_set=args.rule_set,
            prompt_version=args.prompt_version,
            sgr_version=args.sgr_version,
            conversation_from=args.conversation_from,
            conversation_to=args.conversation_to,
        )
    print(f"eval_ok run_id={run_id}")
    return 0


def _cmd_run_diff(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        out = run_diff(conn, run_a=args.run_a, run_b=args.run_b, png_path=args.png, md_path=args.md)
    print(f"diff_ok rules={out['rules']} png={args.png} md={args.md}")
    return 0


def _cmd_llm_logs(args: argparse.Namespace) -> int:
    where = "WHERE run_id=?"
    params: tuple[object, ...] = (args.run_id,)
    if args.failed_only:
        where += " AND (parse_ok=0 OR validation_ok=0 OR error_message<>'')"
    with _conn(args.db) as conn:
        rows = conn.execute(
            f"SELECT call_id, phase, attempt, response_http_status, parse_ok, validation_ok, error_message FROM llm_calls {where} ORDER BY call_id",
            params,
        ).fetchall()
    for r in rows:
        print(
            f"{r['call_id']}|{r['phase']}|attempt={r['attempt']}|"
            f"http={r['response_http_status']}|parse={r['parse_ok']}|validation={r['validation_ok']}|err={r['error_message']}"
        )
    return 0


def _cmd_review_case_create(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        case_id = case_create(conn, title=args.title, business_area=args.business_area)
    print(f"review_case_created case_id={case_id}")
    return 0


def _cmd_review_case_update(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        case_update(conn, case_id=args.case_id, status=args.status)
    print(f"review_case_updated case_id={args.case_id} status={args.status}")
    return 0


def _cmd_review_item_add(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        item_id = item_add(
            conn,
            case_id=args.case_id,
            conversation_id=args.conversation_id,
            message_id=args.message_id,
            decision=args.decision,
            note=args.note,
        )
    print(f"review_item_added item_id={item_id}")
    return 0


def _cmd_review_item_update(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        item_update(conn, item_id=args.item_id, decision=args.decision, note=args.note)
    print(f"review_item_updated item_id={args.item_id}")
    return 0


def _cmd_review_cases(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        rows = list_cases(conn, status=args.status)
    for r in rows:
        print(f"{r['case_id']}|{r['title']}|{r['status']}")
    return 0


def _cmd_review_items(args: argparse.Namespace) -> int:
    with _conn(args.db) as conn:
        rows = list_items(conn, args.case_id)
    for r in rows:
        print(f"{r['item_id']}|{r['message_id']}|{r['decision']}|{r['note']}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dialogs", description="Tiny CLI for dialogs SGR platform")
    sub = parser.add_subparsers(dest="group")

    db = sub.add_parser("db")
    db_sub = db.add_subparsers(dest="cmd")
    db_init = db_sub.add_parser("init")
    db_init.add_argument("--db", default="dialogs.db")
    db_init.set_defaults(func=_cmd_db_init)
    db_stats = db_sub.add_parser("stats")
    db_stats.add_argument("--db", default="dialogs.db")
    db_stats.set_defaults(func=_cmd_db_stats)

    data = sub.add_parser("data")
    data_sub = data.add_subparsers(dest="cmd")
    ingest = data_sub.add_parser("ingest-csv")
    ingest.add_argument("--csv-dir", default="csv")
    ingest.add_argument("--db", default="dialogs.db")
    ingest.add_argument("--replace", action="store_true")
    ingest.set_defaults(func=_cmd_data_ingest)

    conv = sub.add_parser("conversations")
    conv_sub = conv.add_subparsers(dest="cmd")
    conv_list = conv_sub.add_parser("list")
    conv_list.add_argument("--db", default="dialogs.db")
    conv_list.add_argument("--limit", type=int, default=100)
    conv_list.add_argument("--offset", type=int, default=0)
    conv_list.set_defaults(func=_cmd_conversations_list)

    conv_show = conv_sub.add_parser("show")
    conv_show.add_argument("--id", required=True)
    conv_show.add_argument("--db", default="dialogs.db")
    conv_show.set_defaults(func=_cmd_conversations_show)

    msg = sub.add_parser("messages")
    msg_sub = msg.add_subparsers(dest="cmd")
    msg_list = msg_sub.add_parser("list")
    msg_list.add_argument("--conversation-id", required=True)
    msg_list.add_argument("--db", default="dialogs.db")
    msg_list.add_argument("--limit", type=int, default=200)
    msg_list.add_argument("--offset", type=int, default=0)
    msg_list.set_defaults(func=_cmd_messages_list)

    msg_add = msg_sub.add_parser("add")
    msg_add.add_argument("--conversation-id", required=True)
    msg_add.add_argument("--chunk-id", type=int, required=True)
    msg_add.add_argument("--speaker", required=True)
    msg_add.add_argument("--text", required=True)
    msg_add.add_argument("--db", default="dialogs.db")
    msg_add.set_defaults(func=_cmd_messages_add)

    msg_upd = msg_sub.add_parser("update")
    msg_upd.add_argument("--message-id", type=int, required=True)
    msg_upd.add_argument("--speaker")
    msg_upd.add_argument("--text")
    msg_upd.add_argument("--db", default="dialogs.db")
    msg_upd.set_defaults(func=_cmd_messages_update)

    rules = sub.add_parser("rules")
    rules_sub = rules.add_subparsers(dest="cmd")
    r_add = rules_sub.add_parser("add")
    r_add.add_argument("--nl", required=True)
    r_add.add_argument("--lang", default="ru")
    r_add.add_argument("--db", default="dialogs.db")
    r_add.add_argument("--prompt-version", default="v1")
    r_add.add_argument("--model", default="gpt-4.1-mini")
    r_add.set_defaults(func=_cmd_rules_add)

    r_list = rules_sub.add_parser("list")
    r_list.add_argument("--status")
    r_list.add_argument("--db", default="dialogs.db")
    r_list.set_defaults(func=_cmd_rules_list)

    r_approve = rules_sub.add_parser("approve")
    r_approve.add_argument("--rule-id", type=int, required=True)
    r_approve.add_argument("--db", default="dialogs.db")
    r_approve.set_defaults(func=_cmd_rules_approve)

    run = sub.add_parser("run")
    run_sub = run.add_subparsers(dest="cmd")
    run_eval_cmd = run_sub.add_parser("eval")
    run_eval_cmd.add_argument("--mode", choices=["baseline", "sgr"], required=True)
    run_eval_cmd.add_argument("--db", default="dialogs.db")
    run_eval_cmd.add_argument("--rule-set", default="default")
    run_eval_cmd.add_argument("--prompt-version", default="v1")
    run_eval_cmd.add_argument("--sgr-version", default="v1")
    run_eval_cmd.add_argument("--conversation-from", type=int, default=0)
    run_eval_cmd.add_argument("--conversation-to", type=int, default=4)
    run_eval_cmd.add_argument("--model", default="gpt-4.1-mini")
    run_eval_cmd.set_defaults(func=_cmd_run_eval)

    run_diff_cmd = run_sub.add_parser("diff")
    run_diff_cmd.add_argument("--run-a", required=True)
    run_diff_cmd.add_argument("--run-b", required=True)
    run_diff_cmd.add_argument("--png", default="artifacts/accuracy_diff.png")
    run_diff_cmd.add_argument("--md", default="artifacts/metrics.md")
    run_diff_cmd.add_argument("--db", default="dialogs.db")
    run_diff_cmd.set_defaults(func=_cmd_run_diff)

    llm = sub.add_parser("llm")
    llm_sub = llm.add_subparsers(dest="cmd")
    ll = llm_sub.add_parser("logs")
    ll.add_argument("--run-id", required=True)
    ll.add_argument("--failed-only", action="store_true")
    ll.add_argument("--db", default="dialogs.db")
    ll.set_defaults(func=_cmd_llm_logs)

    reviews = sub.add_parser("reviews")
    reviews_sub = reviews.add_subparsers(dest="cmd")

    rc_create = reviews_sub.add_parser("case-create")
    rc_create.add_argument("--title", required=True)
    rc_create.add_argument("--business-area", default="sales_quality")
    rc_create.add_argument("--db", default="dialogs.db")
    rc_create.set_defaults(func=_cmd_review_case_create)

    rc_update = reviews_sub.add_parser("case-update")
    rc_update.add_argument("--case-id", type=int, required=True)
    rc_update.add_argument("--status", required=True)
    rc_update.add_argument("--db", default="dialogs.db")
    rc_update.set_defaults(func=_cmd_review_case_update)

    ri_add = reviews_sub.add_parser("item-add")
    ri_add.add_argument("--case-id", type=int, required=True)
    ri_add.add_argument("--conversation-id", required=True)
    ri_add.add_argument("--message-id", type=int, required=True)
    ri_add.add_argument("--decision", default="pending")
    ri_add.add_argument("--note", default="")
    ri_add.add_argument("--db", default="dialogs.db")
    ri_add.set_defaults(func=_cmd_review_item_add)

    ri_update = reviews_sub.add_parser("item-update")
    ri_update.add_argument("--item-id", type=int, required=True)
    ri_update.add_argument("--decision")
    ri_update.add_argument("--note")
    ri_update.add_argument("--db", default="dialogs.db")
    ri_update.set_defaults(func=_cmd_review_item_update)

    rcases = reviews_sub.add_parser("cases")
    rcases.add_argument("--status")
    rcases.add_argument("--db", default="dialogs.db")
    rcases.set_defaults(func=_cmd_review_cases)

    ritems = reviews_sub.add_parser("items")
    ritems.add_argument("--case-id", type=int, required=True)
    ritems.add_argument("--db", default="dialogs.db")
    ritems.set_defaults(func=_cmd_review_items)

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
