from __future__ import annotations

import json
import sqlite3
import struct
import uuid
import zlib
from pathlib import Path
from typing import Any

from .db import get_state, set_state
from .llm import LLMClient
from .llm_as_judge import build_judge_prompts
from .models import Evidence, EvaluatorResult, JudgeResult
from .sgr_core import (
    all_rules,
    build_chat_context,
    build_evaluator_prompts,
    build_evidence_correction_note,
    evidence_error,
    is_seller_message,
)
from .utils import ensure_parent, jdump, now_utc


def load_messages_for_range(
    conn: sqlite3.Connection, *, conversation_from: int = 0, conversation_to: int = 4
) -> tuple[list[str], list[sqlite3.Row]]:
    if conversation_from < 0:
        raise ValueError("conversation_from must be >= 0")
    if conversation_to < conversation_from:
        raise ValueError("conversation_to must be >= conversation_from")

    total = int(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0])
    if total == 0:
        raise ValueError("no conversations in db; run ingest first")
    if conversation_from >= total:
        raise ValueError(f"conversation_from={conversation_from} is out of range (total={total})")

    ids = [
        r["conversation_id"]
        for r in conn.execute(
            "SELECT conversation_id FROM conversations ORDER BY conversation_id LIMIT ? OFFSET ?",
            (conversation_to - conversation_from + 1, conversation_from),
        ).fetchall()
    ]
    if not ids:
        raise ValueError("no conversations selected")

    placeholders = ",".join("?" for _ in ids)
    messages = conn.execute(
        f"""
        SELECT message_id, conversation_id, message_order, speaker_label, text
        FROM messages
        WHERE conversation_id IN ({placeholders})
        ORDER BY conversation_id, message_order
        """,
        ids,
    ).fetchall()
    if not messages:
        raise ValueError("selected conversations contain no messages")
    return ids, messages


def _warn_non_schema(counters: dict[str, int], *, phase: str, rule_key: str, error: str) -> None:
    counters["non_schema_errors"] += 1
    counters["skipped_due_to_errors"] += 1
    if counters["non_schema_errors"] <= 3:
        print(f"[scan][warn] phase={phase} rule={rule_key} err={error}")
    elif counters["non_schema_errors"] == 4:
        print("[scan][warn] additional non-schema errors suppressed")


def _insert_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    model: str,
    conversation_from: int,
    conversation_to: int,
    selected_conversations: int,
    messages_count: int,
) -> None:
    conn.execute(
        """
        INSERT INTO scan_runs(
          run_id, model, conversation_from, conversation_to,
          selected_conversations, messages_count, status,
          started_at_utc, finished_at_utc, summary_json
        ) VALUES(?, ?, ?, ?, ?, ?, 'running', ?, '', '{}')
        """,
        (
            run_id,
            model,
            int(conversation_from),
            int(conversation_to),
            int(selected_conversations),
            int(messages_count),
            now_utc(),
        ),
    )
    conn.commit()


def _finish_run(conn: sqlite3.Connection, *, run_id: str, status: str, summary: dict[str, Any]) -> None:
    conn.execute(
        "UPDATE scan_runs SET status=?, finished_at_utc=?, summary_json=? WHERE run_id=?",
        (status, now_utc(), jdump(summary), run_id),
    )
    conn.commit()


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def _compute_metrics(conn: sqlite3.Connection, *, run_id: str) -> None:
    conn.execute("DELETE FROM scan_metrics WHERE run_id=?", (run_id,))
    for rule in all_rules():
        row = conn.execute(
            """
            SELECT
              SUM(CASE WHEN eval_hit=1 AND judge_label=1 THEN 1 ELSE 0 END) AS tp,
              SUM(CASE WHEN eval_hit=1 AND judge_label=0 THEN 1 ELSE 0 END) AS fp,
              SUM(CASE WHEN eval_hit=0 AND judge_label=0 THEN 1 ELSE 0 END) AS tn,
              SUM(CASE WHEN eval_hit=0 AND judge_label=1 THEN 1 ELSE 0 END) AS fn,
              SUM(CASE WHEN judge_label IS NOT NULL THEN 1 ELSE 0 END) AS judged_total
            FROM scan_results
            WHERE run_id=? AND rule_key=?
            """,
            (run_id, rule.key),
        ).fetchone()

        tp = float(row["tp"] or 0)
        fp = float(row["fp"] or 0)
        tn = float(row["tn"] or 0)
        fn = float(row["fn"] or 0)
        total = float(row["judged_total"] or 0)

        precision = _safe_div(tp, tp + fp)
        recall = _safe_div(tp, tp + fn)
        metrics = {
            "accuracy": _safe_div(tp + tn, total),
            "precision": precision,
            "recall": recall,
            "f1": _safe_div(2 * precision * recall, precision + recall),
            "coverage": _safe_div(tp + fp, total),
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
            "total": total,
        }

        for metric_name, metric_value in metrics.items():
            conn.execute(
                "INSERT INTO scan_metrics(run_id, rule_key, metric_name, metric_value, created_at_utc) VALUES(?, ?, ?, ?, ?)",
                (run_id, rule.key, metric_name, float(metric_value), now_utc()),
            )
    conn.commit()


def run_scan(
    conn: sqlite3.Connection,
    *,
    llm: LLMClient,
    conversation_from: int = 0,
    conversation_to: int = 4,
) -> str:
    llm.require_live("run scan")

    conversation_ids, messages = load_messages_for_range(
        conn,
        conversation_from=conversation_from,
        conversation_to=conversation_to,
    )
    by_conversation: dict[str, list[sqlite3.Row]] = {}
    for message in messages:
        by_conversation.setdefault(message["conversation_id"], []).append(message)

    rules = all_rules()
    run_id = f"scan_{uuid.uuid4().hex[:12]}"
    seller_messages = sum(1 for m in messages if is_seller_message(str(m["speaker_label"])))

    _insert_run(
        conn,
        run_id=run_id,
        model=getattr(llm, "model", "unknown"),
        conversation_from=conversation_from,
        conversation_to=conversation_to,
        selected_conversations=len(conversation_ids),
        messages_count=len(messages),
    )

    counters = {
        "processed": 0,
        "inserted": 0,
        "judged": 0,
        "skipped_due_to_errors": 0,
        "evidence_mismatch_skipped": 0,
        "schema_errors": 0,
        "non_schema_errors": 0,
    }
    summary = {
        "conversation_from": conversation_from,
        "conversation_to": conversation_to,
        "selected_conversations": len(conversation_ids),
        "messages": len(messages),
        "seller_messages": seller_messages,
        "customer_messages_context_only": len(messages) - seller_messages,
        "rules": len(rules),
    }

    print(
        f"[scan] conversations={len(conversation_ids)} messages={len(messages)} seller_messages={seller_messages} "
        f"rules={len(rules)} range={conversation_from}..{conversation_to}"
    )

    status = "failed"
    conv_pos = {cid: i + 1 for i, cid in enumerate(conversation_ids)}
    context_by_message_id: dict[int, str] = {}
    try:
        for conversation_id in conversation_ids:
            conversation_messages = by_conversation.get(conversation_id, [])
            print(f"[scan] evaluator conversation {conv_pos[conversation_id]}/{len(conversation_ids)} id={conversation_id}")

            for message in conversation_messages:
                if not is_seller_message(str(message["speaker_label"])):
                    continue

                message_id = int(message["message_id"])
                chat_context = build_chat_context(
                    conversation_messages,
                    current_message_order=int(message["message_order"]),
                )
                context_by_message_id[message_id] = chat_context

                for rule in rules:
                    counters["processed"] += 1
                    sys_prompt, user_prompt = build_evaluator_prompts(
                        rule,
                        speaker_label=message["speaker_label"],
                        text=message["text"],
                        message_id=message_id,
                        chat_context=chat_context,
                    )
                    call = llm.call_json_schema(
                        conn,
                        run_id=run_id,
                        phase="evaluator",
                        rule_key=rule.key,
                        conversation_id=conversation_id,
                        message_id=message_id,
                        model_type=EvaluatorResult,
                        system_prompt=sys_prompt,
                        user_prompt=user_prompt,
                        attempt=1,
                    )
                    if call.is_schema_error:
                        counters["schema_errors"] += 1
                        raise ValueError(f"schema_error {call.error_message}")
                    if call.error_message:
                        _warn_non_schema(counters, phase="evaluator", rule_key=rule.key, error=call.error_message)
                        continue
                    if not isinstance(call.parsed, EvaluatorResult):
                        counters["schema_errors"] += 1
                        raise ValueError("schema_error evaluator payload type mismatch")

                    err = evidence_error(call.parsed, message_id=message_id, text=message["text"])
                    if err:
                        retry = llm.call_json_schema(
                            conn,
                            run_id=run_id,
                            phase="evaluator",
                            rule_key=rule.key,
                            conversation_id=conversation_id,
                            message_id=message_id,
                            model_type=EvaluatorResult,
                            system_prompt=sys_prompt,
                            user_prompt=f"{user_prompt}\n\n{build_evidence_correction_note(message_id)}",
                            attempt=2,
                        )
                        if retry.is_schema_error:
                            counters["schema_errors"] += 1
                            raise ValueError(f"schema_error {retry.error_message}")
                        if retry.error_message:
                            _warn_non_schema(counters, phase="evaluator", rule_key=rule.key, error=retry.error_message)
                            continue
                        if not isinstance(retry.parsed, EvaluatorResult):
                            counters["schema_errors"] += 1
                            raise ValueError("schema_error evaluator retry payload type mismatch")
                        err = evidence_error(retry.parsed, message_id=message_id, text=message["text"])
                        if err:
                            counters["evidence_mismatch_skipped"] += 1
                            _warn_non_schema(
                                counters,
                                phase="evaluator",
                                rule_key=rule.key,
                                error=f"evidence_integrity_failed_after_retry: {err}",
                            )
                            continue
                        call = retry

                    conn.execute(
                        """
                        INSERT INTO scan_results(
                          run_id, conversation_id, message_id, rule_key,
                          eval_hit, eval_confidence, evidence_quote, evidence_message_id, eval_reason,
                          judge_label, judge_confidence, judge_rationale, created_at_utc, updated_at_utc
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?)
                        """,
                        (
                            run_id,
                            conversation_id,
                            message_id,
                            rule.key,
                            1 if call.parsed.hit else 0,
                            float(call.parsed.confidence),
                            call.parsed.evidence.quote,
                            int(call.parsed.evidence.message_id),
                            call.parsed.reason,
                            now_utc(),
                            now_utc(),
                        ),
                    )
                    counters["inserted"] += 1
        conn.commit()

        judge_rows = conn.execute(
            """
            SELECT
              sr.result_id, sr.conversation_id, sr.message_id, sr.rule_key,
              sr.eval_hit, sr.eval_confidence, sr.evidence_quote, sr.evidence_message_id, sr.eval_reason,
              m.speaker_label, m.text
            FROM scan_results sr
            JOIN messages m ON m.message_id = sr.message_id
            WHERE sr.run_id=?
            ORDER BY sr.result_id
            """,
            (run_id,),
        ).fetchall()
        by_key = {r.key: r for r in rules}

        current_conv = ""
        for row in judge_rows:
            if row["conversation_id"] != current_conv:
                current_conv = row["conversation_id"]
                print(f"[scan] judge conversation {conv_pos[current_conv]}/{len(conversation_ids)} id={current_conv}")

            message_id = int(row["message_id"])
            chat_context = context_by_message_id.get(
                message_id,
                f"[{row['speaker_label']}] {row['text']}",
            )
            eval_result = EvaluatorResult(
                hit=bool(row["eval_hit"]),
                confidence=float(row["eval_confidence"]),
                evidence=Evidence(
                    quote=row["evidence_quote"],
                    message_id=int(row["evidence_message_id"]),
                ),
                reason=row["eval_reason"],
            )
            rule = by_key[row["rule_key"]]
            sys_prompt, user_prompt = build_judge_prompts(
                rule,
                speaker_label=row["speaker_label"],
                text=row["text"],
                chat_context=chat_context,
                evaluator=eval_result,
            )
            call = llm.call_json_schema(
                conn,
                run_id=run_id,
                phase="judge",
                rule_key=row["rule_key"],
                conversation_id=row["conversation_id"],
                message_id=message_id,
                model_type=JudgeResult,
                system_prompt=sys_prompt,
                user_prompt=user_prompt,
            )
            if call.is_schema_error:
                counters["schema_errors"] += 1
                raise ValueError(f"schema_error {call.error_message}")
            if call.error_message:
                _warn_non_schema(counters, phase="judge", rule_key=row["rule_key"], error=call.error_message)
                continue
            if not isinstance(call.parsed, JudgeResult):
                counters["schema_errors"] += 1
                raise ValueError("schema_error judge payload type mismatch")

            conn.execute(
                """
                UPDATE scan_results
                SET judge_label=?, judge_confidence=?, judge_rationale=?, updated_at_utc=?
                WHERE result_id=?
                """,
                (
                    1 if call.parsed.label else 0,
                    float(call.parsed.confidence),
                    call.parsed.rationale,
                    now_utc(),
                    int(row["result_id"]),
                ),
            )
            counters["judged"] += 1
        conn.commit()

        _compute_metrics(conn, run_id=run_id)

        canonical_run_id = get_state(conn, "canonical_run_id")
        if canonical_run_id is None:
            set_state(conn, "canonical_run_id", run_id)
            canonical_run_id = run_id

        summary.update(counters)
        summary["canonical_run_id"] = canonical_run_id
        status = "success"
        print(
            "[scan] summary "
            f"processed={counters['processed']} inserted={counters['inserted']} judged={counters['judged']} "
            f"skipped_due_to_errors={counters['skipped_due_to_errors']} "
            f"evidence_mismatch_skipped={counters['evidence_mismatch_skipped']} "
            f"schema_errors={counters['schema_errors']}"
        )
        return run_id
    except Exception as exc:
        summary.update(counters)
        summary["error"] = str(exc)
        raise
    finally:
        _finish_run(conn, run_id=run_id, status=status, summary=summary)


def _accuracy_map(conn: sqlite3.Connection, *, run_id: str) -> dict[str, float]:
    rows = conn.execute(
        "SELECT rule_key, metric_value FROM scan_metrics WHERE run_id=? AND metric_name='accuracy' ORDER BY rule_key",
        (run_id,),
    ).fetchall()
    return {row["rule_key"]: float(row["metric_value"]) for row in rows}


def build_report(
    conn: sqlite3.Connection,
    *,
    run_id: str | None = None,
    md_path: str = "artifacts/metrics.md",
    png_path: str = "artifacts/accuracy_diff.png",
) -> dict[str, Any]:
    if run_id is None:
        row = conn.execute(
            "SELECT run_id FROM scan_runs WHERE status='success' ORDER BY started_at_utc DESC LIMIT 1"
        ).fetchone()
        if row is None:
            raise ValueError("no successful scan run found")
        run_id = str(row["run_id"])

    canonical_run_id = get_state(conn, "canonical_run_id")
    if canonical_run_id is None:
        raise ValueError("canonical_run_id is not set")

    current = _accuracy_map(conn, run_id=run_id)
    canonical = _accuracy_map(conn, run_id=canonical_run_id)
    rule_keys = [rule.key for rule in all_rules()]

    vals_can = [canonical.get(key, 0.0) for key in rule_keys]
    vals_cur = [current.get(key, 0.0) for key in rule_keys]
    deltas = [cur - can for can, cur in zip(vals_can, vals_cur)]

    ensure_parent(md_path)
    lines = [
        "# SGR Scan Metrics",
        "",
        f"- canonical_run_id: `{canonical_run_id}`",
        f"- current_run_id: `{run_id}`",
        "",
        "| rule | canonical_accuracy | current_accuracy | delta |",
        "|---|---:|---:|---:|",
    ]
    for idx, key in enumerate(rule_keys):
        delta = deltas[idx]
        sign = "+" if delta > 0 else ""
        lines.append(f"| `{key}` | {vals_can[idx]:.4f} | {vals_cur[idx]:.4f} | {sign}{delta:.4f} |")

    Path(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    ensure_parent(png_path)
    _write_accuracy_diff_png(png_path, rule_keys, vals_can, vals_cur)

    return {
        "run_id": run_id,
        "canonical_run_id": canonical_run_id,
        "rules": len(rule_keys),
        "md_path": md_path,
        "png_path": png_path,
    }


def _write_accuracy_diff_png(path: str, rule_keys: list[str], baseline: list[float], current: list[float]) -> None:
    count = max(1, len(rule_keys))
    bar_w = 20
    gap = 12
    group_w = bar_w * 2 + gap
    margin = 24
    width = margin * 2 + group_w * count
    height = 260
    baseline_y = height - 24
    max_h = 190

    img = [[[255, 255, 255] for _ in range(width)] for _ in range(height)]
    for x in range(margin - 4, width - margin + 4):
        img[baseline_y][x] = [40, 40, 40]

    for i in range(count):
        x0 = margin + i * group_w
        h_a = int(max_h * max(0.0, min(1.0, baseline[i])))
        h_b = int(max_h * max(0.0, min(1.0, current[i])))

        for x in range(x0, x0 + bar_w):
            for y in range(baseline_y - h_a, baseline_y):
                if 0 <= y < height:
                    img[y][x] = [120, 120, 120]

        xb = x0 + bar_w + 4
        for x in range(xb, xb + bar_w):
            for y in range(baseline_y - h_b, baseline_y):
                if 0 <= y < height:
                    img[y][x] = [52, 168, 83]

    _write_png_rgb(path, img)


def _write_png_rgb(path: str, img: list[list[list[int]]]) -> None:
    h = len(img)
    w = len(img[0]) if h else 0
    raw = bytearray()
    for y in range(h):
        raw.append(0)
        for x in range(w):
            raw.extend(bytes(img[y][x]))
    comp = zlib.compress(bytes(raw), level=9)

    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0)
    payload = sig + chunk(b"IHDR", ihdr) + chunk(b"IDAT", comp) + chunk(b"IEND", b"")
    Path(path).write_bytes(payload)
