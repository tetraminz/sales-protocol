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


def _build_accuracy_heatmap_data(
    conn: sqlite3.Connection, *, run_id: str, rule_keys: list[str]
) -> dict[str, list[Any]]:
    conversation_ids = [
        str(row["conversation_id"])
        for row in conn.execute(
            "SELECT DISTINCT conversation_id FROM scan_results WHERE run_id=? ORDER BY conversation_id",
            (run_id,),
        ).fetchall()
    ]
    rows = conn.execute(
        """
        SELECT
          conversation_id,
          rule_key,
          SUM(CASE WHEN judge_label IS NOT NULL THEN 1 ELSE 0 END) AS judged_total,
          SUM(CASE WHEN judge_label IS NOT NULL AND eval_hit=judge_label THEN 1 ELSE 0 END) AS agree_total
        FROM scan_results
        WHERE run_id=?
        GROUP BY conversation_id, rule_key
        """,
        (run_id,),
    ).fetchall()

    by_cell: dict[tuple[str, str], tuple[int, int]] = {
        (str(row["conversation_id"]), str(row["rule_key"])): (
            int(row["judged_total"] or 0),
            int(row["agree_total"] or 0),
        )
        for row in rows
    }

    scores: list[list[float | None]] = []
    judged_totals: list[list[int]] = []
    for conversation_id in conversation_ids:
        row_scores: list[float | None] = []
        row_judged: list[int] = []
        for rule_key in rule_keys:
            judged, agree = by_cell.get((conversation_id, rule_key), (0, 0))
            row_judged.append(judged)
            row_scores.append(_safe_div(float(agree), float(judged)) if judged else None)
        scores.append(row_scores)
        judged_totals.append(row_judged)

    return {
        "conversation_ids": conversation_ids,
        "rule_keys": list(rule_keys),
        "scores": scores,
        "judged_totals": judged_totals,
    }


def _heatmap_zone(score: float | None) -> str:
    if score is None:
        return "na"
    if score >= 0.90:
        return "green"
    if score >= 0.70:
        return "yellow"
    return "red"


def _summarize_heatmap_zones(scores: list[list[float | None]]) -> dict[str, int]:
    counts = {"green": 0, "yellow": 0, "red": 0, "na": 0}
    for row in scores:
        for score in row:
            counts[_heatmap_zone(score)] += 1
    return counts


def _worst_heatmap_cells(
    *,
    conversation_ids: list[str],
    rule_keys: list[str],
    scores: list[list[float | None]],
    judged_totals: list[list[int]],
    limit: int = 10,
) -> list[dict[str, Any]]:
    ranked: list[tuple[float, int, str, str, int]] = []
    for row_idx, conversation_id in enumerate(conversation_ids):
        for col_idx, rule_key in enumerate(rule_keys):
            judged = int(judged_totals[row_idx][col_idx])
            score = scores[row_idx][col_idx]
            if judged <= 0 or score is None:
                continue
            ranked.append((float(score), -judged, conversation_id, rule_key, judged))

    ranked.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    out: list[dict[str, Any]] = []
    for score, _neg_judged, conversation_id, rule_key, judged in ranked[: max(0, int(limit))]:
        out.append(
            {
                "conversation_id": conversation_id,
                "rule_key": rule_key,
                "score": float(score),
                "judged_total": int(judged),
            }
        )
    return out


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
    heatmap = _build_accuracy_heatmap_data(conn, run_id=run_id, rule_keys=rule_keys)
    conversation_ids = [str(x) for x in heatmap["conversation_ids"]]
    scores = heatmap["scores"]
    judged_totals = heatmap["judged_totals"]
    zone_counts = _summarize_heatmap_zones(scores)
    worst_cells = _worst_heatmap_cells(
        conversation_ids=conversation_ids,
        rule_keys=rule_keys,
        scores=scores,
        judged_totals=judged_totals,
    )
    total_cells = len(conversation_ids) * len(rule_keys)
    judged_cells = sum(1 for row in judged_totals for judged in row if int(judged) > 0)

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

    lines.extend(
        [
            "",
            "## Judge-Aligned Heatmap",
            "",
            "- thresholds: `green >= 0.90`, `yellow >= 0.70`, `red < 0.70`, `na = no_judged`",
            f"- conversations: `{len(conversation_ids)}`",
            f"- rules: `{len(rule_keys)}`",
            f"- judged_cells: `{judged_cells}/{total_cells}`",
            "",
            "| zone | cells |",
            "|---|---:|",
            f"| `green` | {zone_counts['green']} |",
            f"| `yellow` | {zone_counts['yellow']} |",
            f"| `red` | {zone_counts['red']} |",
            f"| `na` | {zone_counts['na']} |",
            "",
            "### Worst conversation x rule cells",
            "",
            "| conversation_id | rule | score | judged_total |",
            "|---|---|---:|---:|",
        ]
    )
    if worst_cells:
        for cell in worst_cells:
            lines.append(
                f"| `{cell['conversation_id']}` | `{cell['rule_key']}` | {float(cell['score']):.4f} | {int(cell['judged_total'])} |"
            )
    else:
        lines.append("| `-` | `-` | n/a | 0 |")

    Path(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    ensure_parent(png_path)
    _write_accuracy_diff_png(png_path, rule_keys, conversation_ids, scores)

    return {
        "run_id": run_id,
        "canonical_run_id": canonical_run_id,
        "rules": len(rule_keys),
        "md_path": md_path,
        "png_path": png_path,
}


_FONT_5X7: dict[str, tuple[str, ...]] = {
    " ": ("00000", "00000", "00000", "00000", "00000", "00000", "00000"),
    "-": ("00000", "00000", "00000", "11111", "00000", "00000", "00000"),
    "_": ("00000", "00000", "00000", "00000", "00000", "00000", "11111"),
    ".": ("00000", "00000", "00000", "00000", "00000", "01100", "01100"),
    "/": ("00001", "00010", "00100", "01000", "10000", "00000", "00000"),
    "0": ("01110", "10001", "10011", "10101", "11001", "10001", "01110"),
    "1": ("00100", "01100", "00100", "00100", "00100", "00100", "01110"),
    "2": ("01110", "10001", "00001", "00010", "00100", "01000", "11111"),
    "3": ("11110", "00001", "00001", "01110", "00001", "00001", "11110"),
    "4": ("00010", "00110", "01010", "10010", "11111", "00010", "00010"),
    "5": ("11111", "10000", "10000", "11110", "00001", "00001", "11110"),
    "6": ("01110", "10000", "10000", "11110", "10001", "10001", "01110"),
    "7": ("11111", "00001", "00010", "00100", "01000", "10000", "10000"),
    "8": ("01110", "10001", "10001", "01110", "10001", "10001", "01110"),
    "9": ("01110", "10001", "10001", "01111", "00001", "00001", "01110"),
    "A": ("01110", "10001", "10001", "11111", "10001", "10001", "10001"),
    "B": ("11110", "10001", "10001", "11110", "10001", "10001", "11110"),
    "C": ("01110", "10001", "10000", "10000", "10000", "10001", "01110"),
    "D": ("11100", "10010", "10001", "10001", "10001", "10010", "11100"),
    "E": ("11111", "10000", "10000", "11110", "10000", "10000", "11111"),
    "F": ("11111", "10000", "10000", "11110", "10000", "10000", "10000"),
    "G": ("01110", "10001", "10000", "10111", "10001", "10001", "01110"),
    "H": ("10001", "10001", "10001", "11111", "10001", "10001", "10001"),
    "I": ("01110", "00100", "00100", "00100", "00100", "00100", "01110"),
    "J": ("00111", "00010", "00010", "00010", "00010", "10010", "01100"),
    "K": ("10001", "10010", "10100", "11000", "10100", "10010", "10001"),
    "L": ("10000", "10000", "10000", "10000", "10000", "10000", "11111"),
    "M": ("10001", "11011", "10101", "10101", "10001", "10001", "10001"),
    "N": ("10001", "11001", "10101", "10011", "10001", "10001", "10001"),
    "O": ("01110", "10001", "10001", "10001", "10001", "10001", "01110"),
    "P": ("11110", "10001", "10001", "11110", "10000", "10000", "10000"),
    "Q": ("01110", "10001", "10001", "10001", "10101", "10010", "01101"),
    "R": ("11110", "10001", "10001", "11110", "10100", "10010", "10001"),
    "S": ("01110", "10001", "10000", "01110", "00001", "10001", "01110"),
    "T": ("11111", "00100", "00100", "00100", "00100", "00100", "00100"),
    "U": ("10001", "10001", "10001", "10001", "10001", "10001", "01110"),
    "V": ("10001", "10001", "10001", "10001", "10001", "01010", "00100"),
    "W": ("10001", "10001", "10001", "10101", "10101", "10101", "01010"),
    "X": ("10001", "10001", "01010", "00100", "01010", "10001", "10001"),
    "Y": ("10001", "10001", "01010", "00100", "00100", "00100", "00100"),
    "Z": ("11111", "00001", "00010", "00100", "01000", "10000", "11111"),
    "?": ("01110", "10001", "00001", "00010", "00100", "00000", "00100"),
}


def _new_rgb_image(width: int, height: int, rgb: tuple[int, int, int]) -> list[list[list[int]]]:
    return [[[rgb[0], rgb[1], rgb[2]] for _ in range(width)] for _ in range(height)]


def _fill_rect(
    img: list[list[list[int]]],
    *,
    x: int,
    y: int,
    w: int,
    h: int,
    color: tuple[int, int, int],
) -> None:
    height = len(img)
    width = len(img[0]) if height else 0
    x0 = max(0, x)
    y0 = max(0, y)
    x1 = min(width, x + max(0, w))
    y1 = min(height, y + max(0, h))
    for yy in range(y0, y1):
        for xx in range(x0, x1):
            img[yy][xx] = [color[0], color[1], color[2]]


def _stroke_rect(
    img: list[list[list[int]]],
    *,
    x: int,
    y: int,
    w: int,
    h: int,
    color: tuple[int, int, int],
) -> None:
    if w <= 0 or h <= 0:
        return
    _fill_rect(img, x=x, y=y, w=w, h=1, color=color)
    _fill_rect(img, x=x, y=y + h - 1, w=w, h=1, color=color)
    _fill_rect(img, x=x, y=y, w=1, h=h, color=color)
    _fill_rect(img, x=x + w - 1, y=y, w=1, h=h, color=color)


def _char_bitmap(ch: str) -> tuple[str, ...]:
    return _FONT_5X7.get(ch, _FONT_5X7["?"])


def _measure_text(text: str, *, scale: int = 1, letter_gap: int = 1) -> int:
    if not text:
        return 0
    chars = [ch if ch in _FONT_5X7 else "?" for ch in text.upper()]
    char_w = 5 * scale
    return len(chars) * char_w + max(0, len(chars) - 1) * letter_gap


def _draw_text(
    img: list[list[list[int]]],
    *,
    x: int,
    y: int,
    text: str,
    color: tuple[int, int, int],
    scale: int = 1,
    letter_gap: int = 1,
) -> None:
    xx = x
    for raw in text.upper():
        ch = raw if raw in _FONT_5X7 else "?"
        bitmap = _char_bitmap(ch)
        for row_idx, row_bits in enumerate(bitmap):
            for col_idx, bit in enumerate(row_bits):
                if bit == "1":
                    _fill_rect(
                        img,
                        x=xx + col_idx * scale,
                        y=y + row_idx * scale,
                        w=scale,
                        h=scale,
                        color=color,
                    )
        xx += 5 * scale + letter_gap


def _write_accuracy_diff_png(
    path: str,
    rule_keys: list[str],
    conversation_ids: list[str],
    scores: list[list[float | None]],
) -> None:
    text_color = (22, 22, 22)
    grid_border = (220, 220, 220)
    zone_color = {
        "green": (66, 161, 96),
        "yellow": (227, 182, 67),
        "red": (201, 82, 70),
        "na": (186, 186, 186),
    }

    title_scale = 2
    label_scale = 1
    cell_w = 56
    cell_h = 18
    cell_gap = 2
    left_pad = 16
    right_pad = 16
    top_pad = 14
    bottom_pad = 16
    label_gap = 12
    legend_gap = 12
    legend_chip = 10
    header_label = "CONVERSATION"

    rendered_rows = conversation_ids if conversation_ids else ["NO_DATA"]
    rendered_scores = scores if scores else [[None for _ in rule_keys]]
    longest_row_w = max((_measure_text(row, scale=label_scale) for row in rendered_rows), default=0)
    row_label_w = max(longest_row_w, _measure_text(header_label, scale=label_scale))

    title = "EVAL VS JUDGE HEATMAP"
    title_h = 7 * title_scale
    title_w = _measure_text(title, scale=title_scale)

    legend_items = [
        ("GREEN 0.90-1.00", zone_color["green"]),
        ("YELLOW 0.70-0.89", zone_color["yellow"]),
        ("RED 0.00-0.69", zone_color["red"]),
        ("NA NO_JUDGED", zone_color["na"]),
    ]
    legend_item_widths = [legend_chip + 5 + _measure_text(label, scale=label_scale) for label, _ in legend_items]
    legend_total_w = sum(legend_item_widths) + legend_gap * max(0, len(legend_items) - 1)
    legend_h = max(legend_chip, 7 * label_scale)

    cols = max(1, len(rule_keys))
    grid_w = cols * cell_w + max(0, cols - 1) * cell_gap
    col_label_h = 7 * label_scale
    rows = len(rendered_rows)
    grid_h = rows * cell_h + max(0, rows - 1) * cell_gap

    grid_left = left_pad + row_label_w + label_gap
    col_labels_y = top_pad + title_h + 10 + legend_h + 10
    grid_top = col_labels_y + col_label_h + 8

    width = max(
        grid_left + grid_w + right_pad,
        left_pad + max(title_w, legend_total_w) + right_pad,
    )
    height = grid_top + grid_h + bottom_pad

    img = _new_rgb_image(width, height, (255, 255, 255))
    _draw_text(img, x=left_pad, y=top_pad, text=title, color=text_color, scale=title_scale)

    legend_x = left_pad
    legend_y = top_pad + title_h + 10
    for label, color in legend_items:
        _fill_rect(img, x=legend_x, y=legend_y + 1, w=legend_chip, h=legend_chip, color=color)
        _stroke_rect(img, x=legend_x, y=legend_y + 1, w=legend_chip, h=legend_chip, color=grid_border)
        _draw_text(
            img,
            x=legend_x + legend_chip + 5,
            y=legend_y + 2,
            text=label,
            color=text_color,
            scale=label_scale,
        )
        legend_x += legend_chip + 5 + _measure_text(label, scale=label_scale) + legend_gap

    _draw_text(img, x=left_pad, y=col_labels_y, text=header_label, color=text_color, scale=label_scale)
    for idx, rule_key in enumerate(rule_keys):
        x = grid_left + idx * (cell_w + cell_gap) + 2
        _draw_text(img, x=x, y=col_labels_y, text=rule_key, color=text_color, scale=label_scale)

    for row_idx, conversation_id in enumerate(rendered_rows):
        y = grid_top + row_idx * (cell_h + cell_gap)
        label_y = y + max(0, (cell_h - 7 * label_scale) // 2)
        _draw_text(img, x=left_pad, y=label_y, text=conversation_id, color=text_color, scale=label_scale)
        for col_idx in range(cols):
            x = grid_left + col_idx * (cell_w + cell_gap)
            score = rendered_scores[row_idx][col_idx] if col_idx < len(rendered_scores[row_idx]) else None
            fill = zone_color[_heatmap_zone(score)]
            _fill_rect(img, x=x, y=y, w=cell_w, h=cell_h, color=fill)
            _stroke_rect(img, x=x, y=y, w=cell_w, h=cell_h, color=grid_border)

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
