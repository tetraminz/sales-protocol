from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

from .db import get_state, set_state
from .llm import LLMClient
from .models import Evidence, EvaluatorResult, JudgeResult
from .report_image import write_accuracy_diff_png
from .sgr_core import (
    METRICS_VERSION,
    all_rules,
    build_chat_context,
    build_evaluator_prompts,
    build_evidence_correction_note,
    build_judge_prompts,
    default_reason_code,
    evidence_error,
    heatmap_zone,
    is_seller_message,
    judge_inconsistency_flags,
    normalize_evidence_span,
    quality_thresholds,
    threshold_doc_line,
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
        str(row["conversation_id"])
        for row in conn.execute(
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


def _warn_soft_flag(counters: dict[str, int], *, rule_key: str, message_id: int, flags: list[str]) -> None:
    counters["judge_inconsistency_soft_flags"] += 1
    if counters["judge_inconsistency_soft_flags"] <= 3:
        print(f"[scan][soft-flag] rule={rule_key} msg={message_id} flags={','.join(flags)}")
    elif counters["judge_inconsistency_soft_flags"] == 4:
        print("[scan][soft-flag] additional judge inconsistencies suppressed")


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


def _run_metrics_version(conn: sqlite3.Connection, *, run_id: str | None) -> str | None:
    if not run_id:
        return None
    row = conn.execute(
        "SELECT json_extract(summary_json, '$.metrics_version') AS metrics_version FROM scan_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if row is None:
        return None
    value = row["metrics_version"]
    return None if value in (None, "") else str(value)


def _span_from_quote(*, text: str, quote: str) -> tuple[int, int]:
    if not quote:
        return 0, 0
    idx = text.find(quote)
    if idx < 0:
        return 0, 0
    return idx, idx + len(quote)


def _fallback_evaluator_result(row: sqlite3.Row) -> EvaluatorResult:
    text = str(row["text"])
    quote = str(row["evidence_quote"])

    span_start = row["evidence_span_start"]
    span_end = row["evidence_span_end"]
    if span_start is None or span_end is None:
        span_start, span_end = _span_from_quote(text=text, quote=quote)

    reason_code = str(row["eval_reason_code"] or "")
    if not reason_code:
        reason_code = default_reason_code(str(row["rule_key"]), hit=bool(row["eval_hit"]))

    return EvaluatorResult(
        hit=bool(row["eval_hit"]),
        confidence=float(row["eval_confidence"]),
        evidence=Evidence(
            quote=quote,
            message_id=int(row["evidence_message_id"]),
            span_start=int(span_start),
            span_end=int(span_end),
        ),
        reason_code=reason_code,  # type: ignore[arg-type]
        reason=str(row["eval_reason"]),
    )


def _compute_metrics(conn: sqlite3.Connection, *, run_id: str) -> None:
    conn.execute("DELETE FROM scan_metrics WHERE run_id=?", (run_id,))
    for rule in all_rules():
        row = conn.execute(
            """
            SELECT
              SUM(CASE WHEN judge_label=1 THEN 1 ELSE 0 END) AS judge_true,
              SUM(CASE WHEN judge_label=0 THEN 1 ELSE 0 END) AS judge_false,
              SUM(CASE WHEN judge_label IS NOT NULL THEN 1 ELSE 0 END) AS judged_total
            FROM scan_results
            WHERE run_id=? AND rule_key=?
            """,
            (run_id, rule.key),
        ).fetchone()

        judge_true = int(row["judge_true"] or 0)
        judge_false = int(row["judge_false"] or 0)
        judged_total = int(row["judged_total"] or 0)
        judge_correctness = _safe_div(float(judge_true), float(judged_total))

        conn.execute(
            """
            INSERT INTO scan_metrics(
              run_id, rule_key, judge_correctness, judged_total, judge_true, judge_false, created_at_utc
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                rule.key,
                float(judge_correctness),
                int(judged_total),
                int(judge_true),
                int(judge_false),
                now_utc(),
            ),
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
        by_conversation.setdefault(str(message["conversation_id"]), []).append(message)

    rules = all_rules()
    run_id = f"scan_{uuid.uuid4().hex[:12]}"
    seller_messages = sum(1 for msg in messages if is_seller_message(str(msg["speaker_label"])))
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
        "judge_inconsistency_soft_flags": 0,
    }
    summary = {
        "conversation_from": conversation_from,
        "conversation_to": conversation_to,
        "selected_conversations": len(conversation_ids),
        "messages": len(messages),
        "seller_messages": seller_messages,
        "customer_messages_context_only": len(messages) - seller_messages,
        "rules": len(rules),
        "metrics_version": METRICS_VERSION,
    }

    print(
        f"[scan] conversations={len(conversation_ids)} messages={len(messages)} seller_messages={seller_messages} "
        f"rules={len(rules)} range={conversation_from}..{conversation_to}"
    )

    status = "failed"
    conv_pos = {cid: idx + 1 for idx, cid in enumerate(conversation_ids)}
    context_by_message_id: dict[int, str] = {}
    evaluator_cache: dict[tuple[str, int, str], EvaluatorResult] = {}
    try:
        for conversation_id in conversation_ids:
            conversation_messages = by_conversation.get(conversation_id, [])
            print(f"[scan] evaluator conversation {conv_pos[conversation_id]}/{len(conversation_ids)} id={conversation_id}")

            for message in conversation_messages:
                if not is_seller_message(str(message["speaker_label"])):
                    continue

                message_id = int(message["message_id"])
                message_text = str(message["text"])
                chat_context = build_chat_context(
                    conversation_messages,
                    current_message_order=int(message["message_order"]),
                )
                context_by_message_id[message_id] = chat_context

                for rule in rules:
                    counters["processed"] += 1
                    sys_prompt, user_prompt = build_evaluator_prompts(
                        rule,
                        speaker_label=str(message["speaker_label"]),
                        text=message_text,
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

                    call.parsed = normalize_evidence_span(call.parsed, text=message_text)
                    err = evidence_error(call.parsed, message_id=message_id, text=message_text)
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

                        retry.parsed = normalize_evidence_span(retry.parsed, text=message_text)
                        err = evidence_error(retry.parsed, message_id=message_id, text=message_text)
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

                    evaluator_cache[(conversation_id, message_id, rule.key)] = call.parsed
                    conn.execute(
                        """
                        INSERT INTO scan_results(
                          run_id, conversation_id, message_id, rule_key,
                          eval_hit, eval_confidence, eval_reason_code, eval_reason,
                          evidence_quote, evidence_message_id, evidence_span_start, evidence_span_end,
                          judge_expected_hit, judge_label, judge_confidence, judge_rationale,
                          created_at_utc, updated_at_utc
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?)
                        """,
                        (
                            run_id,
                            conversation_id,
                            message_id,
                            rule.key,
                            1 if call.parsed.hit else 0,
                            float(call.parsed.confidence),
                            str(call.parsed.reason_code),
                            call.parsed.reason,
                            call.parsed.evidence.quote,
                            int(call.parsed.evidence.message_id),
                            int(call.parsed.evidence.span_start),
                            int(call.parsed.evidence.span_end),
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
              sr.eval_hit, sr.eval_confidence, sr.eval_reason_code, sr.eval_reason,
              sr.evidence_quote, sr.evidence_message_id, sr.evidence_span_start, sr.evidence_span_end,
              m.speaker_label, m.text
            FROM scan_results sr
            JOIN messages m ON m.message_id = sr.message_id
            WHERE sr.run_id=?
            ORDER BY sr.result_id
            """,
            (run_id,),
        ).fetchall()

        by_rule = {rule.key: rule for rule in rules}
        current_conv = ""
        for row in judge_rows:
            conversation_id = str(row["conversation_id"])
            if conversation_id != current_conv:
                current_conv = conversation_id
                print(f"[scan] judge conversation {conv_pos[current_conv]}/{len(conversation_ids)} id={current_conv}")

            message_id = int(row["message_id"])
            rule_key = str(row["rule_key"])
            eval_result = evaluator_cache.get((conversation_id, message_id, rule_key), _fallback_evaluator_result(row))
            chat_context = context_by_message_id.get(message_id, f"[{row['speaker_label']}] {row['text']}")
            sys_prompt, user_prompt = build_judge_prompts(
                by_rule[rule_key],
                speaker_label=str(row["speaker_label"]),
                text=str(row["text"]),
                chat_context=chat_context,
                evaluator=eval_result,
            )

            call = llm.call_json_schema(
                conn,
                run_id=run_id,
                phase="judge",
                rule_key=rule_key,
                conversation_id=conversation_id,
                message_id=message_id,
                model_type=JudgeResult,
                system_prompt=sys_prompt,
                user_prompt=user_prompt,
            )
            if call.is_schema_error:
                counters["schema_errors"] += 1
                raise ValueError(f"schema_error {call.error_message}")
            if call.error_message:
                _warn_non_schema(counters, phase="judge", rule_key=rule_key, error=call.error_message)
                continue
            if not isinstance(call.parsed, JudgeResult):
                counters["schema_errors"] += 1
                raise ValueError("schema_error judge payload type mismatch")

            flags = judge_inconsistency_flags(evaluator_hit=eval_result.hit, judge=call.parsed)
            if flags:
                _warn_soft_flag(counters, rule_key=rule_key, message_id=message_id, flags=flags)

            conn.execute(
                """
                UPDATE scan_results
                SET judge_expected_hit=?, judge_label=?, judge_confidence=?, judge_rationale=?, updated_at_utc=?
                WHERE result_id=?
                """,
                (
                    1 if call.parsed.expected_hit else 0,
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
        canonical_version = _run_metrics_version(conn, run_id=canonical_run_id)
        if canonical_run_id is None or canonical_version != METRICS_VERSION:
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
            f"judge_inconsistency_soft_flags={counters['judge_inconsistency_soft_flags']} "
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
        "SELECT rule_key, judge_correctness FROM scan_metrics WHERE run_id=? ORDER BY rule_key",
        (run_id,),
    ).fetchall()
    return {str(row["rule_key"]): float(row["judge_correctness"]) for row in rows}


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
          SUM(CASE WHEN judge_label=1 THEN 1 ELSE 0 END) AS correct_total
        FROM scan_results
        WHERE run_id=?
        GROUP BY conversation_id, rule_key
        """,
        (run_id,),
    ).fetchall()
    by_cell: dict[tuple[str, str], tuple[int, int]] = {
        (str(row["conversation_id"]), str(row["rule_key"])): (
            int(row["judged_total"] or 0),
            int(row["correct_total"] or 0),
        )
        for row in rows
    }

    scores: list[list[float | None]] = []
    judged_totals: list[list[int]] = []
    for conversation_id in conversation_ids:
        row_scores: list[float | None] = []
        row_judged: list[int] = []
        for rule_key in rule_keys:
            judged, correct = by_cell.get((conversation_id, rule_key), (0, 0))
            row_judged.append(judged)
            row_scores.append(_safe_div(float(correct), float(judged)) if judged else None)
        scores.append(row_scores)
        judged_totals.append(row_judged)

    return {
        "conversation_ids": conversation_ids,
        "rule_keys": list(rule_keys),
        "scores": scores,
        "judged_totals": judged_totals,
    }


def _heatmap_zone(score: float | None) -> str:
    return heatmap_zone(score, thresholds=quality_thresholds())


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
    return [
        {
            "conversation_id": conversation_id,
            "rule_key": rule_key,
            "score": float(score),
            "judged_total": int(judged),
        }
        for score, _neg_judged, conversation_id, rule_key, judged in ranked[: max(0, int(limit))]
    ]


def _canonical_run_for_version(
    conn: sqlite3.Connection, *, run_id: str, metrics_version: str
) -> tuple[str, str | None]:
    canonical = get_state(conn, "canonical_run_id")
    if canonical and _run_metrics_version(conn, run_id=canonical) == metrics_version:
        return canonical, None

    row = conn.execute(
        """
        SELECT run_id
        FROM scan_runs
        WHERE status='success' AND json_extract(summary_json, '$.metrics_version')=?
        ORDER BY started_at_utc ASC
        LIMIT 1
        """,
        (metrics_version,),
    ).fetchone()
    if row is not None:
        return str(row["run_id"]), "canonical run switched to version-compatible baseline"
    return run_id, "canonical run fell back to current run due to metrics version mismatch"


def _bad_cases(conn: sqlite3.Connection, *, run_id: str, limit: int = 20) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
          sr.conversation_id,
          sr.message_id,
          sr.rule_key,
          sr.eval_hit,
          sr.judge_expected_hit,
          sr.eval_reason_code,
          sr.eval_reason,
          sr.judge_rationale,
          sr.evidence_quote,
          sr.eval_confidence,
          sr.judge_confidence,
          m.text
        FROM scan_results sr
        JOIN messages m ON m.message_id = sr.message_id
        WHERE sr.run_id=? AND sr.judge_label=0
        ORDER BY ABS(sr.eval_confidence - COALESCE(sr.judge_confidence, 0)) DESC, sr.rule_key, sr.message_id
        LIMIT ?
        """,
        (run_id, int(limit)),
    ).fetchall()
    return [
        {
            "conversation_id": str(row["conversation_id"]),
            "message_id": int(row["message_id"]),
            "rule_key": str(row["rule_key"]),
            "eval_hit": int(row["eval_hit"]),
            "judge_expected_hit": None
            if row["judge_expected_hit"] is None
            else int(row["judge_expected_hit"]),
            "eval_reason_code": str(row["eval_reason_code"]),
            "eval_reason": str(row["eval_reason"]),
            "judge_rationale": str(row["judge_rationale"] or ""),
            "evidence_quote": str(row["evidence_quote"]),
            "eval_confidence": float(row["eval_confidence"]),
            "judge_confidence": None if row["judge_confidence"] is None else float(row["judge_confidence"]),
            "text": str(row["text"]),
        }
        for row in rows
    ]


def _md_cell(text: str, max_len: int = 120) -> str:
    clipped = text.replace("\n", " ").strip()
    if len(clipped) > max_len:
        clipped = clipped[: max(0, max_len - 1)] + "…"
    return clipped.replace("|", "\\|")


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

    metrics_version = _run_metrics_version(conn, run_id=run_id) or METRICS_VERSION
    canonical_run_id, canonical_note = _canonical_run_for_version(
        conn,
        run_id=run_id,
        metrics_version=metrics_version,
    )

    current = _accuracy_map(conn, run_id=run_id)
    canonical = _accuracy_map(conn, run_id=canonical_run_id)
    rule_keys = [rule.key for rule in all_rules()]

    heatmap = _build_accuracy_heatmap_data(conn, run_id=run_id, rule_keys=rule_keys)
    conversation_ids = [str(value) for value in heatmap["conversation_ids"]]
    scores = heatmap["scores"]
    judged_totals = heatmap["judged_totals"]
    zone_counts = _summarize_heatmap_zones(scores)
    worst_cells = _worst_heatmap_cells(
        conversation_ids=conversation_ids,
        rule_keys=rule_keys,
        scores=scores,
        judged_totals=judged_totals,
    )
    bad_cases = _bad_cases(conn, run_id=run_id, limit=20)

    run_summary = conn.execute(
        "SELECT summary_json FROM scan_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    inserted = int(
        conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0]
    )
    judged = int(
        conn.execute(
            "SELECT COUNT(*) FROM scan_results WHERE run_id=? AND judge_label IS NOT NULL",
            (run_id,),
        ).fetchone()[0]
    )
    llm_rows = conn.execute(
        """
        SELECT phase, COUNT(*) AS calls, SUM(CASE WHEN error_message<>'' THEN 1 ELSE 0 END) AS errors
        FROM llm_calls
        WHERE run_id=?
        GROUP BY phase
        ORDER BY phase
        """,
        (run_id,),
    ).fetchall()

    ensure_parent(md_path)
    cfg = quality_thresholds()
    lines = [
        "# SGR Scan Metrics",
        "",
        f"- metrics_version: `{metrics_version}`",
        f"- canonical_run_id: `{canonical_run_id}`",
        f"- current_run_id: `{run_id}`",
        f"- inserted_results: `{inserted}`",
        f"- judged_results: `{judged}`",
        f"- judge_coverage: `{_safe_div(float(judged), float(inserted)):.4f}`",
        "",
        "## Rule Quality (judge_correctness)",
        "",
        "| rule | canonical | current | delta |",
        "|---|---:|---:|---:|",
    ]

    for key in rule_keys:
        can = canonical.get(key, 0.0)
        cur = current.get(key, 0.0)
        delta = cur - can
        sign = "+" if delta > 0 else ""
        lines.append(f"| `{key}` | {can:.4f} | {cur:.4f} | {sign}{delta:.4f} |")

    if canonical_note:
        lines.extend(["", f"- note: {canonical_note}"])

    lines.extend(
        [
            "",
            "## LLM Calls (full trace persisted in `llm_calls`)",
            "",
            "| phase | calls | errors |",
            "|---|---:|---:|",
        ]
    )
    if llm_rows:
        for row in llm_rows:
            lines.append(f"| `{row['phase']}` | {int(row['calls'])} | {int(row['errors'] or 0)} |")
    else:
        lines.append("| `-` | 0 | 0 |")

    lines.extend(
        [
            "",
            "## Judge-Aligned Heatmap",
            "",
            f"- thresholds: `{threshold_doc_line(thresholds=cfg)}`",
            f"- conversations: `{len(conversation_ids)}`",
            f"- rules: `{len(rule_keys)}`",
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

    lines.extend(
        [
            "",
            "## Judge-Confirmed Bad Cases (judge_label=0)",
            "",
            "| conversation_id | message_id | rule | eval_hit | expected_hit | reason_code | evidence_quote |",
            "|---|---:|---|---:|---:|---|---|",
        ]
    )

    if bad_cases:
        for case in bad_cases:
            lines.append(
                f"| `{case['conversation_id']}` | {case['message_id']} | `{case['rule_key']}` | "
                f"{case['eval_hit']} | {case['judge_expected_hit']} | `{_md_cell(str(case['eval_reason_code']), 60)}` | "
                f"`{_md_cell(str(case['evidence_quote']), 80)}` |"
            )

        lines.extend(["", "### Bad Case Details", ""])
        for idx, case in enumerate(bad_cases[:10], start=1):
            lines.append(
                f"{idx}. `{case['conversation_id']}` msg={case['message_id']} rule=`{case['rule_key']}` "
                f"eval_hit={case['eval_hit']} expected_hit={case['judge_expected_hit']}"
            )
            lines.append(f"   text: {_md_cell(str(case['text']), 200)}")
            lines.append(f"   evaluator_reason: {_md_cell(str(case['eval_reason']), 200)}")
            lines.append(f"   judge_rationale: {_md_cell(str(case['judge_rationale']), 200)}")
            lines.append(f"   evidence_quote: {_md_cell(str(case['evidence_quote']), 120)}")
    else:
        lines.append("| `-` | 0 | `-` | 0 | 0 | `-` | `-` |")

    if run_summary is not None:
        lines.extend(["", "## Run Summary JSON", "", f"- summary_json: `{str(run_summary['summary_json'])}`"])

    Path(md_path).write_text("\n".join(lines) + "\n", encoding="utf-8")

    ensure_parent(png_path)
    write_accuracy_diff_png(
        png_path,
        rule_keys=rule_keys,
        conversation_ids=conversation_ids,
        scores=scores,
        thresholds=cfg,
    )

    return {
        "run_id": run_id,
        "canonical_run_id": canonical_run_id,
        "metrics_version": metrics_version,
        "rules": len(rule_keys),
        "md_path": md_path,
        "png_path": png_path,
        "bad_cases": len(bad_cases),
    }
