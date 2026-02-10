from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

from .db import get_state, set_state
from .judge import (
    build_evaluator_bundle_model,
    build_judge_bundle_model,
    build_judge_prompt,
    evaluator_results_by_rule,
    judge_results_by_rule,
)
from .llm import LLMClient
from .report_image import write_accuracy_diff_png
from .sgr_core import (
    METRICS_VERSION,
    all_rules,
    build_chat_context,
    build_evaluator_prompts_bundle,
    build_rule_business_context,
    greeting_window_refs,
    heatmap_zone,
    is_seller_message,
    quality_thresholds,
    seller_message_refs,
    threshold_doc_line,
)
from .utils import ensure_parent, jdump, now_utc

FIXED_BUNDLE_RULES = True
FIXED_JUDGE_MODE = "full"
FIXED_CONTEXT_MODE = "full"
FIXED_LLM_TRACE = "full"


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


def _compute_metrics(conn: sqlite3.Connection, *, run_id: str) -> None:
    conn.execute("DELETE FROM scan_metrics WHERE run_id=?", (run_id,))
    for rule in all_rules():
        row = conn.execute(
            """
            SELECT
              COUNT(*) AS eval_total,
              SUM(CASE WHEN eval_hit=1 THEN 1 ELSE 0 END) AS eval_true,
              SUM(CASE WHEN judge_label=1 THEN 1 ELSE 0 END) AS judge_true,
              SUM(CASE WHEN judge_label=0 THEN 1 ELSE 0 END) AS judge_false,
              SUM(CASE WHEN judge_label IS NOT NULL THEN 1 ELSE 0 END) AS judged_total
            FROM scan_results
            WHERE run_id=? AND rule_key=?
            """,
            (run_id, rule.key),
        ).fetchone()

        eval_total = int(row["eval_total"] or 0)
        eval_true = int(row["eval_true"] or 0)
        judge_true = int(row["judge_true"] or 0)
        judge_false = int(row["judge_false"] or 0)
        judged_total = int(row["judged_total"] or 0)

        evaluator_hit_rate = _safe_div(float(eval_true), float(eval_total))
        judge_correctness = _safe_div(float(judge_true), float(judged_total))
        judge_coverage = _safe_div(float(judged_total), float(eval_total))

        conn.execute(
            """
            INSERT INTO scan_metrics(
              run_id, rule_key, eval_total, eval_true, evaluator_hit_rate,
              judge_correctness, judge_coverage, judged_total, judge_true, judge_false, created_at_utc
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                rule.key,
                eval_total,
                eval_true,
                float(evaluator_hit_rate),
                float(judge_correctness),
                float(judge_coverage),
                judged_total,
                judge_true,
                judge_false,
                now_utc(),
            ),
        )
    conn.commit()


def _llm_error_or_raise(*, phase: str, call_error: str, is_schema_error: bool) -> None:
    err_type = "schema_error" if is_schema_error else "live_error"
    raise ValueError(f"{err_type} phase={phase}: {call_error}")


def run_scan(
    conn: sqlite3.Connection,
    *,
    llm: LLMClient,
    conversation_from: int = 0,
    conversation_to: int = 4,
    run_id_override: str | None = None,
) -> str:
    llm.require_live("run scan")
    bundle_rules = FIXED_BUNDLE_RULES
    judge_mode = FIXED_JUDGE_MODE
    context_mode = FIXED_CONTEXT_MODE
    llm_trace = FIXED_LLM_TRACE
    greeting_window_max = 3

    conversation_ids, messages = load_messages_for_range(
        conn,
        conversation_from=conversation_from,
        conversation_to=conversation_to,
    )
    by_conversation: dict[str, list[sqlite3.Row]] = {}
    for message in messages:
        by_conversation.setdefault(str(message["conversation_id"]), []).append(message)

    rules = all_rules()
    rule_keys = tuple(rule.key for rule in rules)
    evaluator_bundle_model = build_evaluator_bundle_model(rule_keys)
    judge_bundle_model = build_judge_bundle_model(rule_keys)
    rule_business_context = build_rule_business_context(rules)
    run_id = run_id_override or f"scan_{uuid.uuid4().hex[:12]}"
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
        "evaluated_conversations": 0,
        "skipped_conversations_without_seller": 0,
        "processed": 0,
        "inserted": 0,
        "judged": 0,
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
        "metrics_version": METRICS_VERSION,
        "bundle_rules": bool(bundle_rules),
        "judge_mode": judge_mode,
        "context_mode": context_mode,
        "llm_trace": llm_trace,
    }

    print(
        f"[scan] conversations={len(conversation_ids)} messages={len(messages)} seller_messages={seller_messages} "
        f"rules={len(rules)} range={conversation_from}..{conversation_to} "
        f"bundle_rules={bundle_rules} judge_mode={judge_mode} context_mode={context_mode} llm_trace={llm_trace}"
    )

    status = "failed"
    conv_pos = {cid: idx + 1 for idx, cid in enumerate(conversation_ids)}
    try:
        for conversation_id in conversation_ids:
            conversation_messages = by_conversation.get(conversation_id, [])
            print(f"[scan] conversation {conv_pos[conversation_id]}/{len(conversation_ids)} id={conversation_id}")

            seller_catalog = seller_message_refs(conversation_messages)
            if not seller_catalog:
                counters["skipped_conversations_without_seller"] += 1
                continue

            counters["evaluated_conversations"] += 1
            counters["processed"] += len(rules)

            chat_context = build_chat_context(
                conversation_messages,
                mode=context_mode,
            )
            greeting_window = greeting_window_refs(
                conversation_messages,
                max_messages=greeting_window_max,
            )
            greeting_window_ids = {item.message_id for item in greeting_window}
            seller_by_id = {item.message_id: item for item in seller_catalog}

            llm_message_id = int(seller_catalog[0].message_id)
            eval_sys, eval_user = build_evaluator_prompts_bundle(
                rules,
                conversation_id=conversation_id,
                chat_context=chat_context,
                seller_catalog=seller_catalog,
                greeting_window_max=greeting_window_max,
                context_mode=context_mode,
            )
            eval_call = llm.call_json_schema(
                conn,
                run_id=run_id,
                phase="evaluator",
                rule_key="bundle",
                conversation_id=conversation_id,
                message_id=llm_message_id,
                model_type=evaluator_bundle_model,
                system_prompt=eval_sys,
                user_prompt=eval_user,
                attempt=1,
            )
            if eval_call.error_message:
                if eval_call.is_schema_error:
                    counters["schema_errors"] += 1
                else:
                    counters["non_schema_errors"] += 1
                _llm_error_or_raise(
                    phase="evaluator",
                    call_error=eval_call.error_message,
                    is_schema_error=eval_call.is_schema_error,
                )
            if not isinstance(eval_call.parsed, evaluator_bundle_model):
                counters["schema_errors"] += 1
                raise ValueError("schema_error evaluator payload type mismatch")

            eval_by_rule = evaluator_results_by_rule(eval_call.parsed, rule_keys=rule_keys)
            for rule in rules:
                eval_result = eval_by_rule[rule.key]
                if not bool(eval_result.hit):
                    continue

                evidence_quote = str(eval_result.evidence_quote).strip()
                evidence_message_id = eval_result.evidence_message_id
                evidence_message_order = eval_result.evidence_message_order
                if evidence_message_id is None or evidence_message_order is None:
                    counters["schema_errors"] += 1
                    raise ValueError(
                        f"schema_error evaluator evidence anchor is missing "
                        f"(rule={rule.key}, conversation_id={conversation_id})"
                    )
                anchor = seller_by_id.get(int(evidence_message_id))
                if anchor is None:
                    counters["schema_errors"] += 1
                    raise ValueError(
                        f"schema_error evaluator evidence_message_id is not seller message "
                        f"(rule={rule.key}, conversation_id={conversation_id}, evidence_message_id={evidence_message_id})"
                    )
                if int(anchor.message_order) != int(evidence_message_order):
                    counters["schema_errors"] += 1
                    raise ValueError(
                        f"schema_error evaluator evidence_message_order mismatch "
                        f"(rule={rule.key}, conversation_id={conversation_id}, evidence_message_id={evidence_message_id})"
                    )
                if not evidence_quote or evidence_quote not in str(anchor.text):
                    counters["schema_errors"] += 1
                    raise ValueError(
                        "schema_error evaluator evidence_quote is not substring of seller_text "
                        f"(rule={rule.key}, conversation_id={conversation_id}, evidence_message_id={evidence_message_id})"
                    )
                if rule.key == "greeting" and int(evidence_message_id) not in greeting_window_ids:
                    counters["schema_errors"] += 1
                    raise ValueError(
                        "schema_error evaluator greeting evidence is outside first seller window "
                        f"(conversation_id={conversation_id}, evidence_message_id={evidence_message_id})"
                    )

            judge_sys, judge_user = build_judge_prompt(
                conversation_id=conversation_id,
                chat_context=chat_context,
                seller_catalog=seller_catalog,
                evaluator_payload=eval_call.parsed.model_dump(),
                context_mode=context_mode,
                greeting_window_max=greeting_window_max,
                rule_contexts=rule_business_context,
            )
            judge_call = llm.call_json_schema(
                conn,
                run_id=run_id,
                phase="judge",
                rule_key="bundle",
                conversation_id=conversation_id,
                message_id=llm_message_id,
                model_type=judge_bundle_model,
                system_prompt=judge_sys,
                user_prompt=judge_user,
                attempt=1,
            )
            if judge_call.error_message:
                if judge_call.is_schema_error:
                    counters["schema_errors"] += 1
                else:
                    counters["non_schema_errors"] += 1
                _llm_error_or_raise(
                    phase="judge",
                    call_error=judge_call.error_message,
                    is_schema_error=judge_call.is_schema_error,
                )
            if not isinstance(judge_call.parsed, judge_bundle_model):
                counters["schema_errors"] += 1
                raise ValueError("schema_error judge payload type mismatch")
            judge_by_rule = judge_results_by_rule(judge_call.parsed, rule_keys=rule_keys)

            for rule in rules:
                eval_result = eval_by_rule[rule.key]
                judge_result = judge_by_rule[rule.key]
                conn.execute(
                    """
                    INSERT INTO scan_results(
                      run_id, conversation_id, rule_key,
                      eval_hit, eval_confidence, eval_reason_code, eval_reason, evidence_quote,
                      evidence_message_id, evidence_message_order,
                      judge_expected_hit, judge_label, judge_confidence, judge_rationale,
                      created_at_utc, updated_at_utc
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        conversation_id,
                        rule.key,
                        1 if eval_result.hit else 0,
                        float(eval_result.confidence),
                        str(eval_result.reason_code),
                        str(eval_result.reason),
                        str(eval_result.evidence_quote),
                        None if eval_result.evidence_message_id is None else int(eval_result.evidence_message_id),
                        None if eval_result.evidence_message_order is None else int(eval_result.evidence_message_order),
                        1 if judge_result.expected_hit else 0,
                        1 if judge_result.label else 0,
                        float(judge_result.confidence),
                        str(judge_result.rationale),
                        now_utc(),
                        now_utc(),
                    ),
                )
                counters["inserted"] += 1
                counters["judged"] += 1
        conn.commit()

        _compute_metrics(conn, run_id=run_id)

        if counters["inserted"] != counters["judged"]:
            raise ValueError(
                f"judge coverage must be 1.0 in fixed mode: inserted={counters['inserted']} judged={counters['judged']}"
            )

        canonical_run_id = get_state(conn, "canonical_run_id")
        canonical_version = _run_metrics_version(conn, run_id=canonical_run_id)
        if canonical_run_id is None or canonical_version != METRICS_VERSION:
            set_state(conn, "canonical_run_id", run_id)
            canonical_run_id = run_id

        summary.update(counters)
        summary["judge_coverage"] = _safe_div(float(counters["judged"]), float(counters["inserted"]))
        summary["canonical_run_id"] = canonical_run_id
        status = "success"
        print(
            "[scan] summary "
            f"evaluated_conversations={counters['evaluated_conversations']} "
            f"skipped_without_seller={counters['skipped_conversations_without_seller']} "
            f"processed={counters['processed']} "
            f"inserted={counters['inserted']} judged={counters['judged']} "
            f"judge_coverage={summary['judge_coverage']:.4f} "
            f"schema_errors={counters['schema_errors']} non_schema_errors={counters['non_schema_errors']}"
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
          sr.evidence_message_id,
          sr.evidence_message_order,
          sr.rule_key,
          sr.eval_hit,
          sr.judge_expected_hit,
          sr.eval_reason_code,
          sr.eval_reason,
          sr.judge_rationale,
          sr.evidence_quote,
          sr.eval_confidence,
          sr.judge_confidence,
          COALESCE(anchor.text, '') AS evidence_message_text
        FROM scan_results sr
        LEFT JOIN messages anchor ON anchor.message_id = sr.evidence_message_id
        WHERE sr.run_id=? AND sr.judge_label=0
        ORDER BY ABS(sr.eval_confidence - COALESCE(sr.judge_confidence, 0)) DESC,
                 sr.rule_key,
                 COALESCE(sr.evidence_message_order, 0),
                 sr.conversation_id
        LIMIT ?
        """,
        (run_id, int(limit)),
    ).fetchall()
    return [
        {
            "conversation_id": str(row["conversation_id"]),
            "evidence_message_id": None
            if row["evidence_message_id"] is None
            else int(row["evidence_message_id"]),
            "evidence_message_order": None
            if row["evidence_message_order"] is None
            else int(row["evidence_message_order"]),
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
            "evidence_message_text": str(row["evidence_message_text"]),
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

    rule_keys = [rule.key for rule in all_rules()]
    current = _accuracy_map(conn, run_id=run_id)
    canonical = _accuracy_map(conn, run_id=canonical_run_id)
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

    run_summary_row = conn.execute(
        "SELECT summary_json FROM scan_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    inserted = int(conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0])
    judged = int(
        conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=? AND judge_label IS NOT NULL", (run_id,)).fetchone()[0]
    )
    coverage = _safe_div(float(judged), float(inserted))

    metrics_rows = conn.execute(
        """
        SELECT rule_key, eval_total, eval_true, evaluator_hit_rate, judge_correctness, judge_coverage,
               judged_total, judge_true, judge_false
        FROM scan_metrics
        WHERE run_id=?
        ORDER BY rule_key
        """,
        (run_id,),
    ).fetchall()

    llm_rows = conn.execute(
        """
        SELECT
          phase,
          COUNT(*) AS calls,
          SUM(CASE WHEN error_message<>'' THEN 1 ELSE 0 END) AS errors,
          SUM(prompt_chars) AS prompt_chars,
          SUM(response_chars) AS response_chars
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
        "- scan_policy: `bundled=true, judge=full, context=full, llm_trace=full`",
        "- llm_audit_trace: `full request_json + response_json + extracted_json`",
        f"- canonical_run_id: `{canonical_run_id}`",
        f"- current_run_id: `{run_id}`",
        f"- inserted_results: `{inserted}`",
        f"- judged_results: `{judged}`",
        f"- judge_coverage: `{coverage:.4f}`",
        f"- judge_coverage_target: `{cfg.judge_coverage_min:.2f}`",
        "",
        "## Rule Metrics",
        "",
        "| rule | eval_total | eval_true | evaluator_hit_rate | judge_correctness | judge_coverage |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    if metrics_rows:
        for row in metrics_rows:
            lines.append(
                f"| `{row['rule_key']}` | {int(row['eval_total'])} | {int(row['eval_true'])} | "
                f"{float(row['evaluator_hit_rate']):.4f} | {float(row['judge_correctness']):.4f} | "
                f"{float(row['judge_coverage']):.4f} |"
            )
    else:
        lines.append("| `-` | 0 | 0 | 0.0000 | 0.0000 | 0.0000 |")

    lines.extend(
        [
            "",
            "## Rule Quality Delta (judge_correctness)",
            "",
            "| rule | canonical | current | delta |",
            "|---|---:|---:|---:|",
        ]
    )
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
            "## LLM Calls",
            "",
            "| phase | calls | errors | prompt_chars | response_chars |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    if llm_rows:
        for row in llm_rows:
            lines.append(
                f"| `{row['phase']}` | {int(row['calls'])} | {int(row['errors'] or 0)} | "
                f"{int(row['prompt_chars'] or 0)} | {int(row['response_chars'] or 0)} |"
            )
    else:
        lines.append("| `-` | 0 | 0 | 0 | 0 |")

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
            "| conversation_id | evidence_message_id | evidence_message_order | rule | eval_hit | expected_hit | reason_code | evidence_quote |",
            "|---|---:|---:|---|---:|---:|---|---|",
        ]
    )
    if bad_cases:
        for case in bad_cases:
            lines.append(
                f"| `{case['conversation_id']}` | {case['evidence_message_id']} | {case['evidence_message_order']} | `{case['rule_key']}` | "
                f"{case['eval_hit']} | {case['judge_expected_hit']} | `{_md_cell(str(case['eval_reason_code']), 60)}` | "
                f"`{_md_cell(str(case['evidence_quote']), 80)}` |"
            )

        lines.extend(["", "### Bad Case Details", ""])
        for idx, case in enumerate(bad_cases[:10], start=1):
            lines.append(
                f"{idx}. `{case['conversation_id']}` evidence_message_id={case['evidence_message_id']} "
                f"rule=`{case['rule_key']}` eval_hit={case['eval_hit']} expected_hit={case['judge_expected_hit']}"
            )
            lines.append(f"   evidence_message_order: {_md_cell(str(case['evidence_message_order']), 40)}")
            lines.append(f"   evidence_message_text: {_md_cell(str(case['evidence_message_text']), 200)}")
            lines.append(f"   evaluator_reason: {_md_cell(str(case['eval_reason']), 200)}")
            lines.append(f"   judge_rationale: {_md_cell(str(case['judge_rationale']), 200)}")
            lines.append(f"   evidence_quote: {_md_cell(str(case['evidence_quote']), 120)}")
    else:
        lines.append("| `-` | 0 | 0 | `-` | 0 | 0 | `-` | `-` |")

    if run_summary_row is not None:
        lines.extend(["", "## Run Summary JSON", "", f"- summary_json: `{str(run_summary_row['summary_json'])}`"])

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
