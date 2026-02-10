from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from ..db import get_state, set_state
from ..judge import (
    build_evaluator_bundle_model,
    build_judge_bundle_model,
    build_judge_prompt,
    evaluator_results_by_rule,
    judge_results_by_rule,
)
from ..llm import LLMClient
from ..sgr_core import (
    METRICS_VERSION,
    all_rules,
    build_chat_context,
    build_evaluator_prompts_bundle,
    build_rule_business_context,
    fixed_scan_policy,
    greeting_window_refs,
    is_seller_message,
    seller_message_refs,
)
from ..utils import jdump, now_utc


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
    policy = fixed_scan_policy()

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
        "bundle_rules": bool(policy.bundle_rules),
        "judge_mode": policy.judge_mode,
        "context_mode": policy.context_mode,
        "llm_trace": policy.llm_trace,
    }

    print(
        f"[scan] conversations={len(conversation_ids)} messages={len(messages)} seller_messages={seller_messages} "
        f"rules={len(rules)} range={conversation_from}..{conversation_to} "
        f"bundle_rules={policy.bundle_rules} judge_mode={policy.judge_mode} "
        f"context_mode={policy.context_mode} llm_trace={policy.llm_trace}"
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
                mode=policy.context_mode,
            )
            greeting_window = greeting_window_refs(
                conversation_messages,
                max_messages=policy.greeting_window_max,
            )
            greeting_window_ids = {item.message_id for item in greeting_window}
            seller_by_id = {item.message_id: item for item in seller_catalog}

            llm_message_id = int(seller_catalog[0].message_id)
            eval_sys, eval_user = build_evaluator_prompts_bundle(
                rules,
                conversation_id=conversation_id,
                chat_context=chat_context,
                seller_catalog=seller_catalog,
                greeting_window_max=policy.greeting_window_max,
                context_mode=policy.context_mode,
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
                context_mode=policy.context_mode,
                greeting_window_max=policy.greeting_window_max,
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
