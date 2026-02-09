from __future__ import annotations

"""Кор бизнес-логики SGR для eval.
- Строгий JSON schema ответ на каждом шаге.
- Проверка evidence по исходному тексту.
- Запись только валидных решений в БД.
"""

import sqlite3

from .llm import LLMClient
from .models import JudgeDecision, RuleEvaluation, SGRStep
from .rules import RuleWithSpec
from .utils import jdump, now_utc


def load_eval_messages(conn: sqlite3.Connection, *, conversation_from: int, conversation_to: int) -> tuple[list[str], list[sqlite3.Row]]:
    """Окно conversations: 0-based, inclusive."""
    if conversation_from < 0:
        raise ValueError("conversation_from must be >= 0")
    if conversation_to < conversation_from:
        raise ValueError("conversation_to must be >= conversation_from")

    total = int(conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0])
    if total == 0:
        raise ValueError("no conversations in db; ingest csv first")
    if conversation_from >= total:
        raise ValueError(f"conversation_from={conversation_from} is out of range (total={total})")

    rows = conn.execute(
        "SELECT conversation_id FROM conversations ORDER BY conversation_id LIMIT ? OFFSET ?",
        (conversation_to - conversation_from + 1, conversation_from),
    ).fetchall()
    conversation_ids = [r["conversation_id"] for r in rows]
    if not conversation_ids:
        raise ValueError("no conversations selected by the provided range")

    placeholders = ",".join("?" for _ in conversation_ids)
    messages = conn.execute(
        f"SELECT message_id, conversation_id, speaker_label, text FROM messages "
        f"WHERE conversation_id IN ({placeholders}) ORDER BY conversation_id, message_order",
        conversation_ids,
    ).fetchall()
    if not messages:
        raise ValueError("selected conversations contain no messages")
    return conversation_ids, messages


def evidence_is_valid(value: RuleEvaluation, *, message_id: int, text: str) -> bool:
    """Если hit=true, quote обязан быть в этом же message_id."""
    if not value.hit:
        return True
    quote = value.evidence.quote.strip()
    return bool(quote) and value.evidence.message_id == int(message_id) and quote in text


def _warn_non_schema(counters: dict[str, int], *, error: str, phase: str, message_id: int) -> None:
    counters["non_schema_errors"] += 1
    counters["skipped_due_to_errors"] += 1
    if counters["non_schema_errors"] <= 3:
        print(f"[eval][warn] {error} phase={phase} message_id={message_id}")
    elif counters["non_schema_errors"] == 4:
        print("[eval][warn] additional non-schema errors suppressed")


def run_eval_loop(
    conn: sqlite3.Connection,
    *,
    llm: LLMClient,
    mode: str,
    rules: list[RuleWithSpec],
    messages: list[sqlite3.Row],
    conversation_ids: list[str],
    llm_run_id: str,
    exp_run_id: str,
) -> dict[str, int]:
    counters = {"processed": 0, "inserted": 0, "skipped_due_to_errors": 0, "schema_errors": 0, "non_schema_errors": 0}
    conv_pos = {cid: i + 1 for i, cid in enumerate(conversation_ids)}
    phase = "baseline_eval" if mode == "baseline" else "sgr_step"
    model_type = RuleEvaluation if mode == "baseline" else SGRStep
    system_prompt = (
        "Evaluate a business rule on a message. Return strict JSON only."
        if mode == "baseline"
        else "Use SGR style: state, short plan, final function decision. Return strict JSON only."
    )

    print(f"[eval] mode={mode} conversations={len(conversation_ids)} messages={len(messages)} rules={len(rules)}")
    current_conv = ""
    for msg in messages:
        if msg["conversation_id"] != current_conv:
            current_conv = msg["conversation_id"]
            print(f"[eval] conversation {conv_pos[current_conv]}/{len(conversation_ids)} id={current_conv}")

        text = msg["text"]
        prompt_base = (
            f"conversation_id={msg['conversation_id']}\nmessage_id={msg['message_id']}\n"
            f"speaker={msg['speaker_label']}\nmessage={text}"
        )
        for rule in rules:
            counters["processed"] += 1
            step = llm.call_json_schema(
                conn,
                run_id=llm_run_id,
                phase=phase,
                model_type=model_type,
                system_prompt=system_prompt,
                user_prompt=f"rule={rule.spec.model_dump_json()}\n{prompt_base}",
                rule_id=rule.rule_id,
                conversation_id=msg["conversation_id"],
                message_id=msg["message_id"],
            )
            if step.is_schema_error:
                counters["schema_errors"] += 1
                raise ValueError(f"schema_error {step.error_message}")
            if step.error_message:
                _warn_non_schema(counters, error=step.error_message, phase=phase, message_id=msg["message_id"])
                continue

            value = step.parsed if mode == "baseline" else step.parsed.function if isinstance(step.parsed, SGRStep) else None
            if not isinstance(value, RuleEvaluation):
                counters["schema_errors"] += 1
                raise ValueError("schema_error: evaluator returned invalid payload type")
            if not evidence_is_valid(value, message_id=msg["message_id"], text=text):
                counters["schema_errors"] += 1
                raise ValueError(f"schema_error: invalid evidence for message_id={msg['message_id']}")

            judge = llm.call_json_schema(
                conn,
                run_id=llm_run_id,
                phase="judge",
                model_type=JudgeDecision,
                system_prompt="Act as judge and label if rule should be true on this message. Return strict JSON only.",
                user_prompt=f"rule_nl={rule.natural_language}\n{prompt_base}",
                rule_id=rule.rule_id,
                conversation_id=msg["conversation_id"],
                message_id=msg["message_id"],
            )
            if judge.is_schema_error:
                counters["schema_errors"] += 1
                raise ValueError(f"schema_error {judge.error_message}")
            if judge.error_message:
                _warn_non_schema(counters, error=judge.error_message, phase="judge", message_id=msg["message_id"])
                continue
            if not isinstance(judge.parsed, JudgeDecision):
                counters["schema_errors"] += 1
                raise ValueError("schema_error: judge returned invalid payload type")

            conn.execute(
                """INSERT INTO rule_results(
                run_id, rule_id, version_id, conversation_id, message_id, mode, hit, confidence, evidence, reason,
                judge_label, judge_confidence, judge_rationale, created_at_utc
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    exp_run_id,
                    rule.rule_id,
                    rule.version_id,
                    msg["conversation_id"],
                    msg["message_id"],
                    mode,
                    1 if value.hit else 0,
                    value.confidence,
                    jdump(value.evidence.model_dump()),
                    value.reason,
                    1 if judge.parsed.label else 0,
                    judge.parsed.confidence,
                    judge.parsed.rationale,
                    now_utc(),
                ),
            )
            counters["inserted"] += 1

    conn.commit()
    print(f"[eval] summary processed={counters['processed']} inserted={counters['inserted']} skipped_due_to_errors={counters['skipped_due_to_errors']} schema_errors={counters['schema_errors']}")
    return counters
