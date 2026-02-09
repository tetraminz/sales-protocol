from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from typing import Any

from .llm import LLMClient
from .models import CompiledRuleSpec
from .utils import jdump, now_utc


@dataclass
class RuleWithSpec:
    rule_id: int
    rule_key: str
    natural_language: str
    version_id: int
    spec: CompiledRuleSpec


def _slugify(text: str) -> str:
    raw = re.sub(r"[^a-zA-Z0-9_\-]+", "_", text.strip().lower())
    raw = re.sub(r"_+", "_", raw).strip("_")
    return raw[:48] or "rule"

def seed_default_rules(conn: sqlite3.Connection) -> None:
    defaults = [
        (
            "seller_greeting",
            "Проверить, что продавец здоровается с клиентом в сообщении",
            "ru",
            {
                "rule_key": "seller_greeting",
                "title": "Seller greeting",
                "scope": "message",
                "target_speaker": "sales_rep",
                "logic": "keyword_any",
                "include_keywords": ["hello", "hi", "good morning", "привет", "здравствуйте"],
                "exclude_keywords": [],
                "reason_template": "Seller opens with greeting",
            },
        ),
        (
            "seller_empathy",
            "Проверить наличие эмпатии от продавца",
            "ru",
            {
                "rule_key": "seller_empathy",
                "title": "Seller empathy",
                "scope": "message",
                "target_speaker": "sales_rep",
                "logic": "keyword_any",
                "include_keywords": ["understand", "sorry", "appreciate", "понима", "сожале"],
                "exclude_keywords": [],
                "reason_template": "Seller acknowledges customer feelings",
            },
        ),
        (
            "objection_handling",
            "Проверить, что продавец отрабатывает возражения клиента",
            "ru",
            {
                "rule_key": "objection_handling",
                "title": "Objection handling",
                "scope": "message",
                "target_speaker": "sales_rep",
                "logic": "keyword_any",
                "include_keywords": ["let me clarify", "value", "roi", "понимаю ваш вопрос", "давайте разберем"],
                "exclude_keywords": [],
                "reason_template": "Seller handles objection with clarifying/value framing",
            },
        ),
    ]

    for rule_key, nl, lang, spec in defaults:
        row = conn.execute("SELECT rule_id FROM rules WHERE rule_key=?", (rule_key,)).fetchone()
        if row:
            continue
        now = now_utc()
        cur = conn.execute(
            """
            INSERT INTO rules(rule_key, natural_language, language, status, compile_error, created_at_utc, updated_at_utc)
            VALUES(?, ?, ?, 'active', '', ?, ?)
            """,
            (rule_key, nl, lang, now, now),
        )
        rule_id = int(cur.lastrowid)
        conn.execute(
            """
            INSERT INTO rule_versions(rule_id, version_no, compiled_spec_json, prompt_version, created_at_utc)
            VALUES(?, 1, ?, 'v1', ?)
            """,
            (rule_id, jdump(spec), now),
        )
    conn.commit()


def add_rule(
    conn: sqlite3.Connection,
    llm: LLMClient,
    *,
    natural_language: str,
    language: str,
    prompt_version: str,
) -> dict[str, Any]:
    llm.require_live("rules add")

    now = now_utc()
    seed_key = _slugify(natural_language)[:28]
    if not seed_key:
        seed_key = "rule"
    rule_key = seed_key
    suffix = 1
    while conn.execute("SELECT 1 FROM rules WHERE rule_key=?", (rule_key,)).fetchone():
        suffix += 1
        rule_key = f"{seed_key}_{suffix}"

    cur = conn.execute(
        """
        INSERT INTO rules(rule_key, natural_language, language, status, compile_error, created_at_utc, updated_at_utc)
        VALUES(?, ?, ?, 'draft', '', ?, ?)
        """,
        (rule_key, natural_language, language, now, now),
    )
    rule_id = int(cur.lastrowid)
    conn.commit()

    run_id = llm.start_run(
        conn,
        run_kind="rule_compile",
        mode="compile",
        prompt_version=prompt_version,
        sgr_version="v1",
        meta={"rule_id": rule_id},
    )

    sys = "Compile business rule into strict JSON spec. Keep it concise and executable."
    usr = (
        f"Rule language={language}. Rule text: {natural_language}. "
        f"Prefer key={rule_key}. Scope should be message or conversation."
    )
    result = llm.call_json_schema(
        conn,
        run_id=run_id,
        phase="compile_rule",
        model_type=CompiledRuleSpec,
        system_prompt=sys,
        user_prompt=usr,
        rule_id=rule_id,
    )

    compile_error = ""
    fatal_error = ""
    version_id: int | None = None
    if result.is_schema_error:
        compile_error = result.error_message or "compile schema validation failed"
        fatal_error = compile_error
    elif result.validation_ok and isinstance(result.parsed, CompiledRuleSpec):
        spec = result.parsed.model_dump()
        if not spec["include_keywords"]:
            compile_error = "no include_keywords generated"
        else:
            ver = conn.execute(
                "SELECT COALESCE(MAX(version_no), 0) + 1 FROM rule_versions WHERE rule_id=?",
                (rule_id,),
            ).fetchone()[0]
            cur_ver = conn.execute(
                """
                INSERT INTO rule_versions(rule_id, version_no, compiled_spec_json, prompt_version, created_at_utc)
                VALUES(?, ?, ?, ?, ?)
                """,
                (rule_id, int(ver), jdump(spec), prompt_version, now_utc()),
            )
            version_id = int(cur_ver.lastrowid)
    else:
        compile_error = result.error_message or "compile validation failed"

    conn.execute(
        "UPDATE rules SET compile_error=?, updated_at_utc=? WHERE rule_id=?",
        (compile_error, now_utc(), rule_id),
    )
    conn.commit()
    llm.finish_run(conn, run_id, "success" if not compile_error else "failed")
    if fatal_error:
        raise ValueError(fatal_error)

    return {
        "rule_id": rule_id,
        "rule_key": rule_key,
        "status": "draft",
        "compile_error": compile_error,
        "version_id": version_id,
    }


def list_rules(conn: sqlite3.Connection, status: str | None = None) -> list[sqlite3.Row]:
    if status:
        return conn.execute(
            "SELECT * FROM rules WHERE status=? ORDER BY rule_id",
            (status,),
        ).fetchall()
    return conn.execute("SELECT * FROM rules ORDER BY rule_id").fetchall()


def approve_rule(conn: sqlite3.Connection, rule_id: int) -> None:
    row = conn.execute(
        "SELECT compile_error FROM rules WHERE rule_id=?",
        (rule_id,),
    ).fetchone()
    if not row:
        raise ValueError(f"rule {rule_id} not found")
    if (row["compile_error"] or "").strip():
        raise ValueError("rule has compile_error; fix before approval")

    ver = conn.execute(
        "SELECT 1 FROM rule_versions WHERE rule_id=? ORDER BY version_no DESC LIMIT 1",
        (rule_id,),
    ).fetchone()
    if not ver:
        raise ValueError("rule has no compiled version")

    conn.execute(
        "UPDATE rules SET status='active', updated_at_utc=? WHERE rule_id=?",
        (now_utc(), rule_id),
    )
    conn.commit()


def active_rules(conn: sqlite3.Connection) -> list[RuleWithSpec]:
    rows = conn.execute(
        """
        SELECT r.rule_id, r.rule_key, r.natural_language, rv.version_id, rv.compiled_spec_json
        FROM rules r
        JOIN rule_versions rv ON rv.version_id = (
          SELECT version_id FROM rule_versions x WHERE x.rule_id = r.rule_id ORDER BY version_no DESC LIMIT 1
        )
        WHERE r.status='active'
        ORDER BY r.rule_id
        """
    ).fetchall()
    out: list[RuleWithSpec] = []
    for row in rows:
        spec = CompiledRuleSpec.model_validate_json(row["compiled_spec_json"])
        out.append(
            RuleWithSpec(
                rule_id=row["rule_id"],
                rule_key=row["rule_key"],
                natural_language=row["natural_language"],
                version_id=row["version_id"],
                spec=spec,
            )
        )
    return out
