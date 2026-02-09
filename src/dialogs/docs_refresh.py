from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile

from .db import connect, init_db
from .ingest import ingest_csv_dir
from .llm import CallResult
from .models import BundledEvaluatorResult, BundledJudgeResult, RuleEvaluation, RuleJudgeEvaluation
from .pipeline import build_report, run_scan
from .utils import now_utc


def _extract_json_blob(text: str) -> dict[str, object]:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return {}
    try:
        return json.loads(text[start : end + 1])
    except Exception:
        return {}


def _reason_code(rule_key: str, hit: bool) -> str:
    if rule_key == "greeting":
        return "greeting_present" if hit else "greeting_missing"
    if rule_key == "upsell":
        return "upsell_offer" if hit else "upsell_missing"
    return "empathy_acknowledged" if hit else "informational_without_empathy"


def _eval_rule(rule_key: str, text: str, customer_text: str) -> tuple[bool, str]:
    low = text.lower()
    if rule_key == "greeting":
        hit = "здрав" in low or "hello" in low
    elif rule_key == "upsell":
        hit = "пакет" in low or "plan" in low or "доп" in low
    else:
        hit = ("понима" in low or "understand" in low) and bool(customer_text.strip())
    quote = text.split()[0] if hit and text.split() else ""
    return hit, quote


class DocsLLM:
    """Детерминированный LLM-стаб для воспроизводимого docs-refresh."""

    model = "docs-fake-model"

    def require_live(self, purpose: str) -> None:  # noqa: ARG002
        return None

    def call_json_schema(self, conn, **kwargs):  # noqa: ANN001
        run_id = str(kwargs["run_id"])
        phase = str(kwargs["phase"])
        rule_key = str(kwargs["rule_key"])
        conversation_id = str(kwargs["conversation_id"])
        model_type = kwargs["model_type"]
        message_id = int(kwargs["message_id"])
        attempt = int(kwargs.get("attempt", 1))
        context_mode = "full"
        judge_policy = "full"
        trace_mode = "full"
        system_prompt = str(kwargs.get("system_prompt", ""))
        user_prompt = str(kwargs["user_prompt"])
        prompt_chars = len(system_prompt) + len(user_prompt)

        def _persist_log(*, response_chars: int, extracted_payload: dict[str, object]) -> None:
            request_json = json.dumps(
                {
                    "model": self.model,
                    "phase": phase,
                    "rule_key": rule_key,
                    "conversation_id": conversation_id,
                    "message_id": message_id,
                },
                ensure_ascii=False,
            )
            response_json = json.dumps({"provider": "docs_fake", "ok": True}, ensure_ascii=False)
            extracted_json = json.dumps(extracted_payload, ensure_ascii=False)
            conn.execute(
                """
                INSERT INTO llm_calls(
                  run_id, phase, rule_key, conversation_id, message_id, attempt,
                  context_mode, judge_policy, trace_mode, prompt_chars, response_chars,
                  request_json, response_http_status, response_json, extracted_json,
                  parse_ok, validation_ok, error_message, latency_ms, created_at_utc
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 200, ?, ?, 1, 1, '', 0, ?)
                """,
                (
                    run_id,
                    phase,
                    rule_key,
                    conversation_id,
                    message_id,
                    attempt,
                    context_mode,
                    judge_policy,
                    trace_mode,
                    prompt_chars,
                    int(response_chars),
                    request_json,
                    response_json,
                    extracted_json,
                    now_utc(),
                ),
            )
            conn.commit()

        if model_type is BundledEvaluatorResult:
            seller_text = str(conn.execute("SELECT text FROM messages WHERE message_id=?", (message_id,)).fetchone()[0])
            customer_line = next((line for line in user_prompt.splitlines() if line.startswith("customer_text=")), "")
            customer_text = customer_line.partition("=")[2]
            payload = {}
            for rule_key in ("greeting", "upsell", "empathy"):
                hit, quote = _eval_rule(rule_key, seller_text, customer_text)
                payload[rule_key] = RuleEvaluation(
                    hit=hit,
                    confidence=0.8,
                    reason_code=_reason_code(rule_key, hit),
                    reason="ok",
                    evidence_quote=quote,
                )
            parsed = BundledEvaluatorResult(**payload)
            _persist_log(response_chars=len(parsed.model_dump_json()), extracted_payload=parsed.model_dump())
            return CallResult(parsed, True, True, "", False, False)

        if model_type is BundledJudgeResult:
            evaluator_payload = _extract_json_blob(user_prompt)
            evaluator = BundledEvaluatorResult.model_validate(evaluator_payload)
            out: dict[str, RuleJudgeEvaluation] = {}
            for rule_key in ("greeting", "upsell", "empathy"):
                eval_hit = bool(getattr(evaluator, rule_key).hit)
                out[rule_key] = RuleJudgeEvaluation(
                    expected_hit=eval_hit,
                    label=True,
                    confidence=0.75,
                    rationale="ok",
                )
            parsed = BundledJudgeResult(**out)
            _persist_log(response_chars=len(parsed.model_dump_json()), extracted_payload=parsed.model_dump())
            return CallResult(parsed, True, True, "", False, False)

        raise ValueError(f"unsupported model_type: {model_type}")


def refresh_docs(
    *,
    db_path: str,
    csv_dir: str,
    conversation_from: int,
    conversation_to: int,
    md_path: str,
    png_path: str,
) -> dict[str, str]:
    # docs-refresh всегда работает на временной БД, чтобы не затрагивать рабочую dialogs.db.
    with tempfile.TemporaryDirectory() as tmp:
        tmp_db = str(Path(tmp) / "docs_refresh.db")
        init_db(tmp_db)
        with connect(tmp_db) as conn:
            ingest_csv_dir(conn, csv_dir=csv_dir, replace=True)
            run_id = run_scan(
                conn,
                llm=DocsLLM(),
                conversation_from=conversation_from,
                conversation_to=conversation_to,
                run_id_override="docs_refresh",
            )
            report = build_report(conn, run_id=run_id, md_path=md_path, png_path=png_path)
    return {
        "run_id": run_id,
        "md_path": str(report["md_path"]),
        "png_path": str(report["png_path"]),
    }


def check_docs(
    *,
    db_path: str,
    csv_dir: str,
    conversation_from: int,
    conversation_to: int,
    md_path: str,
    png_path: str,
) -> None:
    md_target = Path(md_path)
    png_target = Path(png_path)
    if not md_target.exists() or not png_target.exists():
        raise ValueError("docs artifacts are missing; run refresh first")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_db = str(Path(tmp) / "check.db")
        tmp_md = str(Path(tmp) / "metrics.md")
        tmp_png = str(Path(tmp) / "accuracy_diff.png")
        refresh_docs(
            db_path=tmp_db,
            csv_dir=csv_dir,
            conversation_from=conversation_from,
            conversation_to=conversation_to,
            md_path=tmp_md,
            png_path=tmp_png,
        )
        if md_target.read_text(encoding="utf-8") != Path(tmp_md).read_text(encoding="utf-8"):
            raise ValueError("metrics.md is out of date; run docs refresh")
        if png_target.read_bytes() != Path(tmp_png).read_bytes():
            raise ValueError("accuracy_diff.png is out of date; run docs refresh")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dialogs.docs-refresh")
    parser.add_argument("mode", choices=["refresh", "check"])
    parser.add_argument("--db", default="dialogs.db")
    parser.add_argument("--csv-dir", default="csv")
    parser.add_argument("--conversation-from", type=int, default=0)
    parser.add_argument("--conversation-to", type=int, default=4)
    parser.add_argument("--md", default="artifacts/metrics.md")
    parser.add_argument("--png", default="artifacts/accuracy_diff.png")
    args = parser.parse_args(argv)

    if args.mode == "refresh":
        out = refresh_docs(
            db_path=args.db,
            csv_dir=args.csv_dir,
            conversation_from=args.conversation_from,
            conversation_to=args.conversation_to,
            md_path=args.md,
            png_path=args.png,
        )
        print(f"docs_refresh_ok run_id={out['run_id']} md={out['md_path']} png={out['png_path']}")
        return 0

    check_docs(
        db_path=args.db,
        csv_dir=args.csv_dir,
        conversation_from=args.conversation_from,
        conversation_to=args.conversation_to,
        md_path=args.md,
        png_path=args.png,
    )
    print("docs_check_ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
