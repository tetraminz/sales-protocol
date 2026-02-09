from __future__ import annotations

import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest

from dialogs.cli import build_parser
from dialogs.db import SCHEMA_DICTIONARY_RU, connect, get_state, init_db, schema_dictionary_missing_entries
from dialogs.ingest import ingest_csv_dir
from dialogs.llm import CallResult, LLMClient
from dialogs.models import BundledEvaluatorResult, BundledJudgeResult, RuleEvaluation, RuleJudgeEvaluation
from dialogs.pipeline import _build_accuracy_heatmap_data, _heatmap_zone, build_report, run_scan
from dialogs.sgr_core import METRICS_VERSION, all_rules, quality_thresholds


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
    elif rule_key == "empathy":
        hit = ("понима" in low or "understand" in low) and bool(customer_text.strip())
    else:
        hit = False
    evidence = text.split()[0] if hit and text.split() else ""
    return hit, evidence


class FakeLLM:
    def __init__(self, mode: str = "ok") -> None:
        self.model = "fake-model"
        self.mode = mode
        self.calls = 0
        self.evaluator_calls = 0
        self.judge_calls = 0
        self.context_modes: list[str] = []

    def require_live(self, purpose: str) -> None:  # noqa: ARG002
        return None

    def call_json_schema(self, conn, **kwargs):  # noqa: ANN001
        self.calls += 1
        phase = str(kwargs["phase"])
        attempt = int(kwargs.get("attempt", 1))
        run_id = str(kwargs["run_id"])
        rule_key = str(kwargs["rule_key"])
        conversation_id = str(kwargs["conversation_id"])
        message_id = int(kwargs["message_id"])
        if phase == "evaluator":
            self.evaluator_calls += 1
        if phase == "judge":
            self.judge_calls += 1

        context_mode = "full"
        judge_policy = "full"
        self.context_modes.append(context_mode)

        model_type = kwargs["model_type"]
        user_prompt = str(kwargs["user_prompt"])

        def persist_call(*, error_message: str, parse_ok: bool, validation_ok: bool, extracted_json: str = "{}") -> None:
            prompt_chars = len(str(kwargs.get("system_prompt", ""))) + len(user_prompt)
            conn.execute(
                """
                INSERT INTO llm_calls(
                  run_id, phase, rule_key, conversation_id, message_id, attempt,
                  context_mode, judge_policy, trace_mode, prompt_chars, response_chars,
                  request_json, response_http_status, response_json, extracted_json,
                  parse_ok, validation_ok, error_message, latency_ms, created_at_utc
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    "full",
                    prompt_chars,
                    len(extracted_json),
                    "{}",
                    200,
                    "{}",
                    extracted_json,
                    1 if parse_ok else 0,
                    1 if validation_ok else 0,
                    error_message,
                    0,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()

        if self.mode == "schema_once" and self.calls == 1:
            persist_call(
                error_message="live_call_failed: Error code: 400 - invalid_json_schema",
                parse_ok=False,
                validation_ok=False,
            )
            return CallResult(
                parsed=None,
                parse_ok=False,
                validation_ok=False,
                error_message="live_call_failed: Error code: 400 - invalid_json_schema",
                is_schema_error=True,
                is_live_error=True,
            )

        if self.mode == "non_schema_once" and self.calls == 1:
            persist_call(
                error_message="live_call_failed: Error code: 503 - service unavailable",
                parse_ok=False,
                validation_ok=False,
            )
            return CallResult(
                parsed=None,
                parse_ok=False,
                validation_ok=False,
                error_message="live_call_failed: Error code: 503 - service unavailable",
                is_schema_error=False,
                is_live_error=True,
            )

        if model_type is BundledEvaluatorResult:
            seller_text = str(conn.execute("SELECT text FROM messages WHERE message_id=?", (message_id,)).fetchone()[0])
            customer_line = next((line for line in user_prompt.splitlines() if line.startswith("customer_text=")), "")
            customer_text = customer_line.partition("=")[2]

            payload = {}
            for rule_key in ("greeting", "upsell", "empathy"):
                hit, evidence_quote = _eval_rule(rule_key, seller_text, customer_text)
                payload[rule_key] = RuleEvaluation(
                    hit=hit,
                    confidence=0.8,
                    reason_code=_reason_code(rule_key, hit),
                    reason="ok",
                    evidence_quote=evidence_quote,
                )
            if payload["upsell"].hit and self.mode == "quote_mismatch":
                payload["upsell"] = RuleEvaluation(
                    hit=True,
                    confidence=0.8,
                    reason_code="upsell_offer",
                    reason="ok",
                    evidence_quote="перефразированная цитата которой нет в seller_text",
                )
            parsed = BundledEvaluatorResult(**payload)
            persist_call(error_message="", parse_ok=True, validation_ok=True, extracted_json=parsed.model_dump_json())
            return CallResult(parsed, True, True, "", False, False)

        if model_type is BundledJudgeResult:
            evaluator_payload = _extract_json_blob(user_prompt)
            evaluator = BundledEvaluatorResult.model_validate(evaluator_payload)
            out: dict[str, RuleJudgeEvaluation] = {}
            for rule_key in ("greeting", "upsell", "empathy"):
                eval_hit = bool(getattr(evaluator, rule_key).hit)
                expected = eval_hit
                if self.mode == "regress" and rule_key == "greeting":
                    expected = not expected
                label = eval_hit == expected
                out[rule_key] = RuleJudgeEvaluation(
                    expected_hit=expected,
                    label=label,
                    confidence=0.75,
                    rationale="ok",
                )
            parsed = BundledJudgeResult(**out)
            persist_call(error_message="", parse_ok=True, validation_ok=True, extracted_json=parsed.model_dump_json())
            return CallResult(parsed, True, True, "", False, False)

        raise AssertionError(f"unexpected model_type={model_type}")


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "dialogs.db"


@pytest.fixture()
def csv_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "csv"
    directory.mkdir(parents=True, exist_ok=True)

    header = ["Conversation", "Chunk_id", "Speaker", "Text", "Embedding"]
    for idx in range(6):
        path = directory / f"conv_{idx:02d}.csv"
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerow([f"conv_{idx:02d}", 1, "Customer", "Здравствуйте, у меня сложная ситуация", "[]"])
            writer.writerow([f"conv_{idx:02d}", 2, "Sales Rep", "Здравствуйте! Понимаю вашу ситуацию и помогу", "[]"])
            writer.writerow([f"conv_{idx:02d}", 3, "Customer", "Бюджет ограничен", "[]"])
            writer.writerow([f"conv_{idx:02d}", 4, "Sales Rep", "Могу предложить пакет Plus как доп. вариант", "[]"])

    return directory


def test_rules_are_exactly_three_hardcoded_dataset_style() -> None:
    keys = [rule.key for rule in all_rules()]
    assert keys == ["greeting", "upsell", "empathy"]


def test_scan_default_range_first_five_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    fake = FakeLLM("ok")
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=fake)
        summary = json.loads(conn.execute("SELECT summary_json FROM scan_runs WHERE run_id=?", (run_id,)).fetchone()[0])
        convs = conn.execute(
            "SELECT DISTINCT conversation_id FROM scan_results WHERE run_id=? ORDER BY conversation_id",
            (run_id,),
        ).fetchall()

    assert summary["selected_conversations"] == 5
    assert summary["conversation_from"] == 0
    assert summary["conversation_to"] == 4
    assert summary["seller_messages"] == 10
    assert summary["bundle_rules"] is True
    assert summary["judge_mode"] == "full"
    assert summary["context_mode"] == "full"
    assert summary["llm_trace"] == "full"
    assert summary["metrics_version"] == METRICS_VERSION
    assert [row[0] for row in convs] == ["conv_00", "conv_01", "conv_02", "conv_03", "conv_04"]
    assert set(fake.context_modes) == {"full"}


def test_scan_stores_turn_pair_ids_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        missing_customer = int(
            conn.execute(
                "SELECT COUNT(*) FROM scan_results WHERE run_id=? AND customer_message_id IS NULL",
                (run_id,),
            ).fetchone()[0]
        )
        unique_turn_rules = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                  SELECT DISTINCT run_id, seller_message_id, rule_key
                  FROM scan_results
                  WHERE run_id=?
                )
                """,
                (run_id,),
            ).fetchone()[0]
        )
        total = int(conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0])

    assert missing_customer == 0
    assert unique_turn_rules == total


def test_judge_full_coverage_default_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        inserted = int(conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0])
        judged = int(
            conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=? AND judge_label IS NOT NULL", (run_id,)).fetchone()[0]
        )
        metrics_cov = [
            float(row["judge_coverage"])
            for row in conn.execute("SELECT judge_coverage FROM scan_metrics WHERE run_id=?", (run_id,)).fetchall()
        ]

    assert inserted > 0
    assert judged == inserted
    assert metrics_cov and all(value == pytest.approx(1.0, abs=1e-9) for value in metrics_cov)


def test_quote_mismatch_fails_fast_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        with pytest.raises(ValueError, match="schema_error evaluator evidence_quote is not substring of seller_text"):
            run_scan(conn, llm=FakeLLM("quote_mismatch"), conversation_from=0, conversation_to=1)
        failed = conn.execute(
            "SELECT run_id, summary_json FROM scan_runs WHERE status='failed' ORDER BY started_at_utc DESC LIMIT 1"
        ).fetchone()
        assert failed is not None
        summary = json.loads(str(failed["summary_json"]))
        eval_attempt1 = int(
            conn.execute(
                "SELECT COUNT(*) FROM llm_calls WHERE run_id=? AND phase='evaluator' AND attempt=1",
                (failed["run_id"],),
            ).fetchone()[0]
        )
        eval_attempt2 = int(
            conn.execute(
                "SELECT COUNT(*) FROM llm_calls WHERE run_id=? AND phase='evaluator' AND attempt=2",
                (failed["run_id"],),
            ).fetchone()[0]
        )

    assert summary["schema_errors"] > 0
    assert eval_attempt1 > 0
    assert eval_attempt2 == 0


def test_call_reduction_vs_legacy_estimate_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    fake = FakeLLM("ok")
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=fake, conversation_from=0, conversation_to=1)
        summary = json.loads(conn.execute("SELECT summary_json FROM scan_runs WHERE run_id=?", (run_id,)).fetchone()[0])

    seller_turns = int(summary["seller_turns"])
    llm_calls = int(fake.calls)
    assert llm_calls == seller_turns * 2
    legacy_calls = seller_turns * 6
    reduction = 1.0 - (float(llm_calls) / float(legacy_calls))
    assert reduction >= 0.60


def test_metrics_schema_and_values_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        columns = [str(row["name"]) for row in conn.execute("PRAGMA table_info(scan_metrics)").fetchall()]
        rows = conn.execute(
            """
            SELECT rule_key, eval_total, eval_true, evaluator_hit_rate, judge_correctness, judge_coverage,
                   judged_total, judge_true, judge_false
            FROM scan_metrics
            WHERE run_id=?
            ORDER BY rule_key
            """,
            (run_id,),
        ).fetchall()

    assert columns == [
        "run_id",
        "rule_key",
        "eval_total",
        "eval_true",
        "evaluator_hit_rate",
        "judge_correctness",
        "judge_coverage",
        "judged_total",
        "judge_true",
        "judge_false",
        "created_at_utc",
    ]
    assert len(rows) == 3
    assert all(int(row["eval_total"]) > 0 for row in rows)
    assert all(float(row["judge_coverage"]) == pytest.approx(1.0, abs=1e-9) for row in rows)


def test_heatmap_zone_thresholds_dataset_style() -> None:
    cfg = quality_thresholds()
    assert _heatmap_zone(None) == "na"
    assert _heatmap_zone(cfg.green_min) == "green"
    assert _heatmap_zone(cfg.yellow_min) == "yellow"
    assert _heatmap_zone(cfg.yellow_min - 0.0001) == "red"


def test_heatmap_data_ordering_and_na_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        first_conv = str(
            conn.execute(
                "SELECT conversation_id FROM scan_results WHERE run_id=? ORDER BY conversation_id LIMIT 1",
                (run_id,),
            ).fetchone()[0]
        )
        conn.execute(
            "UPDATE scan_results SET judge_label=NULL WHERE run_id=? AND conversation_id=? AND rule_key='greeting'",
            (run_id, first_conv),
        )
        conn.commit()
        rule_keys = [rule.key for rule in all_rules()]
        heatmap = _build_accuracy_heatmap_data(conn, run_id=run_id, rule_keys=rule_keys)

    conversation_ids = [str(x) for x in heatmap["conversation_ids"]]
    assert conversation_ids == sorted(conversation_ids)
    assert [str(x) for x in heatmap["rule_keys"]] == rule_keys

    row_idx = conversation_ids.index(first_conv)
    col_idx = rule_keys.index("greeting")
    assert int(heatmap["judged_totals"][row_idx][col_idx]) == 0
    assert heatmap["scores"][row_idx][col_idx] is None


def test_report_generation_contains_new_sections_dataset_style(
    db_path: Path, csv_dir: Path, tmp_path: Path
) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        first_run = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        canonical = get_state(conn, "canonical_run_id")
        second_run = run_scan(conn, llm=FakeLLM("regress"), conversation_from=0, conversation_to=1)
        canonical_after = get_state(conn, "canonical_run_id")
        md_path = tmp_path / "metrics.md"
        png_path = tmp_path / "accuracy_diff.png"
        report = build_report(conn, run_id=second_run, md_path=str(md_path), png_path=str(png_path))
        md_text = md_path.read_text(encoding="utf-8")

    assert canonical == first_run
    assert canonical_after == first_run
    assert report["canonical_run_id"] == first_run
    assert report["run_id"] == second_run
    assert md_path.exists()
    assert png_path.exists()
    assert "## Rule Metrics" in md_text
    assert "judge_coverage_target" in md_text
    assert "seller_message_id" in md_text


def test_report_metrics_align_with_scan_metrics_dataset_style(
    db_path: Path, csv_dir: Path, tmp_path: Path
) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        md_path = tmp_path / "metrics.md"
        png_path = tmp_path / "accuracy_diff.png"
        build_report(conn, run_id=run_id, md_path=str(md_path), png_path=str(png_path))
        sql_map = {
            str(row["rule_key"]): float(row["judge_correctness"])
            for row in conn.execute(
                "SELECT rule_key, judge_correctness FROM scan_metrics WHERE run_id=?",
                (run_id,),
            ).fetchall()
        }

    md_text = md_path.read_text(encoding="utf-8")
    md_map: dict[str, float] = {}
    for line in md_text.splitlines():
        match = re.match(r"^\|\s*`([^`]+)`\s*\|\s*([0-9.]+)\s*\|\s*([0-9.]+)\s*\|\s*([+-]?[0-9.]+)\s*\|$", line)
        if not match:
            continue
        md_map[str(match.group(1))] = float(match.group(3))

    assert set(md_map) == set(sql_map)
    for key, value in sql_map.items():
        assert md_map[key] == pytest.approx(value, abs=1e-9)


def test_schema_dictionary_covers_all_tables_and_columns_dataset_style(db_path: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        missing = schema_dictionary_missing_entries(conn)

    assert not missing
    assert "conversations" in SCHEMA_DICTIONARY_RU
    assert "llm_calls" in SCHEMA_DICTIONARY_RU


def test_llm_call_always_persists_full_trace_dataset_style(db_path: Path) -> None:
    init_db(str(db_path))
    llm = LLMClient(model="gpt-4.1-mini", api_key="")

    with connect(str(db_path)) as conn:
        out = llm.call_json_schema(
            conn,
            run_id="scan_test_llm_calls",
            phase="evaluator",
            rule_key="bundle",
            conversation_id="conv_x",
            message_id=1,
            model_type=BundledEvaluatorResult,
            system_prompt="system",
            user_prompt="user",
            attempt=1,
        )
        row = conn.execute(
            """
            SELECT request_json, response_json, extracted_json, parse_ok, validation_ok,
                   response_http_status, error_message, latency_ms, prompt_chars, response_chars, trace_mode
            FROM llm_calls
            ORDER BY call_id DESC LIMIT 1
            """
        ).fetchone()

    assert out.is_live_error is True
    assert row is not None
    assert str(row["trace_mode"]) == "full"
    assert str(row["request_json"]).startswith("{")
    assert str(row["response_json"]).startswith("{")
    assert str(row["extracted_json"]).startswith("{")
    assert int(row["prompt_chars"]) > 0
    assert int(row["response_chars"]) >= 0


def test_live_required_for_scan_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    llm = LLMClient(model="gpt-4.1-mini", api_key="")
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        with pytest.raises(ValueError):
            run_scan(conn, llm=llm)


def test_cli_parser_accepts_fixed_scan_args_dataset_style() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "run",
            "scan",
            "--db",
            "dialogs.db",
            "--model",
            "gpt-4.1-mini",
            "--conversation-from",
            "0",
            "--conversation-to",
            "4",
        ]
    )
    assert args.db == "dialogs.db"
    assert args.model == "gpt-4.1-mini"
    assert args.conversation_from == 0
    assert args.conversation_to == 4


def test_cli_parser_rejects_run_id_override_dataset_style() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "run",
                "scan",
                "--run-id-override",
                "manual_run",
            ]
        )


def test_cli_parser_rejects_removed_mode_flags_dataset_style() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "run",
                "scan",
                "--judge-mode",
                "off",
            ]
        )


def test_doc_contract_files_exist_dataset_style() -> None:
    required = [
        Path("README.md"),
        Path("src/dialogs/sgr_core.py"),
        Path("tests/test_platform_dataset_style.py"),
        Path("notebooks/sgr_quality_demo.ipynb"),
        Path("artifacts/metrics.md"),
        Path("artifacts/accuracy_diff.png"),
        Path("Makefile"),
        Path("docs/stability_case_review.md"),
    ]
    missing = [str(path) for path in required if not path.exists()]
    assert not missing, f"missing doc-contract files: {missing}"


def test_no_inline_review_markers_dataset_style() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    marker = "//" + "коммент:"
    rg_bin = shutil.which("rg")
    if rg_bin:
        proc = subprocess.run(
            [rg_bin, "-n", marker, str(repo_root)],
            check=False,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 1, proc.stdout
        return

    offenders: list[str] = []
    for path in repo_root.rglob("*"):
        if not path.is_file():
            continue
        if ".git" in path.parts:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        if marker in text:
            offenders.append(str(path))
    assert not offenders, "\n".join(offenders)


@pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
def test_live_scan_has_no_invalid_json_schema_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    llm = LLMClient(model="gpt-4.1-mini", api_key=os.getenv("OPENAI_API_KEY", ""))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=llm, conversation_from=0, conversation_to=0)
        invalid = conn.execute(
            "SELECT COUNT(*) FROM llm_calls WHERE run_id=? AND error_message LIKE '%invalid_json_schema%'",
            (run_id,),
        ).fetchone()[0]

    assert invalid == 0
