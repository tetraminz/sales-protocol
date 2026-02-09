from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import re

import pytest

from dialogs.db import SCHEMA_DICTIONARY_RU, connect, get_state, init_db, schema_dictionary_missing_entries
from dialogs.ingest import ingest_csv_dir
from dialogs.llm import CallResult, LLMClient
from dialogs.models import Evidence, EvaluatorResult, JudgeResult
from dialogs.pipeline import _build_accuracy_heatmap_data, _heatmap_zone, build_report, run_scan
from dialogs.sgr_core import METRICS_VERSION, all_rules, evidence_error, normalize_evidence_span, quality_thresholds


def _span(text: str, quote: str) -> tuple[int, int]:
    if not quote:
        return 0, 0
    idx = text.find(quote)
    if idx < 0:
        return 0, 0
    return idx, idx + len(quote)


def _reason_code(rule_key: str, hit: bool) -> str:
    if rule_key == "greeting":
        return "greeting_present" if hit else "greeting_missing"
    if rule_key == "upsell":
        return "upsell_offer" if hit else "upsell_missing"
    return "empathy_acknowledged" if hit else "informational_without_empathy"


class FakeLLM:
    def __init__(self, mode: str = "ok") -> None:
        self.model = "fake-model"
        self.mode = mode
        self.calls = 0
        self.evaluator_calls = 0
        self._mismatch_once_used = False
        self.greeting_eval_attempts: list[int] = []
        self.empathy_eval_prompts: list[str] = []
        self.empathy_judge_prompts: list[str] = []

    def require_live(self, purpose: str) -> None:  # noqa: ARG002
        return None

    def call_json_schema(self, conn, **kwargs):  # noqa: ANN001
        self.calls += 1
        phase = kwargs["phase"]
        if phase == "evaluator":
            self.evaluator_calls += 1
        attempt = int(kwargs.get("attempt", 1))
        model_type = kwargs["model_type"]
        run_id = kwargs["run_id"]
        rule_key = kwargs["rule_key"]
        message_id = int(kwargs["message_id"])
        user_prompt = str(kwargs["user_prompt"])

        if phase == "evaluator" and rule_key == "empathy":
            self.empathy_eval_prompts.append(user_prompt)
        if phase == "judge" and rule_key == "empathy":
            self.empathy_judge_prompts.append(user_prompt)
        if phase == "evaluator" and rule_key == "greeting":
            self.greeting_eval_attempts.append(attempt)

        if self.mode == "schema_once" and self.calls == 1:
            return CallResult(
                parsed=None,
                parse_ok=False,
                validation_ok=False,
                error_message="live_call_failed: Error code: 400 - invalid_json_schema",
                is_schema_error=True,
                is_live_error=True,
            )

        if self.mode == "non_schema_once" and self.calls == 1:
            return CallResult(
                parsed=None,
                parse_ok=False,
                validation_ok=False,
                error_message="live_call_failed: Error code: 503 - service unavailable",
                is_schema_error=False,
                is_live_error=True,
            )

        if model_type is EvaluatorResult:
            full_text = str(conn.execute("SELECT text FROM messages WHERE message_id=?", (message_id,)).fetchone()[0])
            text = full_text.lower()

            if self.mode == "evidence_mismatch_once" and rule_key == "greeting" and attempt == 1 and not self._mismatch_once_used:
                self._mismatch_once_used = True
                quote = full_text.split()[0]
                parsed = EvaluatorResult(
                    hit=True,
                    confidence=0.8,
                    evidence=Evidence(
                        quote=quote,
                        message_id=message_id + 3,
                        span_start=0,
                        span_end=len(quote),
                    ),
                    reason_code="greeting_present",
                    reason="mismatch-once",
                )
                return CallResult(parsed, True, True, "", False, False)

            if self.mode == "evidence_mismatch_always" and rule_key == "greeting":
                quote = full_text.split()[0]
                parsed = EvaluatorResult(
                    hit=True,
                    confidence=0.8,
                    evidence=Evidence(
                        quote=quote,
                        message_id=message_id + 3,
                        span_start=0,
                        span_end=len(quote),
                    ),
                    reason_code="greeting_present",
                    reason="mismatch-always",
                )
                return CallResult(parsed, True, True, "", False, False)

            if rule_key == "greeting":
                hit = "здрав" in text or "hello" in text
            elif rule_key == "upsell":
                hit = "пакет" in text or "plan" in text or "доп" in text
            elif rule_key == "empathy":
                hit = ("понима" in text or "understand" in text) and ("customer:" in user_prompt.lower())
            else:
                hit = False

            quote = full_text.split()[0] if hit else ""
            span_start, span_end = _span(full_text, quote)
            parsed = EvaluatorResult(
                hit=hit,
                confidence=0.8,
                evidence=Evidence(
                    quote=quote,
                    message_id=message_id,
                    span_start=span_start,
                    span_end=span_end,
                ),
                reason_code=_reason_code(rule_key, hit),
                reason="ok",
            )
            return CallResult(parsed, True, True, "", False, False)

        if model_type is JudgeResult:
            row = conn.execute(
                "SELECT eval_hit FROM scan_results WHERE run_id=? AND message_id=? AND rule_key=?",
                (run_id, message_id, rule_key),
            ).fetchone()
            eval_hit = bool(row[0]) if row else False
            expected_hit = eval_hit
            if self.mode == "regress" and rule_key == "greeting":
                expected_hit = not eval_hit
            label = eval_hit == expected_hit
            rationale = "ok"
            if self.mode == "judge_rationale_conflict" and rule_key == "upsell":
                label = False
                rationale = "Оценка корректна, но label выставлен некорректно."

            parsed = JudgeResult(
                expected_hit=expected_hit,
                label=label,
                confidence=0.75,
                rationale=rationale,
            )
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


EVIDENCE_CASES = [
    (
        EvaluatorResult(
            hit=True,
            confidence=0.9,
            evidence=Evidence(quote="Здравствуйте", message_id=11, span_start=0, span_end=len("Здравствуйте")),
            reason_code="greeting_present",
            reason="ok",
        ),
        11,
        "Здравствуйте, рад помочь",
        None,
    ),
    (
        EvaluatorResult(
            hit=True,
            confidence=0.9,
            evidence=Evidence(quote="", message_id=11, span_start=0, span_end=0),
            reason_code="greeting_present",
            reason="ok",
        ),
        11,
        "Здравствуйте, рад помочь",
        "evidence.quote пустой",
    ),
    (
        EvaluatorResult(
            hit=True,
            confidence=0.9,
            evidence=Evidence(quote="Здравствуйте", message_id=99, span_start=0, span_end=len("Здравствуйте")),
            reason_code="greeting_present",
            reason="ok",
        ),
        11,
        "Здравствуйте, рад помочь",
        "evidence.message_id",
    ),
]


def test_rules_are_exactly_three_hardcoded_dataset_style() -> None:
    keys = [rule.key for rule in all_rules()]
    assert keys == ["greeting", "upsell", "empathy"]


@pytest.mark.parametrize("value,message_id,text,expected", EVIDENCE_CASES)
def test_evidence_referential_integrity_dataset_style(
    value: EvaluatorResult,
    message_id: int,
    text: str,
    expected: str | None,
) -> None:
    err = evidence_error(value, message_id=message_id, text=text)
    if expected is None:
        assert err is None
    else:
        assert err is not None and expected in err


def test_normalize_evidence_span_repairs_slice_mismatch_dataset_style() -> None:
    text = "Здравствуйте, рад помочь"
    result = EvaluatorResult(
        hit=True,
        confidence=0.9,
        evidence=Evidence(
            quote="рад помочь",
            message_id=11,
            span_start=0,
            span_end=5,
        ),
        reason_code="empathy_acknowledged",
        reason="ok",
    )
    fixed = normalize_evidence_span(result, text=text)
    start = text.find("рад помочь")
    assert fixed.evidence.span_start == start
    assert fixed.evidence.span_end == start + len("рад помочь")
    assert evidence_error(fixed, message_id=11, text=text) is None


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
    assert summary["metrics_version"] == METRICS_VERSION
    assert [row[0] for row in convs] == ["conv_00", "conv_01", "conv_02", "conv_03", "conv_04"]


def test_seller_only_results_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        non_seller_rows = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM scan_results r
                JOIN messages m ON m.message_id = r.message_id
                WHERE r.run_id=? AND m.speaker_label <> 'Sales Rep'
                """,
                (run_id,),
            ).fetchone()[0]
        )

    assert non_seller_rows == 0


def test_context_is_passed_for_empathy_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    fake = FakeLLM("ok")
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_scan(conn, llm=fake, conversation_from=0, conversation_to=0)

    assert fake.empathy_eval_prompts
    assert fake.empathy_judge_prompts
    assert all("Контекст чата" in p for p in fake.empathy_eval_prompts)
    assert all("Customer:" in p for p in fake.empathy_eval_prompts)
    assert all("Customer:" in p for p in fake.empathy_judge_prompts)


def test_scan_non_schema_error_skips_and_continues_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("non_schema_once"), conversation_from=0, conversation_to=0)
        row = conn.execute("SELECT status, summary_json FROM scan_runs WHERE run_id=?", (run_id,)).fetchone()
        summary = json.loads(row["summary_json"])

    assert row["status"] == "success"
    assert summary["skipped_due_to_errors"] >= 1
    assert summary["inserted"] >= 1


def test_scan_schema_error_fails_fast_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        with pytest.raises(ValueError):
            run_scan(conn, llm=FakeLLM("schema_once"), conversation_from=0, conversation_to=0)
        row = conn.execute("SELECT status FROM scan_runs ORDER BY started_at_utc DESC LIMIT 1").fetchone()

    assert row["status"] == "failed"


def test_evaluator_evidence_mismatch_retried_once_then_success_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    fake = FakeLLM("evidence_mismatch_once")
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=fake, conversation_from=0, conversation_to=0)
        status = conn.execute("SELECT status FROM scan_runs WHERE run_id=?", (run_id,)).fetchone()[0]
        rows = int(conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0])

    assert status == "success"
    assert rows == 6
    assert 2 in fake.greeting_eval_attempts


def test_evaluator_evidence_mismatch_twice_should_skip_not_fail_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("evidence_mismatch_always"), conversation_from=0, conversation_to=0)
        row = conn.execute("SELECT status, summary_json FROM scan_runs WHERE run_id=?", (run_id,)).fetchone()
        summary = json.loads(row["summary_json"])
        greeting_rows = int(
            conn.execute(
                "SELECT COUNT(*) FROM scan_results WHERE run_id=? AND rule_key='greeting'",
                (run_id,),
            ).fetchone()[0]
        )

    assert row["status"] == "success"
    assert summary["skipped_due_to_errors"] >= 1
    assert summary["evidence_mismatch_skipped"] >= 1
    assert greeting_rows == 0


def test_judge_phase_updates_existing_rows_and_no_duplicates_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        total = int(conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0])
        judged = int(
            conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=? AND judge_label IS NOT NULL", (run_id,)).fetchone()[0]
        )
        expected_set = int(
            conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=? AND judge_expected_hit IS NOT NULL", (run_id,)).fetchone()[0]
        )
        unique_rows = int(
            conn.execute(
                """
                SELECT COUNT(*)
                FROM (
                  SELECT DISTINCT run_id, message_id, rule_key
                  FROM scan_results
                  WHERE run_id=?
                )
                """,
                (run_id,),
            ).fetchone()[0]
        )

    assert total > 0
    assert judged == total
    assert expected_set == total
    assert unique_rows == total


def test_metrics_minimal_schema_and_values_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        columns = [
            str(row["name"])
            for row in conn.execute("PRAGMA table_info(scan_metrics)").fetchall()
        ]
        rows = conn.execute(
            "SELECT rule_key, judge_correctness, judged_total, judge_true, judge_false FROM scan_metrics WHERE run_id=? ORDER BY rule_key",
            (run_id,),
        ).fetchall()

    assert columns == [
        "run_id",
        "rule_key",
        "judge_correctness",
        "judged_total",
        "judge_true",
        "judge_false",
        "created_at_utc",
    ]
    assert len(rows) == 3
    assert all(float(row["judge_correctness"]) >= 0.0 for row in rows)
    assert all(int(row["judged_total"]) >= 0 for row in rows)


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


def test_canonical_first_run_and_delta_report_dataset_style(db_path: Path, csv_dir: Path, tmp_path: Path) -> None:
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
    assert "## Rule Quality (judge_correctness)" in md_text
    assert "## Judge-Confirmed Bad Cases (judge_label=0)" in md_text
    assert "Bad Case Details" in md_text


def test_report_metrics_align_with_scan_metrics_dataset_style(db_path: Path, csv_dir: Path, tmp_path: Path) -> None:
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


def test_llm_call_persists_full_trace_dataset_style(db_path: Path) -> None:
    init_db(str(db_path))
    llm = LLMClient(model="gpt-4.1-mini", api_key="")

    with connect(str(db_path)) as conn:
        out = llm.call_json_schema(
            conn,
            run_id="scan_test_llm_calls",
            phase="evaluator",
            rule_key="greeting",
            conversation_id="conv_x",
            message_id=1,
            model_type=EvaluatorResult,
            system_prompt="system",
            user_prompt="user",
            attempt=1,
        )
        row = conn.execute(
            """
            SELECT request_json, response_json, extracted_json, parse_ok, validation_ok,
                   response_http_status, error_message, latency_ms
            FROM llm_calls
            ORDER BY call_id DESC LIMIT 1
            """
        ).fetchone()

    assert out.is_live_error is True
    assert row is not None
    assert str(row["request_json"]).startswith("{")
    assert str(row["response_json"]).startswith("{")
    assert isinstance(row["extracted_json"], str)
    assert int(row["parse_ok"]) in (0, 1)
    assert int(row["validation_ok"]) in (0, 1)
    assert int(row["response_http_status"]) >= 0
    assert str(row["error_message"]) != ""
    assert int(row["latency_ms"]) >= 0


def test_live_required_for_scan_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    llm = LLMClient(model="gpt-4.1-mini", api_key="")
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        with pytest.raises(ValueError):
            run_scan(conn, llm=llm)


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
