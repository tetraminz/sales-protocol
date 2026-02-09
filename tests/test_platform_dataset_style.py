from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import pytest

from dialogs.db import connect, get_state, init_db
from dialogs.ingest import ingest_csv_dir
from dialogs.llm import CallResult, LLMClient
from dialogs.models import Evidence, EvaluatorResult, JudgeResult
from dialogs.pipeline import build_report, run_scan
from dialogs.sgr_core import all_rules, evidence_error


class FakeLLM:
    def __init__(self, mode: str = "ok") -> None:
        self.model = "fake-model"
        self.mode = mode
        self.calls = 0
        self._judge_error_done = False

    def require_live(self, purpose: str) -> None:  # noqa: ARG002
        return None

    def call_json_schema(self, conn, **kwargs):  # noqa: ANN001
        self.calls += 1
        phase = kwargs["phase"]
        model_type = kwargs["model_type"]
        run_id = kwargs["run_id"]
        rule_key = kwargs["rule_key"]
        message_id = int(kwargs["message_id"])

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

        if self.mode == "judge_non_schema_once" and phase == "judge" and not self._judge_error_done:
            self._judge_error_done = True
            return CallResult(
                parsed=None,
                parse_ok=False,
                validation_ok=False,
                error_message="live_call_failed: Error code: 502 - bad gateway",
                is_schema_error=False,
                is_live_error=True,
            )

        if model_type is EvaluatorResult:
            text = str(
                conn.execute("SELECT text FROM messages WHERE message_id=?", (message_id,)).fetchone()[0]
            )
            text_l = text.lower()
            if rule_key == "greeting":
                hit = "здрав" in text_l or "привет" in text_l
            elif rule_key == "upsell":
                hit = "пакет" in text_l or "тариф" in text_l or "доп" in text_l
            elif rule_key == "empathy":
                hit = "понима" in text_l or "сожале" in text_l
            else:
                hit = False

            if hit:
                quote = text.split()[0]
            else:
                quote = ""

            parsed = EvaluatorResult(
                hit=hit,
                confidence=0.8,
                evidence=Evidence(quote=quote, message_id=message_id),
                reason="ok",
            )
            return CallResult(parsed, True, True, "", False, False)

        if model_type is JudgeResult:
            row = conn.execute(
                "SELECT eval_hit FROM scan_results WHERE run_id=? AND message_id=? AND rule_key=?",
                (run_id, message_id, rule_key),
            ).fetchone()
            eval_hit = bool(row[0]) if row else False
            if self.mode == "regress" and rule_key == "greeting":
                eval_hit = not eval_hit
            parsed = JudgeResult(label=eval_hit, confidence=0.75, rationale="ok")
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
    texts = [
        "Здравствуйте! Понимаю ваш запрос, могу предложить пакет Plus.",
        "Здравствуйте, чем могу помочь?",
        "Могу предложить расширенный тариф Plus.",
        "Понимаю вашу ситуацию и постараюсь помочь.",
        "Спасибо за обращение.",
        "Здравствуйте, могу предложить пакет и понимаю ваш бюджет.",
    ]

    for idx, text in enumerate(texts):
        path = directory / f"conv_{idx:02d}.csv"
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            writer.writerow([f"conv_{idx:02d}", 1, "Sales Rep", text, "[]"])

    return directory


EVIDENCE_CASES = [
    {
        "код": "hit_true_quote_inside_text",
        "описание": "При hit=true quote обязан быть точной подстрокой сообщения.",
        "value": EvaluatorResult(
            hit=True,
            confidence=0.9,
            evidence=Evidence(quote="Здравствуйте", message_id=11),
            reason="ok",
        ),
        "message_id": 11,
        "text": "Здравствуйте, рад помочь",
        "ожидается": None,
    },
    {
        "код": "hit_true_empty_quote",
        "описание": "Пустая цитата при hit=true запрещена.",
        "value": EvaluatorResult(
            hit=True,
            confidence=0.9,
            evidence=Evidence(quote="", message_id=11),
            reason="ok",
        ),
        "message_id": 11,
        "text": "Здравствуйте, рад помочь",
        "ожидается": "evidence.quote пустой",
    },
    {
        "код": "hit_true_wrong_message_id",
        "описание": "source message_id должен совпадать с текущим сообщением.",
        "value": EvaluatorResult(
            hit=True,
            confidence=0.9,
            evidence=Evidence(quote="Здравствуйте", message_id=99),
            reason="ok",
        ),
        "message_id": 11,
        "text": "Здравствуйте, рад помочь",
        "ожидается": "evidence.message_id",
    },
    {
        "код": "hit_true_quote_not_substring",
        "описание": "Перефразирование запрещено: quote должен совпасть как подстрока.",
        "value": EvaluatorResult(
            hit=True,
            confidence=0.9,
            evidence=Evidence(quote="Добрый день", message_id=11),
            reason="ok",
        ),
        "message_id": 11,
        "text": "Здравствуйте, рад помочь",
        "ожидается": "точной подстрокой",
    },
]


def test_rules_are_exactly_three_hardcoded_dataset_style() -> None:
    keys = [rule.key for rule in all_rules()]
    assert keys == ["greeting", "upsell", "empathy"]


@pytest.mark.parametrize("case", EVIDENCE_CASES, ids=[c["код"] for c in EVIDENCE_CASES])
def test_evidence_referential_integrity_dataset_style(case: dict[str, object]) -> None:
    err = evidence_error(
        case["value"],  # type: ignore[arg-type]
        message_id=case["message_id"],  # type: ignore[arg-type]
        text=case["text"],  # type: ignore[arg-type]
    )
    expected = case["ожидается"]
    if expected is None:
        assert err is None
    else:
        assert err is not None and str(expected) in err


def test_scan_default_range_first_five_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"))
        summary = json.loads(conn.execute("SELECT summary_json FROM scan_runs WHERE run_id=?", (run_id,)).fetchone()[0])
        convs = conn.execute(
            "SELECT DISTINCT conversation_id FROM scan_results WHERE run_id=? ORDER BY conversation_id",
            (run_id,),
        ).fetchall()

    assert summary["selected_conversations"] == 5
    assert summary["conversation_from"] == 0
    assert summary["conversation_to"] == 4
    assert [row[0] for row in convs] == ["conv_00", "conv_01", "conv_02", "conv_03", "conv_04"]


def test_scan_explicit_range_inclusive_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=2, conversation_to=3)
        convs = conn.execute(
            "SELECT DISTINCT conversation_id FROM scan_results WHERE run_id=? ORDER BY conversation_id",
            (run_id,),
        ).fetchall()

    assert [row[0] for row in convs] == ["conv_02", "conv_03"]


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


def test_judge_phase_updates_existing_rows_and_no_duplicates_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_scan(conn, llm=FakeLLM("ok"), conversation_from=0, conversation_to=1)
        total = int(conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=?", (run_id,)).fetchone()[0])
        judged = int(
            conn.execute("SELECT COUNT(*) FROM scan_results WHERE run_id=? AND judge_label IS NOT NULL", (run_id,)).fetchone()[0]
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
    assert unique_rows == total


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
    assert "delta" in md_text


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
