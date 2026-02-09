from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import pytest

from dialogs.db import connect, init_db
from dialogs.eval import run_eval
from dialogs.ingest import ingest_csv_dir
from dialogs.llm import CallResult, LLMClient
from dialogs.models import CompiledRuleSpec, Evidence, JudgeDecision, RuleEvaluation, SGRStep
from dialogs.rules import add_rule, seed_default_rules
from dialogs.sgr_core import evidence_is_valid

# Тестовые схемы (документация):
# - каждая запись описывает бизнес-сценарий и ожидаемое поведение системы.
SCHEMA_MODEL_CASES = [
    {
        "код": "compiled_rule_spec_forbid_extra",
        "описание": "CompiledRuleSpec: объектная схема должна запрещать лишние поля.",
        "модель": CompiledRuleSpec,
    },
    {
        "код": "evidence_forbid_extra",
        "описание": "Evidence: объектная схема должна запрещать лишние поля.",
        "модель": Evidence,
    },
    {
        "код": "rule_evaluation_forbid_extra",
        "описание": "RuleEvaluation: объектная схема должна запрещать лишние поля.",
        "модель": RuleEvaluation,
    },
    {
        "код": "judge_decision_forbid_extra",
        "описание": "JudgeDecision: объектная схема должна запрещать лишние поля.",
        "модель": JudgeDecision,
    },
    {
        "код": "sgr_step_forbid_extra",
        "описание": "SGRStep: объектная схема должна запрещать лишние поля.",
        "модель": SGRStep,
    },
]

RULE_EVAL_VALIDATION_CASES = [
    {
        "код": "валидный_payload",
        "описание": "Валидный JSON должен проходить RuleEvaluation.model_validate.",
        "payload": {
            "hit": True,
            "confidence": 0.7,
            "evidence": {"quote": "Hello", "message_id": 1},
            "reason": "ok",
        },
        "ожидается_ошибка": False,
    },
    {
        "код": "невалидный_confidence",
        "описание": "Confidence вне диапазона [0,1] должен приводить к ошибке валидации.",
        "payload": {
            "hit": True,
            "confidence": 1.5,
            "evidence": {"quote": "Hello", "message_id": 1},
            "reason": "ok",
        },
        "ожидается_ошибка": True,
    },
]

EVIDENCE_CASES = [
    {
        "код": "quote_в_тексте_и_message_id_совпадает",
        "описание": "Для hit=true evidence валиден, когда quote входит в текст и message_id совпадает.",
        "value": RuleEvaluation(hit=True, confidence=0.8, evidence=Evidence(quote="Hello", message_id=11), reason="ok"),
        "message_id": 11,
        "text": "Hello customer",
        "ожидается": True,
    },
    {
        "код": "quote_отсутствует_в_тексте",
        "описание": "Для hit=true evidence невалиден, когда quote не найден в тексте.",
        "value": RuleEvaluation(hit=True, confidence=0.8, evidence=Evidence(quote="missing", message_id=11), reason="ok"),
        "message_id": 11,
        "text": "Hello customer",
        "ожидается": False,
    },
    {
        "код": "message_id_не_совпадает",
        "описание": "Для hit=true evidence невалиден, когда message_id не соответствует текущему сообщению.",
        "value": RuleEvaluation(hit=True, confidence=0.8, evidence=Evidence(quote="Hello", message_id=99), reason="ok"),
        "message_id": 11,
        "text": "Hello customer",
        "ожидается": False,
    },
]


class FakeLLM:
    def __init__(self, mode: str = "ok") -> None:
        self.model = "fake-model"
        self.mode = mode
        self.call_no = 0

    def require_live(self, purpose: str) -> None:
        return None

    def start_run(self, conn, **kwargs):  # noqa: ANN001
        return "llm_fake_run"

    def finish_run(self, conn, run_id: str, status: str) -> None:  # noqa: ARG002
        return None

    def call_json_schema(self, conn, **kwargs):  # noqa: ANN001
        self.call_no += 1
        model_type = kwargs["model_type"]
        message_id = int(kwargs["message_id"])
        user_prompt = kwargs["user_prompt"]
        text = user_prompt.split("message=", 1)[1]

        if self.mode == "schema_once" and self.call_no == 1:
            return CallResult(
                parsed=None,
                parse_ok=False,
                validation_ok=False,
                error_message="live_call_failed: Error code: 400 - invalid_json_schema",
                is_schema_error=True,
                is_live_error=True,
            )
        if self.mode == "non_schema_once" and self.call_no == 1:
            return CallResult(
                parsed=None,
                parse_ok=False,
                validation_ok=False,
                error_message="live_call_failed: Error code: 503 - service unavailable",
                is_schema_error=False,
                is_live_error=True,
            )

        if model_type is RuleEvaluation:
            quote = "Hello" if "Hello" in text else text.strip().split()[0]
            value = RuleEvaluation(
                hit=True,
                confidence=0.9,
                evidence=Evidence(quote=quote, message_id=message_id),
                reason="ok",
            )
            return CallResult(value, True, True, "", False, False)

        if model_type is SGRStep:
            quote = "Hello" if "Hello" in text else text.strip().split()[0]
            fn = RuleEvaluation(
                hit=True,
                confidence=0.92,
                evidence=Evidence(quote=quote, message_id=message_id),
                reason="ok",
            )
            step = SGRStep(
                current_state="evaluate",
                plan_remaining_steps_brief=["check"],
                task_completed=True,
                function=fn,
            )
            return CallResult(step, True, True, "", False, False)

        if model_type is JudgeDecision:
            judge = JudgeDecision(label=True, confidence=0.8, rationale="ok")
            return CallResult(judge, True, True, "", False, False)

        raise AssertionError(f"unexpected model_type={model_type}")


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "dialogs.db"


@pytest.fixture()
def csv_dir(tmp_path: Path) -> Path:
    d = tmp_path / "csv"
    d.mkdir(parents=True, exist_ok=True)
    header = ["Conversation", "Chunk_id", "Speaker", "Text", "Embedding"]
    rows: list[tuple[str, int, str, str, str]] = []
    for i in range(6):
        rows.append((f"conv_{i:02d}", 1, "Sales Rep", f"Hello from conv {i}", "[]"))
    by_file: dict[str, list[tuple[str, int, str, str, str]]] = {}
    for row in rows:
        by_file.setdefault(f"{row[0]}.csv", []).append(row)

    for file_name, file_rows in by_file.items():
        with (d / file_name).open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(header)
            for row in file_rows:
                writer.writerow(row)
    return d


def _all_object_nodes_have_no_additional(schema: object) -> None:
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            assert schema.get("additionalProperties") is False
        for value in schema.values():
            _all_object_nodes_have_no_additional(value)
    elif isinstance(schema, list):
        for value in schema:
            _all_object_nodes_have_no_additional(value)


@pytest.mark.parametrize("case", SCHEMA_MODEL_CASES, ids=[c["код"] for c in SCHEMA_MODEL_CASES])
def test_schema_contract_objects_forbid_extra_dataset_style(case: dict[str, object]) -> None:
    _all_object_nodes_have_no_additional(case["модель"].model_json_schema())  # type: ignore[attr-defined]
    assert "запрещать лишние поля" in str(case["описание"])


@pytest.mark.parametrize("case", RULE_EVAL_VALIDATION_CASES, ids=[c["код"] for c in RULE_EVAL_VALIDATION_CASES])
def test_rule_evaluation_validation_cases_dataset_style(case: dict[str, object]) -> None:
    payload = case["payload"]
    if case["ожидается_ошибка"]:
        with pytest.raises(Exception):
            RuleEvaluation.model_validate(payload)
    else:
        parsed = RuleEvaluation.model_validate(payload)
        assert parsed.hit is True


@pytest.mark.parametrize("case", EVIDENCE_CASES, ids=[c["код"] for c in EVIDENCE_CASES])
def test_evidence_cases_dataset_style(case: dict[str, object]) -> None:
    assert evidence_is_valid(case["value"], message_id=case["message_id"], text=case["text"]) is case["ожидается"]


def test_eval_default_range_first_five_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        seed_default_rules(conn)
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_eval(
            conn,
            llm=FakeLLM("ok"),
            mode="baseline",
            rule_set="default",
            prompt_version="v1",
            sgr_version="v1",
        )
        summary = json.loads(conn.execute("SELECT summary_json FROM experiment_runs WHERE run_id=?", (run_id,)).fetchone()[0])
        convs = conn.execute("SELECT DISTINCT conversation_id FROM rule_results WHERE run_id=? ORDER BY conversation_id", (run_id,)).fetchall()

    assert summary["selected_conversations"] == 5
    assert summary["conversation_from"] == 0
    assert summary["conversation_to"] == 4
    assert [r[0] for r in convs] == ["conv_00", "conv_01", "conv_02", "conv_03", "conv_04"]


def test_eval_explicit_range_inclusive_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        seed_default_rules(conn)
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_eval(
            conn,
            llm=FakeLLM("ok"),
            mode="sgr",
            rule_set="default",
            prompt_version="v1",
            sgr_version="v1",
            conversation_from=2,
            conversation_to=3,
        )
        convs = conn.execute("SELECT DISTINCT conversation_id FROM rule_results WHERE run_id=? ORDER BY conversation_id", (run_id,)).fetchall()

    assert [r[0] for r in convs] == ["conv_02", "conv_03"]


def test_eval_non_schema_error_skips_and_continues_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        seed_default_rules(conn)
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_id = run_eval(
            conn,
            llm=FakeLLM("non_schema_once"),
            mode="baseline",
            rule_set="default",
            prompt_version="v1",
            sgr_version="v1",
            conversation_from=0,
            conversation_to=0,
        )
        summary = json.loads(conn.execute("SELECT summary_json FROM experiment_runs WHERE run_id=?", (run_id,)).fetchone()[0])
        status = conn.execute("SELECT status FROM experiment_runs WHERE run_id=?", (run_id,)).fetchone()[0]

    assert status == "success"
    assert summary["skipped_due_to_errors"] >= 1
    assert summary["inserted"] >= 1


def test_eval_schema_error_fails_fast_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        seed_default_rules(conn)
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        with pytest.raises(ValueError):
            run_eval(
                conn,
                llm=FakeLLM("schema_once"),
                mode="baseline",
                rule_set="default",
                prompt_version="v1",
                sgr_version="v1",
                conversation_from=0,
                conversation_to=0,
            )
        status = conn.execute("SELECT status FROM experiment_runs ORDER BY started_at_utc DESC LIMIT 1").fetchone()[0]

    assert status == "failed"


def test_require_live_for_eval_and_rules_add_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    llm = LLMClient(model="gpt-4.1-mini", api_key="")
    with connect(str(db_path)) as conn:
        seed_default_rules(conn)
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        with pytest.raises(ValueError):
            run_eval(conn, llm=llm, mode="baseline", rule_set="default", prompt_version="v1", sgr_version="v1")

    init_db(str(db_path))
    with connect(str(db_path)) as conn:
        before = conn.execute("SELECT COUNT(*) FROM rules").fetchone()[0]
        with pytest.raises(ValueError):
            add_rule(conn, llm, natural_language="Проверить приветствие", language="ru", prompt_version="v1")
        after = conn.execute("SELECT COUNT(*) FROM rules").fetchone()[0]

    assert before == after


@pytest.mark.skipif(not os.getenv("OPENAI_API_KEY"), reason="OPENAI_API_KEY not set")
def test_live_eval_has_no_invalid_json_schema_dataset_style(db_path: Path, csv_dir: Path) -> None:
    init_db(str(db_path))
    llm = LLMClient(model="gpt-4.1-mini", api_key=os.getenv("OPENAI_API_KEY", ""))
    with connect(str(db_path)) as conn:
        seed_default_rules(conn)
        ingest_csv_dir(conn, str(csv_dir), replace=True)
        run_eval(
            conn,
            llm=llm,
            mode="baseline",
            rule_set="default",
            prompt_version="v1",
            sgr_version="v1",
            conversation_from=0,
            conversation_to=0,
        )
        llm_run_id = conn.execute("SELECT run_id FROM llm_runs ORDER BY started_at_utc DESC LIMIT 1").fetchone()[0]
        invalid = conn.execute(
            "SELECT COUNT(*) FROM llm_calls WHERE run_id=? AND error_message LIKE '%invalid_json_schema%'",
            (llm_run_id,),
        ).fetchone()[0]

    assert invalid == 0
