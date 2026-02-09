from __future__ import annotations

import pytest

from dialogs.models import Evidence, EvaluatorResult, JudgeResult

STRICT_SCHEMA_CASES = [Evidence, EvaluatorResult, JudgeResult]


PARSER_CASES = [
    (
        EvaluatorResult,
        {
            "hit": True,
            "confidence": 0.8,
            "evidence": {"quote": "Здравствуйте", "message_id": 1, "span_start": 0, "span_end": 11},
            "reason_code": "greeting_present",
            "reason": "Есть приветствие",
        },
        False,
    ),
    (
        EvaluatorResult,
        {
            "hit": True,
            "confidence": 1.2,
            "evidence": {"quote": "Здравствуйте", "message_id": 1, "span_start": 0, "span_end": 11},
            "reason_code": "greeting_present",
            "reason": "Некорректная confidence",
        },
        True,
    ),
    (
        EvaluatorResult,
        {
            "hit": False,
            "confidence": 0.2,
            "evidence": {"quote": "", "message_id": 1, "span_start": 0, "span_end": 0},
            "reason_code": "invalid_code",
            "reason": "Неверный code",
        },
        True,
    ),
    (
        Evidence,
        {"quote": "abc", "message_id": 1, "span_start": 3, "span_end": 2},
        True,
    ),
    (
        JudgeResult,
        {"expected_hit": True, "label": True, "confidence": 0.9, "rationale": "ok"},
        False,
    ),
    (
        JudgeResult,
        {"expected_hit": True, "label": True, "confidence": 0.9},
        True,
    ),
]


def _assert_object_nodes_strict(schema: object, *, model_name: str) -> None:
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            props = schema.get("properties") or {}
            required = schema.get("required")
            assert isinstance(required, list), f"{model_name}: required must be list"
            missing = sorted(k for k in props.keys() if k not in set(required))
            assert not missing, f"{model_name}: missing required keys: {missing}"
            assert schema.get("additionalProperties") is False, f"{model_name}: additionalProperties must be false"
        for value in schema.values():
            _assert_object_nodes_strict(value, model_name=model_name)
    elif isinstance(schema, list):
        for value in schema:
            _assert_object_nodes_strict(value, model_name=model_name)


@pytest.mark.parametrize("model", STRICT_SCHEMA_CASES)
def test_openai_strict_schema_contract(model: type[object]) -> None:
    _assert_object_nodes_strict(model.model_json_schema(), model_name=model.__name__)  # type: ignore[attr-defined]


@pytest.mark.parametrize("model,payload,should_fail", PARSER_CASES)
def test_prepared_json_validation_without_llm(
    model: type[object], payload: dict[str, object], should_fail: bool
) -> None:
    if should_fail:
        with pytest.raises(Exception):
            model.model_validate(payload)  # type: ignore[attr-defined]
    else:
        parsed = model.model_validate(payload)  # type: ignore[attr-defined]
        assert parsed is not None
