from __future__ import annotations

import pytest

from dialogs.models import BundledEvaluatorResult, BundledJudgeResult, RuleEvaluation, RuleJudgeEvaluation

STRICT_SCHEMA_CASES = [RuleEvaluation, BundledEvaluatorResult, RuleJudgeEvaluation, BundledJudgeResult]


PARSER_CASES = [
    (
        BundledEvaluatorResult,
        {
            "greeting": {
                "hit": True,
                "confidence": 0.8,
                "reason_code": "greeting_present",
                "reason": "Есть приветствие",
                "evidence_quote": "Здравствуйте",
                "evidence_message_id": 11,
                "evidence_message_order": 2,
            },
            "upsell": {
                "hit": False,
                "confidence": 0.7,
                "reason_code": "upsell_missing",
                "reason": "Нет следующего шага",
                "evidence_quote": "",
                "evidence_message_id": None,
                "evidence_message_order": None,
            },
            "empathy": {
                "hit": True,
                "confidence": 0.9,
                "reason_code": "empathy_acknowledged",
                "reason": "Есть признание ситуации",
                "evidence_quote": "Понимаю",
                "evidence_message_id": 12,
                "evidence_message_order": 4,
            },
        },
        False,
    ),
    (
        BundledEvaluatorResult,
        {
            "greeting": {
                "hit": True,
                "confidence": 1.2,
                "reason_code": "greeting_present",
                "reason": "Некорректная confidence",
                "evidence_quote": "Здравствуйте",
                "evidence_message_id": 11,
                "evidence_message_order": 2,
            },
            "upsell": {
                "hit": False,
                "confidence": 0.7,
                "reason_code": "upsell_missing",
                "reason": "Нет следующего шага",
                "evidence_quote": "",
                "evidence_message_id": None,
                "evidence_message_order": None,
            },
            "empathy": {
                "hit": True,
                "confidence": 0.9,
                "reason_code": "empathy_acknowledged",
                "reason": "Есть признание ситуации",
                "evidence_quote": "Понимаю",
                "evidence_message_id": 12,
                "evidence_message_order": 4,
            },
        },
        True,
    ),
    (
        RuleEvaluation,
        {
            "hit": False,
            "confidence": 0.9,
            "reason_code": "greeting_late",
            "reason": "Приветствие вне первых трех сообщений продавца",
            "evidence_quote": "",
            "evidence_message_id": None,
            "evidence_message_order": None,
        },
        False,
    ),
    (
        RuleEvaluation,
        {
            "hit": True,
            "confidence": 0.9,
            "reason_code": "greeting_present",
            "reason": "Есть приветствие",
            "evidence_quote": "Здравствуйте",
            "evidence_message_id": None,
            "evidence_message_order": None,
        },
        True,
    ),
    (
        RuleEvaluation,
        {
            "hit": True,
            "confidence": 0.9,
            "reason_code": "invalid_code",
            "reason": "Неверный код",
            "evidence_quote": "Здравствуйте",
            "evidence_message_id": 13,
            "evidence_message_order": 2,
        },
        True,
    ),
    (
        BundledJudgeResult,
        {
            "greeting": {"expected_hit": True, "label": True, "confidence": 0.9, "rationale": "ok"},
            "upsell": {"expected_hit": False, "label": True, "confidence": 0.7, "rationale": "ok"},
            "empathy": {"expected_hit": True, "label": True, "confidence": 0.8, "rationale": "ok"},
        },
        False,
    ),
    (
        BundledJudgeResult,
        {
            "greeting": {"expected_hit": True, "label": True, "confidence": 0.9},
            "upsell": {"expected_hit": False, "label": True, "confidence": 0.7, "rationale": "ok"},
            "empathy": {"expected_hit": True, "label": True, "confidence": 0.8, "rationale": "ok"},
        },
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
