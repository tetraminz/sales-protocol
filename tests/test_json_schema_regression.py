from __future__ import annotations

import pytest

from dialogs.models import Evidence, EvaluatorResult, JudgeResult


STRICT_SCHEMA_CASES = [
    {
        "код": "evidence_schema_contract",
        "описание": "Evidence: strict object schema, required для всех properties, without extra.",
        "модель": Evidence,
    },
    {
        "код": "evaluator_schema_contract",
        "описание": "EvaluatorResult: strict object schema, required для всех properties, without extra.",
        "модель": EvaluatorResult,
    },
    {
        "код": "judge_schema_contract",
        "описание": "JudgeResult: strict object schema, required для всех properties, without extra.",
        "модель": JudgeResult,
    },
]

PARSER_CASES = [
    {
        "код": "evaluator_valid",
        "описание": "Валидный evaluator JSON проходит парсер.",
        "модель": EvaluatorResult,
        "payload": {
            "hit": True,
            "confidence": 0.73,
            "evidence": {"quote": "Здравствуйте", "message_id": 10},
            "reason": "Есть корректное приветствие",
        },
        "ожидается_ошибка": False,
    },
    {
        "код": "evaluator_confidence_out_of_range",
        "описание": "Граница confidence нарушена (>1) => ошибка валидации.",
        "модель": EvaluatorResult,
        "payload": {
            "hit": True,
            "confidence": 1.2,
            "evidence": {"quote": "Здравствуйте", "message_id": 10},
            "reason": "Неверно",
        },
        "ожидается_ошибка": True,
    },
    {
        "код": "evaluator_extra_field",
        "описание": "Лишнее поле в strict schema запрещено.",
        "модель": EvaluatorResult,
        "payload": {
            "hit": False,
            "confidence": 0.2,
            "evidence": {"quote": "", "message_id": 10},
            "reason": "Нет условия",
            "extra": "not allowed",
        },
        "ожидается_ошибка": True,
    },
    {
        "код": "judge_valid",
        "описание": "Валидный judge JSON проходит парсер.",
        "модель": JudgeResult,
        "payload": {
            "label": True,
            "confidence": 0.91,
            "rationale": "Evaluator корректно применил правило",
        },
        "ожидается_ошибка": False,
    },
    {
        "код": "judge_missing_required",
        "описание": "Отсутствие обязательного поля (rationale) дает ошибку.",
        "модель": JudgeResult,
        "payload": {
            "label": True,
            "confidence": 0.91,
        },
        "ожидается_ошибка": True,
    },
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


@pytest.mark.parametrize("case", STRICT_SCHEMA_CASES, ids=[c["код"] for c in STRICT_SCHEMA_CASES])
def test_openai_strict_schema_contract(case: dict[str, object]) -> None:
    model = case["модель"]
    _assert_object_nodes_strict(model.model_json_schema(), model_name=model.__name__)  # type: ignore[attr-defined]
    assert "strict" in str(case["описание"]).lower()


@pytest.mark.parametrize("case", PARSER_CASES, ids=[c["код"] for c in PARSER_CASES])
def test_prepared_json_validation_without_llm(case: dict[str, object]) -> None:
    model = case["модель"]
    payload = case["payload"]
    if case["ожидается_ошибка"]:
        with pytest.raises(Exception):
            model.model_validate(payload)  # type: ignore[attr-defined]
    else:
        parsed = model.model_validate(payload)  # type: ignore[attr-defined]
        assert parsed is not None
