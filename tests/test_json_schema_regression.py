from __future__ import annotations

from dialogs.models import CompiledRuleSpec, Evidence, JudgeDecision, RuleEvaluation, SGRStep


def _assert_openai_strict_required(schema: object, *, model_name: str) -> None:
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            props = schema.get("properties") or {}
            required = schema.get("required")
            assert isinstance(required, list), f"{model_name}: object schema must define required list"
            missing = sorted(k for k in props.keys() if k not in set(required))
            assert not missing, f"{model_name}: required must include every property key, missing={missing}"
            assert schema.get("additionalProperties") is False, f"{model_name}: additionalProperties must be false"
        for value in schema.values():
            _assert_openai_strict_required(value, model_name=model_name)
    elif isinstance(schema, list):
        for value in schema:
            _assert_openai_strict_required(value, model_name=model_name)


def test_openai_strict_json_schema_required_regression() -> None:
    for model in [CompiledRuleSpec, Evidence, RuleEvaluation, JudgeDecision, SGRStep]:
        _assert_openai_strict_required(model.model_json_schema(), model_name=model.__name__)
