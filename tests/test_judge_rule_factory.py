from __future__ import annotations

from pathlib import Path

import pytest

from dialogs.judge.schema_factory import build_judge_bundle_model
from dialogs.sgr_core import all_rules


def _judge_payload(rule_keys: tuple[str, ...]) -> dict[str, dict[str, object]]:
    return {
        key: {
            "expected_hit": True,
            "label": True,
            "confidence": 0.75,
            "rationale": "ok",
        }
        for key in rule_keys
    }


def test_judge_schema_matches_current_rules() -> None:
    rule_keys = tuple(rule.key for rule in all_rules())
    model = build_judge_bundle_model(rule_keys)
    schema = model.model_json_schema()

    assert set((schema.get("properties") or {}).keys()) == set(rule_keys)
    assert set(schema.get("required") or []) == set(rule_keys)
    assert schema.get("additionalProperties") is False


def test_judge_schema_supports_future_rule_without_manual_bundle_changes() -> None:
    base_keys = tuple(rule.key for rule in all_rules())
    extended_keys = base_keys + ("next_step",)

    model = build_judge_bundle_model(extended_keys)
    schema = model.model_json_schema()

    assert "next_step" in set((schema.get("properties") or {}).keys())
    assert "next_step" in set(schema.get("required") or [])

    parsed = model.model_validate(_judge_payload(extended_keys))
    assert bool(getattr(parsed, "next_step").label) is True


def test_judge_schema_requires_every_rule_field() -> None:
    rule_keys = tuple(rule.key for rule in all_rules())
    model = build_judge_bundle_model(rule_keys)

    payload = _judge_payload(rule_keys)
    payload.pop(rule_keys[0])

    with pytest.raises(Exception):
        model.model_validate(payload)


def test_rule_addition_guides_are_documented() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sgr_core = (repo_root / "src/dialogs/sgr_core.py").read_text(encoding="utf-8")
    schema_factory = (repo_root / "src/dialogs/judge/schema_factory.py").read_text(encoding="utf-8")
    judge_doc = repo_root / "docs/judge_module.md"

    assert "КАК ДОБАВИТЬ НОВОЕ RULE" in sgr_core
    assert "КАК ДОБАВИТЬ НОВОЕ RULE" in schema_factory
    assert judge_doc.exists()
    doc_text = judge_doc.read_text(encoding="utf-8")
    assert "checklist" in doc_text.lower()
