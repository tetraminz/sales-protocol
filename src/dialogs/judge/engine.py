from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel

from ..models import RuleEvaluation, RuleJudgeEvaluation
from .contracts import normalized_rule_keys


def _bundle_results_by_rule(parsed_bundle: BaseModel, *, rule_keys: Sequence[str]) -> dict[str, object]:
    keys = normalized_rule_keys(rule_keys)
    out: dict[str, object] = {}
    for key in keys:
        try:
            out[key] = getattr(parsed_bundle, key)
        except AttributeError as exc:  # pragma: no cover
            raise ValueError(f"parsed bundle has no field for rule_key={key}") from exc
    return out


def evaluator_results_by_rule(parsed_bundle: BaseModel, *, rule_keys: Sequence[str]) -> dict[str, RuleEvaluation]:
    """Извлекает evaluator verdicts по rule_key из dynamic bundled модели."""

    raw = _bundle_results_by_rule(parsed_bundle, rule_keys=rule_keys)
    out: dict[str, RuleEvaluation] = {}
    for key, value in raw.items():
        out[key] = value if isinstance(value, RuleEvaluation) else RuleEvaluation.model_validate(value)
    return out


def judge_results_by_rule(parsed_bundle: BaseModel, *, rule_keys: Sequence[str]) -> dict[str, RuleJudgeEvaluation]:
    """Извлекает judge verdicts по rule_key из dynamic bundled модели."""

    raw = _bundle_results_by_rule(parsed_bundle, rule_keys=rule_keys)
    out: dict[str, RuleJudgeEvaluation] = {}
    for key, value in raw.items():
        out[key] = value if isinstance(value, RuleJudgeEvaluation) else RuleJudgeEvaluation.model_validate(value)
    return out
