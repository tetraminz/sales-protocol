from __future__ import annotations

from collections.abc import Sequence
from functools import lru_cache
import re

from pydantic import BaseModel, ConfigDict, create_model

from ..models import RuleEvaluation, RuleJudgeEvaluation
from .contracts import normalized_rule_keys


class _StrictBundleModel(BaseModel):
    """База для dynamic bundled-схем со strict OpenAI-contract."""

    model_config = ConfigDict(extra="forbid")


# КАК ДОБАВИТЬ НОВОЕ RULE:
# 1) Добавьте RuleCard в src/dialogs/sgr_core.py -> RULES.
# 2) Обновите reason_codes/anti_patterns в sgr_core для нового ключа.
# 3) Ничего не меняйте здесь вручную: bundled schema строится по rule_keys автоматически.
# 4) Инварианты, которые нельзя нарушать:
#    - поля bundled-схемы обязательны для всех rule_keys;
#    - additionalProperties=false (strict-contract);
#    - порядок rule_keys должен быть стабильным (из all_rules()).
def _safe_token(value: str) -> str:
    token = re.sub(r"[^0-9a-zA-Z]+", "_", str(value)).strip("_")
    return token or "rule"


def _bundle_model_name(prefix: str, rule_keys: tuple[str, ...]) -> str:
    suffix = "__".join(_safe_token(key) for key in rule_keys)
    return f"{prefix}__{suffix}"


@lru_cache(maxsize=32)
def _build_bundle_model_cached(kind: str, rule_keys: tuple[str, ...]) -> type[BaseModel]:
    keys = normalized_rule_keys(rule_keys)
    if kind == "evaluator":
        field_type = RuleEvaluation
    elif kind == "judge":
        field_type = RuleJudgeEvaluation
    else:
        raise ValueError(f"unknown bundle kind: {kind}")

    fields = {key: (field_type, ...) for key in keys}
    model_name = _bundle_model_name(prefix=f"Bundled{kind.title()}Result", rule_keys=keys)
    return create_model(model_name, __base__=_StrictBundleModel, **fields)


def build_evaluator_bundle_model(rule_keys: Sequence[str]) -> type[BaseModel]:
    """Возвращает strict pydantic-модель bundled evaluator payload для заданных правил."""

    keys = normalized_rule_keys(rule_keys)
    return _build_bundle_model_cached("evaluator", keys)


def build_judge_bundle_model(rule_keys: Sequence[str]) -> type[BaseModel]:
    """Возвращает strict pydantic-модель bundled judge payload для заданных правил."""

    keys = normalized_rule_keys(rule_keys)
    return _build_bundle_model_cached("judge", keys)
