from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

ReasonCode = Literal[
    "greeting_present",
    "greeting_missing",
    "upsell_offer",
    "upsell_missing",
    "discount_without_upsell",
    "empathy_acknowledged",
    "courtesy_without_empathy",
    "informational_without_empathy",
]


class RuleEvaluation(BaseModel):
    """Результат evaluator по одному правилу внутри bundled-ответа."""

    model_config = ConfigDict(extra="forbid")

    hit: bool
    confidence: float = Field(ge=0, le=1)
    reason_code: ReasonCode
    reason: str
    evidence_quote: str


class BundledEvaluatorResult(BaseModel):
    """Ответ evaluator для одной реплики продавца сразу по всем правилам."""

    model_config = ConfigDict(extra="forbid")

    greeting: RuleEvaluation
    upsell: RuleEvaluation
    empathy: RuleEvaluation


class RuleJudgeEvaluation(BaseModel):
    """Вердикт judge по одному правилу для конкретной реплики продавца."""

    model_config = ConfigDict(extra="forbid")

    expected_hit: bool
    label: bool
    confidence: float = Field(ge=0, le=1)
    rationale: str


class BundledJudgeResult(BaseModel):
    """Ответ judge для одной реплики продавца сразу по всем правилам."""

    model_config = ConfigDict(extra="forbid")

    greeting: RuleJudgeEvaluation
    upsell: RuleJudgeEvaluation
    empathy: RuleJudgeEvaluation
