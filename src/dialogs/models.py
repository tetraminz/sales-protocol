from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

ReasonCode = Literal[
    "greeting_present",
    "greeting_missing",
    "greeting_late",
    "next_step_present",
    "next_step_missing",
    "cta_without_next_step",
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
    evidence_message_id: int | None = Field(ge=1)
    evidence_message_order: int | None = Field(ge=1)

    @model_validator(mode="after")
    def validate_evidence_contract(self) -> "RuleEvaluation":
        if self.hit:
            if not str(self.evidence_quote).strip():
                raise ValueError("evidence_quote must be non-empty when hit=true")
            if self.evidence_message_id is None or self.evidence_message_order is None:
                raise ValueError("evidence_message_id/evidence_message_order are required when hit=true")
            return self

        if self.evidence_message_id is not None or self.evidence_message_order is not None:
            raise ValueError("evidence_message_id/evidence_message_order must be null when hit=false")
        return self


class RuleJudgeEvaluation(BaseModel):
    """Вердикт judge по одному правилу для конкретной реплики продавца."""

    model_config = ConfigDict(extra="forbid")

    expected_hit: bool
    label: bool
    confidence: float = Field(ge=0, le=1)
    rationale: str
