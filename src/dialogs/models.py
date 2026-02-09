from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

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


class Evidence(BaseModel):
    """Ссылка на источник: точная цитата из конкретного сообщения."""

    model_config = ConfigDict(extra="forbid")

    quote: str
    message_id: int
    span_start: int = Field(ge=0)
    span_end: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_span(self) -> Evidence:
        if self.span_end < self.span_start:
            raise ValueError("span_end must be >= span_start")
        return self


class EvaluatorResult(BaseModel):
    """Ответ evaluator для одного сообщения и одного правила."""

    model_config = ConfigDict(extra="forbid")

    hit: bool
    confidence: float = Field(ge=0, le=1)
    evidence: Evidence
    reason_code: ReasonCode
    reason: str


class JudgeResult(BaseModel):
    """Ответ judge: evaluator прав или нет для данного случая."""

    model_config = ConfigDict(extra="forbid")

    expected_hit: bool
    label: bool
    confidence: float = Field(ge=0, le=1)
    rationale: str
