from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class Evidence(BaseModel):
    """Ссылка на источник: точная цитата из конкретного сообщения."""

    model_config = ConfigDict(extra="forbid")

    quote: str
    message_id: int


class EvaluatorResult(BaseModel):
    """Ответ evaluator для одного сообщения и одного правила."""

    model_config = ConfigDict(extra="forbid")

    hit: bool
    confidence: float = Field(ge=0, le=1)
    evidence: Evidence
    reason: str


class JudgeResult(BaseModel):
    """Ответ judge: evaluator прав или нет для данного случая."""

    model_config = ConfigDict(extra="forbid")

    label: bool
    confidence: float = Field(ge=0, le=1)
    rationale: str
