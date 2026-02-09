from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field  # type: ignore


class CompiledRuleSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rule_key: str = Field(min_length=1)
    title: str = Field(min_length=1)
    scope: Literal["message", "conversation"]
    target_speaker: Literal["sales_rep", "customer", "any"]
    logic: Literal["keyword_any", "keyword_all"]
    include_keywords: list[str]
    exclude_keywords: list[str]
    reason_template: str


class Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    quote: str
    message_id: int


class RuleEvaluation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    hit: bool
    confidence: float = Field(ge=0, le=1)
    evidence: Evidence
    reason: str


class JudgeDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    label: bool
    confidence: float = Field(ge=0, le=1)
    rationale: str


class SGRStep(BaseModel):
    model_config = ConfigDict(extra="forbid")

    current_state: str
    plan_remaining_steps_brief: list[str] = Field(min_length=1, max_length=5)
    task_completed: bool
    function: RuleEvaluation
