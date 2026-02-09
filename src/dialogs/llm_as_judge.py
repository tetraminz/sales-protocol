from __future__ import annotations

from .models import EvaluatorResult
from .sgr_core import RuleCard, build_judge_prompts as _build_judge_prompts


def build_judge_prompts(
    rule: RuleCard,
    *,
    speaker_label: str,
    text: str,
    chat_context: str,
    evaluator: EvaluatorResult,
) -> tuple[str, str]:
    """Тонкая обертка: бизнес-политика prompt живет в sgr_core."""

    return _build_judge_prompts(
        rule,
        speaker_label=speaker_label,
        text=text,
        chat_context=chat_context,
        evaluator=evaluator,
    )
