from __future__ import annotations

import json

from .models import EvaluatorResult
from .sgr_core import RuleCard


def build_judge_prompts(
    rule: RuleCard,
    *,
    speaker_label: str,
    text: str,
    chat_context: str,
    evaluator: EvaluatorResult,
) -> tuple[str, str]:
    """Judge получает исходное сообщение и готовое решение evaluator."""

    system_prompt = (
        "Ты независимый судья качества. "
        "Проверь, корректно ли evaluator применил правило. "
        "Для эмпатии решение должно опираться строго на контекст чата, "
        "Верни только JSON по схеме JudgeResult."
    )
    user_prompt = (
        f"Правило: {rule.key} ({rule.title_ru})\n"
        f"Критерий: {rule.what_to_check}\n"
        f"Сообщение: speaker={speaker_label}, text={text}\n"
        "Контекст чата до текущего сообщения:\n"
        f"{chat_context}\n"
        "Ответ evaluator (JSON):\n"
        f"{json.dumps(evaluator.model_dump(), ensure_ascii=False)}\n"
        "Инструкция: label=true, если решение evaluator корректно; иначе false."
    )
    return system_prompt, user_prompt
