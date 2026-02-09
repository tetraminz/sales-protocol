from __future__ import annotations

"""Презентационный SGR-core без SQL/OpenAI-деталей.

Здесь только 3 правила и единый контракт evaluator.
"""

from dataclasses import dataclass

from .models import EvaluatorResult


@dataclass(frozen=True)
class RuleCard:
    key: str
    title_ru: str
    what_to_check: str
    why_it_matters: str


RULES: tuple[RuleCard, ...] = (
    # 1) Приветствие: продавец должен открыть контакт корректно.
    RuleCard(
        key="greeting",
        title_ru="Приветствие",
        what_to_check="Есть в сообщении продавца явное приветствие клиенту.",
        why_it_matters="Первое касание задаёт тон диалога и влияет на доверие.",
    ),
    # 2) Допродажа: продавец предлагает следующий релевантный шаг/опцию.
    RuleCard(
        key="upsell",
        title_ru="Допродажа",
        what_to_check="Есть предложение доп. опции/тарифа/пакета, уместное контексту.",
        why_it_matters="Рост среднего чека без потери качества общения.",
    ),
    # 3) Эмпатия: продавец признаёт состояние клиента и отвечает бережно.
    RuleCard(
        key="empathy",
        title_ru="Эмпатия",
        what_to_check="Есть фраза с признанием ситуации клиента и поддержкой.",
        why_it_matters="Снижает трение и повышает вероятность конструктивного шага.",
    ),
)


def all_rules() -> tuple[RuleCard, ...]:
    return RULES


def build_evaluator_prompts(rule: RuleCard, *, speaker_label: str, text: str, message_id: int) -> tuple[str, str]:
    system_prompt = (
        "Ты evaluator для контроля качества продаж. "
        "Думай в стиле SGR (состояние -> короткий план -> решение), "
        "но верни только JSON по схеме без лишних полей."
    )
    user_prompt = (
        f"Правило: {rule.key} ({rule.title_ru})\n"
        f"Что проверяем: {rule.what_to_check}\n"
        f"Зачем: {rule.why_it_matters}\n"
        f"Сообщение: message_id={message_id}, speaker={speaker_label}, text={text}\n"
        "Ограничения evidence:\n"
        "- если hit=true, evidence.quote должен быть точной подстрокой text\n"
        "- если hit=true, evidence.message_id должен быть равен message_id\n"
        "- если hit=false, reason всё равно обязателен"
    )
    return system_prompt, user_prompt


def build_rules_doc() -> str:
    lines = ["# 3 hardcoded правила SGR", ""]
    for rule in RULES:
        lines.append(f"- `{rule.key}`: {rule.title_ru}")
        lines.append(f"  Проверяем: {rule.what_to_check}")
        lines.append(f"  Почему важно: {rule.why_it_matters}")
    return "\n".join(lines)


def evidence_error(result: EvaluatorResult, *, message_id: int, text: str) -> str | None:
    """Механическая проверка аудита: цитата обязана указывать на исходное сообщение."""

    if not result.hit:
        return None

    quote = result.evidence.quote.strip()
    if not quote:
        return "evidence.quote пустой при hit=true"
    if result.evidence.message_id != int(message_id):
        return (
            "evidence.message_id не совпадает с текущим message_id: "
            f"{result.evidence.message_id} != {int(message_id)}"
        )
    if quote not in text:
        return "evidence.quote не является точной подстрокой message.text"
    return None
