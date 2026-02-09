from __future__ import annotations

"""Core-бизнес логика SGR.

Здесь сосредоточены:
- карточки правил и prompt policy;
- пороги качества и логика зон;
- валидация evidence и soft-валидация консистентности judge.
"""

from collections.abc import Sequence
from dataclasses import dataclass
import json
import re

from .models import EvaluatorResult, JudgeResult, ReasonCode

METRICS_VERSION = "v2_judge_correctness"


@dataclass(frozen=True)
class RuleCard:
    key: str
    title_ru: str
    what_to_check: str
    why_it_matters: str


@dataclass(frozen=True)
class QualityThresholds:
    green_min: float = 0.90
    yellow_min: float = 0.80
    rule_alert_min: float = 0.85
    judge_coverage_min: float = 0.98


RULES: tuple[RuleCard, ...] = (
    RuleCard(
        key="greeting",
        title_ru="Приветствие",
        what_to_check="Есть в сообщении продавца явное приветствие клиенту.",
        why_it_matters="Первое касание задаёт тон диалога и влияет на доверие.",
    ),
    RuleCard(
        key="upsell",
        title_ru="Допродажа",
        what_to_check="Есть предложение доп. опции/тарифа/пакета, уместное контексту.",
        why_it_matters="Рост среднего чека без потери качества общения.",
    ),
    RuleCard(
        key="empathy",
        title_ru="Эмпатия",
        what_to_check="По контексту диалога эмпатия уместна/нужна и в текущей реплике продавца она явно выражена.",
        why_it_matters="Контекстная эмпатия снижает трение и повышает шанс конструктивного шага клиента.",
    ),
)

QUALITY_THRESHOLDS = QualityThresholds()

RULE_REASON_CODES: dict[str, tuple[ReasonCode, ...]] = {
    "greeting": ("greeting_present", "greeting_missing"),
    "upsell": ("upsell_offer", "upsell_missing", "discount_without_upsell"),
    "empathy": ("empathy_acknowledged", "courtesy_without_empathy", "informational_without_empathy"),
}

RULE_ANTI_PATTERNS: dict[str, tuple[str, ...]] = {
    "upsell": (
        "Скидка/промокод сами по себе не считаются допродажей без новой опции/пакета/тарифа.",
        "Информирование о статусе заказа без предложения следующего платного шага не является upsell.",
    ),
    "empathy": (
        "Приветствие и вежливые формулы (How are you / Have a great day / You're welcome) не равны эмпатии.",
        "Позитивный тон без признания состояния/сложности клиента не считается эмпатией.",
        "Промо/продажная информация без эмоционального acknowledgment не считается эмпатией.",
    ),
}

_POSITIVE_RATIONALE_MARKERS = (
    "оценка корректна",
    "решение корректно",
    "evaluator корректно",
    "evaluator правильно",
    "evaluator верно",
    "assessment is correct",
    "correctly",
)
_NEGATIVE_RATIONALE_MARKERS = ("некоррект", "неправиль", "ошиб", "incorrect", "wrong")


def all_rules() -> tuple[RuleCard, ...]:
    return RULES


def quality_thresholds() -> QualityThresholds:
    return QUALITY_THRESHOLDS


def heatmap_zone(score: float | None, *, thresholds: QualityThresholds | None = None) -> str:
    cfg = thresholds or QUALITY_THRESHOLDS
    if score is None:
        return "na"
    if score >= cfg.green_min:
        return "green"
    if score >= cfg.yellow_min:
        return "yellow"
    return "red"


def threshold_doc_line(*, thresholds: QualityThresholds | None = None) -> str:
    cfg = thresholds or QUALITY_THRESHOLDS
    return (
        f"green >= {cfg.green_min:.2f}, "
        f"yellow >= {cfg.yellow_min:.2f}, "
        f"red < {cfg.yellow_min:.2f}, na = no_judged"
    )


def is_seller_message(speaker_label: str) -> bool:
    return speaker_label == "Sales Rep"


def build_chat_context(conversation_messages: Sequence[object], *, current_message_order: int) -> str:
    lines: list[str] = []
    for item in conversation_messages:
        order = int(item["message_order"])  # type: ignore[index]
        if order > int(current_message_order):
            continue
        speaker = str(item["speaker_label"])  # type: ignore[index]
        text = str(item["text"])  # type: ignore[index]
        lines.append(f"[{order}] {speaker}: {text}")
    return "\n".join(lines)


def _reason_codes_for_rule(rule_key: str) -> str:
    values = RULE_REASON_CODES.get(rule_key, ())
    return ", ".join(f"`{value}`" for value in values)


def default_reason_code(rule_key: str, *, hit: bool) -> ReasonCode:
    """Дефолтный business-code для тех редких случаев, когда payload evaluator не сохранился в кэше."""

    if rule_key == "greeting":
        return "greeting_present" if hit else "greeting_missing"
    if rule_key == "upsell":
        return "upsell_offer" if hit else "upsell_missing"
    return "empathy_acknowledged" if hit else "informational_without_empathy"


def _rule_anti_patterns(rule_key: str) -> str:
    patterns = RULE_ANTI_PATTERNS.get(rule_key, ())
    if not patterns:
        return ""
    return "\n".join(f"- {item}" for item in patterns)


def build_evaluator_prompts(
    rule: RuleCard,
    *,
    speaker_label: str,
    text: str,
    message_id: int,
    chat_context: str,
) -> tuple[str, str]:
    anti_patterns = _rule_anti_patterns(rule.key)
    system_prompt = (
        "Ты evaluator качества продаж. "
        "Верни только JSON по строгой схеме. "
        "Решение для empathy принимай строго по контексту чата. "
        "Используй reason_code из разрешённого списка для текущего rule_key."
    )
    user_prompt = (
        f"Правило: {rule.key} ({rule.title_ru})\n"
        f"Что проверяем: {rule.what_to_check}\n"
        f"Зачем: {rule.why_it_matters}\n"
        f"Сообщение: message_id={message_id}, speaker={speaker_label}, text={text}\n"
        "Контекст чата до текущего сообщения:\n"
        f"{chat_context}\n"
        "Ограничения evidence для hit=true:\n"
        "- evidence.message_id обязан быть равен message_id\n"
        "- evidence.quote обязан быть точной подстрокой text\n"
        "- evidence.span_start/evidence.span_end обязаны указывать точные границы quote в text\n"
        "- проверка: evidence.quote == text[evidence.span_start:evidence.span_end]\n"
        f"Разрешённые reason_code для этого правила: {_reason_codes_for_rule(rule.key)}\n"
    )
    if anti_patterns:
        user_prompt += f"Антипаттерны для {rule.key}:\n{anti_patterns}\n"
    user_prompt += "Если hit=false, reason и reason_code всё равно обязательны.\n"
    return system_prompt, user_prompt


def build_judge_prompts(
    rule: RuleCard,
    *,
    speaker_label: str,
    text: str,
    chat_context: str,
    evaluator: EvaluatorResult,
) -> tuple[str, str]:
    system_prompt = (
        "Ты независимый judge качества. "
        "Сначала определи expected_hit по правилу и контексту. "
        "Затем проверь корректность evaluator: label=true только если evaluator.hit == expected_hit. "
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
        "Инструкция:\n"
        "- expected_hit: твоя независимая оценка, должен ли быть hit\n"
        "- label=true, если evaluator корректен; иначе false\n"
        "- rationale: кратко и без противоречий к label"
    )
    return system_prompt, user_prompt


def build_rules_doc() -> str:
    cfg = quality_thresholds()
    lines = [
        "# 3 hardcoded правила SGR",
        "",
        "- thresholds (Balanced):",
        f"  - green >= {cfg.green_min:.2f}",
        f"  - yellow >= {cfg.yellow_min:.2f}",
        f"  - red < {cfg.yellow_min:.2f}",
        f"  - rule alert: judge_correctness < {cfg.rule_alert_min:.2f}",
        f"  - run health alert: judge_coverage < {cfg.judge_coverage_min:.2f}",
    ]
    for rule in RULES:
        lines.append(f"- `{rule.key}`: {rule.title_ru}")
        lines.append(f"  Проверяем: {rule.what_to_check}")
        lines.append(f"  Почему важно: {rule.why_it_matters}")
    return "\n".join(lines)


def build_evidence_correction_note(message_id: int) -> str:
    return (
        "Коррекция evidence:\n"
        f"- evidence.message_id должен быть равен {int(message_id)}\n"
        "- evidence.quote должен быть точной подстрокой текущего text\n"
        "- выставь evidence.span_start/evidence.span_end точно по границам quote в text\n"
        "- проверка: evidence.quote == text[evidence.span_start:evidence.span_end]\n"
        "- верни JSON строго по той же схеме"
    )


def normalize_evidence_span(result: EvaluatorResult, *, text: str) -> EvaluatorResult:
    """Мягко чинит только span, если quote корректный, но индексы смещены."""

    if not result.hit:
        return result
    evidence = result.evidence
    quote = evidence.quote.strip()
    if not quote:
        return result
    if 0 <= evidence.span_start <= evidence.span_end <= len(text):
        if text[evidence.span_start : evidence.span_end] == quote:
            return result

    start = text.find(quote)
    if start < 0:
        return result
    end = start + len(quote)
    if start == evidence.span_start and end == evidence.span_end:
        return result
    return result.model_copy(
        update={
            "evidence": evidence.model_copy(
                update={
                    "span_start": start,
                    "span_end": end,
                }
            )
        }
    )


def evidence_error(result: EvaluatorResult, *, message_id: int, text: str) -> str | None:
    if not result.hit:
        return None

    evidence = result.evidence
    quote = evidence.quote.strip()
    if not quote:
        return "evidence.quote пустой при hit=true"
    if evidence.message_id != int(message_id):
        return (
            "evidence.message_id не совпадает с текущим message_id: "
            f"{evidence.message_id} != {int(message_id)}"
        )
    if evidence.span_end <= evidence.span_start:
        return "evidence.span_end должен быть > evidence.span_start"
    if evidence.span_end > len(text):
        return "evidence.span_end выходит за длину message.text"
    span_text = text[evidence.span_start : evidence.span_end]
    if quote != span_text:
        return "evidence.quote не совпадает с text[span_start:span_end]"
    return None


def judge_inconsistency_flags(
    *,
    evaluator_hit: bool,
    judge: JudgeResult,
) -> list[str]:
    flags: list[str] = []
    expected_label = evaluator_hit == judge.expected_hit
    if bool(judge.label) != bool(expected_label):
        flags.append("label_expected_hit_mismatch")

    rationale = re.sub(r"\s+", " ", judge.rationale.lower()).strip()
    if not rationale:
        return flags
    if not judge.label and any(marker in rationale for marker in _POSITIVE_RATIONALE_MARKERS):
        flags.append("label_rationale_positive_contradiction")
    if judge.label and any(marker in rationale for marker in _NEGATIVE_RATIONALE_MARKERS):
        flags.append("label_rationale_negative_contradiction")
    return flags
