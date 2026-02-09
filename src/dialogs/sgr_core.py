from __future__ import annotations

"""Core SGR-логика для оценки качества реплик продаж.

Роль модуля в пайплайне:
- задает стабильный бизнес-контракт правил (`greeting`, `upsell`, `empathy`);
- задает пороги качества для heatmap и отчетов;
- формирует prompt-шаблоны evaluator/judge;
- фиксирует quote-contract как обязательное условие доказуемости.

Границы ответственности:
- здесь нет SQL, сетевых вызовов и файловых операций;
- модуль только описывает правила, инварианты и чистые helper-функции.
"""

from collections.abc import Sequence
from dataclasses import dataclass
import json

from .models import BundledEvaluatorResult, BundledJudgeResult, ReasonCode, RuleEvaluation, RuleJudgeEvaluation

METRICS_VERSION = "v4_bundled_full_judge"


@dataclass(frozen=True)
class RuleCard:
    """Карточка бизнес-правила оценки реплики продавца."""

    key: str
    title_ru: str
    what_to_check: str
    why_it_matters: str


@dataclass(frozen=True)
class QualityThresholds:
    """Пороги зон качества в отчетах и executive-визуализации."""

    green_min: float = 0.90
    yellow_min: float = 0.80
    rule_alert_min: float = 0.85
    judge_coverage_min: float = 1.0


RULES: tuple[RuleCard, ...] = (
    RuleCard(
        key="greeting",
        title_ru="Приветствие",
        what_to_check="Есть в реплике продавца явное приветствие клиенту.",
        why_it_matters="Первое касание задает тон и влияет на доверие.",
    ),
    RuleCard(
        key="upsell",
        title_ru="Допродажа",
        what_to_check="Есть уместное предложение следующего платного шага.",
        why_it_matters="Рост среднего чека без потери качества общения.",
    ),
    RuleCard(
        key="empathy",
        title_ru="Эмпатия",
        what_to_check="В контексте есть явное признание ситуации клиента.",
        why_it_matters="Снижает трение и повышает шанс конструктивного ответа клиента.",
    ),
)

# Централизованные пороги для heatmap/report.
QUALITY_THRESHOLDS = QualityThresholds()

# Бизнес-правила для оценки каждой реплики продавца.
RULE_REASON_CODES: dict[str, tuple[ReasonCode, ...]] = {
    "greeting": ("greeting_present", "greeting_missing"),
    "upsell": ("upsell_offer", "upsell_missing", "discount_without_upsell"),
    "empathy": ("empathy_acknowledged", "courtesy_without_empathy", "informational_without_empathy"),
}

RULE_ANTI_PATTERNS: dict[str, tuple[str, ...]] = {
    "upsell": (
        "Скидка/промокод без новой опции не считается допродажей.",
        "Статус/инфо-ответ без платного следующего шага не считается upsell.",
    ),
    "empathy": (
        "Вежливость и small talk не равны эмпатии.",
        "Позитивный тон без признания состояния клиента не считается эмпатией.",
    ),
}


# Блок доступа к контракту правил и порогов.
def all_rules() -> tuple[RuleCard, ...]:
    """Возвращает фиксированный набор бизнес-правил."""

    return RULES


def rule_keys() -> tuple[str, ...]:
    """Возвращает стабильный порядок ключей правил."""

    return tuple(rule.key for rule in RULES)


def quality_thresholds() -> QualityThresholds:
    """Возвращает централизованные пороги зон качества."""

    return QUALITY_THRESHOLDS


def heatmap_zone(score: float | None, *, thresholds: QualityThresholds | None = None) -> str:
    """Классифицирует score в `green/yellow/red/na`."""

    cfg = thresholds or QUALITY_THRESHOLDS
    if score is None:
        return "na"
    if score >= cfg.green_min:
        return "green"
    if score >= cfg.yellow_min:
        return "yellow"
    return "red"


def threshold_doc_line(*, thresholds: QualityThresholds | None = None) -> str:
    """Форматирует строку порогов для markdown-отчетов."""

    cfg = thresholds or QUALITY_THRESHOLDS
    return (
        f"green >= {cfg.green_min:.2f}, "
        f"yellow >= {cfg.yellow_min:.2f}, "
        f"red < {cfg.yellow_min:.2f}, na = no_judged"
    )


# Блок распознавания ролей сообщений.
def is_seller_message(speaker_label: str) -> bool:
    """True только для реплик продавца."""

    return speaker_label == "Sales Rep"


def is_customer_message(speaker_label: str) -> bool:
    """True только для реплик покупателя."""

    return speaker_label == "Customer"


def _ordered_messages(conversation_messages: Sequence[object]) -> list[object]:
    return sorted(conversation_messages, key=lambda item: int(item["message_order"]))  # type: ignore[index]


# Блок построения контекста вокруг текущей seller-реплики.
def previous_customer_message(
    conversation_messages: Sequence[object], *, current_message_order: int
) -> object | None:
    """Возвращает ближайшую предыдущую реплику покупателя перед текущей seller-репликой."""

    candidate: object | None = None
    for item in _ordered_messages(conversation_messages):
        order = int(item["message_order"])  # type: ignore[index]
        if order >= int(current_message_order):
            break
        if is_customer_message(str(item["speaker_label"])):  # type: ignore[index]
            candidate = item
    return candidate


def build_chat_context(
    conversation_messages: Sequence[object],
    *,
    current_message_order: int,
    mode: str = "full",
) -> str:
    """Собирает контекст для evaluator/judge.

    - `full`: все сообщения до текущего включительно.
    - `turn`: последняя реплика Customer + текущая реплика Sales Rep.
    """

    if mode not in {"full", "turn"}:
        raise ValueError(f"unknown context mode: {mode}")

    ordered = _ordered_messages(conversation_messages)
    if mode == "full":
        lines: list[str] = []
        for item in ordered:
            order = int(item["message_order"])  # type: ignore[index]
            if order > int(current_message_order):
                continue
            speaker = str(item["speaker_label"])  # type: ignore[index]
            text = str(item["text"])  # type: ignore[index]
            lines.append(f"[{order}] {speaker}: {text}")
        return "\n".join(lines)

    seller_item = None
    for item in ordered:
        if int(item["message_order"]) == int(current_message_order):  # type: ignore[index]
            seller_item = item
            break
    if seller_item is None:
        raise ValueError(f"current_message_order not found: {current_message_order}")

    previous_customer = previous_customer_message(
        ordered,
        current_message_order=int(current_message_order),
    )
    lines = []
    if previous_customer is not None:
        lines.append(f"Customer: {str(previous_customer['text'])}")  # type: ignore[index]
    lines.append(f"Sales Rep: {str(seller_item['text'])}")  # type: ignore[index]
    return "\n".join(lines)


# Внутренние helper-форматтеры prompt-контента.
def _reason_codes_for_rule(rule_key: str) -> str:
    values = RULE_REASON_CODES.get(rule_key, ())
    return ", ".join(f"`{value}`" for value in values)


def _anti_patterns_for_rule(rule_key: str) -> str:
    values = RULE_ANTI_PATTERNS.get(rule_key, ())
    if not values:
        return ""
    return "\n".join(f"- {item}" for item in values)


# Блок сборки prompt-ов evaluator/judge.
def build_evaluator_prompts_bundle(
    rules: Sequence[RuleCard],
    *,
    seller_message_id: int,
    seller_text: str,
    customer_text: str,
    chat_context: str,
    context_mode: str,
) -> tuple[str, str]:
    """Формирует bundled-prompt evaluator: один вызов на реплику, все правила сразу."""

    # Quote-contract: это обязательное бизнес-условие доказуемости.
    # Для hit=true evaluator обязан вернуть не смысловой пересказ, а буквальный фрагмент seller_text.
    system_prompt = (
        "Ты evaluator качества продаж. "
        "Верни только JSON по схеме BundledEvaluatorResult. "
        "Оцени greeting/upsell/empathy отдельно. "
        "reason пиши на русском. "
        "Quote-contract обязателен: evidence_quote для hit=true должен быть дословной непустой "
        "contiguous подстрокой seller_text, символ-в-символ, на том же языке. "
        "Нельзя переводить, перефразировать, менять пунктуацию и кавычки. "
        "Для hit=false допускается пустой evidence_quote."
    )

    lines = [
        f"seller_message_id={int(seller_message_id)}",
        f"context_mode={context_mode}",
        f"customer_text={customer_text}",
        "BEGIN_SELLER_TEXT",
        seller_text,
        "END_SELLER_TEXT",
        "Контекст чата до текущей реплики:",
        chat_context,
        "",
        "Правила для оценки:",
    ]
    for rule in rules:
        lines.extend(
            [
                f"- {rule.key} ({rule.title_ru})",
                f"  что проверяем: {rule.what_to_check}",
                f"  зачем это бизнесу: {rule.why_it_matters}",
                f"  reason_code: {_reason_codes_for_rule(rule.key)}",
            ]
        )
        anti = _anti_patterns_for_rule(rule.key)
        if anti:
            lines.append("  антипаттерны:")
            for item in anti.splitlines():
                lines.append(f"  {item}")
    lines.extend(
        [
            "",
            "Проверь доказуемость:",
            "- если hit=true, evidence_quote обязан быть непустой дословной contiguous подстрокой seller_text;",
            "- language evidence_quote должен совпадать с language seller_text;",
            "- перевод/перефраз/улучшение формулировки evidence_quote недопустимы;",
            "- если hit=false, reason и reason_code все равно обязательны.",
        ]
    )
    return system_prompt, "\n".join(lines)


def build_judge_prompts_bundle(
    rules: Sequence[RuleCard],
    *,
    seller_text: str,
    customer_text: str,
    chat_context: str,
    evaluator: BundledEvaluatorResult,
    context_mode: str,
) -> tuple[str, str]:
    """Формирует bundled-prompt judge: один вызов на реплику, все правила сразу."""

    system_prompt = (
        "Ты независимый judge качества. "
        "Для каждого правила сначала вычисли expected_hit по контексту, "
        "затем выставь label=true только если evaluator.hit совпал с expected_hit. "
        "Верни только JSON по схеме BundledJudgeResult. "
        "rationale пиши кратко на русском."
    )

    lines = [
        f"context_mode={context_mode}",
        f"customer_text={customer_text}",
        f"seller_text={seller_text}",
        "Контекст чата до текущей реплики:",
        chat_context,
        "",
        "Правила для проверки:",
    ]
    for rule in rules:
        lines.append(f"- {rule.key}: {rule.what_to_check}")
    lines.extend(
        [
            "",
            "Ответ evaluator (JSON):",
            json.dumps(evaluator.model_dump(), ensure_ascii=False),
        ]
    )
    return system_prompt, "\n".join(lines)


# Блок маппинга bundled payload в rule-key словари.
def evaluator_results_by_rule(result: BundledEvaluatorResult) -> dict[str, RuleEvaluation]:
    """Удобный доступ к bundled evaluator-результатам по ключу правила."""

    return {
        "greeting": result.greeting,
        "upsell": result.upsell,
        "empathy": result.empathy,
    }


def judge_results_by_rule(result: BundledJudgeResult) -> dict[str, RuleJudgeEvaluation]:
    """Удобный доступ к bundled judge-результатам по ключу правила."""

    return {
        "greeting": result.greeting,
        "upsell": result.upsell,
        "empathy": result.empathy,
    }
