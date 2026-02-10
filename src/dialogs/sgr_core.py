from __future__ import annotations

"""Core SGR-логика для dialog-level оценки качества продаж.

Роль модуля в пайплайне:
- задает стабильный бизнес-контракт правил (`greeting`, `upsell`, `empathy`);
- задает пороги качества для heatmap и отчетов;
- формирует prompt-шаблон evaluator;
- фиксирует quote-contract как обязательное условие доказуемости.

Границы ответственности:
- здесь нет SQL, сетевых вызовов и файловых операций;
- модуль только описывает правила, инварианты и чистые helper-функции.
"""

from collections.abc import Sequence
from dataclasses import dataclass
import json
from typing import Literal

from .models import ReasonCode

METRICS_VERSION = "v5_dialog_level_bundle"

EvaluationScope = Literal["conversation"]
HitPolicy = Literal["any_occurrence"]


@dataclass(frozen=True)
class RuleCard:
    """Карточка бизнес-правила dialog-level оценки."""

    key: str
    title_ru: str
    what_to_check: str
    why_it_matters: str
    evaluation_scope: EvaluationScope
    seller_window_max: int | None
    hit_policy: HitPolicy


@dataclass(frozen=True)
class QualityThresholds:
    """Пороги зон качества в отчетах и executive-визуализации."""

    green_min: float = 0.90
    yellow_min: float = 0.80
    rule_alert_min: float = 0.85
    judge_coverage_min: float = 1.0


@dataclass(frozen=True)
class SellerMessageRef:
    """Ссылка на seller-сообщение в рамках диалога."""

    message_id: int
    message_order: int
    text: str


RULES: tuple[RuleCard, ...] = (
    RuleCard(
        key="greeting",
        title_ru="Приветствие",
        what_to_check="Есть ли приветствие в первых трех сообщениях продавца.",
        why_it_matters="Первое касание задает тон и влияет на доверие.",
        evaluation_scope="conversation",
        seller_window_max=3,
        hit_policy="any_occurrence",
    ),
    RuleCard(
        key="upsell",
        title_ru="Допродажа",
        what_to_check="Есть ли в диалоге уместное предложение следующего платного шага.",
        why_it_matters="Рост среднего чека без потери качества общения.",
        evaluation_scope="conversation",
        seller_window_max=None,
        hit_policy="any_occurrence",
    ),
    RuleCard(
        key="empathy",
        title_ru="Эмпатия",
        what_to_check="Есть ли в диалоге явное признание ситуации клиента.",
        why_it_matters="Снижает трение и повышает шанс конструктивного ответа клиента.",
        evaluation_scope="conversation",
        seller_window_max=None,
        hit_policy="any_occurrence",
    ),
)

QUALITY_THRESHOLDS = QualityThresholds()

# Бизнес-правила для оценки каждого диалога.
RULE_REASON_CODES: dict[str, tuple[ReasonCode, ...]] = {
    "greeting": ("greeting_present", "greeting_missing", "greeting_late"),
    "upsell": ("upsell_offer", "upsell_missing", "discount_without_upsell"),
    "empathy": ("empathy_acknowledged", "courtesy_without_empathy", "informational_without_empathy"),
}

RULE_ANTI_PATTERNS: dict[str, tuple[str, ...]] = {
    "greeting": (
        "Приветствие после третьего seller-сообщения не засчитывается (reason_code=`greeting_late`).",
    ),
    "upsell": (
        "Скидка/промокод без новой опции не считается допродажей.",
        "Статус/инфо-ответ без платного следующего шага не считается upsell.",
    ),
    "empathy": (
        "Вежливость и small talk не равны эмпатии.",
        "Позитивный тон без признания состояния клиента не считается эмпатией.",
    ),
}

# КАК ДОБАВИТЬ НОВОЕ RULE:
# 1) Добавьте новую RuleCard в RULES (key/title_ru/what_to_check/why_it_matters/...).
# 2) Добавьте reason_codes в RULE_REASON_CODES и антипаттерны в RULE_ANTI_PATTERNS.
# 3) Проверьте тестовые фикстуры и fake-LLM в tests/ и docs_refresh (они должны брать rule_keys из all_rules()).
# 4) Прогоните `make test && make docs` и убедитесь, что scan/report контракт остался стабильным.
# 5) Если изменился публичный бизнес-термин, синхронно обновите doc-contract файлы из docs/stability_case_review.md.


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


def seller_message_refs(conversation_messages: Sequence[object]) -> list[SellerMessageRef]:
    """Собирает seller-сообщения диалога с id/order/text."""

    refs: list[SellerMessageRef] = []
    for item in _ordered_messages(conversation_messages):
        if not is_seller_message(str(item["speaker_label"])):  # type: ignore[index]
            continue
        refs.append(
            SellerMessageRef(
                message_id=int(item["message_id"]),  # type: ignore[index]
                message_order=int(item["message_order"]),  # type: ignore[index]
                text=str(item["text"]),  # type: ignore[index]
            )
        )
    return refs


def greeting_window_refs(
    conversation_messages: Sequence[object],
    *,
    max_messages: int = 3,
) -> list[SellerMessageRef]:
    """Возвращает seller-сообщения, попадающие в окно greeting."""

    return seller_message_refs(conversation_messages)[: max(0, int(max_messages))]


def build_chat_context(
    conversation_messages: Sequence[object],
    *,
    mode: str = "full",
) -> str:
    """Собирает контекст диалога для evaluator/judge.

    Поддерживается только `full`: все сообщения диалога по порядку.
    """

    if mode != "full":
        raise ValueError(f"unknown context mode: {mode}")

    lines: list[str] = []
    for item in _ordered_messages(conversation_messages):
        order = int(item["message_order"])  # type: ignore[index]
        speaker = str(item["speaker_label"])  # type: ignore[index]
        text = str(item["text"])  # type: ignore[index]
        lines.append(f"[{order}] {speaker}: {text}")
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


def build_rule_business_context(rules: Sequence[RuleCard]) -> list[dict[str, object]]:
    """Готовит бизнес-контекст правил для внешних модулей оценки (например, judge)."""

    return [
        {
            "key": str(rule.key),
            "title_ru": str(rule.title_ru),
            "what_to_check": str(rule.what_to_check),
            "why_it_matters": str(rule.why_it_matters),
            "evaluation_scope": str(rule.evaluation_scope),
            "seller_window_max": None if rule.seller_window_max is None else int(rule.seller_window_max),
            "hit_policy": str(rule.hit_policy),
            "reason_codes": tuple(str(code) for code in RULE_REASON_CODES.get(rule.key, ())),
            "anti_patterns": tuple(str(item) for item in RULE_ANTI_PATTERNS.get(rule.key, ())),
        }
        for rule in rules
    ]


def _seller_catalog_json(seller_catalog: Sequence[SellerMessageRef]) -> str:
    payload = [
        {
            "message_id": int(item.message_id),
            "message_order": int(item.message_order),
            "text": str(item.text),
        }
        for item in seller_catalog
    ]
    return json.dumps(payload, ensure_ascii=False)


def _seller_catalog_copy_blocks(seller_catalog: Sequence[SellerMessageRef]) -> str:
    lines: list[str] = []
    for item in seller_catalog:
        lines.extend(
            [
                f"ANCHOR message_id={int(item.message_id)} message_order={int(item.message_order)}",
                "BEGIN_ANCHOR_TEXT",
                str(item.text),
                "END_ANCHOR_TEXT",
            ]
        )
    return "\n".join(lines)


# Блок сборки prompt-ов evaluator/judge.
def build_evaluator_prompts_bundle(
    rules: Sequence[RuleCard],
    *,
    conversation_id: str,
    chat_context: str,
    seller_catalog: Sequence[SellerMessageRef],
    greeting_window_max: int,
    context_mode: str,
) -> tuple[str, str]:
    """Формирует bundled-prompt evaluator: один вызов на диалог, все правила сразу."""

    system_prompt = (
        "Ты evaluator качества продаж. "
        "Верни только JSON по bundled evaluator schema. "
        "Оцени каждое правило из блока 'Правила для оценки' отдельно на уровне всего диалога. "
        "reason пиши на русском. "
        "Quote-contract обязателен: для hit=true evidence_quote должен быть дословной непустой "
        "contiguous подстрокой anchor seller-сообщения, символ-в-символ и на том же языке. "
        "Anchor задается полями evidence_message_id и evidence_message_order. "
        "При hit=false evidence_quote может быть пустым, а anchor-поля должны быть null. "
        "Запрещено изменять регистр, пунктуацию, кавычки, апострофы, диакритику и пробелы внутри evidence_quote. "
        "Запрещено сокращать цитату многоточием, пересказывать, переводить или нормализовать символы."
    )

    lines = [
        f"conversation_id={conversation_id}",
        f"context_mode={context_mode}",
        f"greeting_window_max={int(greeting_window_max)}",
        "BEGIN_SELLER_CATALOG_JSON",
        _seller_catalog_json(seller_catalog),
        "END_SELLER_CATALOG_JSON",
        "Каталог anchor-сообщений для точного COPY-PASTE:",
        "BEGIN_SELLER_ANCHOR_BLOCKS",
        _seller_catalog_copy_blocks(seller_catalog),
        "END_SELLER_ANCHOR_BLOCKS",
        "Контекст чата:",
        chat_context,
        "",
        "Правила для оценки:",
    ]
    for rule in rules:
        lines.extend(
            [
                f"- {rule.key} ({rule.title_ru})",
                f"  scope: {rule.evaluation_scope}",
                f"  seller_window_max: {rule.seller_window_max}",
                f"  hit_policy: {rule.hit_policy}",
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
            "- если hit=true: evidence_quote непустой, evidence_message_id/evidence_message_order обязательны;",
            "- evidence_quote обязан быть contiguous подстрокой текста seller-сообщения по evidence_message_id;",
            "- evidence_message_order должен совпадать с порядком этого же сообщения в seller_catalog;",
            "- для greeting anchor должен быть в первых greeting_window_max seller-сообщениях;",
            "- перевод/перефраз/улучшение формулировки evidence_quote недопустимы;",
            "- если не можешь дать exact quote, ставь hit=false и не выдумывай цитату.",
            "",
            "SELF-CHECK перед JSON:",
            "1) Найди anchor_text по evidence_message_id;",
            "2) Проверь condition: evidence_quote in anchor_text (строго, symbol-for-symbol);",
            "3) Если condition=false, исправь evidence_quote или поставь hit=false.",
        ]
    )
    return system_prompt, "\n".join(lines)
