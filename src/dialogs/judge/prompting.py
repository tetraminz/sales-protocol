from __future__ import annotations

from collections.abc import Mapping, Sequence
import json

from .contracts import JudgeRuleContext

BASE_JUDGE_SYSTEM_PROMPT = (
    "Ты независимый judge качества. "
    "Для каждого правила сначала вычисли expected_hit по контексту диалога, "
    "затем выставь label=true только если evaluator.hit совпал с expected_hit. "
    "Учитывай greeting только в первых greeting_window_max seller-сообщениях. "
    "Верни только JSON по предоставленной schema bundled judge. "
    "rationale пиши кратко на русском."
)


def _item_value(item: object, key: str) -> object:
    if isinstance(item, dict):
        return item[key]
    return getattr(item, key)


def _seller_catalog_json(seller_catalog: Sequence[object]) -> str:
    payload = [
        {
            "message_id": int(_item_value(item, "message_id")),
            "message_order": int(_item_value(item, "message_order")),
            "text": str(_item_value(item, "text")),
        }
        for item in seller_catalog
    ]
    return json.dumps(payload, ensure_ascii=False)


def _rule_context_from_mapping(raw: Mapping[str, object]) -> JudgeRuleContext:
    reason_codes = tuple(str(item) for item in (raw.get("reason_codes") or ()))
    anti_patterns = tuple(str(item) for item in (raw.get("anti_patterns") or ()))
    return JudgeRuleContext(
        key=str(raw["key"]),
        title_ru=str(raw.get("title_ru", "")),
        what_to_check=str(raw.get("what_to_check", "")),
        why_it_matters=str(raw.get("why_it_matters", "")),
        evaluation_scope=str(raw.get("evaluation_scope", "")),
        seller_window_max=None if raw.get("seller_window_max") is None else int(raw["seller_window_max"]),
        hit_policy=str(raw.get("hit_policy", "")),
        reason_codes=reason_codes,
        anti_patterns=anti_patterns,
    )


def _normalize_rule_contexts(rule_contexts: Sequence[JudgeRuleContext | Mapping[str, object]]) -> list[JudgeRuleContext]:
    out: list[JudgeRuleContext] = []
    for item in rule_contexts:
        if isinstance(item, JudgeRuleContext):
            out.append(item)
        else:
            out.append(_rule_context_from_mapping(item))
    return out


def build_judge_prompt(
    *,
    conversation_id: str,
    chat_context: str,
    seller_catalog: Sequence[object],
    evaluator_payload: Mapping[str, object],
    context_mode: str,
    greeting_window_max: int,
    rule_contexts: Sequence[JudgeRuleContext | Mapping[str, object]],
) -> tuple[str, str]:
    """Строит bundled judge prompt из независимого judge-слоя."""

    rules = _normalize_rule_contexts(rule_contexts)
    lines = [
        f"conversation_id={conversation_id}",
        f"context_mode={context_mode}",
        f"greeting_window_max={int(greeting_window_max)}",
        "BEGIN_SELLER_CATALOG_JSON",
        _seller_catalog_json(seller_catalog),
        "END_SELLER_CATALOG_JSON",
        "Контекст чата:",
        chat_context,
        "",
        "Правила для проверки:",
    ]

    for rule in rules:
        lines.extend(
            [
                f"- {rule.key}: scope={rule.evaluation_scope}, hit_policy={rule.hit_policy}, seller_window_max={rule.seller_window_max}",
                f"  что проверяем: {rule.what_to_check}",
                f"  зачем это бизнесу: {rule.why_it_matters}",
            ]
        )
        if rule.reason_codes:
            joined = ", ".join(f"`{value}`" for value in rule.reason_codes)
            lines.append(f"  reason_code: {joined}")
        if rule.anti_patterns:
            lines.append("  антипаттерны:")
            lines.extend(f"  - {item}" for item in rule.anti_patterns)

    lines.extend(
        [
            "",
            "Ответ evaluator (JSON):",
            json.dumps(dict(evaluator_payload), ensure_ascii=False),
        ]
    )

    return BASE_JUDGE_SYSTEM_PROMPT, "\n".join(lines)
