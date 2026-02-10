from __future__ import annotations

from collections.abc import Mapping, Sequence


def reason_code_for_rule(rule_key: str, hit: bool) -> str:
    if rule_key == "greeting":
        return "greeting_present" if hit else "greeting_missing"
    if rule_key == "upsell":
        return "upsell_offer" if hit else "upsell_missing"
    return "empathy_acknowledged" if hit else "informational_without_empathy"


def _is_greeting(text: str) -> bool:
    low = text.lower()
    return "здрав" in low or "hello" in low


def _is_upsell(text: str) -> bool:
    low = text.lower()
    return "пакет" in low or "plan" in low or "доп" in low


def _is_empathy(text: str) -> bool:
    low = text.lower()
    return "понима" in low or "understand" in low


def rule_eval_for_dialog(
    rule_key: str,
    seller_rows: Sequence[Mapping[str, object]],
) -> tuple[bool, str, str, int | None, int | None]:
    matcher = {
        "greeting": _is_greeting,
        "upsell": _is_upsell,
        "empathy": _is_empathy,
    }.get(rule_key, lambda _text: False)

    greeting_window = seller_rows[:3]
    if rule_key == "greeting":
        for row in greeting_window:
            text = str(row["text"])
            if matcher(text):
                quote = text.split()[0] if text.split() else ""
                return True, "greeting_present", quote, int(row["message_id"]), int(row["message_order"])
        for row in seller_rows:
            text = str(row["text"])
            if matcher(text):
                return False, "greeting_late", "", None, None
        return False, "greeting_missing", "", None, None

    for row in seller_rows:
        text = str(row["text"])
        if matcher(text):
            quote = text.split()[0] if text.split() else ""
            return True, reason_code_for_rule(rule_key, True), quote, int(row["message_id"]), int(row["message_order"])
    return False, reason_code_for_rule(rule_key, False), "", None, None
