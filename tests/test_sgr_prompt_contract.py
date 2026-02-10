from __future__ import annotations

import json

from dialogs.sgr_core import (
    all_rules,
    build_chat_context,
    build_evaluator_prompts_bundle,
    seller_message_refs,
)


def _conversation_messages() -> list[dict[str, object]]:
    return [
        {"message_id": 1, "message_order": 1, "speaker_label": "Sales Rep", "text": "Hello, I can help with jackets."},
        {"message_id": 2, "message_order": 2, "speaker_label": "Customer", "text": "I need something warm."},
        {
            "message_id": 3,
            "message_order": 3,
            "speaker_label": "Sales Rep",
            "text": "I understand your concern. We have a lightweight premium model.",
        },
    ]


def _build_prompt_pair() -> tuple[str, str]:
    messages = _conversation_messages()
    return build_evaluator_prompts_bundle(
        all_rules(),
        conversation_id="conv_demo",
        chat_context=build_chat_context(messages, mode="full"),
        seller_catalog=seller_message_refs(messages),
        greeting_window_max=3,
        context_mode="full",
    )


def _section(text: str, begin: str, end: str) -> str:
    after_begin = text.split(begin, 1)[1]
    return after_begin.split(end, 1)[0].strip()


def test_evaluator_prompt_contains_required_markers_and_quote_contract() -> None:
    system_prompt, user_prompt = _build_prompt_pair()

    assert "bundled evaluator schema" in system_prompt
    assert "BEGIN_SELLER_CATALOG_JSON" in user_prompt
    assert "END_SELLER_CATALOG_JSON" in user_prompt
    assert "BEGIN_SELLER_ANCHOR_BLOCKS" in user_prompt
    assert "END_SELLER_ANCHOR_BLOCKS" in user_prompt
    assert "Правила для оценки:" in user_prompt
    assert "SELF-CHECK перед JSON:" in user_prompt

    assert "Quote-contract (обязательно):" in user_prompt
    assert "evidence_quote" in user_prompt
    assert "contiguous" in user_prompt
    assert "anchor-поля" in user_prompt
    assert "null" in user_prompt


def test_evaluator_catalog_json_is_compact_without_text() -> None:
    _system_prompt, user_prompt = _build_prompt_pair()

    raw_catalog = _section(user_prompt, "BEGIN_SELLER_CATALOG_JSON", "END_SELLER_CATALOG_JSON")
    payload = json.loads(raw_catalog)

    assert payload
    assert all(set(item.keys()) == {"message_id", "message_order"} for item in payload)
    assert all("text" not in item for item in payload)


def test_evaluator_prompt_keeps_greeting_window_config() -> None:
    _system_prompt, user_prompt = _build_prompt_pair()
    assert "greeting_window_max=3" in user_prompt


def test_evaluator_prompt_is_deterministic_for_equal_input() -> None:
    first = _build_prompt_pair()
    second = _build_prompt_pair()
    assert first == second
