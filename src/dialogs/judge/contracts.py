from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class JudgeRuleContext:
    """Бизнес-контекст правила, передаваемый в независимый judge-слой."""

    key: str
    title_ru: str
    what_to_check: str
    why_it_matters: str
    evaluation_scope: str
    seller_window_max: int | None
    hit_policy: str
    reason_codes: tuple[str, ...]
    anti_patterns: tuple[str, ...]


def normalized_rule_keys(rule_keys: Sequence[str]) -> tuple[str, ...]:
    """Нормализует список ключей правил и проверяет базовые инварианты."""

    keys = tuple(str(key).strip() for key in rule_keys if str(key).strip())
    if not keys:
        raise ValueError("rule_keys must not be empty")

    duplicates = [key for key, count in Counter(keys).items() if count > 1]
    if duplicates:
        raise ValueError(f"rule_keys contain duplicates: {sorted(duplicates)}")

    return keys
