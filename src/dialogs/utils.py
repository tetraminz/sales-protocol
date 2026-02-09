from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_speaker(raw: str) -> str:
    text = (raw or "").strip().strip("*").strip().lower()
    if text in {"sales rep", "sales representative", "rep", "seller", "продавец"}:
        return "Sales Rep"
    if text in {"customer", "client", "buyer", "покупатель", "клиент"}:
        return "Customer"
    return "Unknown"


def git_value(args: list[str], fallback: str) -> str:
    try:
        out = subprocess.check_output(args, stderr=subprocess.DEVNULL, text=True).strip()
        return out or fallback
    except Exception:
        return fallback


def git_commit() -> str:
    return git_value(["git", "rev-parse", "--short", "HEAD"], "unknown")


def git_branch() -> str:
    return git_value(["git", "rev-parse", "--abbrev-ref", "HEAD"], "unknown")


def jdump(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def ensure_parent(path: str | Path) -> None:
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
