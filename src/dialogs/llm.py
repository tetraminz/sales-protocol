from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from .utils import jdump, now_utc

try:  # pragma: no cover
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore

T = TypeVar("T", bound=BaseModel)


@dataclass
class CallResult:
    parsed: BaseModel | None
    parse_ok: bool
    validation_ok: bool
    error_message: str
    is_schema_error: bool
    is_live_error: bool


def _looks_like_schema_error(text: str) -> bool:
    lower = text.lower()
    return (
        "invalid_json_schema" in lower
        or "invalid schema for response_format" in lower
        or "text.format.schema" in lower
        or "json_parse_failed" in lower
        or "validation_failed" in lower
        or "schema_contract_failed" in lower
    )


def _assert_openai_schema_contract(schema: object, *, model_name: str) -> None:
    if isinstance(schema, dict):
        if schema.get("type") == "object":
            props = schema.get("properties") or {}
            required = schema.get("required")
            if not isinstance(required, list):
                raise ValueError(f"{model_name}: required must exist for every object")
            missing = sorted(k for k in props.keys() if k not in set(required))
            if missing:
                raise ValueError(f"{model_name}: required must include all properties, missing={missing}")
            if schema.get("additionalProperties") is not False:
                raise ValueError(f"{model_name}: additionalProperties must be false")
        for value in schema.values():
            _assert_openai_schema_contract(value, model_name=model_name)
    elif isinstance(schema, list):
        for value in schema:
            _assert_openai_schema_contract(value, model_name=model_name)


class LLMClient:
    def __init__(self, model: str = "gpt-4.1-mini", api_key: str | None = None):
        self.model = model
        self.api_key = api_key or ""
        self._client = OpenAI(api_key=self.api_key) if (self.api_key and OpenAI is not None) else None

    @property
    def is_live(self) -> bool:
        return self._client is not None

    def require_live(self, purpose: str) -> None:
        if not self.is_live:
            raise ValueError(f"OPENAI_API_KEY is required for {purpose}")

    def call_json_schema(
        self,
        conn: sqlite3.Connection,
        *,
        run_id: str,
        phase: str,
        rule_key: str,
        conversation_id: str,
        message_id: int,
        model_type: type[T],
        system_prompt: str,
        user_prompt: str,
        attempt: int = 1,
    ) -> CallResult:
        started = time.time()
        schema = model_type.model_json_schema()

        response_http_status = 0
        response_json = "{}"
        extracted = "{}"
        parse_ok = False
        validation_ok = False
        error_message = ""
        parsed: BaseModel | None = None
        is_schema_error = False
        is_live_error = False

        try:
            _assert_openai_schema_contract(schema, model_name=model_type.__name__)
        except Exception as exc:
            error_message = f"schema_contract_failed: {exc}"
            is_schema_error = True
            response_json = jdump({"provider": "openai", "error": error_message})
            extracted = jdump({"error": error_message})

        request_payload: dict[str, Any] = {
            "model": self.model,
            "input": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": model_type.__name__,
                    "schema": schema,
                    "strict": True,
                }
            },
        }

        prompt_chars = len(system_prompt) + len(user_prompt)

        if not error_message:
            if self._client is None:
                error_message = "live_call_failed: OPENAI_API_KEY is not set"
                is_live_error = True
                response_json = jdump({"provider": "openai", "error": error_message})
                extracted = jdump({"error": error_message})
            else:
                try:
                    response = self._client.responses.create(**request_payload)
                    response_http_status = 200
                    if hasattr(response, "model_dump_json"):
                        response_json = response.model_dump_json()
                    else:  # pragma: no cover
                        response_json = jdump(response)
                    extracted = getattr(response, "output_text", "") or "{}"
                except Exception as exc:
                    response_http_status = int(getattr(exc, "status_code", 0) or 0)
                    error_message = f"live_call_failed: {exc}"
                    is_live_error = True
                    is_schema_error = _looks_like_schema_error(error_message)
                    response_json = jdump({"provider": "openai", "error": error_message})
                    extracted = jdump({"error": error_message})

        payload: Any | None = None
        if not error_message:
            try:
                payload = json.loads(extracted)
                parse_ok = True
            except Exception as exc:
                error_message = f"json_parse_failed: {exc}"
                is_schema_error = True

        if payload is not None and not error_message:
            try:
                parsed = model_type.model_validate(payload)
                validation_ok = True
            except ValidationError as exc:
                error_message = f"validation_failed: {exc.errors()}"
                is_schema_error = True

        response_chars = len(extracted)
        latency_ms = int((time.time() - started) * 1000)

        # Audit trace is always full in v3 stabilization mode.
        trace_mode = "full"
        stored_request = jdump(request_payload)
        stored_response = response_json
        stored_extracted = extracted

        conn.execute(
            """
            INSERT INTO llm_calls(
              run_id, phase, rule_key, conversation_id, message_id, attempt,
              context_mode, judge_policy, trace_mode, prompt_chars, response_chars,
              request_json, response_http_status, response_json, extracted_json,
              parse_ok, validation_ok, error_message, latency_ms, created_at_utc
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                phase,
                rule_key,
                conversation_id,
                int(message_id),
                int(attempt),
                "full",
                "full",
                trace_mode,
                int(prompt_chars),
                int(response_chars),
                stored_request,
                int(response_http_status),
                stored_response,
                stored_extracted,
                1 if parse_ok else 0,
                1 if validation_ok else 0,
                error_message,
                int(latency_ms),
                now_utc(),
            ),
        )
        conn.commit()

        return CallResult(
            parsed=parsed,
            parse_ok=parse_ok,
            validation_ok=validation_ok,
            error_message=error_message,
            is_schema_error=is_schema_error,
            is_live_error=is_live_error,
        )
