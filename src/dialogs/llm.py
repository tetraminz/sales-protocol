from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError  # type: ignore

from .utils import git_branch, git_commit, jdump, now_utc

try:
    from openai import OpenAI  # type: ignore
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

    def start_run(
        self,
        conn: sqlite3.Connection,
        *,
        run_kind: str,
        mode: str,
        prompt_version: str,
        sgr_version: str,
        meta: dict[str, Any] | None = None,
    ) -> str:
        run_id = f"llm_{uuid.uuid4().hex[:12]}"
        now = now_utc()
        conn.execute(
            """
            INSERT INTO llm_runs(
              run_id, run_kind, mode, model, git_commit, git_branch,
              prompt_version, sgr_version, started_at_utc, finished_at_utc,
              status, meta_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, '', 'running', ?)
            """,
            (
                run_id,
                run_kind,
                mode,
                self.model,
                git_commit(),
                git_branch(),
                prompt_version,
                sgr_version,
                now,
                jdump(meta or {}),
            ),
        )
        conn.commit()
        return run_id

    def finish_run(self, conn: sqlite3.Connection, run_id: str, status: str) -> None:
        conn.execute(
            "UPDATE llm_runs SET status=?, finished_at_utc=? WHERE run_id=?",
            (status, now_utc(), run_id),
        )
        conn.commit()

    @staticmethod
    def _looks_like_schema_error(text: str) -> bool:
        lower = text.lower()
        return (
            "invalid_json_schema" in lower
            or "invalid schema for response_format" in lower
            or "text.format.schema" in lower
            or "json_parse_failed" in lower
            or "validation_failed" in lower
        )

    def call_json_schema(
        self,
        conn: sqlite3.Connection,
        *,
        run_id: str,
        phase: str,
        model_type: type[T],
        system_prompt: str,
        user_prompt: str,
        rule_id: int | None = None,
        conversation_id: str = "",
        message_id: int = 0,
        attempt: int = 1,
    ) -> CallResult:
        started = time.time()
        schema = model_type.model_json_schema()
        request_payload = {
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

        response_http_status = 0
        response_json = "{}"
        extracted = "{}"
        parse_ok = False
        validation_ok = False
        error_message = ""
        parsed: BaseModel | None = None
        is_schema_error = False
        is_live_error = False

        if self._client is None:
            error_message = "live_call_failed: OPENAI_API_KEY is not set"
            is_live_error = True
            response_json = jdump({"provider": "openai", "error": error_message})
        else:
            try:
                response = self._client.responses.create(**request_payload)
                response_http_status = 200
                if hasattr(response, "model_dump_json"):
                    response_json = response.model_dump_json()
                else:
                    response_json = jdump(response)  # pragma: no cover
                extracted = getattr(response, "output_text", "") or "{}"
            except Exception as exc:
                response_http_status = int(getattr(exc, "status_code", 0) or 0)
                error_message = f"live_call_failed: {exc}"
                is_live_error = True
                is_schema_error = self._looks_like_schema_error(error_message)
                response_json = jdump({"provider": "openai", "error": error_message})

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

        latency_ms = int((time.time() - started) * 1000)
        conn.execute(
            """
            INSERT INTO llm_calls(
              run_id, rule_id, conversation_id, message_id, phase, attempt,
              request_json, response_http_status, response_json, extracted_json,
              parse_ok, validation_ok, error_message, latency_ms, tokens_json, created_at_utc
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?)
            """,
            (
                run_id,
                rule_id,
                conversation_id,
                message_id,
                phase,
                attempt,
                jdump(request_payload),
                response_http_status,
                response_json,
                extracted,
                1 if parse_ok else 0,
                1 if validation_ok else 0,
                error_message,
                latency_ms,
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
