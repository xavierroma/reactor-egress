"""Structured logging and redaction helpers."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from urllib.parse import urlparse, urlunparse


class JsonFormatter(logging.Formatter):
    """Emit JSON logs with stable top-level keys."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            payload.update(fields)
        return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def configure_logging(level: str) -> None:
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)


def log_event(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    job_id: str,
    sink_type: str,
    state: str,
    attempt: int,
    error_code: str | None = None,
    **extra_fields: object,
) -> None:
    fields: dict[str, object] = {
        "job_id": job_id,
        "sink_type": sink_type,
        "state": state,
        "attempt": attempt,
    }
    if error_code:
        fields["error_code"] = error_code

    fields.update(extra_fields)
    logger.log(level, message, extra={"fields": fields})


def redact_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"rtmp", "rtmps"}:
        return url

    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        redacted_path = parsed.path
    elif len(parts) == 1:
        redacted_path = "/***"
    else:
        redacted_path = "/" + "/".join(parts[:-1] + ["***"])

    return urlunparse((parsed.scheme, parsed.netloc, redacted_path, "", "", ""))
