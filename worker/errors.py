"""Domain errors and classification helpers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class WorkerError(Exception):
    code: str
    message: str
    retryable: bool

    def __str__(self) -> str:  # pragma: no cover - Exception formatting only
        return f"{self.code}: {self.message}"


class RetryableError(WorkerError):
    def __init__(self, code: str, message: str):
        super().__init__(code=code, message=message, retryable=True)


class TerminalError(WorkerError):
    def __init__(self, code: str, message: str):
        super().__init__(code=code, message=message, retryable=False)


def classify_source_exception(exc: Exception) -> WorkerError:
    text = str(exc).lower()
    if any(token in text for token in ("unauthorized", "forbidden", "invalid api key", "bad api key", "permission")):
        return TerminalError("source_auth_error", str(exc))
    if any(token in text for token in ("invalid", "schema", "unsupported", "not found model")):
        return TerminalError("source_config_error", str(exc))
    return RetryableError("source_runtime_error", str(exc))


def classify_sink_exception(exc: Exception) -> WorkerError:
    text = str(exc).lower()
    if any(token in text for token in ("ffmpeg not found", "invalid rtmp", "unsupported")):
        return TerminalError("sink_config_error", str(exc))
    if any(token in text for token in ("connection refused", "broken pipe", "timed out", "eof")):
        return RetryableError("sink_io_error", str(exc))
    return RetryableError("sink_runtime_error", str(exc))
