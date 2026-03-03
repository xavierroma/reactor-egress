"""Configuration schema and loading."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator


class ConfigError(Exception):
    """Raised for invalid runtime config."""


class BootstrapConfig(BaseModel):
    start_prompt: str | None = None
    auto_start: bool = True


class JobConfig(BaseModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)


class SourceConfig(BaseModel):
    type: Literal["reactor"] = "reactor"
    model_name: str = Field(min_length=1)
    api_key_ref: str = Field(min_length=5)
    track_timeout_sec: int = Field(default=30, ge=1, le=600)
    ready_timeout_sec: int = Field(default=30, ge=1, le=600)
    bootstrap: BootstrapConfig = Field(default_factory=BootstrapConfig)


class SinkConfig(BaseModel):
    type: Literal["rtmp"] = "rtmp"
    url: str = Field(min_length=1)
    stream_key_ref: str | None = None

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        if not value.startswith(("rtmp://", "rtmps://")):
            raise ValueError("sink.url must start with rtmp:// or rtmps://")
        return value


class VideoConfig(BaseModel):
    fps: int = Field(default=24, ge=1, le=120)
    width: int = Field(default=1280, ge=16, le=7680)
    height: int = Field(default=720, ge=16, le=4320)
    pixel_format: Literal["yuv420p", "rgb24"] = "yuv420p"
    bitrate_kbps: int = Field(default=1200, ge=100, le=100000)
    keyframe_interval_sec: int = Field(default=2, ge=1, le=30)


class AudioConfig(BaseModel):
    inject_silence: bool = True
    sample_rate: int = Field(default=48000, ge=8000, le=192000)
    channels: int = Field(default=2, ge=1, le=8)


class RetryConfig(BaseModel):
    max_attempts: int = Field(default=5, ge=1, le=100)
    base_backoff_sec: float = Field(default=2.0, ge=0.1, le=300)
    max_backoff_sec: float = Field(default=32.0, ge=0.1, le=3600)
    jitter_ratio: float = Field(default=0.2, ge=0.0, le=1.0)


class RuntimeConfig(BaseModel):
    frame_queue_size: int = Field(default=48, ge=1, le=4096)
    log_level: Literal["debug", "info", "warning", "error"] = "info"


class WorkerConfig(BaseModel):
    job: JobConfig
    source: SourceConfig
    sink: SinkConfig
    video: VideoConfig
    audio: AudioConfig = Field(default_factory=AudioConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)


@dataclass(frozen=True, slots=True)
class ResolvedSecrets:
    reactor_api_key: str
    sink_url_with_key: str


def load_config(path: str | Path) -> WorkerConfig:
    file_path = Path(path)
    try:
        text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Failed to read config file: {file_path}") from exc

    try:
        raw = _parse_config_text(text, file_path.suffix.lower())
        return WorkerConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Config validation failed: {exc}") from exc
    except (json.JSONDecodeError, yaml.YAMLError, ValueError) as exc:
        raise ConfigError(f"Config parse failed: {exc}") from exc


def _parse_config_text(text: str, suffix: str) -> dict[str, Any]:
    if suffix == ".json":
        return json.loads(text)
    if suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(text)
        if not isinstance(data, dict):
            raise ValueError("YAML root must be a map/object")
        return data

    data = yaml.safe_load(text)
    if isinstance(data, dict):
        return data

    fallback = json.loads(text)
    if not isinstance(fallback, dict):
        raise ValueError("Config root must be a map/object")
    return fallback


def resolve_secret_ref(ref: str) -> str:
    if not ref.startswith("env:"):
        raise ConfigError(f"Unsupported secret ref: {ref}")
    env_name = ref.removeprefix("env:")
    value = os.getenv(env_name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {env_name}")
    return value


def resolve_secrets(config: WorkerConfig) -> ResolvedSecrets:
    api_key = resolve_secret_ref(config.source.api_key_ref)

    sink_url = config.sink.url
    if config.sink.stream_key_ref:
        stream_key = resolve_secret_ref(config.sink.stream_key_ref)
        sink_url = _append_stream_key(sink_url, stream_key)

    return ResolvedSecrets(reactor_api_key=api_key, sink_url_with_key=sink_url)


def _append_stream_key(url: str, stream_key: str) -> str:
    trimmed = url.rstrip("/")
    return f"{trimmed}/{stream_key}"
