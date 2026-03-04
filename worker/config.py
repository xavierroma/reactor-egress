"""Configuration schema and validation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Literal, NotRequired, TypedDict, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator


class ConfigError(Exception):
    """Raised for invalid runtime config."""


class JobConfigDict(TypedDict):
    id: str
    name: str


class SourceConfigDict(TypedDict):
    type: Literal["reactor"]


class SinkConfigDict(TypedDict):
    type: Literal["rtmp"]
    url: str
    stream_key_ref: NotRequired[str]


class VideoConfigDict(TypedDict):
    fps: int
    width: int
    height: int
    pixel_format: Literal["yuv420p", "rgb24"]
    bitrate_kbps: int
    keyframe_interval_sec: int


class AudioConfigDict(TypedDict, total=False):
    inject_silence: bool
    sample_rate: int
    channels: int


class RetryConfigDict(TypedDict, total=False):
    max_attempts: int
    base_backoff_sec: float
    max_backoff_sec: float
    jitter_ratio: float


class RuntimeConfigDict(TypedDict, total=False):
    frame_queue_size: int
    log_level: Literal["debug", "info", "warning", "error"]


class WorkerConfigDict(TypedDict):
    job: JobConfigDict
    source: SourceConfigDict
    sink: SinkConfigDict
    video: VideoConfigDict
    audio: NotRequired[AudioConfigDict]
    retry: NotRequired[RetryConfigDict]
    runtime: NotRequired[RuntimeConfigDict]


class JobConfig(BaseModel):
    id: str = Field(min_length=1)
    name: str = Field(min_length=1)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["reactor"] = "reactor"


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


WorkerConfigInput: TypeAlias = WorkerConfig | WorkerConfigDict


@dataclass(frozen=True, slots=True)
class ResolvedSecrets:
    sink_url_with_key: str


def load_config(config: WorkerConfigInput) -> WorkerConfig:
    if isinstance(config, WorkerConfig):
        return config

    if isinstance(config, dict):
        try:
            return WorkerConfig.model_validate(config)
        except ValidationError as exc:
            raise ConfigError(f"Config validation failed: {exc}") from exc

    raise TypeError("config must be a WorkerConfig or WorkerConfigDict")


def resolve_secret_ref(ref: str) -> str:
    if not ref.startswith("env:"):
        raise ConfigError(f"Unsupported secret ref: {ref}")
    env_name = ref.removeprefix("env:")
    value = os.getenv(env_name)
    if not value:
        raise ConfigError(f"Missing required environment variable: {env_name}")
    return value


def resolve_secrets(config: WorkerConfig) -> ResolvedSecrets:
    sink_url = config.sink.url
    if config.sink.stream_key_ref:
        stream_key = resolve_secret_ref(config.sink.stream_key_ref)
        sink_url = _append_stream_key(sink_url, stream_key)

    return ResolvedSecrets(sink_url_with_key=sink_url)


def _append_stream_key(url: str, stream_key: str) -> str:
    trimmed = url.rstrip("/")
    return f"{trimmed}/{stream_key}"
