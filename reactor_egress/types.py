"""Public runtime types and options."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from reactor_egress.errors import ConfigError


@dataclass(frozen=True, slots=True)
class VideoOptions:
    fps: int = 24
    width: int = 1280
    height: int = 720
    pixel_format: Literal["yuv420p", "rgb24"] = "yuv420p"
    bitrate_kbps: int = 1200
    keyframe_interval_sec: int = 2

    def __post_init__(self) -> None:
        _ensure_range("video.fps", self.fps, minimum=1, maximum=120)
        _ensure_range("video.width", self.width, minimum=16, maximum=7680)
        _ensure_range("video.height", self.height, minimum=16, maximum=4320)
        if self.pixel_format not in {"yuv420p", "rgb24"}:
            raise ConfigError("video.pixel_format must be one of: yuv420p, rgb24")
        _ensure_range("video.bitrate_kbps", self.bitrate_kbps, minimum=100, maximum=100000)
        _ensure_range("video.keyframe_interval_sec", self.keyframe_interval_sec, minimum=1, maximum=30)


@dataclass(frozen=True, slots=True)
class AudioOptions:
    inject_silence: bool = True
    sample_rate: int = 48000
    channels: int = 2

    def __post_init__(self) -> None:
        _ensure_range("audio.sample_rate", self.sample_rate, minimum=8000, maximum=192000)
        _ensure_range("audio.channels", self.channels, minimum=1, maximum=8)


@dataclass(frozen=True, slots=True)
class RtmpTarget:
    url: str
    stream_key: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.url, str) or not self.url.strip():
            raise ConfigError("target.url must be a non-empty string")
        if not self.url.startswith(("rtmp://", "rtmps://")):
            raise ConfigError("target.url must start with rtmp:// or rtmps://")
        if self.stream_key is not None and not self.stream_key.strip():
            raise ConfigError("target.stream_key must be None or a non-empty string")

    def resolved_url(self) -> str:
        if not self.stream_key:
            return self.url
        return f"{self.url.rstrip('/')}/{self.stream_key}"


@dataclass(slots=True)
class SourceInfo:
    width: int
    height: int
    fps: int
    pixel_format: str


@dataclass(slots=True)
class VideoFrame:
    data: bytes
    width: int
    height: int
    pixel_format: str
    pts_ms: int | None = None


def _ensure_range(name: str, value: int, *, minimum: int, maximum: int) -> None:
    if not isinstance(value, int):
        raise ConfigError(f"{name} must be an integer")
    if value < minimum or value > maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
