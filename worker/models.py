"""Shared runtime models."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class WorkerState(StrEnum):
    INIT = "INIT"
    CONNECTING_SOURCE = "CONNECTING_SOURCE"
    WAITING_TRACK = "WAITING_TRACK"
    OPENING_SINK = "OPENING_SINK"
    RUNNING = "RUNNING"
    DEGRADED = "DEGRADED"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


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
