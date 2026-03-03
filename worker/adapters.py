"""Protocol contracts for source and sink adapters."""

from __future__ import annotations

from typing import Protocol

from worker.models import SourceInfo, VideoFrame


class SourceAdapter(Protocol):
    async def open(self) -> SourceInfo: ...

    async def recv(self) -> VideoFrame: ...

    async def close(self) -> None: ...


class SinkAdapter(Protocol):
    async def open(self, source: SourceInfo) -> None: ...

    async def write(self, frame: VideoFrame) -> None: ...

    async def close(self) -> None: ...
