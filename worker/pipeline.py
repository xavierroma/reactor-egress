"""Bounded frame queue with drop-oldest policy."""

from __future__ import annotations

import asyncio

from worker.metrics import WorkerMetrics
from worker.models import VideoFrame


class BoundedFrameQueue:
    def __init__(self, maxsize: int, metrics: WorkerMetrics) -> None:
        self._queue: asyncio.Queue[VideoFrame] = asyncio.Queue(maxsize=maxsize)
        self._metrics = metrics

    async def push(self, frame: VideoFrame) -> None:
        if self._queue.full():
            try:
                self._queue.get_nowait()
                self._metrics.inc_frame_drop()
            except asyncio.QueueEmpty:  # pragma: no cover - race protection
                pass
        self._queue.put_nowait(frame)

    async def pop(self, timeout_sec: float = 0.5) -> VideoFrame | None:
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout_sec)
        except TimeoutError:
            return None
