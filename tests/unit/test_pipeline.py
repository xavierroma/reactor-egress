from __future__ import annotations

import pytest

from worker.metrics import WorkerMetrics
from worker.models import VideoFrame
from worker.pipeline import BoundedFrameQueue


@pytest.mark.asyncio
async def test_drop_oldest_when_queue_full() -> None:
    metrics = WorkerMetrics()
    queue = BoundedFrameQueue(maxsize=2, metrics=metrics)

    f1 = VideoFrame(data=b"aaaaaa", width=2, height=2, pixel_format="yuv420p")
    f2 = VideoFrame(data=b"bbbbbb", width=2, height=2, pixel_format="yuv420p")
    f3 = VideoFrame(data=b"cccccc", width=2, height=2, pixel_format="yuv420p")

    await queue.push(f1)
    await queue.push(f2)
    await queue.push(f3)

    out1 = await queue.pop(timeout_sec=0.1)
    out2 = await queue.pop(timeout_sec=0.1)

    assert out1 is not None and out1.data == b"bbbbbb"
    assert out2 is not None and out2.data == b"cccccc"
    assert metrics.egress_frame_drop_total == 1
