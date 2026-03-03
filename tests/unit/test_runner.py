from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pytest

from worker.config import load_config
from worker.models import SourceInfo, VideoFrame
from worker.runner import WorkerRunner


class FakeSource:
    def __init__(self) -> None:
        self.closed = False

    async def open(self) -> SourceInfo:
        return SourceInfo(width=16, height=16, fps=24, pixel_format="yuv420p")

    async def recv(self) -> VideoFrame:
        await asyncio.sleep(0.01)
        return VideoFrame(data=b"aaaaaa", width=2, height=2, pixel_format="yuv420p")

    async def close(self) -> None:
        self.closed = True


class FakeSink:
    def __init__(self) -> None:
        self.closed = False

    async def open(self, _source: SourceInfo) -> None:
        return None

    async def write(self, _frame: VideoFrame) -> None:
        await asyncio.sleep(0.001)

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_runner_returns_130_on_interrupt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REACTOR_API_KEY", "rk_test")
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(
        """
job:
  id: job_1
  name: demo
source:
  type: reactor
  model_name: livecore
  api_key_ref: env:REACTOR_API_KEY
sink:
  type: rtmp
  url: rtmp://localhost/live
video:
  fps: 24
  width: 16
  height: 16
  pixel_format: yuv420p
  bitrate_kbps: 300
  keyframe_interval_sec: 2
""",
        encoding="utf-8",
    )
    cfg = load_config(cfg_file)

    source = FakeSource()
    sink = FakeSink()

    runner = WorkerRunner(
        config=cfg,
        source_factory=lambda: source,
        sink_factory=lambda: sink,
        logger=logging.getLogger(__name__),
    )

    task = asyncio.create_task(runner.run())
    await asyncio.sleep(0.05)
    runner.request_stop(interrupted=True)
    exit_code = await asyncio.wait_for(task, timeout=2.0)

    assert exit_code == 130
    assert source.closed is True
    assert sink.closed is True
