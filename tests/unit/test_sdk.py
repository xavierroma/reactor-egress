from __future__ import annotations

import asyncio
import logging

import pytest

from worker.errors import TerminalError
from worker.models import SourceInfo, VideoFrame, WorkerState
from worker.sdk import EgressWorker, normalize_config, run_egress


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


class FailingSource(FakeSource):
    async def recv(self) -> VideoFrame:  # type: ignore[override]
        raise TerminalError("source_failed", "boom")


class FakeSink:
    def __init__(self) -> None:
        self.closed = False

    async def open(self, _source: SourceInfo) -> None:
        return None

    async def write(self, _frame: VideoFrame) -> None:
        await asyncio.sleep(0)

    async def close(self) -> None:
        self.closed = True


class FakeFailingTrack:
    kind = "video"
    width = 16
    height = 16
    fps = 24
    pixel_format = "yuv420p"

    async def recv(self) -> VideoFrame:
        raise TerminalError("source_failed", "boom")


class FakeInjectedClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def connect(self) -> None:
        self.calls.append("connect")

    async def start(self) -> None:
        self.calls.append("start")

    async def disconnect(self) -> None:
        self.calls.append("disconnect")

    async def get_remote_tracks(self) -> dict[str, object]:
        self.calls.append("get_remote_tracks")
        return {"video-main": FakeFailingTrack()}


def _config_mapping() -> dict[str, object]:
    return {
        "job": {"id": "job_sdk", "name": "sdk"},
        "source": {
            "type": "reactor",
        },
        "sink": {
            "type": "rtmp",
            "url": "rtmp://localhost/live",
        },
        "video": {
            "fps": 24,
            "width": 16,
            "height": 16,
            "pixel_format": "yuv420p",
            "bitrate_kbps": 300,
            "keyframe_interval_sec": 2,
        },
    }


@pytest.mark.asyncio
async def test_egress_worker_from_config_runs_and_stops() -> None:
    source = FakeSource()
    sink = FakeSink()
    worker = EgressWorker.from_config(
        _config_mapping(),
        source_factory=lambda: source,
        sink_factory=lambda: sink,
        logger=logging.getLogger(__name__),
        configure_root_logger=False,
    )

    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.05)
    worker.request_stop(interrupted=True)
    exit_code = await asyncio.wait_for(task, timeout=2.0)

    assert exit_code == 130
    assert source.closed is True
    assert sink.closed is True
    assert worker.state is WorkerState.STOPPED


@pytest.mark.asyncio
async def test_run_egress_accepts_mapping_config() -> None:
    source = FailingSource()
    sink = FakeSink()
    exit_code = await run_egress(
        _config_mapping(),
        source_factory=lambda: source,
        sink_factory=lambda: sink,
        logger=logging.getLogger(__name__),
        configure_root_logger=False,
    )

    assert exit_code == 1
    assert source.closed is True
    assert sink.closed is True


@pytest.mark.asyncio
async def test_run_egress_uses_injected_reactor_client_without_lifecycle_calls() -> None:
    client = FakeInjectedClient()

    exit_code = await run_egress(
        _config_mapping(),
        reactor_client=client,
        logger=logging.getLogger(__name__),
        configure_root_logger=False,
    )

    assert exit_code == 1
    assert "get_remote_tracks" in client.calls
    assert "connect" not in client.calls
    assert "start" not in client.calls
    assert "disconnect" not in client.calls


def test_reactor_client_required_without_source_factory() -> None:
    with pytest.raises(ValueError, match="reactor_client is required"):
        EgressWorker.from_config(
            _config_mapping(),
            configure_root_logger=False,
        )


def test_rejects_source_factory_and_reactor_client_together() -> None:
    client = FakeInjectedClient()
    with pytest.raises(ValueError, match="either source_factory or reactor_client"):
        EgressWorker.from_config(
            _config_mapping(),
            source_factory=lambda: FakeSource(),
            reactor_client=client,
            configure_root_logger=False,
        )


def test_normalize_config_rejects_invalid_type() -> None:
    with pytest.raises(TypeError, match="WorkerConfig or WorkerConfigDict"):
        normalize_config("config.yaml")  # type: ignore[arg-type]
