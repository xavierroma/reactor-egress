from __future__ import annotations

from dataclasses import dataclass

import pytest

from worker.config import SourceConfig, VideoConfig
from worker.errors import RetryableError
from worker.models import VideoFrame
from worker.source.reactor import ReactorSourceAdapter


@dataclass
class FakeFrame:
    data: bytes
    width: int = 16
    height: int = 16
    pixel_format: str = "yuv420p"


class FakeTrack:
    width = 16
    height = 16
    fps = 24
    pixel_format = "yuv420p"

    async def recv(self) -> FakeFrame:
        return FakeFrame(data=b"aaaaaa")


class FakeClient:
    def __init__(self, *, ready_after: int = 1, track_after: int = 1) -> None:
        self.calls: list[str] = []
        self._ready_after = ready_after
        self._track_after = track_after
        self._status_calls = 0
        self._track_calls = 0

    async def connect(self) -> None:
        self.calls.append("connect")

    def status(self) -> str:
        self._status_calls += 1
        if self._status_calls >= self._ready_after:
            return "READY"
        return "CONNECTING"

    async def schedule_prompt(self, *, timestamp: int, new_prompt: str) -> None:
        self.calls.append("schedule_prompt")

    async def start(self) -> None:
        self.calls.append("start")

    async def get_remote_track(self) -> FakeTrack | None:
        self._track_calls += 1
        if self._track_calls >= self._track_after:
            self.calls.append("get_remote_track")
            return FakeTrack()
        return None

    async def disconnect(self) -> None:
        self.calls.append("disconnect")


@pytest.mark.asyncio
async def test_bootstrap_prompt_happens_before_start() -> None:
    client = FakeClient(ready_after=1, track_after=1)
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
            bootstrap={"start_prompt": "hello", "auto_start": True},
        ),
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        api_key="rk_test",
        logger=__import__("logging").getLogger(__name__),
        client_factory=lambda _model, _key: client,
    )

    info = await adapter.open()
    frame = await adapter.recv()
    await adapter.close()

    assert info.width == 16
    assert isinstance(frame, VideoFrame)
    assert client.calls.index("schedule_prompt") < client.calls.index("start")


@pytest.mark.asyncio
async def test_ready_timeout_is_retryable() -> None:
    client = FakeClient(ready_after=999, track_after=1)
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
            ready_timeout_sec=1,
        ),
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        api_key="rk_test",
        logger=__import__("logging").getLogger(__name__),
        client_factory=lambda _model, _key: client,
    )

    with pytest.raises(RetryableError, match="source_ready_timeout"):
        await adapter.open()


@pytest.mark.asyncio
async def test_track_timeout_is_retryable() -> None:
    client = FakeClient(ready_after=1, track_after=999)
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
            track_timeout_sec=1,
        ),
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        api_key="rk_test",
        logger=__import__("logging").getLogger(__name__),
        client_factory=lambda _model, _key: client,
    )

    with pytest.raises(RetryableError, match="source_track_timeout"):
        await adapter.open()
