from __future__ import annotations

import asyncio
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
    kind = "video"
    width = 16
    height = 16
    fps = 24
    pixel_format = "yuv420p"

    async def recv(self) -> FakeFrame:
        return FakeFrame(data=b"aaaaaa")


class FakeTrackNoMetadata:
    kind = "video"

    async def recv(self) -> FakeFrame:
        return FakeFrame(data=b"a" * ((32 * 24 * 3) // 2), width=32, height=24, pixel_format="yuv420p")


class FakeTrackMismatchedMetadata:
    kind = "video"
    width = 1280
    height = 720
    fps = 24
    pixel_format = "yuv420p"

    async def recv(self) -> FakeFrame:
        return FakeFrame(data=b"a" * ((32 * 24 * 3) // 2), width=32, height=24, pixel_format="yuv420p")


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


class FakeAudioTrack:
    kind = "audio"

    async def recv(self) -> FakeFrame:
        raise AssertionError("audio track should not be selected")


class FakeClientPluralTracks:
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

    async def get_remote_tracks(self) -> dict[str, object]:
        self._track_calls += 1
        if self._track_calls >= self._track_after:
            self.calls.append("get_remote_tracks")
            return {"audio-main": FakeAudioTrack(), "video-main": FakeTrack()}
        return {}

    async def disconnect(self) -> None:
        self.calls.append("disconnect")


class FakeClientNoTrackMetadata:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def connect(self) -> None:
        self.calls.append("connect")

    def status(self) -> str:
        return "READY"

    async def start(self) -> None:
        self.calls.append("start")

    async def get_remote_track(self) -> FakeTrackNoMetadata:
        self.calls.append("get_remote_track")
        return FakeTrackNoMetadata()

    async def disconnect(self) -> None:
        self.calls.append("disconnect")


class FakeClientMismatchedTrackMetadata:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def connect(self) -> None:
        self.calls.append("connect")

    def status(self) -> str:
        return "READY"

    async def start(self) -> None:
        self.calls.append("start")

    async def get_remote_track(self) -> FakeTrackMismatchedMetadata:
        self.calls.append("get_remote_track")
        return FakeTrackMismatchedMetadata()

    async def disconnect(self) -> None:
        self.calls.append("disconnect")


class FakeClientEventTrack:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self._listeners: dict[str, list[object]] = {}

    async def connect(self) -> None:
        self.calls.append("connect")

    def status(self) -> str:
        return "READY"

    async def start(self) -> None:
        self.calls.append("start")

        async def _emit_track() -> None:
            await asyncio.sleep(0.05)
            for handler in self._listeners.get("track_received", []):
                handler("main_video", FakeTrack())

        asyncio.create_task(_emit_track())

    async def get_remote_tracks(self) -> dict[str, object]:
        self.calls.append("get_remote_tracks")
        return {}

    def on(self, event_name: str, handler: object) -> None:
        self._listeners.setdefault(event_name, []).append(handler)

    def off(self, event_name: str, handler: object) -> None:
        handlers = self._listeners.get(event_name, [])
        if handler in handlers:
            handlers.remove(handler)

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
async def test_supports_injected_reactor_client_instance() -> None:
    client = FakeClient(ready_after=1, track_after=1)
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
        ),
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        api_key="rk_test",
        logger=__import__("logging").getLogger(__name__),
        client=client,
    )

    info = await adapter.open()
    frame = await adapter.recv()
    await adapter.close()

    assert info.width == 16
    assert isinstance(frame, VideoFrame)
    assert "connect" in client.calls
    assert "disconnect" in client.calls


def test_rejects_client_and_client_factory_together() -> None:
    client = FakeClient(ready_after=1, track_after=1)
    with pytest.raises(ValueError, match="either client or client_factory"):
        ReactorSourceAdapter(
            config=SourceConfig(
                model_name="livecore",
                api_key_ref="env:REACTOR_API_KEY",
            ),
            video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
            api_key="rk_test",
            logger=__import__("logging").getLogger(__name__),
            client=client,
            client_factory=lambda _model, _key: client,
        )


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


@pytest.mark.asyncio
async def test_supports_plural_remote_tracks_api() -> None:
    client = FakeClientPluralTracks(ready_after=1, track_after=1)
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
            bootstrap={"auto_start": False},
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
    assert "get_remote_tracks" in client.calls


@pytest.mark.asyncio
async def test_falls_back_to_track_event_when_remote_tracks_empty() -> None:
    client = FakeClientEventTrack()
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
            track_timeout_sec=2,
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
    assert "get_remote_tracks" in client.calls


@pytest.mark.asyncio
async def test_probes_first_frame_when_track_has_no_metadata() -> None:
    client = FakeClientNoTrackMetadata()
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
        ),
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        api_key="rk_test",
        logger=__import__("logging").getLogger(__name__),
        client_factory=lambda _model, _key: client,
    )

    info = await adapter.open()
    frame = await adapter.recv()
    await adapter.close()

    assert info.width == 32
    assert info.height == 24
    assert frame.width == 32
    assert frame.height == 24


@pytest.mark.asyncio
async def test_first_frame_overrides_mismatched_track_metadata() -> None:
    client = FakeClientMismatchedTrackMetadata()
    adapter = ReactorSourceAdapter(
        config=SourceConfig(
            model_name="livecore",
            api_key_ref="env:REACTOR_API_KEY",
        ),
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        api_key="rk_test",
        logger=__import__("logging").getLogger(__name__),
        client_factory=lambda _model, _key: client,
    )

    info = await adapter.open()
    frame = await adapter.recv()
    await adapter.close()

    assert info.width == 32
    assert info.height == 24
    assert frame.width == 32
    assert frame.height == 24
