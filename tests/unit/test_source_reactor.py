from __future__ import annotations

from dataclasses import dataclass

import pytest

from worker.config import VideoConfig
from worker.errors import RetryableError, TerminalError
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


class FakeAudioTrack:
    kind = "audio"

    async def recv(self) -> FakeFrame:
        raise AssertionError("audio track should not be selected")


class FakeClient:
    def __init__(self, tracks: object) -> None:
        self.calls: list[str] = []
        self._tracks = tracks

    async def connect(self) -> None:
        self.calls.append("connect")

    async def start(self) -> None:
        self.calls.append("start")

    async def disconnect(self) -> None:
        self.calls.append("disconnect")

    async def get_remote_tracks(self) -> object:
        self.calls.append("get_remote_tracks")
        return self._tracks


class FakeClientMissingTracksApi:
    pass


@pytest.mark.asyncio
async def test_uses_video_track_from_get_remote_tracks() -> None:
    client = FakeClient({"audio-main": FakeAudioTrack(), "video-main": FakeTrack()})
    adapter = ReactorSourceAdapter(
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=__import__("logging").getLogger(__name__),
        client=client,
    )

    info = await adapter.open()
    frame = await adapter.recv()
    await adapter.close()

    assert info.width == 16
    assert isinstance(frame, VideoFrame)
    assert "get_remote_tracks" in client.calls
    assert "connect" not in client.calls
    assert "start" not in client.calls
    assert "disconnect" not in client.calls


@pytest.mark.asyncio
async def test_no_remote_video_track_is_retryable() -> None:
    client = FakeClient({"audio-main": FakeAudioTrack()})
    adapter = ReactorSourceAdapter(
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=__import__("logging").getLogger(__name__),
        client=client,
    )

    with pytest.raises(RetryableError, match="source_track_unavailable"):
        await adapter.open()


@pytest.mark.asyncio
async def test_missing_get_remote_tracks_is_terminal() -> None:
    adapter = ReactorSourceAdapter(
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=__import__("logging").getLogger(__name__),
        client=FakeClientMissingTracksApi(),
    )

    with pytest.raises(TerminalError, match="source_track_api_missing"):
        await adapter.open()


def test_client_is_required() -> None:
    with pytest.raises(ValueError, match="client is required"):
        ReactorSourceAdapter(
            video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
            logger=__import__("logging").getLogger(__name__),
            client=None,
        )


@pytest.mark.asyncio
async def test_probes_first_frame_when_track_has_no_metadata() -> None:
    client = FakeClient({"video-main": FakeTrackNoMetadata()})
    adapter = ReactorSourceAdapter(
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=__import__("logging").getLogger(__name__),
        client=client,
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
    client = FakeClient({"video-main": FakeTrackMismatchedMetadata()})
    adapter = ReactorSourceAdapter(
        video=VideoConfig(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=__import__("logging").getLogger(__name__),
        client=client,
    )

    info = await adapter.open()
    frame = await adapter.recv()
    await adapter.close()

    assert info.width == 32
    assert info.height == 24
    assert frame.width == 32
    assert frame.height == 24
