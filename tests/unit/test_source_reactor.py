from __future__ import annotations

from dataclasses import dataclass
import logging

import pytest

from reactor_egress import ConfigError, ReactorSource, SourceError, VideoFrame, VideoOptions


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

    def get_remote_tracks(self) -> object:
        self.calls.append("get_remote_tracks")
        return self._tracks


class FakeClientMissingTracksApi:
    pass


@pytest.mark.asyncio
async def test_uses_video_track_from_get_remote_tracks() -> None:
    client = FakeClient({"audio-main": FakeAudioTrack(), "video-main": FakeTrack()})
    source = ReactorSource(
        reactor_client=client,
        video=VideoOptions(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=logging.getLogger(__name__),
    )

    info = await source.open()
    frame = await source.recv()
    await source.close()

    assert info.width == 16
    assert isinstance(frame, VideoFrame)
    assert "get_remote_tracks" in client.calls
    assert "connect" not in client.calls
    assert "start" not in client.calls
    assert "disconnect" not in client.calls


@pytest.mark.asyncio
async def test_no_remote_video_track_raises_source_error() -> None:
    client = FakeClient({"audio-main": FakeAudioTrack()})
    source = ReactorSource(
        reactor_client=client,
        video=VideoOptions(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=logging.getLogger(__name__),
    )

    with pytest.raises(SourceError, match="no remote video track"):
        await source.open()


@pytest.mark.asyncio
async def test_missing_get_remote_tracks_raises_config_error() -> None:
    source = ReactorSource(
        reactor_client=FakeClientMissingTracksApi(),
        video=VideoOptions(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=logging.getLogger(__name__),
    )

    with pytest.raises(ConfigError, match="get_remote_tracks"):
        await source.open()


def test_client_is_required() -> None:
    with pytest.raises(ConfigError, match="reactor_client is required"):
        ReactorSource(
            reactor_client=None,
            video=VideoOptions(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
            logger=logging.getLogger(__name__),
        )


@pytest.mark.asyncio
async def test_probes_first_frame_when_track_has_no_metadata() -> None:
    client = FakeClient({"video-main": FakeTrackNoMetadata()})
    source = ReactorSource(
        reactor_client=client,
        video=VideoOptions(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=logging.getLogger(__name__),
    )

    info = await source.open()
    frame = await source.recv()
    await source.close()

    assert info.width == 32
    assert info.height == 24
    assert frame.width == 32
    assert frame.height == 24


@pytest.mark.asyncio
async def test_first_frame_overrides_mismatched_track_metadata() -> None:
    client = FakeClient({"video-main": FakeTrackMismatchedMetadata()})
    source = ReactorSource(
        reactor_client=client,
        video=VideoOptions(width=16, height=16, fps=24, pixel_format="yuv420p", bitrate_kbps=300, keyframe_interval_sec=2),
        logger=logging.getLogger(__name__),
    )

    info = await source.open()
    frame = await source.recv()
    await source.close()

    assert info.width == 32
    assert info.height == 24
    assert frame.width == 32
    assert frame.height == 24
