from __future__ import annotations

import asyncio
import logging

import pytest

from worker.config import AudioConfig, SinkConfig, VideoConfig
from worker.errors import RetryableError, TerminalError
from worker.models import SourceInfo, VideoFrame
from worker.sink.rtmp import RtmpSinkAdapter


class _BrokenPipeWriter:
    def write(self, _data: bytes) -> None:
        raise BrokenPipeError("broken pipe")

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


class _FakeProc:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.stdin = _BrokenPipeWriter()
        self.stderr = None

    def terminate(self) -> None:
        return None

    def kill(self) -> None:
        return None

    async def wait(self) -> int:
        return 0


def _video_cfg() -> VideoConfig:
    return VideoConfig(
        fps=24,
        width=16,
        height=16,
        pixel_format="yuv420p",
        bitrate_kbps=300,
        keyframe_interval_sec=2,
    )


def _audio_cfg() -> AudioConfig:
    return AudioConfig(inject_silence=True, sample_rate=48000, channels=2)


def test_build_ffmpeg_cmd_contains_expected_flags() -> None:
    cmd = RtmpSinkAdapter.build_ffmpeg_cmd(
        ffmpeg_path="ffmpeg",
        output_url="rtmp://localhost/live/abc",
        video=_video_cfg(),
        audio=_audio_cfg(),
    )

    assert "-c:v" in cmd
    assert "libx264" in cmd
    assert "-f" in cmd
    assert "flv" in cmd
    assert cmd[-1] == "rtmp://localhost/live/abc"


@pytest.mark.asyncio
async def test_open_fails_when_ffmpeg_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("worker.sink.rtmp.shutil.which", lambda _name: None)

    sink = RtmpSinkAdapter(
        config=SinkConfig(url="rtmp://localhost/live"),
        video=_video_cfg(),
        audio=_audio_cfg(),
        output_url="rtmp://localhost/live/abc",
        logger=logging.getLogger(__name__),
    )

    with pytest.raises(TerminalError, match="sink_ffmpeg_missing"):
        await sink.open(SourceInfo(width=16, height=16, fps=24, pixel_format="yuv420p"))


@pytest.mark.asyncio
async def test_write_broken_pipe_is_retryable() -> None:
    sink = RtmpSinkAdapter(
        config=SinkConfig(url="rtmp://localhost/live"),
        video=_video_cfg(),
        audio=_audio_cfg(),
        output_url="rtmp://localhost/live/abc",
        logger=logging.getLogger(__name__),
    )
    sink._proc = _FakeProc()  # noqa: SLF001 - white-box adapter test

    frame = VideoFrame(data=b"a" * 384, width=16, height=16, pixel_format="yuv420p")
    with pytest.raises(RetryableError, match="sink_broken_pipe"):
        await sink.write(frame)


@pytest.mark.asyncio
async def test_close_kills_when_wait_times_out() -> None:
    class _NeverExitProc(_FakeProc):
        def __init__(self) -> None:
            super().__init__()
            self._killed = False

        async def wait(self) -> int:  # type: ignore[override]
            if self._killed:
                self.returncode = -9
                return -9
            await asyncio.sleep(60)
            return 0

    class _Writer:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            return None

        def write(self, _data: bytes) -> None:
            return None

        async def drain(self) -> None:
            return None

    proc = _NeverExitProc()
    proc.stdin = _Writer()
    called = {"kill": False}

    def _kill() -> None:
        proc._killed = True  # noqa: SLF001 - test-only flag
        called["kill"] = True

    proc.kill = _kill  # type: ignore[assignment]

    sink = RtmpSinkAdapter(
        config=SinkConfig(url="rtmp://localhost/live"),
        video=_video_cfg(),
        audio=_audio_cfg(),
        output_url="rtmp://localhost/live/abc",
        logger=logging.getLogger(__name__),
    )
    sink._proc = proc  # noqa: SLF001

    await sink.close()
    assert called["kill"] is True
