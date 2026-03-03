"""RTMP sink adapter backed by ffmpeg."""

from __future__ import annotations

import asyncio
import logging
import shutil
from asyncio.subprocess import DEVNULL

from worker.config import AudioConfig, SinkConfig, VideoConfig
from worker.errors import RetryableError, TerminalError, WorkerError, classify_sink_exception
from worker.models import SourceInfo, VideoFrame


class RtmpSinkAdapter:
    def __init__(
        self,
        *,
        config: SinkConfig,
        video: VideoConfig,
        audio: AudioConfig,
        output_url: str,
        logger: logging.Logger,
    ) -> None:
        self._config = config
        self._video = video
        self._audio = audio
        self._output_url = output_url
        self._logger = logger

        self._proc: asyncio.subprocess.Process | None = None
        self._source_info: SourceInfo | None = None

    async def open(self, source: SourceInfo) -> None:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise TerminalError("sink_ffmpeg_missing", "ffmpeg not found in PATH")

        if not self._output_url.startswith(("rtmp://", "rtmps://")):
            raise TerminalError("sink_invalid_rtmp_url", "invalid RTMP URL")

        self._source_info = source
        cmd = self.build_ffmpeg_cmd(ffmpeg_path=ffmpeg, output_url=self._output_url, video=self._video, audio=self._audio)

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=DEVNULL,
                stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.sleep(0.2)
            if self._proc.returncode is not None:
                stderr = await self._read_stderr_tail()
                raise _classify_ffmpeg_failure(stderr)
        except WorkerError:
            raise
        except Exception as exc:
            raise classify_sink_exception(exc) from exc

    async def write(self, frame: VideoFrame) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise TerminalError("sink_not_open", "RTMP sink is not open")

        if self._proc.returncode is not None:
            stderr = await self._read_stderr_tail()
            raise _classify_ffmpeg_failure(stderr)

        self._validate_frame(frame)

        try:
            self._proc.stdin.write(frame.data)
            await self._proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise RetryableError("sink_broken_pipe", str(exc)) from exc
        except Exception as exc:
            raise classify_sink_exception(exc) from exc

    async def close(self) -> None:
        if self._proc is None:
            return

        proc = self._proc
        self._proc = None

        if proc.stdin is not None:
            try:
                proc.stdin.close()
                await proc.stdin.wait_closed()
            except Exception:
                pass

        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except TimeoutError:
                proc.kill()
                await proc.wait()

    @staticmethod
    def build_ffmpeg_cmd(
        *,
        ffmpeg_path: str,
        output_url: str,
        video: VideoConfig,
        audio: AudioConfig,
    ) -> list[str]:
        gop = video.fps * video.keyframe_interval_sec
        cmd: list[str] = [
            ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostats",
            "-f",
            "rawvideo",
            "-pix_fmt",
            video.pixel_format,
            "-s",
            f"{video.width}x{video.height}",
            "-r",
            str(video.fps),
            "-i",
            "pipe:0",
        ]

        if audio.inject_silence:
            channel_layout = "mono" if audio.channels == 1 else "stereo"
            cmd.extend(
                [
                    "-f",
                    "lavfi",
                    "-i",
                    f"anullsrc=channel_layout={channel_layout}:sample_rate={audio.sample_rate}",
                    "-shortest",
                ]
            )

        cmd.extend(
            [
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-tune",
                "zerolatency",
                "-pix_fmt",
                "yuv420p",
                "-g",
                str(gop),
                "-keyint_min",
                str(gop),
                "-b:v",
                f"{video.bitrate_kbps}k",
                "-maxrate",
                f"{video.bitrate_kbps}k",
                "-bufsize",
                f"{video.bitrate_kbps * 2}k",
            ]
        )

        if audio.inject_silence:
            cmd.extend(
                [
                    "-c:a",
                    "aac",
                    "-ar",
                    str(audio.sample_rate),
                    "-ac",
                    str(audio.channels),
                    "-b:a",
                    "128k",
                ]
            )
        else:
            cmd.append("-an")

        cmd.extend(["-f", "flv", output_url])
        return cmd

    async def _read_stderr_tail(self) -> str:
        if self._proc is None or self._proc.stderr is None:
            return ""
        try:
            content = await asyncio.wait_for(self._proc.stderr.read(), timeout=0.2)
            text = content.decode("utf-8", errors="replace")
            return text.strip().splitlines()[-1] if text.strip() else ""
        except TimeoutError:
            return ""

    def _validate_frame(self, frame: VideoFrame) -> None:
        if frame.width != self._video.width or frame.height != self._video.height:
            raise TerminalError(
                "sink_frame_size_mismatch",
                f"Expected {self._video.width}x{self._video.height}, got {frame.width}x{frame.height}",
            )

        if frame.pixel_format != self._video.pixel_format:
            raise TerminalError(
                "sink_frame_format_mismatch",
                f"Expected {self._video.pixel_format}, got {frame.pixel_format}",
            )

        expected = _expected_frame_size(frame.width, frame.height, frame.pixel_format)
        if len(frame.data) != expected:
            raise TerminalError(
                "sink_frame_payload_mismatch",
                f"Expected frame payload {expected} bytes, got {len(frame.data)} bytes",
            )


def _expected_frame_size(width: int, height: int, pixel_format: str) -> int:
    if pixel_format == "rgb24":
        return width * height * 3
    if pixel_format == "yuv420p":
        return (width * height * 3) // 2
    raise TerminalError("sink_pixel_format_unsupported", f"Unsupported pixel format: {pixel_format}")


def _classify_ffmpeg_failure(stderr: str) -> WorkerError:
    text = stderr.lower()
    if "connection refused" in text or "broken pipe" in text or "timed out" in text:
        return RetryableError("sink_connect_error", stderr or "ffmpeg sink connection failed")
    if "protocol not found" in text or "invalid argument" in text:
        return TerminalError("sink_invalid_config", stderr or "ffmpeg rejected sink args")
    if "unknown encoder" in text:
        return TerminalError("sink_encoder_unavailable", stderr or "ffmpeg missing required codec")
    return RetryableError("sink_ffmpeg_exit", stderr or "ffmpeg exited unexpectedly")
