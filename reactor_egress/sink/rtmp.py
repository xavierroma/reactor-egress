"""RTMP sink backed by ffmpeg."""

from __future__ import annotations

import asyncio
import logging
import shutil
from asyncio.subprocess import DEVNULL

from reactor_egress.errors import ConfigError, SinkError
from reactor_egress.types import AudioOptions, RtmpTarget, SourceInfo, VideoFrame, VideoOptions


class RtmpSink:
    def __init__(
        self,
        target: RtmpTarget,
        video: VideoOptions,
        audio: AudioOptions,
        logger: logging.Logger | None = None,
    ) -> None:
        self._target = target
        self._video = video
        self._audio = audio
        self._logger = logger or logging.getLogger("reactor_egress.sink.rtmp")

        self._proc: asyncio.subprocess.Process | None = None
        self._ffmpeg_path: str | None = None
        self._source_info: SourceInfo | None = None

    async def open(self, source: SourceInfo) -> None:
        ffmpeg = shutil.which("ffmpeg")
        if not ffmpeg:
            raise ConfigError("ffmpeg not found in PATH")

        self._ffmpeg_path = ffmpeg
        self._source_info = source
        await self._start_proc(source)

    async def write(self, frame: VideoFrame) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise SinkError("RTMP sink is not open")

        if self._proc.returncode is not None:
            stderr = await self._read_stderr_tail()
            raise SinkError(stderr or "ffmpeg exited unexpectedly")

        self._validate_frame(frame)

        try:
            self._proc.stdin.write(frame.data)
            await self._proc.stdin.drain()
        except (BrokenPipeError, ConnectionResetError) as exc:
            raise SinkError(f"ffmpeg pipe failed: {exc}") from exc
        except Exception as exc:  # pragma: no cover - defensive mapping
            raise SinkError(f"failed to write RTMP frame: {exc}") from exc

    async def close(self) -> None:
        if self._proc is None:
            return

        proc = self._proc
        self._proc = None
        await self._terminate_proc(proc)

    @staticmethod
    def build_ffmpeg_cmd(
        *,
        ffmpeg_path: str,
        output_url: str,
        video: VideoOptions,
        audio: AudioOptions,
        source: SourceInfo | None = None,
    ) -> list[str]:
        input_width = source.width if source else video.width
        input_height = source.height if source else video.height
        input_fps = source.fps if source else video.fps
        input_pixel_format = source.pixel_format if source else video.pixel_format

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
            input_pixel_format,
            "-s",
            f"{input_width}x{input_height}",
            "-r",
            str(input_fps),
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

        filters: list[str] = []
        if source is not None:
            if source.width != video.width or source.height != video.height:
                filters.append(f"scale={video.width}:{video.height}")
            if source.fps != video.fps:
                filters.append(f"fps={video.fps}")

        if filters:
            cmd.extend(["-vf", ",".join(filters)])

        cmd.extend(
            [
                "-r",
                str(video.fps),
                "-vsync",
                "cfr",
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
        except asyncio.TimeoutError:
            return ""

    async def _start_proc(self, source: SourceInfo) -> None:
        if not self._ffmpeg_path:
            raise ConfigError("ffmpeg path not initialized")

        cmd = self.build_ffmpeg_cmd(
            ffmpeg_path=self._ffmpeg_path,
            output_url=self._target.resolved_url(),
            video=self._video,
            audio=self._audio,
            source=source,
        )

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
                raise SinkError(stderr or "ffmpeg exited before streaming started")
        except (ConfigError, SinkError):
            raise
        except Exception as exc:
            raise SinkError(f"failed to start ffmpeg: {exc}") from exc

    async def _terminate_proc(self, proc: asyncio.subprocess.Process) -> None:
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
            except asyncio.TimeoutError:
                self._logger.warning("ffmpeg did not exit after terminate; sending kill")
                proc.kill()
                await proc.wait()

    def _validate_frame(self, frame: VideoFrame) -> None:
        expected_width = self._source_info.width if self._source_info else self._video.width
        expected_height = self._source_info.height if self._source_info else self._video.height
        expected_pixel_format = self._source_info.pixel_format if self._source_info else self._video.pixel_format

        if frame.width != expected_width or frame.height != expected_height:
            raise SinkError(f"expected frame size {expected_width}x{expected_height}, got {frame.width}x{frame.height}")

        if frame.pixel_format != expected_pixel_format:
            raise SinkError(f"expected pixel format {expected_pixel_format}, got {frame.pixel_format}")

        expected = _expected_frame_size(frame.width, frame.height, frame.pixel_format)
        if len(frame.data) != expected:
            raise SinkError(f"expected frame payload {expected} bytes, got {len(frame.data)} bytes")


def _expected_frame_size(width: int, height: int, pixel_format: str) -> int:
    if pixel_format == "rgb24":
        return width * height * 3
    if pixel_format == "yuv420p":
        return (width * height * 3) // 2
    raise ConfigError(f"unsupported pixel format: {pixel_format}")
