"""Reactor source adapter implementation."""

from __future__ import annotations

import asyncio
from collections import deque
import logging
from numbers import Real
import time
from typing import cast

from aiortc import MediaStreamTrack
from reactor_sdk import Reactor

from reactor_egress.errors import ConfigError, SourceError
from reactor_egress.types import SourceInfo, VideoFrame, VideoOptions


class ReactorSource:
    def __init__(
        self,
        reactor_client: Reactor,
        video: VideoOptions,
        track_wait_timeout_sec: float = 0.0,
        logger: logging.Logger | None = None,
    ) -> None:
        if reactor_client is None:
            raise ConfigError("reactor_client is required")
        if isinstance(track_wait_timeout_sec, bool) or not isinstance(track_wait_timeout_sec, Real):
            raise ConfigError("track_wait_timeout_sec must be numeric")
        if track_wait_timeout_sec < 0:
            raise ConfigError("track_wait_timeout_sec must be >= 0")

        self._video = video
        self._logger = logger or logging.getLogger("reactor_egress.source.reactor")
        self._client = reactor_client
        self._track_wait_timeout_sec = float(track_wait_timeout_sec)

        self._track: MediaStreamTrack | None = None
        self._prefetched_frames: deque[VideoFrame] = deque()

    async def open(self) -> SourceInfo:
        try:
            self._track = await self._wait_for_track()
            source_info = self._derive_source_info(self._track)
            return await self._probe_source_info_from_frame(source_info)
        except (ConfigError, SourceError):
            raise
        except Exception as exc:  # pragma: no cover - defensive mapping
            raise SourceError(f"failed to open reactor source: {exc}") from exc

    async def recv(self) -> VideoFrame:
        if self._track is None:
            raise SourceError("reactor source is not open")
        if self._prefetched_frames:
            return self._prefetched_frames.popleft()

        try:
            raw_frame = await self._track.recv()
            return self._to_video_frame(raw_frame)
        except (ConfigError, SourceError):
            raise
        except Exception as exc:
            raise SourceError(f"failed to read reactor frame: {exc}") from exc

    async def close(self) -> None:
        self._track = None
        self._prefetched_frames.clear()

    async def _wait_for_track(self) -> MediaStreamTrack:
        get_tracks = getattr(self._client, "get_remote_tracks", None)
        if not callable(get_tracks):
            raise ConfigError("reactor_client must expose get_remote_tracks()")

        timeout_sec = self._track_wait_timeout_sec
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout_sec
        next_log_at = loop.time()

        while True:
            tracks = cast(dict[str, MediaStreamTrack], get_tracks())
            track = _select_remote_track(tracks)
            if track is not None:
                return track

            now = loop.time()
            if now >= next_log_at and timeout_sec > 0:
                remaining = max(0.0, deadline - now)
                status = _reactor_status_value(self._client)
                self._logger.info(
                    "waiting for remote video track (status=%s, %.0fs remaining)",
                    status,
                    remaining,
                )
                next_log_at = now + 5.0

            if now >= deadline:
                status = _reactor_status_value(self._client)
                if timeout_sec <= 0:
                    raise SourceError("no remote video track available from reactor_client")
                raise SourceError(
                    f"timed out waiting for remote video track after {timeout_sec:.0f}s (status={status})"
                )

            await asyncio.sleep(min(0.5, deadline - now))

    def _derive_source_info(self, track: MediaStreamTrack) -> SourceInfo:
        fps = int(getattr(track, "fps", self._video.fps))
        return SourceInfo(
            width=self._video.width,
            height=self._video.height,
            fps=fps,
            pixel_format=self._video.pixel_format,
        )

    async def _probe_source_info_from_frame(self, base: SourceInfo) -> SourceInfo:
        if self._track is None:
            return base

        deadline = time.monotonic() + 3.0
        observed: VideoFrame | None = None

        # Reactor can emit a short black warmup burst at 1280x720 before switching
        # to the real generation resolution (for example 832x480). Probe enough
        # initial frames to lock onto the steady-state source shape.
        for _ in range(30):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                raw_frame = await asyncio.wait_for(self._track.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break

            frame = self._to_video_frame(raw_frame)
            self._prefetched_frames.append(frame)
            observed = frame

        if observed is None:
            self._logger.warning("timed out probing initial Reactor frames; using metadata defaults")
            return base

        return base

    def _to_video_frame(self, raw_frame: object) -> VideoFrame:
        if isinstance(raw_frame, VideoFrame):
            return raw_frame

        # Normalize incoming frames to configured output geometry/pixel format so
        # downstream sink/ffmpeg dimensions remain stable across Reactor warmup
        # resolution changes.
        reformat = getattr(raw_frame, "reformat", None)
        to_ndarray = getattr(raw_frame, "to_ndarray", None)
        if callable(reformat) and callable(to_ndarray):
            try:
                normalized = reformat(
                    width=self._video.width,
                    height=self._video.height,
                    format=self._video.pixel_format,
                )
                array = normalized.to_ndarray(format=self._video.pixel_format)
                data = bytes(array.tobytes())
                pts = getattr(normalized, "pts", None)
                pts_ms = int(pts) if isinstance(pts, int) else None
                return VideoFrame(
                    data=data,
                    width=self._video.width,
                    height=self._video.height,
                    pixel_format=self._video.pixel_format,
                    pts_ms=pts_ms,
                )
            except Exception:
                pass

        data, pixel_format = _coerce_frame_data(raw_frame, target_format=self._video.pixel_format)
        width = int(getattr(raw_frame, "width", self._video.width))
        height = int(getattr(raw_frame, "height", self._video.height))
        if pixel_format is None:
            pixel_format = _coerce_pixel_format(
                getattr(raw_frame, "pixel_format", getattr(raw_frame, "format", self._video.pixel_format))
            )

        pts_ms = getattr(raw_frame, "pts_ms", None)
        if pts_ms is None:
            pts = getattr(raw_frame, "pts", None)
            pts_ms = int(pts) if isinstance(pts, int) else None

        return VideoFrame(data=data, width=width, height=height, pixel_format=pixel_format, pts_ms=pts_ms)


def _coerce_frame_data(raw_frame: object, *, target_format: str) -> tuple[bytes, str | None]:
    if isinstance(raw_frame, (bytes, bytearray, memoryview)):
        return bytes(raw_frame), None

    data = getattr(raw_frame, "data", None)
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data), None

    to_bytes = getattr(raw_frame, "to_bytes", None)
    if callable(to_bytes):
        out = to_bytes()
        if isinstance(out, (bytes, bytearray, memoryview)):
            return bytes(out), None

    to_ndarray = getattr(raw_frame, "to_ndarray", None)
    if callable(to_ndarray):
        try:
            array = to_ndarray(format=target_format)
        except TypeError:
            array = to_ndarray()
        tobytes = getattr(array, "tobytes", None)
        if callable(tobytes):
            return bytes(tobytes()), target_format

    raise SourceError("unable to extract raw bytes from Reactor frame")


def _coerce_pixel_format(value: object) -> str:
    if isinstance(value, str):
        return value
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    return str(value)


def _select_remote_track(tracks: dict[str, MediaStreamTrack]) -> MediaStreamTrack | None:
    for track in tracks.values():
        if _is_video_track(track):
            return track
    return None


def _is_video_track(track: MediaStreamTrack) -> bool:
    kind = getattr(track, "kind", None)
    if isinstance(kind, str):
        return kind.lower() == "video"
    return True


def _reactor_status_value(reactor_client: Reactor) -> str:
    get_status = getattr(reactor_client, "get_status", None)
    if not callable(get_status):
        return "unknown"
    try:
        status = get_status()
    except Exception:  # pragma: no cover - defensive
        return "unknown"

    value = getattr(status, "value", None)
    if isinstance(value, str):
        return value
    return str(status)
