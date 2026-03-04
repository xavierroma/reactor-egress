"""Reactor source adapter implementation."""

from __future__ import annotations

import asyncio
from collections import deque
import inspect
import logging
import time
from typing import Any

from worker.config import VideoConfig
from worker.errors import RetryableError, TerminalError, WorkerError, classify_source_exception
from worker.models import SourceInfo, VideoFrame


class ReactorSourceAdapter:
    def __init__(
        self,
        *,
        video: VideoConfig,
        logger: logging.Logger,
        client: Any,
    ) -> None:
        if client is None:
            raise ValueError("client is required for ReactorSourceAdapter")

        self._video = video
        self._logger = logger
        self._injected_client = client

        self._client: Any | None = None
        self._track: Any | None = None
        self._prefetched_frames: deque[VideoFrame] = deque()

    async def open(self) -> SourceInfo:
        try:
            self._client = self._injected_client
            self._track = await self._wait_for_track()
            source_info = self._derive_source_info(self._track)
            source_info = await self._probe_source_info_from_frame(source_info)
            return source_info
        except WorkerError:
            raise
        except Exception as exc:
            raise classify_source_exception(exc) from exc

    async def recv(self) -> VideoFrame:
        if self._track is None:
            raise TerminalError("source_track_unavailable", "Reactor track is not available")
        if self._prefetched_frames:
            return self._prefetched_frames.popleft()
        try:
            raw_frame = await _maybe_await(self._track.recv())
            return self._to_video_frame(raw_frame)
        except WorkerError:
            raise
        except Exception as exc:
            raise classify_source_exception(exc) from exc

    async def close(self) -> None:
        # The Reactor client lifecycle is owned by the caller.
        self._track = None
        self._prefetched_frames.clear()
        self._client = None

    async def _wait_for_track(self) -> Any:
        if self._client is None:
            raise TerminalError("source_client_missing", "Reactor client missing")

        get_tracks = getattr(self._client, "get_remote_tracks", None)
        if not callable(get_tracks):
            raise TerminalError(
                "source_track_api_missing",
                "Reactor client missing get_remote_tracks()",
            )

        tracks = await _maybe_await(get_tracks())
        track = _select_remote_track(tracks)
        if track is None:
            raise RetryableError(
                "source_track_unavailable",
                "No remote video track available from Reactor client",
            )
        return track

    def _derive_source_info(self, track: Any) -> SourceInfo:
        width = int(getattr(track, "width", self._video.width))
        height = int(getattr(track, "height", self._video.height))
        fps = int(getattr(track, "fps", self._video.fps))
        pixel_format = str(getattr(track, "pixel_format", self._video.pixel_format))
        return SourceInfo(width=width, height=height, fps=fps, pixel_format=pixel_format)

    async def _probe_source_info_from_frame(self, base: SourceInfo) -> SourceInfo:
        if self._track is None:
            return base

        deadline = time.monotonic() + 3.0
        observed: VideoFrame | None = None

        for _ in range(5):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            try:
                raw_frame = await asyncio.wait_for(_maybe_await(self._track.recv()), timeout=remaining)
            except asyncio.TimeoutError:
                break

            frame = self._to_video_frame(raw_frame)
            self._prefetched_frames.append(frame)
            observed = frame

        if observed is None:
            self._logger.warning("Timed out probing initial Reactor frames; using default source info")
            return base

        return SourceInfo(
            width=observed.width,
            height=observed.height,
            fps=base.fps,
            pixel_format=observed.pixel_format,
        )

    def _to_video_frame(self, raw_frame: Any) -> VideoFrame:
        if isinstance(raw_frame, VideoFrame):
            return raw_frame

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


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _coerce_frame_data(raw_frame: Any, *, target_format: str) -> tuple[bytes, str | None]:
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

    raise TerminalError("source_frame_decode_error", "Unable to extract raw bytes from Reactor frame")


def _coerce_pixel_format(value: Any) -> str:
    if isinstance(value, str):
        return value
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    return str(value)


def _select_remote_track(tracks: Any) -> Any | None:
    if tracks is None:
        return None

    candidates: list[Any]
    if isinstance(tracks, dict):
        candidates = list(tracks.values())
    elif isinstance(tracks, (list, tuple, set)):
        candidates = list(tracks)
    else:
        candidates = [tracks]

    for track in candidates:
        if _is_video_track(track):
            return track
    return None


def _is_video_track(track: Any) -> bool:
    if track is None:
        return False
    recv = getattr(track, "recv", None)
    if not callable(recv):
        return False
    kind = getattr(track, "kind", None)
    if isinstance(kind, str):
        return kind.lower() == "video"
    return True
