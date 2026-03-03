"""Reactor source adapter implementation."""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Any, Callable

from worker.config import SourceConfig, VideoConfig
from worker.errors import RetryableError, TerminalError, WorkerError, classify_source_exception
from worker.models import SourceInfo, VideoFrame


class ReactorSourceAdapter:
    def __init__(
        self,
        *,
        config: SourceConfig,
        video: VideoConfig,
        api_key: str,
        logger: logging.Logger,
        client_factory: Callable[[str, str], Any] | None = None,
    ) -> None:
        self._config = config
        self._video = video
        self._api_key = api_key
        self._logger = logger
        self._client_factory = client_factory

        self._client: Any | None = None
        self._track: Any | None = None

    async def open(self) -> SourceInfo:
        try:
            self._client = self._create_client()
            await _maybe_await(self._client.connect())
            await self._wait_until_ready()
            await self._bootstrap_if_configured()
            self._track = await self._wait_for_track()
            return self._derive_source_info(self._track)
        except WorkerError:
            raise
        except Exception as exc:
            raise classify_source_exception(exc) from exc

    async def recv(self) -> VideoFrame:
        if self._track is None:
            raise TerminalError("source_track_unavailable", "Reactor track is not available")
        try:
            raw_frame = await _maybe_await(self._track.recv())
            return self._to_video_frame(raw_frame)
        except WorkerError:
            raise
        except Exception as exc:
            raise classify_source_exception(exc) from exc

    async def close(self) -> None:
        if self._client is None:
            return

        disconnect = getattr(self._client, "disconnect", None)
        close = getattr(self._client, "close", None)

        try:
            if callable(disconnect):
                await _maybe_await(disconnect())
            elif callable(close):
                await _maybe_await(close())
        finally:
            self._track = None
            self._client = None

    def _create_client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory(self._config.model_name, self._api_key)

        try:
            from reactor_sdk import Reactor  # type: ignore
        except ImportError as exc:
            try:
                from reactor import Reactor  # type: ignore
            except ImportError as legacy_exc:
                raise TerminalError(
                    "source_sdk_missing",
                    "Reactor SDK import failed. Install with `uv add reactor-sdk`.",
                ) from legacy_exc

        return Reactor(self._config.model_name, self._api_key)

    async def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + self._config.ready_timeout_sec
        while time.monotonic() < deadline:
            status = await self._read_status()
            if isinstance(status, str) and status.upper() == "READY":
                return
            await asyncio.sleep(0.2)

        raise RetryableError(
            "source_ready_timeout",
            f"Reactor status did not reach READY within {self._config.ready_timeout_sec}s",
        )

    async def _read_status(self) -> str | None:
        if self._client is None:
            return None

        status_attr = getattr(self._client, "status", None)
        if callable(status_attr):
            value = await _maybe_await(status_attr())
            return _normalize_status(value)
        if status_attr is not None:
            return _normalize_status(status_attr)

        getter = getattr(self._client, "get_status", None)
        if callable(getter):
            value = await _maybe_await(getter())
            return _normalize_status(value)

        self._logger.debug("Reactor client has no status API; assuming READY")
        return "READY"

    async def _bootstrap_if_configured(self) -> None:
        if self._client is None:
            return

        bootstrap = self._config.bootstrap
        if bootstrap.start_prompt:
            await self._schedule_prompt(bootstrap.start_prompt)

        if bootstrap.auto_start:
            await self._start_session()

    async def _schedule_prompt(self, prompt: str) -> None:
        if self._client is None:
            raise TerminalError("source_client_missing", "Reactor client missing")

        schedule = getattr(self._client, "schedule_prompt", None)
        if callable(schedule):
            await _maybe_await(schedule(timestamp=0, new_prompt=prompt))
            return

        send_command = getattr(self._client, "send_command", None)
        if callable(send_command):
            await _call_send_command(
                send_command=send_command,
                command="schedule_prompt",
                data={"timestamp": 0, "new_prompt": prompt},
            )
            return

        raise TerminalError("source_command_unsupported", "Reactor client does not support schedule_prompt")

    async def _start_session(self) -> None:
        if self._client is None:
            raise TerminalError("source_client_missing", "Reactor client missing")

        start = getattr(self._client, "start", None)
        if callable(start):
            await _maybe_await(start())
            return

        send_command = getattr(self._client, "send_command", None)
        if callable(send_command):
            await _call_send_command(send_command=send_command, command="start", data={})
            return

        raise TerminalError("source_command_unsupported", "Reactor client does not support start command")

    async def _wait_for_track(self) -> Any:
        if self._client is None:
            raise TerminalError("source_client_missing", "Reactor client missing")

        get_track = getattr(self._client, "get_remote_track", None)
        if not callable(get_track):
            raise TerminalError("source_track_api_missing", "Reactor client missing get_remote_track()")

        deadline = time.monotonic() + self._config.track_timeout_sec
        while time.monotonic() < deadline:
            track = await _maybe_await(get_track())
            if track is not None:
                return track
            await asyncio.sleep(0.2)

        raise RetryableError(
            "source_track_timeout",
            f"No remote track received within {self._config.track_timeout_sec}s",
        )

    def _derive_source_info(self, track: Any) -> SourceInfo:
        width = int(getattr(track, "width", self._video.width))
        height = int(getattr(track, "height", self._video.height))
        fps = int(getattr(track, "fps", self._video.fps))
        pixel_format = str(getattr(track, "pixel_format", self._video.pixel_format))
        return SourceInfo(width=width, height=height, fps=fps, pixel_format=pixel_format)

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


def _normalize_status(value: Any) -> str:
    if value is None:
        return ""
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    raw_value = getattr(value, "value", None)
    if isinstance(raw_value, str):
        return raw_value
    text = str(value)
    if "." in text:
        return text.rsplit(".", maxsplit=1)[-1]
    return text


async def _call_send_command(*, send_command: Any, command: str, data: dict[str, Any]) -> None:
    try:
        await _maybe_await(send_command(command, data))
    except TypeError:
        await _maybe_await(send_command({"command": command, **data}))
