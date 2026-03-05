"""Composable egress session orchestration."""

from __future__ import annotations

import asyncio
import logging

from reactor_sdk import Reactor

from reactor_egress.errors import EgressError, SinkError, SourceError
from reactor_egress.sink.rtmp import RtmpSink
from reactor_egress.source.reactor import ReactorSource
from reactor_egress.types import AudioOptions, RtmpTarget, SourceInfo, VideoOptions


class EgressSession:
    def __init__(self, source: ReactorSource, sink: RtmpSink, logger: logging.Logger | None = None) -> None:
        self._source = source
        self._sink = sink
        self._logger = logger or logging.getLogger("reactor_egress.session")

        self._opened = False
        self._source_info: SourceInfo | None = None

    @classmethod
    def reactor_to_rtmp(
        cls,
        *,
        reactor_client: Reactor,
        target: RtmpTarget,
        video: VideoOptions = VideoOptions(),
        audio: AudioOptions = AudioOptions(),
        logger: logging.Logger | None = None,
    ) -> EgressSession:
        source = ReactorSource(reactor_client=reactor_client, video=video, logger=logger)
        sink = RtmpSink(target=target, video=video, audio=audio, logger=logger)
        return cls(source=source, sink=sink, logger=logger)

    async def open(self) -> SourceInfo:
        if self._opened and self._source_info is not None:
            return self._source_info

        source_info = await self._source.open()
        try:
            await self._sink.open(source_info)
        except Exception:
            try:
                await self._source.close()
            finally:
                self._opened = False
                self._source_info = None
            raise

        self._opened = True
        self._source_info = source_info
        return source_info

    async def step(self) -> None:
        if not self._opened:
            raise EgressError("session must be opened before calling step()")

        frame = await self._source.recv()
        await self._sink.write(frame)

    async def run_until_cancelled(self) -> None:
        if not self._opened:
            await self.open()

        try:
            while True:
                await self.step()
        except asyncio.CancelledError:
            try:
                await self.close()
            except Exception as exc:  # pragma: no cover - best effort cleanup
                self._logger.warning("failed to close session during cancellation: %s", exc)
            raise

    async def close(self) -> None:
        sink_error: SinkError | None = None
        source_error: SourceError | None = None

        try:
            await self._sink.close()
        except Exception as exc:
            sink_error = SinkError(f"failed to close sink: {exc}")

        try:
            await self._source.close()
        except Exception as exc:
            source_error = SourceError(f"failed to close source: {exc}")

        self._opened = False
        self._source_info = None

        if sink_error is not None and source_error is not None:
            raise EgressError(f"{sink_error}; {source_error}")
        if sink_error is not None:
            raise sink_error
        if source_error is not None:
            raise source_error


async def stream_reactor_to_rtmp(
    *,
    reactor_client: Reactor,
    target: RtmpTarget,
    video: VideoOptions = VideoOptions(),
    audio: AudioOptions = AudioOptions(),
    logger: logging.Logger | None = None,
) -> None:
    session = EgressSession.reactor_to_rtmp(
        reactor_client=reactor_client,
        target=target,
        video=video,
        audio=audio,
        logger=logger,
    )
    await session.run_until_cancelled()
