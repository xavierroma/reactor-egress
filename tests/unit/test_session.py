from __future__ import annotations

import asyncio

import pytest

from reactor_egress import EgressError, EgressSession, RtmpTarget, SinkError, SourceInfo, VideoFrame


class FakeSource:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.closed = False

    async def open(self) -> SourceInfo:
        return SourceInfo(width=16, height=16, fps=24, pixel_format="yuv420p")

    async def recv(self) -> VideoFrame:
        await asyncio.sleep(0)
        return VideoFrame(data=b"a" * 384, width=16, height=16, pixel_format="yuv420p")

    async def close(self) -> None:
        self.closed = True
        self.order.append("source")


class FakeSink:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.closed = False
        self.writes = 0

    async def open(self, _source: SourceInfo) -> None:
        return None

    async def write(self, _frame: VideoFrame) -> None:
        self.writes += 1
        await asyncio.sleep(0)

    async def close(self) -> None:
        self.closed = True
        self.order.append("sink")


class FailingSink(FakeSink):
    async def open(self, _source: SourceInfo) -> None:  # type: ignore[override]
        raise SinkError("sink boom")


class FakeReactorClient:
    def get_remote_tracks(self) -> dict[str, object]:
        return {}


@pytest.mark.asyncio
async def test_step_requires_open() -> None:
    order: list[str] = []
    session = EgressSession(source=FakeSource(order), sink=FakeSink(order))

    with pytest.raises(EgressError, match="opened"):
        await session.step()


@pytest.mark.asyncio
async def test_open_step_close_happy_path() -> None:
    order: list[str] = []
    source = FakeSource(order)
    sink = FakeSink(order)
    session = EgressSession(source=source, sink=sink)

    source_info = await session.open()
    await session.step()
    await session.close()

    assert source_info.width == 16
    assert sink.writes == 1
    assert sink.closed is True
    assert source.closed is True


@pytest.mark.asyncio
async def test_open_closes_source_when_sink_open_fails() -> None:
    order: list[str] = []
    source = FakeSource(order)
    sink = FailingSink(order)
    session = EgressSession(source=source, sink=sink)

    with pytest.raises(SinkError, match="sink boom"):
        await session.open()

    assert source.closed is True


@pytest.mark.asyncio
async def test_close_guarantees_sink_then_source_order() -> None:
    order: list[str] = []
    session = EgressSession(source=FakeSource(order), sink=FakeSink(order))

    await session.open()
    await session.close()

    assert order == ["sink", "source"]


@pytest.mark.asyncio
async def test_run_until_cancelled_closes_and_reraises_cancelled_error() -> None:
    order: list[str] = []
    source = FakeSource(order)
    sink = FakeSink(order)
    session = EgressSession(source=source, sink=sink)

    task = asyncio.create_task(session.run_until_cancelled())
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert sink.closed is True
    assert source.closed is True


def test_reactor_to_rtmp_wires_track_wait_timeout() -> None:
    session = EgressSession.reactor_to_rtmp(
        reactor_client=FakeReactorClient(),  # type: ignore[arg-type]
        target=RtmpTarget(url="rtmp://localhost/live"),
        track_wait_timeout_sec=12.5,
    )

    assert getattr(session._source, "_track_wait_timeout_sec") == 12.5


def test_reactor_to_rtmp_rejects_negative_track_wait_timeout() -> None:
    with pytest.raises(EgressError, match="track_wait_timeout_sec must be >= 0"):
        EgressSession.reactor_to_rtmp(
            reactor_client=FakeReactorClient(),  # type: ignore[arg-type]
            target=RtmpTarget(url="rtmp://localhost/live"),
            track_wait_timeout_sec=-1,
        )
