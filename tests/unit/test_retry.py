from __future__ import annotations

import asyncio

import pytest

from worker.retry import compute_backoff, sleep_with_cancel


def test_compute_backoff_without_jitter() -> None:
    assert compute_backoff(1, base_backoff_sec=2, max_backoff_sec=32, jitter_ratio=0) == 2
    assert compute_backoff(2, base_backoff_sec=2, max_backoff_sec=32, jitter_ratio=0) == 4
    assert compute_backoff(5, base_backoff_sec=2, max_backoff_sec=32, jitter_ratio=0) == 32


def test_compute_backoff_with_jitter() -> None:
    value = compute_backoff(
        3,
        base_backoff_sec=2,
        max_backoff_sec=32,
        jitter_ratio=0.2,
        rng=lambda: 0.0,
    )
    assert value == pytest.approx(6.4)


@pytest.mark.asyncio
async def test_sleep_with_cancel_stops_early() -> None:
    stop = asyncio.Event()

    async def trigger() -> None:
        await asyncio.sleep(0.01)
        stop.set()

    task = asyncio.create_task(trigger())
    completed = await sleep_with_cancel(1.0, stop)
    await task
    assert completed is False
