"""Retry and backoff utilities."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable


def compute_backoff(
    attempt: int,
    *,
    base_backoff_sec: float,
    max_backoff_sec: float,
    jitter_ratio: float,
    rng: Callable[[], float] | None = None,
) -> float:
    if attempt < 1:
        raise ValueError("attempt must be >= 1")

    base = min(max_backoff_sec, base_backoff_sec * (2 ** (attempt - 1)))
    if jitter_ratio <= 0:
        return base

    random_value = (rng or random.random)()
    jitter_scale = 1 - jitter_ratio + (2 * jitter_ratio * random_value)
    return max(0.0, base * jitter_scale)


async def sleep_with_cancel(delay_sec: float, stop_event: asyncio.Event) -> bool:
    """Sleep for delay unless stop_event fires first.

    Returns True if sleep completed, False if cancelled by stop_event.
    """
    if delay_sec <= 0:
        return True

    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_sec)
        return False
    except TimeoutError:
        return True
