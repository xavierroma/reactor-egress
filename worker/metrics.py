"""Local metrics aggregation and emission."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(slots=True)
class MetricsSnapshot:
    egress_frames_in_total: int
    egress_frames_out_total: int
    egress_frame_drop_total: int
    egress_reconnect_total: int
    egress_sink_bitrate_kbps: float


class WorkerMetrics:
    def __init__(self) -> None:
        self.egress_frames_in_total = 0
        self.egress_frames_out_total = 0
        self.egress_frame_drop_total = 0
        self.egress_reconnect_total = 0
        self._bytes_out_total = 0
        self._last_bitrate_bytes = 0
        self._last_bitrate_time = time.monotonic()

    def inc_frames_in(self, count: int = 1) -> None:
        self.egress_frames_in_total += count

    def inc_frames_out(self, frame_bytes: int, count: int = 1) -> None:
        self.egress_frames_out_total += count
        self._bytes_out_total += frame_bytes

    def inc_frame_drop(self, count: int = 1) -> None:
        self.egress_frame_drop_total += count

    def inc_reconnect(self, count: int = 1) -> None:
        self.egress_reconnect_total += count

    def snapshot(self) -> MetricsSnapshot:
        now = time.monotonic()
        elapsed = max(1e-6, now - self._last_bitrate_time)
        delta_bytes = self._bytes_out_total - self._last_bitrate_bytes
        kbps = (delta_bytes * 8.0 / 1000.0) / elapsed

        self._last_bitrate_time = now
        self._last_bitrate_bytes = self._bytes_out_total

        return MetricsSnapshot(
            egress_frames_in_total=self.egress_frames_in_total,
            egress_frames_out_total=self.egress_frames_out_total,
            egress_frame_drop_total=self.egress_frame_drop_total,
            egress_reconnect_total=self.egress_reconnect_total,
            egress_sink_bitrate_kbps=round(kbps, 2),
        )

    async def report_periodically(
        self,
        logger: logging.Logger,
        *,
        interval_sec: float,
        stop_event: asyncio.Event,
        context_provider: Callable[[], dict[str, object]],
    ) -> None:
        while not stop_event.is_set():
            await asyncio.sleep(interval_sec)
            snapshot = self.snapshot()
            logger.info(
                "metrics",
                extra={
                    "fields": {
                        **context_provider(),
                        "egress_frames_in_total": snapshot.egress_frames_in_total,
                        "egress_frames_out_total": snapshot.egress_frames_out_total,
                        "egress_frame_drop_total": snapshot.egress_frame_drop_total,
                        "egress_reconnect_total": snapshot.egress_reconnect_total,
                        "egress_sink_bitrate_kbps": snapshot.egress_sink_bitrate_kbps,
                    }
                },
            )
