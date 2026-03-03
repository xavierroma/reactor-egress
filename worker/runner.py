"""Worker lifecycle orchestration."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable

from worker.adapters import SinkAdapter, SourceAdapter
from worker.config import WorkerConfig, resolve_secrets
from worker.errors import RetryableError, TerminalError, WorkerError, classify_sink_exception, classify_source_exception
from worker.logging_utils import log_event, redact_url
from worker.metrics import WorkerMetrics
from worker.models import WorkerState
from worker.pipeline import BoundedFrameQueue
from worker.retry import compute_backoff, sleep_with_cancel
from worker.sink.rtmp import RtmpSinkAdapter
from worker.source.reactor import ReactorSourceAdapter


class WorkerRunner:
    def __init__(
        self,
        config: WorkerConfig,
        *,
        source_factory: Callable[[], SourceAdapter] | None = None,
        sink_factory: Callable[[], SinkAdapter] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._secrets = resolve_secrets(config)

        self._logger = logger or logging.getLogger("reactor_egress.worker")
        self._state = WorkerState.INIT
        self._attempt = 0
        self._stop_event = asyncio.Event()
        self._interrupted = False

        self._metrics = WorkerMetrics()
        self._source_factory = source_factory or self._default_source_factory
        self._sink_factory = sink_factory or self._default_sink_factory

    @property
    def state(self) -> WorkerState:
        return self._state

    def request_stop(self, *, interrupted: bool = False) -> None:
        if interrupted:
            self._interrupted = True
        self._stop_event.set()

    async def run(self) -> int:
        self._log(logging.INFO, "worker_starting", output_url=redact_url(self._secrets.sink_url_with_key))
        reporter_task = asyncio.create_task(self._run_metrics_reporter())
        failed = False

        try:
            max_attempts = self._config.retry.max_attempts
            for attempt in range(1, max_attempts + 1):
                if self._stop_event.is_set():
                    break

                self._attempt = attempt
                try:
                    await self._run_once()
                    return 130 if self._interrupted else 0
                except TerminalError as exc:
                    failed = True
                    self._set_state(WorkerState.FAILED, error_code=exc.code, message=exc.message, level=logging.ERROR)
                    return 1
                except RetryableError as exc:
                    if attempt >= max_attempts:
                        failed = True
                        self._set_state(
                            WorkerState.FAILED,
                            error_code=exc.code,
                            message=f"Retry exhaustion: {exc.message}",
                            level=logging.ERROR,
                        )
                        return 1

                    self._metrics.inc_reconnect()
                    self._set_state(WorkerState.DEGRADED, error_code=exc.code, message=exc.message, level=logging.WARNING)
                    delay = compute_backoff(
                        attempt,
                        base_backoff_sec=self._config.retry.base_backoff_sec,
                        max_backoff_sec=self._config.retry.max_backoff_sec,
                        jitter_ratio=self._config.retry.jitter_ratio,
                    )
                    self._log(logging.INFO, "retry_scheduled", retry_delay_sec=round(delay, 3))
                    completed = await sleep_with_cancel(delay, self._stop_event)
                    if not completed:
                        break
                except WorkerError as exc:
                    if exc.retryable:
                        raise RetryableError(exc.code, exc.message) from exc
                    raise TerminalError(exc.code, exc.message) from exc
                except Exception as exc:
                    wrapped = classify_source_exception(exc)
                    if wrapped.retryable and attempt < max_attempts:
                        self._metrics.inc_reconnect()
                        self._set_state(
                            WorkerState.DEGRADED,
                            error_code=wrapped.code,
                            message=wrapped.message,
                            level=logging.WARNING,
                        )
                        delay = compute_backoff(
                            attempt,
                            base_backoff_sec=self._config.retry.base_backoff_sec,
                            max_backoff_sec=self._config.retry.max_backoff_sec,
                            jitter_ratio=self._config.retry.jitter_ratio,
                        )
                        self._log(logging.INFO, "retry_scheduled", retry_delay_sec=round(delay, 3))
                        completed = await sleep_with_cancel(delay, self._stop_event)
                        if not completed:
                            break
                        continue

                    failed = True
                    self._set_state(WorkerState.FAILED, error_code=wrapped.code, message=wrapped.message, level=logging.ERROR)
                    return 1

            return 130 if self._interrupted else 0
        finally:
            if not failed:
                self._set_state(WorkerState.STOPPING)
            self._stop_event.set()
            reporter_task.cancel()
            await asyncio.gather(reporter_task, return_exceptions=True)
            if not failed:
                self._set_state(WorkerState.STOPPED)
            self._log(logging.INFO, "worker_stopped")

    async def _run_once(self) -> None:
        source = self._source_factory()
        sink = self._sink_factory()
        queue = BoundedFrameQueue(maxsize=self._config.runtime.frame_queue_size, metrics=self._metrics)

        producer: asyncio.Task[None] | None = None
        consumer: asyncio.Task[None] | None = None
        stop_waiter: asyncio.Task[None] | None = None

        try:
            self._set_state(WorkerState.CONNECTING_SOURCE)
            source_info = await source.open()

            self._set_state(WorkerState.OPENING_SINK)
            await sink.open(source_info)

            self._set_state(WorkerState.RUNNING)

            producer = asyncio.create_task(self._producer_loop(source, queue))
            consumer = asyncio.create_task(self._consumer_loop(sink, queue))
            stop_waiter = asyncio.create_task(self._stop_event.wait())

            done, pending = await asyncio.wait(
                {producer, consumer, stop_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            if stop_waiter in done and self._stop_event.is_set():
                return

            for task in done:
                if task is stop_waiter:
                    continue
                exc = task.exception()
                if exc:
                    if isinstance(exc, WorkerError):
                        raise exc
                    raise classify_source_exception(exc) from exc
                raise RetryableError("pipeline_unexpected_exit", "Pipeline task exited unexpectedly")
        finally:
            await self._safe_close(sink=sink, source=source)
            for task in (producer, consumer, stop_waiter):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(*(t for t in (producer, consumer, stop_waiter) if t), return_exceptions=True)

    async def _producer_loop(self, source: SourceAdapter, queue: BoundedFrameQueue) -> None:
        while not self._stop_event.is_set():
            try:
                frame = await source.recv()
            except WorkerError:
                raise
            except Exception as exc:
                raise classify_source_exception(exc) from exc

            self._metrics.inc_frames_in()
            await queue.push(frame)

    async def _consumer_loop(self, sink: SinkAdapter, queue: BoundedFrameQueue) -> None:
        while not self._stop_event.is_set():
            frame = await queue.pop(timeout_sec=0.5)
            if frame is None:
                continue

            try:
                await sink.write(frame)
            except WorkerError:
                raise
            except Exception as exc:
                raise classify_sink_exception(exc) from exc

            self._metrics.inc_frames_out(frame_bytes=len(frame.data))

    async def _safe_close(self, *, sink: SinkAdapter, source: SourceAdapter) -> None:
        try:
            await sink.close()
        except Exception as exc:
            self._log(logging.WARNING, "sink_close_failed", error_code="sink_close_error", detail=str(exc))

        try:
            await source.close()
        except Exception as exc:
            self._log(logging.WARNING, "source_close_failed", error_code="source_close_error", detail=str(exc))

    async def _run_metrics_reporter(self) -> None:
        await self._metrics.report_periodically(
            self._logger,
            interval_sec=10.0,
            stop_event=self._stop_event,
            context_provider=self._metrics_context,
        )

    def _metrics_context(self) -> dict[str, object]:
        return {
            "job_id": self._config.job.id,
            "sink_type": self._config.sink.type,
            "state": self._state.value,
            "attempt": self._attempt,
        }

    def _default_source_factory(self) -> SourceAdapter:
        return ReactorSourceAdapter(
            config=self._config.source,
            video=self._config.video,
            api_key=self._secrets.reactor_api_key,
            logger=self._logger,
        )

    def _default_sink_factory(self) -> SinkAdapter:
        return RtmpSinkAdapter(
            config=self._config.sink,
            video=self._config.video,
            audio=self._config.audio,
            output_url=self._secrets.sink_url_with_key,
            logger=self._logger,
        )

    def _set_state(
        self,
        state: WorkerState,
        *,
        error_code: str | None = None,
        message: str = "state_changed",
        level: int = logging.INFO,
    ) -> None:
        self._state = state
        self._log(level, message, error_code=error_code)

    def _log(self, level: int, message: str, *, error_code: str | None = None, **extra_fields: object) -> None:
        log_event(
            self._logger,
            level,
            message,
            job_id=self._config.job.id,
            sink_type=self._config.sink.type,
            state=self._state.value,
            attempt=self._attempt,
            error_code=error_code,
            **extra_fields,
        )
