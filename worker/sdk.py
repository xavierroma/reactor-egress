"""Programmatic SDK interface for embedding Reactor egress in Python apps."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, TypeAlias

from pydantic import ValidationError

from worker.adapters import SinkAdapter, SourceAdapter
from worker.config import ConfigError, WorkerConfig, load_config
from worker.logging_utils import configure_logging
from worker.models import WorkerState
from worker.runner import WorkerRunner

ConfigInput: TypeAlias = WorkerConfig | str | Path | Mapping[str, Any]


def normalize_config(config: ConfigInput) -> WorkerConfig:
    """Load and validate config from an object, mapping, or config file path."""
    if isinstance(config, WorkerConfig):
        return config

    if isinstance(config, (str, Path)):
        return load_config(config)

    if isinstance(config, Mapping):
        try:
            return WorkerConfig.model_validate(dict(config))
        except ValidationError as exc:
            raise ConfigError(f"Config validation failed: {exc}") from exc

    raise TypeError("config must be a WorkerConfig, path-like value, or mapping")


class EgressWorker:
    """Embedding-friendly interface for running egress inside application code."""

    def __init__(
        self,
        config: WorkerConfig,
        *,
        source_factory: Callable[[], SourceAdapter] | None = None,
        sink_factory: Callable[[], SinkAdapter] | None = None,
        reactor_client: Any | None = None,
        logger: logging.Logger | None = None,
        configure_root_logger: bool = True,
    ) -> None:
        self._config = config
        if configure_root_logger:
            configure_logging(config.runtime.log_level)
        self._logger = logger or logging.getLogger("reactor_egress.worker")
        self._runner = WorkerRunner(
            config=config,
            source_factory=source_factory,
            sink_factory=sink_factory,
            reactor_client=reactor_client,
            logger=self._logger,
        )
        self._installed_signal_handlers: set[signal.Signals] = set()

    @classmethod
    def from_config(
        cls,
        config: ConfigInput,
        *,
        source_factory: Callable[[], SourceAdapter] | None = None,
        sink_factory: Callable[[], SinkAdapter] | None = None,
        reactor_client: Any | None = None,
        logger: logging.Logger | None = None,
        configure_root_logger: bool = True,
    ) -> EgressWorker:
        return cls(
            normalize_config(config),
            source_factory=source_factory,
            sink_factory=sink_factory,
            reactor_client=reactor_client,
            logger=logger,
            configure_root_logger=configure_root_logger,
        )

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        source_factory: Callable[[], SourceAdapter] | None = None,
        sink_factory: Callable[[], SinkAdapter] | None = None,
        reactor_client: Any | None = None,
        logger: logging.Logger | None = None,
        configure_root_logger: bool = True,
    ) -> EgressWorker:
        return cls(
            load_config(path),
            source_factory=source_factory,
            sink_factory=sink_factory,
            reactor_client=reactor_client,
            logger=logger,
            configure_root_logger=configure_root_logger,
        )

    @property
    def config(self) -> WorkerConfig:
        return self._config

    @property
    def state(self) -> WorkerState:
        return self._runner.state

    def request_stop(self, *, interrupted: bool = False) -> None:
        self._runner.request_stop(interrupted=interrupted)

    async def run(self, *, install_signal_handlers: bool = False) -> int:
        loop = asyncio.get_running_loop()
        if install_signal_handlers:
            self._install_signal_handlers(loop)

        try:
            return await self._runner.run()
        finally:
            if install_signal_handlers:
                self._remove_signal_handlers(loop)

    def run_sync(self, *, install_signal_handlers: bool = True) -> int:
        try:
            return asyncio.run(self.run(install_signal_handlers=install_signal_handlers))
        except KeyboardInterrupt:
            return 130

    def _install_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        if self._installed_signal_handlers:
            return

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._on_signal, sig)
            except NotImplementedError:  # pragma: no cover - Windows signal limitation
                continue
            self._installed_signal_handlers.add(sig)

    def _remove_signal_handlers(self, loop: asyncio.AbstractEventLoop) -> None:
        for sig in self._installed_signal_handlers:
            loop.remove_signal_handler(sig)
        self._installed_signal_handlers.clear()

    def _on_signal(self, sig: signal.Signals) -> None:
        self._logger.warning(
            "signal_received",
            extra={
                "fields": {
                    "job_id": self._config.job.id,
                    "sink_type": self._config.sink.type,
                    "state": self.state.value,
                    "attempt": 0,
                    "signal": sig.name,
                }
            },
        )
        self.request_stop(interrupted=True)


async def run_egress(
    config: ConfigInput,
    *,
    source_factory: Callable[[], SourceAdapter] | None = None,
    sink_factory: Callable[[], SinkAdapter] | None = None,
    reactor_client: Any | None = None,
    logger: logging.Logger | None = None,
    configure_root_logger: bool = True,
    install_signal_handlers: bool = False,
) -> int:
    worker = EgressWorker.from_config(
        config,
        source_factory=source_factory,
        sink_factory=sink_factory,
        reactor_client=reactor_client,
        logger=logger,
        configure_root_logger=configure_root_logger,
    )
    return await worker.run(install_signal_handlers=install_signal_handlers)


def run_egress_sync(
    config: ConfigInput,
    *,
    source_factory: Callable[[], SourceAdapter] | None = None,
    sink_factory: Callable[[], SinkAdapter] | None = None,
    reactor_client: Any | None = None,
    logger: logging.Logger | None = None,
    configure_root_logger: bool = True,
    install_signal_handlers: bool = True,
) -> int:
    worker = EgressWorker.from_config(
        config,
        source_factory=source_factory,
        sink_factory=sink_factory,
        reactor_client=reactor_client,
        logger=logger,
        configure_root_logger=configure_root_logger,
    )
    return worker.run_sync(install_signal_handlers=install_signal_handlers)
