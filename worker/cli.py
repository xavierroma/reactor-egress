"""CLI entrypoint for the local worker."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from worker.config import ConfigError, load_config
from worker.logging_utils import configure_logging
from worker.runner import WorkerRunner


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="reactor-egress-worker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a single worker job from config")
    run_parser.add_argument("--config", required=True, help="Path to JSON/YAML config")
    return parser


async def _run_command(config_path: str) -> int:
    try:
        config = load_config(config_path)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    configure_logging(config.runtime.log_level)
    logger = logging.getLogger("reactor_egress.worker")
    runner = WorkerRunner(config=config, logger=logger)

    loop = asyncio.get_running_loop()

    def _on_signal(sig: signal.Signals) -> None:
        logger.warning(
            "signal_received",
            extra={
                "fields": {
                    "job_id": config.job.id,
                    "sink_type": config.sink.type,
                    "state": runner.state.value,
                    "attempt": 0,
                    "signal": sig.name,
                }
            },
        )
        runner.request_stop(interrupted=True)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _on_signal, sig)
        except NotImplementedError:  # pragma: no cover - Windows signal limitation
            pass

    return await runner.run()


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        try:
            return asyncio.run(_run_command(args.config))
        except KeyboardInterrupt:
            return 130

    parser.print_help()
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
