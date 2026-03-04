"""CLI entrypoint for the local worker."""

from __future__ import annotations

import argparse
import asyncio
import sys

from worker.config import ConfigError
from worker.sdk import EgressWorker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="reactor-egress-worker")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run a single worker job from config")
    run_parser.add_argument("--config", required=True, help="Path to JSON/YAML config")
    return parser


async def _run_command(config_path: str) -> int:
    try:
        worker = EgressWorker.from_file(config_path)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    return await worker.run(install_signal_handlers=True)


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
