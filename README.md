# Reactor Egress Worker (Phase 1)

Local single-job worker that pulls one Reactor remote video track and publishes to one RTMP sink through `ffmpeg`.

## Setup (uv)

```bash
uv sync --extra test
```

## Run

```bash
uv run reactor-egress-worker run --config ./config.example.yaml
```

## Programmatic Example (YouTube RTMP)

```bash
export REACTOR_API_KEY="rk_..."
export YOUTUBE_STREAM_KEY="xxxx-xxxx-xxxx-xxxx-xxxx"

uv run python -c "import asyncio; from examples.youtube_worker_example import run_youtube_example; raise SystemExit(asyncio.run(run_youtube_example()))"
```

This is a pure Python usage example (no argparse/CLI flags) showing direct use of `WorkerConfig` and `WorkerRunner` against YouTube ingest (`rtmps://a.rtmp.youtube.com/live2`).

Exit codes:
- `0`: clean stop/completion
- `1`: terminal runtime failure
- `2`: config validation failure
- `130`: interrupted (`SIGINT`/`SIGTERM`)

## Example Config

See `config.example.yaml`.

## Notes

- Requires `ffmpeg` in `PATH`.
- Reactor Python SDK is included as project dependency (`reactor-sdk`).
- This phase intentionally excludes Go control plane, IPC, persistence, and WHIP.
