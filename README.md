# Reactor Egress

Deploy-anywhere Python library for Reactor source egress.

This package is source-only for `reactor-sdk` and currently provides an RTMP sink.
You decide where and how to run it (server, worker system, local process, container, etc.).

## Install

```bash
pip install reactor-egress
```

To use this package from another local project:

1. Create/activate a Python 3.11+ virtual environment in your app project.
2. Install this repository as an editable dependency:

```bash
pip install -e /path/to/reactor-egress
```

If you use `uv`:

```bash
uv add --editable /path/to/reactor-egress
```

`RtmpSink` requires `ffmpeg` to be available in `PATH`.

For a pinned local build install:

```bash
cd /path/to/reactor-egress
uv build
pip install dist/reactor_egress-0.1.0-py3-none-any.whl
```

## Usage

```python
import asyncio
from reactor_sdk import Reactor

from reactor_egress import AudioOptions, RtmpTarget, VideoOptions, to_rtmp


async def main() -> None:
    reactor = Reactor(model_name="livecore", api_key="...")

    # Reactor client lifecycle is owned by your app.
    await reactor.connect()
    await reactor.send_command("start", {})

    await to_rtmp(
        reactor_client=reactor,
        target=RtmpTarget(
            url="rtmps://a.rtmp.youtube.com/live2",
            stream_key="your-stream-key",
        ),
        video=VideoOptions(
            fps=30,
            width=832,
            height=480,
            pixel_format="yuv420p",
            bitrate_kbps=1000,
            keyframe_interval_sec=2,
        ),
        audio=AudioOptions(inject_silence=True, sample_rate=48000, channels=2),
        # Wait up to 180s for a remote video track before failing.
        track_wait_timeout_sec=180.0,
    )


if __name__ == "__main__":
    asyncio.run(main())
```

## Runtime model

- No built-in retry loop.
- Waits for a remote video track at startup (default 30s, configurable).
- No signal handling.
- No state machine or worker exit codes.
- One source to one sink per session.
- `run_until_cancelled()` closes resources and re-raises cancellation.

## Release

Build distribution artifacts:

```bash
uv build
```

Publish to PyPI (when credentials are configured):

```bash
uv run --with build --with twine python -m twine upload dist/*
```
