"""Programmatic example: use EgressWorker to publish Reactor output to YouTube Live.

This module intentionally avoids argparse/CLI parsing. Import `run_youtube_example`
from your own app code and execute it inside an asyncio event loop. Pass an
already-created Reactor client instance to keep client lifecycle in the host app.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from reactor_sdk import Reactor, ReactorStatus

from worker import EgressWorker

# Required env vars:
# - REACTOR_API_KEY
# - YOUTUBE_STREAM_KEY

YOUTUBE_RTMP_BASE_URL = "rtmps://a.rtmp.youtube.com/live2"
PROMPT = "cinematic flyover over San Francisco at dusk"
MODEL_NAME = "livecore"
JOB_ID = "job_youtube_example"
JOB_NAME = "youtube-demo"


async def _create_started_reactor(*, model_name: str, api_key: str, prompt: str) -> Reactor:
    reactor = Reactor(model_name=model_name, api_key=api_key)
    ready = asyncio.Event()

    @reactor.on_status(ReactorStatus.READY)
    def _on_ready(_status: ReactorStatus) -> None:
        ready.set()

    await reactor.connect()
    await ready.wait()

    await reactor.send_command(
        "schedule_prompt",
        {
            "new_prompt": prompt,
            "timestamp": 0,
        },
    )
    await reactor.send_command("start", {})
    return reactor


async def run_youtube_example(*, reactor_client: Any | None = None) -> int:
    if not os.getenv("YOUTUBE_STREAM_KEY"):
        raise RuntimeError("YOUTUBE_STREAM_KEY must be set")

    if reactor_client is None:
        api_key = os.getenv("REACTOR_API_KEY")
        if not api_key:
            raise RuntimeError("REACTOR_API_KEY must be set")
        reactor_client = await _create_started_reactor(
            model_name=MODEL_NAME,
            api_key=api_key,
            prompt=PROMPT,
        )

    config = {
        "job": {
            "id": JOB_ID,
            "name": JOB_NAME,
        },
        "source": {
            "type": "reactor",
        },
        "sink": {
            "type": "rtmp",
            "url": YOUTUBE_RTMP_BASE_URL,
            "stream_key_ref": "env:YOUTUBE_STREAM_KEY",
        },
        "video": {
            "fps": 30,
            "width": 832,
            "height": 480,
            "pixel_format": "yuv420p",
            "bitrate_kbps": 1000,
            "keyframe_interval_sec": 2,
        },
        "audio": {
            "inject_silence": True,
            "sample_rate": 48000,
            "channels": 2,
        },
        "retry": {
            "max_attempts": 5,
            "base_backoff_sec": 2,
            "max_backoff_sec": 32,
            "jitter_ratio": 0.2,
        },
        "runtime": {
            "frame_queue_size": 48,
            "log_level": "info",
        },
    }

    worker = EgressWorker.from_config(config, reactor_client=reactor_client)
    return await worker.run(install_signal_handlers=True)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_youtube_example()))
