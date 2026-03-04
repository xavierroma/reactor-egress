"""Programmatic example: use EgressWorker to publish Reactor output to YouTube Live.

This module intentionally avoids argparse/CLI parsing. Import `run_youtube_example`
from your own app code and execute it inside an asyncio event loop.
"""

from __future__ import annotations

import asyncio
import os

from worker import EgressWorker
from worker.config import WorkerConfig

# Required env vars:
# - REACTOR_API_KEY
# - YOUTUBE_STREAM_KEY

YOUTUBE_RTMP_BASE_URL = "rtmps://a.rtmp.youtube.com/live2"
PROMPT = "cinematic flyover over San Francisco at dusk"
MODEL_NAME = "livecore"
JOB_ID = "job_youtube_example"
JOB_NAME = "youtube-demo"


async def run_youtube_example() -> int:
    if not os.getenv("REACTOR_API_KEY"):
        raise RuntimeError("REACTOR_API_KEY must be set")
    if not os.getenv("YOUTUBE_STREAM_KEY"):
        raise RuntimeError("YOUTUBE_STREAM_KEY must be set")

    config = WorkerConfig.model_validate(
        {
            "job": {
                "id": JOB_ID,
                "name": JOB_NAME,
            },
            "source": {
                "type": "reactor",
                "model_name": MODEL_NAME,
                "api_key_ref": "env:REACTOR_API_KEY",
                "track_timeout_sec": 30,
                "ready_timeout_sec": 30,
                "bootstrap": {
                    "start_prompt": PROMPT,
                    "auto_start": True,
                },
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
    )

    worker = EgressWorker.from_config(config)
    return await worker.run(install_signal_handlers=True)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run_youtube_example()))
