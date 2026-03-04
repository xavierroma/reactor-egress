# Reactor Egress Worker

RTMP egress worker for Reactor video streams.

## SDK-Only Reactor Source

This is an SDK package (not a standalone worker process). Reactor source requires an already initialized Reactor client. The worker only reads tracks via `get_remote_tracks()`.

## Typed Python Dict Config

Config is passed as a typed Python dict (or a validated `WorkerConfig`), not YAML/JSON files.

```python
from reactor_sdk import Reactor
from worker import EgressWorker

reactor = Reactor(model_name="livecore", api_key=api_key)
# connect/start Reactor in your app code before running egress

config = {
    "job": {"id": "job_1", "name": "demo"},
    "source": {"type": "reactor"},
    "sink": {"type": "rtmp", "url": "rtmp://localhost/live", "stream_key_ref": "env:RTMP_STREAM_KEY"},
    "video": {
        "fps": 24,
        "width": 1280,
        "height": 720,
        "pixel_format": "yuv420p",
        "bitrate_kbps": 1200,
        "keyframe_interval_sec": 2,
    },
}

worker = EgressWorker.from_config(config, reactor_client=reactor)
exit_code = await worker.run()
```

You can also use helpers:

```python
from worker import run_egress

exit_code = await run_egress(config, reactor_client=reactor)
```

There is no CLI entrypoint for runtime execution.
