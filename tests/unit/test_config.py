from __future__ import annotations

import pytest

from worker.config import ConfigError, load_config, resolve_secrets


def test_load_config_and_resolve_stream_key_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RTMP_STREAM_KEY", "stream123")

    cfg = load_config(
        {
            "job": {"id": "job_1", "name": "demo"},
            "source": {"type": "reactor"},
            "sink": {
                "type": "rtmp",
                "url": "rtmp://localhost/live",
                "stream_key_ref": "env:RTMP_STREAM_KEY",
            },
            "video": {
                "fps": 24,
                "width": 1280,
                "height": 720,
                "pixel_format": "yuv420p",
                "bitrate_kbps": 1200,
                "keyframe_interval_sec": 2,
            },
        }
    )
    secrets = resolve_secrets(cfg)

    assert cfg.job.id == "job_1"
    assert secrets.sink_url_with_key == "rtmp://localhost/live/stream123"


def test_missing_stream_key_env_ref_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RTMP_STREAM_KEY", raising=False)

    cfg = load_config(
        {
            "job": {"id": "job_1", "name": "demo"},
            "source": {"type": "reactor"},
            "sink": {
                "type": "rtmp",
                "url": "rtmp://localhost/live",
                "stream_key_ref": "env:RTMP_STREAM_KEY",
            },
            "video": {
                "fps": 24,
                "width": 1280,
                "height": 720,
                "pixel_format": "yuv420p",
                "bitrate_kbps": 1200,
                "keyframe_interval_sec": 2,
            },
        }
    )

    with pytest.raises(ConfigError, match="Missing required environment variable"):
        resolve_secrets(cfg)


def test_source_extra_fields_are_rejected() -> None:
    with pytest.raises(ConfigError, match="Extra inputs are not permitted|extra_forbidden"):
        load_config(
            {
                "job": {"id": "job_1", "name": "demo"},
                "source": {"type": "reactor", "model_name": "livecore"},
                "sink": {"type": "rtmp", "url": "rtmp://localhost/live"},
                "video": {
                    "fps": 24,
                    "width": 1280,
                    "height": 720,
                    "pixel_format": "yuv420p",
                    "bitrate_kbps": 1200,
                    "keyframe_interval_sec": 2,
                },
            }
        )


def test_invalid_sink_url_fails_validation() -> None:
    with pytest.raises(ConfigError, match="sink.url"):
        load_config(
            {
                "job": {"id": "job_1", "name": "demo"},
                "source": {"type": "reactor"},
                "sink": {"type": "rtmp", "url": "http://localhost/live"},
                "video": {
                    "fps": 24,
                    "width": 1280,
                    "height": 720,
                    "pixel_format": "yuv420p",
                    "bitrate_kbps": 1200,
                    "keyframe_interval_sec": 2,
                },
            }
        )


def test_invalid_config_input_type_raises() -> None:
    with pytest.raises(TypeError, match="WorkerConfig or WorkerConfigDict"):
        load_config("config.yaml")  # type: ignore[arg-type]
