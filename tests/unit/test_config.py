from __future__ import annotations

from pathlib import Path

import pytest

from worker.config import ConfigError, load_config, resolve_secrets


def _write_yaml(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_load_config_and_resolve_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REACTOR_API_KEY", "rk_test")
    monkeypatch.setenv("RTMP_STREAM_KEY", "stream123")

    config_path = _write_yaml(
        tmp_path / "cfg.yaml",
        """
job:
  id: job_1
  name: demo
source:
  type: reactor
  model_name: livecore
  api_key_ref: env:REACTOR_API_KEY
sink:
  type: rtmp
  url: rtmp://localhost/live
  stream_key_ref: env:RTMP_STREAM_KEY
video:
  fps: 24
  width: 1280
  height: 720
  pixel_format: yuv420p
  bitrate_kbps: 1200
  keyframe_interval_sec: 2
""",
    )

    cfg = load_config(config_path)
    secrets = resolve_secrets(cfg)

    assert cfg.job.id == "job_1"
    assert secrets.reactor_api_key == "rk_test"
    assert secrets.sink_url_with_key == "rtmp://localhost/live/stream123"


def test_missing_env_ref_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REACTOR_API_KEY", raising=False)
    config_path = _write_yaml(
        tmp_path / "cfg.yaml",
        """
job:
  id: job_1
  name: demo
source:
  type: reactor
  model_name: livecore
  api_key_ref: env:REACTOR_API_KEY
sink:
  type: rtmp
  url: rtmp://localhost/live
video:
  fps: 24
  width: 1280
  height: 720
  pixel_format: yuv420p
  bitrate_kbps: 1200
  keyframe_interval_sec: 2
""",
    )

    cfg = load_config(config_path)
    with pytest.raises(ConfigError, match="Missing required environment variable"):
        resolve_secrets(cfg)


def test_invalid_sink_url_fails_validation(tmp_path: Path) -> None:
    config_path = _write_yaml(
        tmp_path / "cfg.yaml",
        """
job:
  id: job_1
  name: demo
source:
  type: reactor
  model_name: livecore
  api_key_ref: env:REACTOR_API_KEY
sink:
  type: rtmp
  url: http://localhost/live
video:
  fps: 24
  width: 1280
  height: 720
  pixel_format: yuv420p
  bitrate_kbps: 1200
  keyframe_interval_sec: 2
""",
    )

    with pytest.raises(ConfigError, match="sink.url"):
        load_config(config_path)
