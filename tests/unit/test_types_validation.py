from __future__ import annotations

import pytest

from reactor_egress import AudioOptions, ConfigError, RtmpTarget, VideoOptions


def test_video_options_validate_ranges() -> None:
    with pytest.raises(ConfigError, match="video.fps"):
        VideoOptions(fps=0)

    with pytest.raises(ConfigError, match="video.width"):
        VideoOptions(width=8)



def test_audio_options_validate_ranges() -> None:
    with pytest.raises(ConfigError, match="audio.sample_rate"):
        AudioOptions(sample_rate=1000)

    with pytest.raises(ConfigError, match="audio.channels"):
        AudioOptions(channels=0)



def test_rtmp_target_requires_rtmp_url() -> None:
    with pytest.raises(ConfigError, match="rtmp://"):
        RtmpTarget(url="https://example.com/live")



def test_rtmp_target_rejects_empty_stream_key() -> None:
    with pytest.raises(ConfigError, match="stream_key"):
        RtmpTarget(url="rtmp://localhost/live", stream_key="")



def test_rtmp_target_resolves_output_url() -> None:
    assert RtmpTarget(url="rtmp://localhost/live").resolved_url() == "rtmp://localhost/live"
    assert (
        RtmpTarget(url="rtmp://localhost/live", stream_key="key123").resolved_url()
        == "rtmp://localhost/live/key123"
    )
