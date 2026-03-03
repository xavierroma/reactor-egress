from __future__ import annotations

from worker.logging_utils import redact_url


def test_redact_url_masks_last_path_segment() -> None:
    redacted = redact_url("rtmp://example.com/live/stream-key-123")
    assert redacted == "rtmp://example.com/live/***"


def test_redact_url_non_rtmp_untouched() -> None:
    url = "https://example.com/x/y"
    assert redact_url(url) == url
