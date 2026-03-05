from __future__ import annotations

import importlib

import pytest

from reactor_egress import __all__ as public_symbols


EXPECTED_PUBLIC = {
    "AudioOptions",
    "ConfigError",
    "EgressError",
    "EgressSession",
    "ReactorSource",
    "RtmpSink",
    "RtmpTarget",
    "SinkError",
    "SourceError",
    "SourceInfo",
    "VideoFrame",
    "VideoOptions",
    "stream_reactor_to_rtmp",
}


def test_public_api_surface_is_intentional() -> None:
    assert set(public_symbols) == EXPECTED_PUBLIC


def test_worker_package_is_no_longer_importable() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("worker")
