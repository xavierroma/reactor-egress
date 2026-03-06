"""Programmatic example: publish Reactor output to YouTube Live with reactor_egress."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from reactor_sdk import Reactor, ReactorStatus

from reactor_egress import AudioOptions, RtmpTarget, VideoOptions, to_rtmp

YOUTUBE_RTMP_BASE_URL = "rtmps://a.rtmp.youtube.com/live2"
PROMPT = "cinematic flyover over San Francisco at dusk"
MODEL_NAME = "livecore"


async def _create_started_reactor(*, model_name: str, api_key: str, prompt: str) -> Reactor:
    reactor = Reactor(model_name=model_name, api_key=api_key)
    ready = asyncio.Event()

    @reactor.on_status(ReactorStatus.READY)
    def _on_ready(_status: ReactorStatus) -> None:
        ready.set()

    await reactor.connect()
    ready_timeout_sec = _get_ready_timeout_sec()
    try:
        await asyncio.wait_for(ready.wait(), timeout=ready_timeout_sec)
    except TimeoutError:
        status = reactor.get_status().value
        raise RuntimeError(
            f"Timed out waiting for Reactor READY status after {ready_timeout_sec:.0f}s (status={status})"
        ) from None

    await reactor.send_command(
        "schedule_prompt",
        {
            "new_prompt": prompt,
            "timestamp": 0,
        },
    )
    await reactor.send_command("start", {})
    return reactor


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value

    value = _read_dotenv_value(name)
    if value:
        return value
    raise RuntimeError(f"{name} must be set")


def _read_dotenv_value(name: str, *, dotenv_path: str = ".env") -> str | None:
    path = Path(dotenv_path)
    if not path.exists():
        return None

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() != name:
            continue
        resolved = value.strip().strip("'").strip('"')
        return resolved or None
    return None


def _get_track_timeout_sec() -> float:
    raw = os.getenv("REACTOR_TRACK_TIMEOUT_SEC") or _read_dotenv_value("REACTOR_TRACK_TIMEOUT_SEC")
    if raw is None:
        return 180.0
    try:
        value = float(raw)
    except ValueError:
        raise RuntimeError("REACTOR_TRACK_TIMEOUT_SEC must be numeric") from None
    if value <= 0:
        raise RuntimeError("REACTOR_TRACK_TIMEOUT_SEC must be > 0")
    return value


def _get_ready_timeout_sec() -> float:
    raw = os.getenv("REACTOR_READY_TIMEOUT_SEC") or _read_dotenv_value("REACTOR_READY_TIMEOUT_SEC")
    if raw is None:
        return 60.0
    try:
        value = float(raw)
    except ValueError:
        raise RuntimeError("REACTOR_READY_TIMEOUT_SEC must be numeric") from None
    if value <= 0:
        raise RuntimeError("REACTOR_READY_TIMEOUT_SEC must be > 0")
    return value


async def start() -> None:
    stream_key = _get_required_env("YOUTUBE_STREAM_KEY")
    api_key = _get_required_env("REACTOR_API_KEY")

    client = await _create_started_reactor(
        model_name=MODEL_NAME,
        api_key=api_key,
        prompt=PROMPT,
    )

    try:
        await to_rtmp(
            reactor_client=client,
            target=RtmpTarget(url=YOUTUBE_RTMP_BASE_URL, stream_key=stream_key),
            video=VideoOptions(
                fps=30,
                width=832,
                height=480,
                pixel_format="yuv420p",
                bitrate_kbps=1000,
                keyframe_interval_sec=2,
            ),
            audio=AudioOptions(inject_silence=True, sample_rate=48000, channels=2),
            track_wait_timeout_sec=_get_track_timeout_sec(),
        )
    finally:
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(start())
    except KeyboardInterrupt:
        pass
