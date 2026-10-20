"""HTTP and WebSocket surface (see PROTOCOL.md) and the `wslive serve` command."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hmac
import os
import re
import sys
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Response, WebSocket
from fastapi.staticfiles import StaticFiles

from wslive.feed import run_feed
from wslive.hub import Hub
from wslive.models import CHANNEL_PATTERN, PublishRequest

# Exactly one WebSocket implementation is pinned, because the `naive` strategy only blocks behind
# a stalled client if `send` waits for the socket to drain. websockets-sansio does (it waits on
# pause_writing/resume_writing before every send). `auto` would pick it today, but a silent
# change of implementation would change what the benchmark measures.
WS_IMPLEMENTATION = "websockets-sansio"
LOOPBACK_HOSTS = ("127.0.0.1", "localhost", "::1")


@dataclass(frozen=True)
class FeedOptions:
    """Settings for the optional in-process feed."""

    channel: str = "orders"
    rate: float = 20.0
    seed: int = 7
    pad_bytes: int = 0
    count: int | None = None


def default_web_dir() -> Path:
    override = os.environ.get("WSLIVE_WEB_DIR")
    return Path(override) if override else Path(__file__).resolve().parents[2] / "web"


def _valid_channel(name: str) -> bool:
    return re.fullmatch(CHANNEL_PATTERN, name) is not None


def _check_token(authorization: str | None) -> None:
    token = os.environ.get("PUBLISH_TOKEN")
    if not token:  # unset or empty: open, for local development
        return
    scheme, _, supplied = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not hmac.compare_digest(supplied.encode(), token.encode()):
        raise HTTPException(401, "missing or wrong publish token", {"WWW-Authenticate": "Bearer"})


def create_app(
    hub: Hub | None = None,
    web_dir: Path | None = None,
    feed: FeedOptions | None = None,
) -> FastAPI:
    hub = hub or Hub()

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        task = None
        if feed is not None:
            task = asyncio.create_task(
                run_feed(hub, feed.channel, feed.rate, feed.seed, feed.pad_bytes, feed.count),
                name="feed",
            )
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                await asyncio.wait({task})

    app = FastAPI(title="websocket-live", lifespan=lifespan)
    app.state.hub = hub

    @app.websocket("/ws/{channel}")
    async def subscribe(
        ws: WebSocket,
        channel: str,
        since: int | None = Query(None, ge=0),
        client: str | None = Query(None, max_length=64),
    ) -> None:
        await ws.accept()
        if not _valid_channel(channel):
            await ws.close(code=1008, reason="invalid channel")
            return
        await hub.serve_socket(ws, channel, since, client)
        with contextlib.suppress(Exception):  # the peer may already be gone
            await ws.close()

    @app.post("/publish/{channel}", status_code=202)
    async def publish(
        channel: str, body: PublishRequest, authorization: str | None = Header(None)
    ) -> dict[str, int]:
        _check_token(authorization)
        if not _valid_channel(channel):
            raise HTTPException(404, "no such channel")
        try:
            envelope = await hub.publish(channel, body.type, body.data)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        return {"seq": envelope.seq}

    @app.get("/poll/{channel}")
    async def poll(channel: str, since: int | None = Query(None, ge=0)) -> Response:
        if not _valid_channel(channel):
            raise HTTPException(404, "no such channel")
        return Response(hub.poll_json(channel, since), media_type="application/json")

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/stats")
    async def stats() -> dict[str, Any]:
        return hub.stats()

    web_dir = web_dir or default_web_dir()
    if web_dir.is_dir():  # last, so it never shadows an API route
        app.mount("/", StaticFiles(directory=web_dir, html=True), name="web")
    return app


def add_serve_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--strategy", choices=("naive", "queued"), default="queued")
    p.add_argument("--queue-size", type=int, default=64, help="per-client queue (queued only)")
    p.add_argument("--ring-size", type=int, default=1000, help="events kept for resume and poll")
    p.add_argument("--heartbeat", type=float, default=10.0, help="seconds between heartbeats")
    p.add_argument("--feed-rate", type=float, default=0.0, help="built-in feed, events/s (0 = off)")
    p.add_argument("--feed-channel", default="orders")
    p.add_argument("--feed-seed", type=int, default=7)
    p.add_argument("--pad-bytes", type=int, default=0, help="filler added to each feed event")
    p.add_argument(
        "--ws-ping-interval",
        type=float,
        default=0.0,
        help="protocol-level ping seconds (0 = off; a stalled client must stay connected)",
    )
    p.add_argument("--log-level", default="warning", choices=("debug", "info", "warning", "error"))
    p.set_defaults(func=serve)


def server_config(
    app: FastAPI,
    host: str,
    port: int,
    log_level: str = "warning",
    ws_ping_interval: float | None = None,
    **overrides: Any,
) -> uvicorn.Config:
    """The uvicorn settings the benchmark depends on, in one place (tests reuse them)."""
    return uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level=log_level,
        ws=WS_IMPLEMENTATION,
        ws_ping_interval=ws_ping_interval,
        ws_ping_timeout=ws_ping_interval,
        **overrides,
    )


def serve(args: argparse.Namespace) -> int:
    if args.feed_rate > 0 and not _valid_channel(args.feed_channel):
        print(f"invalid --feed-channel: {args.feed_channel!r}", file=sys.stderr)
        return 2
    if args.host not in LOOPBACK_HOSTS and not os.environ.get("PUBLISH_TOKEN"):
        print("warning: listening beyond loopback with PUBLISH_TOKEN unset", file=sys.stderr)

    hub = Hub(args.strategy, args.queue_size, args.ring_size, args.heartbeat)
    feed = None
    if args.feed_rate > 0:
        feed = FeedOptions(args.feed_channel, args.feed_rate, args.feed_seed, args.pad_bytes)
    ping = args.ws_ping_interval if args.ws_ping_interval > 0 else None
    app = create_app(hub, feed=feed)
    config = server_config(app, args.host, args.port, args.log_level, ws_ping_interval=ping)
    uvicorn.Server(config).run()
    return 0
