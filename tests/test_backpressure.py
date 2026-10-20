"""Real sockets: a client that stops reading, against a real uvicorn server.

This is the effect the whole project measures, so it is checked end to end rather than assumed:
with `naive` the stalled client blocks `publish` and starves a healthy client; with `queued` the
healthy client is unaffected and the stalled one just loses its oldest events.
"""

import asyncio
import contextlib
import json
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import websockets.asyncio.client

from wslive.app import create_app, server_config
from wslive.hub import Hub
from wslive.rawws import open_stalled

PAD = "x" * 65536  # 64 KB per event: a handful fill the stalled client's socket buffers
EVENTS = 40
HOST = "127.0.0.1"


@contextlib.contextmanager
def running_server(hub: Hub, tmp_path: Path) -> Iterator[int]:
    import uvicorn

    app = create_app(hub, web_dir=tmp_path / "no-web")
    config = server_config(app, HOST, 0, timeout_graceful_shutdown=2)
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert thread.is_alive(), "server thread died during startup"
        assert time.monotonic() < deadline, "server did not start"
        time.sleep(0.01)
    try:
        yield server.servers[0].sockets[0].getsockname()[1]
    finally:
        server.should_exit = True
        thread.join(10)


async def wait_for(predicate, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.005)


def subscriber_count(hub: Hub) -> int:
    return len(hub.stats()["subscribers"])


def count_events(messages: list[dict]) -> int:
    return sum(m["type"] == "event" for m in messages)


async def collect(socket: websockets.asyncio.client.ClientConnection, into: list[dict]) -> None:
    with contextlib.suppress(Exception):
        async for raw in socket:
            into.append(json.loads(raw))


async def run_scenario(strategy: str, tmp_path: Path) -> dict:
    """Stalled client first (so naive visits it first), healthy client second, then publish."""
    hub = Hub(strategy, queue_size=8, ring_size=8)  # type: ignore[arg-type]
    received: list[dict] = []
    slowest_post = 0.0
    completed = 0
    blocked = False

    with running_server(hub, tmp_path) as port:
        stalled = await open_stalled(HOST, port, "/ws/orders?client=stalled", rcvbuf=4096)
        try:
            await wait_for(lambda: subscriber_count(hub) == 1)
            async with websockets.asyncio.client.connect(
                f"ws://{HOST}:{port}/ws/orders?client=healthy", max_size=None
            ) as healthy:
                await wait_for(lambda: subscriber_count(hub) == 2)
                reader = asyncio.create_task(collect(healthy, received))
                async with httpx.AsyncClient(base_url=f"http://{HOST}:{port}") as http:
                    for _ in range(EVENTS):
                        started = time.monotonic()
                        try:
                            await asyncio.wait_for(
                                http.post(
                                    "/publish/orders",
                                    json={"type": "event", "data": {"pad": PAD}},
                                ),
                                timeout=1.0,
                            )
                        except TimeoutError:
                            blocked = True
                            break
                        slowest_post = max(slowest_post, time.monotonic() - started)
                        completed += 1
                    if blocked:
                        # Give anything still in flight time to arrive before counting.
                        await wait_for(lambda: count_events(received) >= completed)
                        await asyncio.sleep(0.3)
                    else:
                        await wait_for(lambda: count_events(received) >= EVENTS)
                subs = {s["client"]: s for s in hub.stats()["subscribers"]}
                seq = hub.stats()["channels"]["orders"]["seq"]
                reader.cancel()
        finally:
            await stalled.close()

    return {
        "blocked": blocked,
        "completed": completed,
        "slowest_post": slowest_post,
        "received": received,
        "subs": subs,
        "seq": seq,
    }


def test_naive_fanout_stalls_behind_a_client_that_stopped_reading(tmp_path: Path) -> None:
    result = asyncio.run(run_scenario("naive", tmp_path))

    assert result["blocked"], "publish never blocked: the stalled client did not exert backpressure"
    assert 0 < result["completed"] < EVENTS
    # The event whose publish is stuck was numbered, but the healthy client never got it, and
    # nothing published after it could have been sent either.
    healthy_seqs = [m["seq"] for m in result["received"] if m["type"] == "event"]
    assert healthy_seqs == list(range(1, result["completed"] + 1))
    assert result["seq"] > healthy_seqs[-1]
    assert result["subs"]["healthy"]["dropped"] == 0


def test_queued_fanout_keeps_healthy_clients_flowing(tmp_path: Path) -> None:
    result = asyncio.run(run_scenario("queued", tmp_path))

    assert not result["blocked"]
    assert result["completed"] == EVENTS
    assert result["slowest_post"] < 0.5
    greeting, *rest = result["received"]
    assert greeting["type"] == "heartbeat"
    assert [(m["type"], m["seq"]) for m in rest] == [("event", n) for n in range(1, EVENTS + 1)]
    assert result["subs"]["healthy"]["dropped"] == 0
    assert result["subs"]["stalled"]["dropped"] > 0, "the stalled client should have lost events"


@pytest.mark.parametrize("strategy", ["naive", "queued"])
def test_a_stalled_client_does_not_outlive_its_socket(strategy: str, tmp_path: Path) -> None:
    async def scenario() -> int:
        hub = Hub(strategy, queue_size=8)  # type: ignore[arg-type]
        with running_server(hub, tmp_path) as port:
            stalled = await open_stalled(HOST, port, "/ws/orders", rcvbuf=4096)
            await wait_for(lambda: subscriber_count(hub) == 1)
            await stalled.close()
            await wait_for(lambda: subscriber_count(hub) == 0)
            return subscriber_count(hub)

    assert asyncio.run(scenario()) == 0
