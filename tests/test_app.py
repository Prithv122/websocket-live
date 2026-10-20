"""HTTP and WebSocket routes through Starlette's test client."""

import argparse
import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from wslive.app import FeedOptions, add_serve_args, create_app, default_web_dir, serve
from wslive.hub import Hub


@pytest.fixture(autouse=True)
def open_publishing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PUBLISH_TOKEN", raising=False)
    monkeypatch.delenv("WSLIVE_WEB_DIR", raising=False)


@pytest.fixture
def hub() -> Hub:
    return Hub("queued", ring_size=100, heartbeat_s=0.05)


@pytest.fixture
def client(hub: Hub, tmp_path: Path) -> Iterator[TestClient]:
    # One portal for the whole test, so the websocket handler and the HTTP calls share a loop.
    with TestClient(create_app(hub, web_dir=tmp_path / "no-web")) as c:
        yield c


def publish(client: TestClient, n: int, channel: str = "orders") -> None:
    for i in range(n):
        response = client.post(f"/publish/{channel}", json={"type": "event", "data": {"n": i}})
        assert response.status_code == 202


def wait_until(predicate: Callable[[], bool], timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached in time"
        time.sleep(0.005)


def subscriber_count(client: TestClient) -> int:
    return sum(c["subscribers"] for c in client.get("/stats").json()["channels"].values())


def test_health(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_publish_returns_the_assigned_seq(client: TestClient) -> None:
    first = client.post("/publish/orders", json={"type": "event", "data": {"a": 1}})
    second = client.post("/publish/orders", json={"data": {"a": 2}})  # type defaults to event
    other = client.post("/publish/alerts", json={"type": "event", "data": {}})
    assert (first.status_code, first.json()) == (202, {"seq": 1})
    assert second.json() == {"seq": 2}
    assert other.json() == {"seq": 1}


def test_publish_rejects_bad_bodies(client: TestClient) -> None:
    assert client.post("/publish/orders", json={"type": "heartbeat", "data": {}}).status_code == 422
    assert client.post("/publish/orders", json={"type": "event", "data": []}).status_code == 422
    assert client.post("/publish/orders", content=b"not json").status_code == 422


def test_websocket_receives_live_events(client: TestClient) -> None:
    with client.websocket_connect("/ws/orders?client=tester") as ws:
        wait_until(lambda: subscriber_count(client) == 1)
        publish(client, 2)
        greeting = json.loads(ws.receive_text())
        messages = [json.loads(ws.receive_text()) for _ in range(2)]
    assert (greeting["type"], greeting["seq"]) == ("heartbeat", 0)
    assert [(m["v"], m["channel"], m["seq"], m["type"]) for m in messages] == [
        (1, "orders", 1, "event"),
        (1, "orders", 2, "event"),
    ]
    assert messages[1]["data"] == {"n": 1}


def test_websocket_resumes_from_since(client: TestClient) -> None:
    publish(client, 5)
    with client.websocket_connect("/ws/orders?since=2") as ws:
        messages = [json.loads(ws.receive_text()) for _ in range(4)]
    assert [(m["type"], m["seq"]) for m in messages] == [
        ("heartbeat", 5),
        ("event", 3),
        ("event", 4),
        ("event", 5),
    ]


def test_websocket_older_than_the_ring_gets_a_snapshot(tmp_path: Path) -> None:
    small = Hub("queued", ring_size=3)
    with TestClient(create_app(small, web_dir=tmp_path / "no-web")) as client:
        publish(client, 8)
        with client.websocket_connect("/ws/orders?since=1") as ws:
            greeting = json.loads(ws.receive_text())
            snapshot = json.loads(ws.receive_text())
    assert (greeting["type"], greeting["seq"]) == ("heartbeat", 8)
    assert snapshot["type"] == "snapshot"
    assert snapshot["seq"] == 8
    assert snapshot["data"]["oldest"] == 6
    assert [e["seq"] for e in snapshot["data"]["events"]] == [6, 7, 8]


def test_websocket_opens_with_a_heartbeat_then_keeps_sending_them(
    client: TestClient, hub: Hub
) -> None:
    publish(client, 1)
    with client.websocket_connect("/ws/orders") as ws:
        greeting = json.loads(ws.receive_text())
        later = json.loads(ws.receive_text())  # heartbeat_s is 0.05 in this fixture
    for beat in (greeting, later):
        assert (beat["type"], beat["seq"], beat["data"]) == ("heartbeat", 1, {"epoch": hub.epoch})


def test_disconnect_removes_the_subscriber(client: TestClient) -> None:
    with client.websocket_connect("/ws/orders?client=a"), client.websocket_connect("/ws/orders"):
        wait_until(lambda: subscriber_count(client) == 2)
        clients = {s["client"] for s in client.get("/stats").json()["subscribers"]}
        assert clients == {"a", None}
    wait_until(lambda: subscriber_count(client) == 0)


def test_invalid_channel_names_are_refused(client: TestClient) -> None:
    for name in ("Orders", "a" * 33, "has.dot"):
        assert client.post(f"/publish/{name}", json={"data": {}}).status_code == 404
        assert client.get(f"/poll/{name}").status_code == 404
        with (
            pytest.raises(WebSocketDisconnect) as closed,
            client.websocket_connect(f"/ws/{name}") as ws,
        ):
            ws.receive_text()
        assert closed.value.code == 1008


def test_websocket_rejects_a_negative_since(client: TestClient) -> None:
    with (
        pytest.raises(WebSocketDisconnect) as closed,
        client.websocket_connect("/ws/orders?since=-1"),
    ):
        pass
    assert closed.value.code == 1008


# --- publish auth -----------------------------------------------------------------------------


def post_with(client: TestClient, headers: dict[str, str]) -> int:
    return client.post("/publish/orders", json={"data": {}}, headers=headers).status_code


def test_publish_requires_the_token_when_one_is_set(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PUBLISH_TOKEN", "s3cret")
    assert post_with(client, {}) == 401
    assert post_with(client, {"Authorization": "Bearer wrong"}) == 401
    assert post_with(client, {"Authorization": "s3cret"}) == 401
    assert post_with(client, {"Authorization": "Basic s3cret"}) == 401
    assert post_with(client, {"Authorization": "Bearer s3cret"}) == 202
    assert post_with(client, {"Authorization": "bearer s3cret"}) == 202
    refused = client.post("/publish/orders", json={"data": {}})
    assert refused.headers["www-authenticate"] == "Bearer"
    # a refused publish must not have consumed a sequence number
    assert client.get("/poll/orders").json()["seq"] == 2


def test_publish_is_open_when_no_token_is_set(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert post_with(client, {}) == 202
    monkeypatch.setenv("PUBLISH_TOKEN", "")  # an empty value counts as unset
    assert post_with(client, {}) == 202


def test_reading_stays_open_when_publishing_is_protected(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PUBLISH_TOKEN", "s3cret")
    assert client.get("/health").status_code == 200
    assert client.get("/poll/orders").status_code == 200
    assert client.get("/stats").status_code == 200


# --- poll, stats, static files ---------------------------------------------------------------


def test_poll_returns_events_after_since(client: TestClient) -> None:
    publish(client, 4)
    body = client.get("/poll/orders?since=2").json()
    assert body["seq"] == 4
    assert [e["seq"] for e in body["events"]] == [3, 4]
    assert body["events"][0]["type"] == "event"
    assert client.get("/poll/orders").json() == {"seq": 4, "events": []}
    assert client.get("/poll/unused?since=0").json() == {"seq": 0, "events": []}
    assert client.get("/poll/orders?since=-5").status_code == 422


def test_stats_has_every_documented_field(client: TestClient, hub: Hub) -> None:
    publish(client, 2)
    client.get("/poll/orders?since=0")
    stats = client.get("/stats").json()
    assert set(stats) == {
        "strategy",
        "queue_size",
        "channels",
        "subscribers",
        "ws_messages_sent",
        "ws_bytes_sent",
        "http_poll_requests",
        "http_poll_bytes",
        "rss_bytes",
    }
    assert stats["strategy"] == "queued"
    assert stats["queue_size"] == 64
    assert stats["channels"] == {"orders": {"seq": 2, "subscribers": 0}}
    assert stats["http_poll_requests"] == 1
    assert stats["http_poll_bytes"] > 0
    assert stats["rss_bytes"] > 0
    with client.websocket_connect("/ws/orders?since=0&client=x") as ws:
        ws.receive_text()
        ws.receive_text()
        (sub,) = client.get("/stats").json()["subscribers"]
    assert set(sub) == {"id", "channel", "client", "dropped", "queued", "sent"}
    assert sub["client"] == "x"
    assert hub.stats()["ws_messages_sent"] >= 2


def test_static_files_are_served_after_the_api_routes(hub: Hub, tmp_path: Path) -> None:
    (tmp_path / "index.html").write_text("<h1>dashboard</h1>")
    (tmp_path / "app.js").write_text("export {};")
    with TestClient(create_app(hub, web_dir=tmp_path)) as client:
        assert "dashboard" in client.get("/").text
        assert client.get("/app.js").status_code == 200
        assert client.get("/health").json() == {"status": "ok"}
        assert client.get("/missing.txt").status_code == 404


def test_a_missing_web_directory_is_skipped(hub: Hub, tmp_path: Path) -> None:
    with TestClient(create_app(hub, web_dir=tmp_path / "absent")) as client:
        assert client.get("/").status_code == 404
        assert client.get("/health").status_code == 200


def test_default_web_dir_follows_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    assert default_web_dir().name == "web"
    monkeypatch.setenv("WSLIVE_WEB_DIR", "elsewhere")
    assert default_web_dir() == Path("elsewhere")


def test_in_process_feed_publishes_while_the_app_runs(tmp_path: Path) -> None:
    feed = FeedOptions(channel="orders", rate=500.0, seed=7, count=5)
    with TestClient(create_app(Hub(), web_dir=tmp_path / "absent", feed=feed)) as client:
        wait_until(lambda: client.get("/poll/orders").json()["seq"] == 5)
        events = client.get("/poll/orders?since=0").json()["events"]
    assert [e["data"]["order_id"] for e in events] == [f"ord-{n:06d}" for n in range(1, 6)]
    assert all("t_sched" in e["data"] for e in events)


# --- command line -----------------------------------------------------------------------------


def parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    add_serve_args(parser)
    return parser.parse_args(argv)


def test_serve_options_and_defaults() -> None:
    args = parse([])
    assert args.func is serve
    assert (args.host, args.port, args.strategy) == ("127.0.0.1", 8000, "queued")
    assert (args.queue_size, args.ring_size, args.heartbeat) == (64, 1000, 10.0)
    assert (args.feed_rate, args.feed_channel, args.feed_seed, args.pad_bytes) == (
        0,
        "orders",
        7,
        0,
    )
    assert args.log_level == "warning"
    custom = parse(
        ["--strategy", "naive", "--port", "9001", "--feed-rate", "20", "--pad-bytes", "4096"]
    )
    assert (custom.strategy, custom.port, custom.feed_rate, custom.pad_bytes) == (
        "naive",
        9001,
        20,
        4096,
    )
    with pytest.raises(SystemExit):
        parse(["--strategy", "bogus"])


def test_serve_pins_the_websocket_implementation(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    captured: dict[str, Any] = {}

    class FakeServer:
        def __init__(self, config: uvicorn.Config) -> None:
            captured["config"] = config

        def run(self) -> None:
            captured["ran"] = True

    monkeypatch.setattr(uvicorn, "Server", FakeServer)
    assert serve(parse(["--strategy", "naive", "--feed-rate", "5"])) == 0
    config = captured["config"]
    assert captured["ran"]
    assert config.ws == "websockets-sansio"
    assert config.ws_ping_interval is None  # a stalled client must stay connected
    hub = config.app.state.hub
    assert (hub.strategy, hub.queue_size) == ("naive", 64)


def test_serve_rejects_a_bad_feed_channel(capsys: pytest.CaptureFixture[str]) -> None:
    assert serve(parse(["--feed-rate", "5", "--feed-channel", "Bad Name"])) == 2
    assert "feed-channel" in capsys.readouterr().err
