import argparse
import asyncio
import itertools
import json
import threading
import time
from collections import Counter
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import ClassVar

import pytest

from wslive.feed import add_feed_args, feed_main, generate_orders, run_feed
from wslive.models import STATUSES, ZONES, OrderEvent

FIXTURE = Path(__file__).parent / "fixtures" / "orders_60.jsonl"


class RecordingHub:
    """Stands in for the hub; `delay` makes every publish slow, like a blocked naive fan-out."""

    def __init__(self, delay: float = 0.0) -> None:
        self.delay = delay
        self.calls: list[tuple[str, str, dict, float]] = []

    async def publish(self, channel: str, type: str, data: dict) -> None:
        self.calls.append((channel, type, data, time.time()))
        if self.delay:
            await asyncio.sleep(self.delay)


def test_generator_reproduces_the_fixture_byte_for_byte() -> None:
    lines = [
        json.dumps(order.model_dump(), sort_keys=True)
        for order in itertools.islice(generate_orders(7), 60)
    ]
    assert FIXTURE.read_bytes() == ("\n".join(lines) + "\n").encode()


def test_generator_is_deterministic_per_seed() -> None:
    first = [o.model_dump() for o in itertools.islice(generate_orders(1), 50)]
    again = [o.model_dump() for o in itertools.islice(generate_orders(1), 50)]
    other = [o.model_dump() for o in itertools.islice(generate_orders(2), 50)]
    assert first == again
    assert first != other


def test_generator_shape_and_skew() -> None:
    orders = list(itertools.islice(generate_orders(7), 3000))
    assert [o.order_id for o in orders[:2]] == ["ord-000001", "ord-000002"]
    assert len({o.order_id for o in orders}) == len(orders)
    assert all(o.zone in ZONES and o.status in STATUSES for o in orders)
    assert all(o.amount > 0 and o.amount == round(o.amount, 2) for o in orders)
    zones = Counter(o.zone for o in orders)
    assert zones["north"] > 3 * zones["airport"]  # uneven on purpose
    statuses = Counter(o.status for o in orders)
    assert 0 < statuses["cancelled"] < statuses["delivered"] / 4


def test_run_feed_publishes_on_a_fixed_schedule() -> None:
    hub = RecordingHub()
    start = time.time()
    published = asyncio.run(run_feed(hub, "orders", rate=50.0, seed=7, count=5))
    elapsed = time.time() - start

    assert published == 5
    assert elapsed >= 4 / 50  # the fifth event is due 80 ms after the first
    t_sched = [data["t_sched"] for _, _, data, _ in hub.calls]
    assert [round(b - a, 6) for a, b in itertools.pairwise(t_sched)] == [0.02] * 4
    # never early: each publish ran at or after its scheduled time
    assert all(now >= data["t_sched"] for _, _, data, now in hub.calls)
    channel, type_, data, _ = hub.calls[0]
    assert (channel, type_) == ("orders", "event")
    assert OrderEvent.model_validate(data).order_id == "ord-000001"
    assert "pad" not in data


def test_run_feed_matches_the_generator_and_pads() -> None:
    hub = RecordingHub()
    asyncio.run(run_feed(hub, "orders", rate=1000.0, seed=7, pad_bytes=32, count=60))
    sent = [{k: v for k, v in d.items() if k not in ("t_sched", "pad")} for _, _, d, _ in hub.calls]
    expected = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
    assert sent == expected
    assert all(d["pad"] == "x" * 32 for _, _, d, _ in hub.calls)


def test_run_feed_catches_up_without_skipping_or_moving_the_schedule() -> None:
    # Each publish takes 50 ms but the schedule asks for one every 10 ms.
    hub = RecordingHub(delay=0.05)
    start = time.time()
    published = asyncio.run(run_feed(hub, "orders", rate=100.0, seed=7, count=6))
    elapsed = time.time() - start

    assert published == 6
    ids = [d["order_id"] for _, _, d, _ in hub.calls]
    assert ids == [f"ord-{n:06d}" for n in range(1, 7)]  # nothing skipped
    t_sched = [d["t_sched"] for _, _, d, _ in hub.calls]
    assert [round(b - a, 6) for a, b in itertools.pairwise(t_sched)] == [0.01] * 5
    # the publisher ran ~250 ms behind its schedule by the end, and t_sched still says so
    assert elapsed >= 6 * 0.05
    assert hub.calls[-1][3] - t_sched[-1] > 0.15


class _Capture(BaseHTTPRequestHandler):
    received: ClassVar[list[dict]] = []

    def do_POST(self) -> None:
        length = int(self.headers["Content-Length"])
        self.received.append(
            {
                "path": self.path,
                "auth": self.headers.get("Authorization"),
                "body": json.loads(self.rfile.read(length)),
            }
        )
        self.send_response(202)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def capture_server():
    _Capture.received = []
    server = HTTPServer(("127.0.0.1", 0), _Capture)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", _Capture.received
    server.shutdown()
    server.server_close()


def _feed_args(url: str, **overrides: object) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    add_feed_args(parser)
    args = parser.parse_args(["--url", url, "--rate", "500", "--count", "3"])
    for key, value in overrides.items():
        setattr(args, key, value)
    return args


def test_feed_command_posts_events(monkeypatch, capture_server) -> None:
    url, received = capture_server
    monkeypatch.delenv("PUBLISH_TOKEN", raising=False)
    args = _feed_args(url)
    assert args.func is feed_main

    assert feed_main(args) == 0
    assert [r["path"] for r in received] == ["/publish/orders"] * 3
    assert all(r["auth"] is None for r in received)
    assert all(r["body"]["type"] == "event" for r in received)
    assert [r["body"]["data"]["order_id"] for r in received] == [
        "ord-000001",
        "ord-000002",
        "ord-000003",
    ]
    assert all("t_sched" in r["body"]["data"] for r in received)


def test_feed_command_sends_the_bearer_token(monkeypatch, capture_server) -> None:
    url, received = capture_server
    monkeypatch.setenv("PUBLISH_TOKEN", "s3cret")
    assert feed_main(_feed_args(url, channel="alerts", count=1)) == 0
    assert received[0]["path"] == "/publish/alerts"
    assert received[0]["auth"] == "Bearer s3cret"


def test_feed_command_reports_an_unreachable_server(capsys) -> None:
    assert feed_main(_feed_args("http://127.0.0.1:9", count=1)) == 1
    assert "cannot reach" in capsys.readouterr().err


def test_feed_command_rejects_bad_options(capsys) -> None:
    assert feed_main(_feed_args("ftp://example.com")) == 2
    assert feed_main(_feed_args("http://127.0.0.1:9", channel="Bad Channel")) == 2
    assert feed_main(_feed_args("http://127.0.0.1:9", rate=0)) == 2
