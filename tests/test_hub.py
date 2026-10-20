"""Hub behaviour over in-memory sockets: no network, so every ordering here is deterministic."""

import asyncio
import contextlib
import json
import re
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest

from wslive.hub import Hub
from wslive.models import Envelope

FIXTURES = Path(__file__).parent / "fixtures" / "envelopes"
STRATEGIES = ["naive", "queued"]
ISO_MS = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$")


class Disconnected(Exception):
    pass


class FakeSocket:
    """Records what the hub sends. `hold()` makes sends wait, like a client that stopped reading."""

    def __init__(self, name: str = "sock", log: list[tuple[str, int]] | None = None) -> None:
        self.name = name
        self.sent: list[dict[str, Any]] = []
        self.fail = False
        self._log = log
        self._open = asyncio.Event()
        self._open.set()
        self._inbox: asyncio.Queue[str | None] = asyncio.Queue()

    def hold(self) -> None:
        self._open.clear()

    def release(self) -> None:
        self._open.set()

    def disconnect(self) -> None:
        self._inbox.put_nowait(None)

    async def send_text(self, data: str) -> None:
        await self._open.wait()
        if self.fail:
            raise Disconnected
        message = json.loads(data)
        self.sent.append(message)
        if self._log is not None:
            self._log.append((self.name, message["seq"]))

    async def receive_text(self) -> str:
        item = await self._inbox.get()
        if item is None:
            raise Disconnected
        return item

    def of_type(self, type_: str) -> list[dict[str, Any]]:
        return [m for m in self.sent if m["type"] == type_]

    @property
    def seqs(self) -> list[int]:
        return [m["seq"] for m in self.sent]


def order(n: int) -> dict[str, Any]:
    return {"order_id": f"ord-{n:06d}", "zone": "south", "amount": 100.0 + n, "status": "placed"}


async def wait_for(predicate: Callable[[], bool], timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.001)


def subscribers(hub: Hub, channel: str = "orders") -> int:
    return hub.stats()["channels"].get(channel, {}).get("subscribers", 0)


@contextlib.asynccontextmanager
async def connected(
    hub: Hub, sock: FakeSocket, since: int | None = None, channel: str = "orders"
) -> AsyncIterator[asyncio.Task[None]]:
    before = subscribers(hub, channel)
    task = asyncio.create_task(hub.serve_socket(sock, channel, since, sock.name))
    await wait_for(lambda: subscribers(hub, channel) > before)
    try:
        yield task
    finally:
        sock.disconnect()
        sock.release()
        await asyncio.wait_for(task, 3)


async def publish_n(hub: Hub, first: int, last: int) -> None:
    for n in range(first, last + 1):
        await hub.publish("orders", "event", order(n))


def normalise(value: Any) -> Any:
    """Replace every `ts` so a live envelope can be compared with a fixture."""
    if isinstance(value, dict):
        return {k: "<ts>" if k == "ts" else normalise(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalise(v) for v in value]
    return value


def fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / f"{name}.json").read_text())


# --- envelope shape ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ["event", "heartbeat", "snapshot", "gap"])
def test_fixture_envelopes_are_valid_protocol_messages(name: str) -> None:
    example = fixture(name)
    assert Envelope.model_validate(example).model_dump() == example
    assert example["type"] == name
    assert example["v"] == 1
    assert ISO_MS.match(example["ts"])


def test_event_envelope_matches_the_fixture() -> None:
    async def scenario() -> list[dict[str, Any]]:
        hub = Hub()
        sock = FakeSocket()
        async with connected(hub, sock):
            await hub.publish("orders", "event", order(1))
            await wait_for(lambda: len(sock.sent) == 1)
        return sock.sent

    (message,) = asyncio.run(scenario())
    assert ISO_MS.match(message["ts"])
    assert normalise(message) == normalise(fixture("event"))


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_heartbeat_envelope_matches_the_fixture(strategy: str) -> None:
    async def scenario() -> FakeSocket:
        hub = Hub(strategy, heartbeat_s=0.01)
        sock = FakeSocket()
        async with connected(hub, sock):
            await hub.publish("orders", "event", order(1))
            await wait_for(lambda: len(sock.of_type("heartbeat")) >= 1)
        return sock

    sock = asyncio.run(scenario())
    assert ISO_MS.match(sock.of_type("heartbeat")[0]["ts"])
    assert normalise(sock.of_type("heartbeat")[0]) == normalise(fixture("heartbeat"))


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_snapshot_envelope_matches_the_fixture(strategy: str) -> None:
    async def scenario() -> FakeSocket:
        hub = Hub(strategy, ring_size=2)
        await publish_n(hub, 1, 3)
        sock = FakeSocket()
        async with connected(hub, sock, since=0):
            await wait_for(lambda: len(sock.sent) == 1)
        return sock

    sock = asyncio.run(scenario())
    assert normalise(sock.sent[0]) == normalise(fixture("snapshot"))


# --- sequence numbers, ring and polling -----------------------------------------------------


def test_seq_is_gap_free_and_ring_keeps_the_newest() -> None:
    async def scenario() -> Hub:
        hub = Hub(ring_size=100)
        envelopes = [await hub.publish("orders", "event", order(n)) for n in range(1, 501)]
        assert [e.seq for e in envelopes] == list(range(1, 501))
        await hub.publish("alerts", "event", {})  # channels count independently
        assert hub.stats()["channels"] == {
            "orders": {"seq": 500, "subscribers": 0},
            "alerts": {"seq": 1, "subscribers": 0},
        }
        return hub

    hub = asyncio.run(scenario())
    polled = hub.poll("orders", 0)
    assert [e["seq"] for e in polled["events"]] == list(range(401, 501))
    assert polled["seq"] == 500


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a_keeping_up_client_sees_every_seq_in_order(strategy: str) -> None:
    async def scenario() -> list[int]:
        hub = Hub(strategy, queue_size=1000)
        sock = FakeSocket()
        async with connected(hub, sock):
            for n in range(1, 301):
                await hub.publish("orders", "event", order(n))
                await asyncio.sleep(0)
            await wait_for(lambda: len(sock.sent) == 300)
        return sock.seqs

    assert asyncio.run(scenario()) == list(range(1, 301))


def test_poll_returns_the_ring_after_since() -> None:
    async def scenario() -> Hub:
        hub = Hub(ring_size=5)
        await publish_n(hub, 1, 8)
        return hub

    hub = asyncio.run(scenario())
    assert hub.poll("orders", None) == {"seq": 8, "events": []}
    assert [e["seq"] for e in hub.poll("orders", 5)["events"]] == [6, 7, 8]
    assert [e["seq"] for e in hub.poll("orders", 0)["events"]] == [4, 5, 6, 7, 8]  # ring only
    assert hub.poll("orders", 8) == {"seq": 8, "events": []}
    assert hub.poll("nope", 0) == {"seq": 0, "events": []}
    body = json.loads(hub.poll_json("orders", 6))
    assert [e["seq"] for e in body["events"]] == [7, 8]
    assert body["events"][0]["data"] == order(7)


def test_only_events_and_valid_channels_can_be_published() -> None:
    hub = Hub()
    with pytest.raises(ValueError, match="event"):
        asyncio.run(hub.publish("orders", "heartbeat", {}))
    with pytest.raises(ValueError, match="channel"):
        asyncio.run(hub.publish("Bad Channel", "event", {}))
    assert hub.stats()["channels"] == {}
    with pytest.raises(ValueError):
        Hub("nope")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Hub(queue_size=0)


# --- resume -----------------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_resume_inside_the_ring_replays_exactly_the_missed_events(strategy: str) -> None:
    async def scenario() -> list[int]:
        hub = Hub(strategy, ring_size=50)
        await publish_n(hub, 1, 10)
        sock = FakeSocket()
        async with connected(hub, sock, since=4):
            await wait_for(lambda: len(sock.sent) == 6)
            await hub.publish("orders", "event", order(11))
            await wait_for(lambda: len(sock.sent) == 7)
        return sock.seqs

    assert asyncio.run(scenario()) == [5, 6, 7, 8, 9, 10, 11]


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_events_published_during_a_replay_are_neither_lost_nor_repeated(strategy: str) -> None:
    async def scenario() -> list[int]:
        hub = Hub(strategy, ring_size=50)
        await publish_n(hub, 1, 5)
        sock = FakeSocket()
        sock.hold()  # the replay stalls on its first message
        async with connected(hub, sock, since=2):
            await publish_n(hub, 6, 8)  # lands in the middle of the replay
            assert sock.sent == []
            sock.release()
            await wait_for(lambda: len(sock.sent) == 6)
            await publish_n(hub, 9, 9)
            await wait_for(lambda: len(sock.sent) == 7)
        return sock.seqs

    assert asyncio.run(scenario()) == [3, 4, 5, 6, 7, 8, 9]


def test_naive_publish_waiting_on_the_fanout_lock_does_not_double_send_to_a_new_client() -> None:
    async def scenario() -> tuple[list[int], list[int]]:
        hub = Hub("naive", ring_size=50)
        slow, joiner = FakeSocket("slow"), FakeSocket("joiner")
        async with connected(hub, slow):
            await publish_n(hub, 1, 1)
            slow.hold()
            first = asyncio.create_task(publish_n(hub, 2, 2))  # blocked sending to `slow`
            await asyncio.sleep(0.01)
            assert not first.done()
            second = asyncio.create_task(publish_n(hub, 3, 3))  # waits on the fan-out lock
            await asyncio.sleep(0.01)
            async with connected(hub, joiner, since=1):
                slow.release()
                await asyncio.gather(first, second)
                await wait_for(lambda: len(joiner.sent) == 2)
                await publish_n(hub, 4, 4)
                await wait_for(lambda: len(joiner.sent) == 3)
        return slow.seqs, joiner.seqs

    slow_seqs, joiner_seqs = asyncio.run(scenario())
    assert slow_seqs == [1, 2, 3, 4]
    assert joiner_seqs == [2, 3, 4]


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_resume_older_than_the_ring_gets_one_snapshot_then_live(strategy: str) -> None:
    async def scenario() -> list[dict[str, Any]]:
        hub = Hub(strategy, ring_size=3)
        await publish_n(hub, 1, 10)
        sock = FakeSocket()
        async with connected(hub, sock, since=2):
            await wait_for(lambda: len(sock.sent) == 1)
            await hub.publish("orders", "event", order(11))
            await wait_for(lambda: len(sock.sent) == 2)
        return sock.sent

    snapshot, live = asyncio.run(scenario())
    assert (snapshot["type"], snapshot["seq"]) == ("snapshot", 10)
    assert snapshot["data"]["oldest"] == 8
    assert [e["seq"] for e in snapshot["data"]["events"]] == [8, 9, 10]
    assert snapshot["data"]["events"][0]["type"] == "event"
    assert (live["type"], live["seq"]) == ("event", 11)


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_the_ring_boundary_is_exact(strategy: str) -> None:
    async def first_message(since: int) -> dict[str, Any]:
        hub = Hub(strategy, ring_size=3)
        await publish_n(hub, 1, 10)  # the ring holds 8, 9, 10
        sock = FakeSocket()
        async with connected(hub, sock, since=since):
            await wait_for(lambda: len(sock.sent) >= 1)
        return sock.sent[0]

    still_in_ring = asyncio.run(first_message(7))  # needs 8
    assert (still_in_ring["type"], still_in_ring["seq"]) == ("event", 8)
    assert asyncio.run(first_message(6))["type"] == "snapshot"  # needs 7, which is gone


@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize("since", [None, 5, 99])
def test_resume_at_or_after_latest_is_live_only(strategy: str, since: int | None) -> None:
    async def scenario() -> list[int]:
        hub = Hub(strategy)
        await publish_n(hub, 1, 5)
        sock = FakeSocket()
        async with connected(hub, sock, since=since):
            await asyncio.sleep(0.02)
            assert sock.sent == []
            await hub.publish("orders", "event", order(6))
            await wait_for(lambda: len(sock.sent) == 1)
        return sock.seqs

    assert asyncio.run(scenario()) == [6]


# --- strategies -------------------------------------------------------------------------------


async def queued_scenario_with_two_drops() -> tuple[FakeSocket, dict[str, Any]]:
    """Queue of 2: event 1 is in flight, 2 and 3 are dropped for being oldest, 4 and 5 wait."""
    hub = Hub("queued", queue_size=2)
    sock = FakeSocket()
    async with connected(hub, sock):
        sock.hold()
        await hub.publish("orders", "event", order(1))
        await wait_for(lambda: hub.stats()["subscribers"][0]["queued"] == 0)  # writer took it
        await publish_n(hub, 2, 5)
        before_release = hub.stats()["subscribers"][0]
        sock.release()
        await wait_for(lambda: len(sock.sent) == 4)
    return sock, before_release


def test_queued_drops_the_oldest_and_tells_the_client_where() -> None:
    sock, before_release = asyncio.run(queued_scenario_with_two_drops())
    assert [(m["type"], m["seq"]) for m in sock.sent] == [
        ("event", 1),
        ("gap", 3),
        ("event", 4),
        ("event", 5),
    ]
    assert sock.sent[1]["data"] == {"from": 2, "to": 3, "reason": "slow_consumer"}
    assert before_release["dropped"] == 2
    assert before_release["queued"] == 2


def test_gap_envelope_matches_the_fixture() -> None:
    sock, _ = asyncio.run(queued_scenario_with_two_drops())
    assert ISO_MS.match(sock.of_type("gap")[0]["ts"])
    assert normalise(sock.of_type("gap")[0]) == normalise(fixture("gap"))


def test_queued_publish_never_waits_on_a_stalled_client() -> None:
    async def scenario() -> tuple[list[int], list[int]]:
        hub = Hub("queued", queue_size=4)
        stalled, healthy = FakeSocket("stalled"), FakeSocket("healthy")
        stalled.hold()
        async with connected(hub, stalled), connected(hub, healthy):
            async with asyncio.timeout(1):  # a stalled client must not slow the publisher
                for n in range(1, 21):
                    await hub.publish("orders", "event", order(n))
                    await asyncio.sleep(0)  # lets the healthy writer drain its small queue
            await wait_for(lambda: len(healthy.sent) == 20)
            stalled.release()
            await wait_for(lambda: stalled.seqs[-1:] == [20])
        return stalled.seqs, healthy.seqs

    stalled_seqs, healthy_seqs = asyncio.run(scenario())
    assert healthy_seqs == list(range(1, 21))
    assert stalled_seqs[-1] == 20  # caught up to the newest, with a gap in front of it


def test_naive_sends_to_subscribers_one_after_another() -> None:
    async def scenario() -> list[tuple[str, int]]:
        log: list[tuple[str, int]] = []
        hub = Hub("naive")
        socks = [FakeSocket(name, log) for name in "abc"]
        async with connected(hub, socks[0]), connected(hub, socks[1]), connected(hub, socks[2]):
            await publish_n(hub, 1, 2)
        return log

    assert asyncio.run(scenario()) == [("a", 1), ("b", 1), ("c", 1), ("a", 2), ("b", 2), ("c", 2)]


def test_naive_publish_waits_behind_a_stalled_client() -> None:
    async def scenario() -> tuple[bool, list[int], list[int]]:
        hub = Hub("naive")
        stalled, healthy = FakeSocket("stalled"), FakeSocket("healthy")
        async with connected(hub, stalled), connected(hub, healthy):
            stalled.hold()
            publishing = asyncio.create_task(hub.publish("orders", "event", order(1)))
            done, _ = await asyncio.wait({publishing}, timeout=0.05)
            blocked, seen_while_blocked = not done, list(healthy.seqs)
            stalled.release()
            await asyncio.wait_for(publishing, 1)
            await wait_for(lambda: len(healthy.sent) == 1)
        return blocked, seen_while_blocked, healthy.seqs

    blocked, seen_while_blocked, seen_after = asyncio.run(scenario())
    assert blocked
    assert seen_while_blocked == []
    assert seen_after == [1]


def test_naive_concurrent_publishes_keep_seq_order_per_client() -> None:
    async def scenario() -> list[int]:
        hub = Hub("naive")
        sock = FakeSocket()
        async with connected(hub, sock):
            sock.hold()
            tasks = [
                asyncio.create_task(hub.publish("orders", "event", order(n))) for n in range(5)
            ]
            await asyncio.sleep(0.01)
            sock.release()
            await asyncio.gather(*tasks)
            await wait_for(lambda: len(sock.sent) == 5)
        return sock.seqs

    assert asyncio.run(scenario()) == [1, 2, 3, 4, 5]


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_a_failing_socket_is_removed_and_the_others_carry_on(strategy: str) -> None:
    async def scenario() -> tuple[bool, list[int], int]:
        hub = Hub(strategy)
        broken, fine = FakeSocket("broken"), FakeSocket("fine")
        async with connected(hub, broken) as broken_task, connected(hub, fine):
            broken.fail = True
            await hub.publish("orders", "event", order(1))
            await asyncio.wait_for(broken_task, 3)  # serve_socket ends by itself
            await hub.publish("orders", "event", order(2))
            await wait_for(lambda: len(fine.sent) == 2)
            return broken_task.done(), fine.seqs, subscribers(hub)

    ended, fine_seqs, remaining = asyncio.run(scenario())
    assert ended
    assert fine_seqs == [1, 2]
    assert remaining == 1


# --- heartbeat, disconnects, stats ---------------------------------------------------------


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_idle_connections_get_heartbeats_carrying_the_latest_seq(strategy: str) -> None:
    async def scenario() -> FakeSocket:
        hub = Hub(strategy, heartbeat_s=0.02)
        await publish_n(hub, 1, 3)
        sock = FakeSocket()
        async with connected(hub, sock):
            await wait_for(lambda: len(sock.of_type("heartbeat")) >= 3)
        return sock

    sock = asyncio.run(scenario())
    assert all(m["seq"] == 3 and m["data"] == {} for m in sock.of_type("heartbeat"))


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_heartbeats_never_break_event_order(strategy: str) -> None:
    async def scenario() -> FakeSocket:
        hub = Hub(strategy, heartbeat_s=0.002)
        sock = FakeSocket()
        async with connected(hub, sock):
            for n in range(1, 41):
                await hub.publish("orders", "event", order(n))
                await asyncio.sleep(0.001)
            await wait_for(lambda: len(sock.of_type("event")) == 40)
        return sock

    sock = asyncio.run(scenario())
    assert [m["seq"] for m in sock.of_type("event")] == list(range(1, 41))
    assert len(sock.of_type("heartbeat")) > 0


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_disconnect_removes_the_subscriber_and_leaves_no_tasks(strategy: str) -> None:
    async def scenario() -> tuple[int, int, int]:
        hub = Hub(strategy)
        socks = [FakeSocket(f"s{i}") for i in range(3)]
        async with (
            connected(hub, socks[0]) as first,
            connected(hub, socks[1]),
            connected(hub, socks[2]),
        ):
            during = subscribers(hub)
            socks[0].disconnect()
            await asyncio.wait_for(first, 3)
            after_one = subscribers(hub)
        await asyncio.sleep(0)
        return during, after_one, len(asyncio.all_tasks()) - 1

    during, after_one, leftover = asyncio.run(scenario())
    assert (during, after_one) == (3, 2)
    assert leftover == 0


@pytest.mark.parametrize("strategy", STRATEGIES)
def test_cancelling_serve_socket_cleans_up(strategy: str) -> None:
    async def scenario() -> tuple[int, int]:
        hub = Hub(strategy)
        sock = FakeSocket()
        task = asyncio.create_task(hub.serve_socket(sock, "orders", None, None))
        await wait_for(lambda: subscribers(hub) == 1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        return subscribers(hub), len(asyncio.all_tasks()) - 1

    assert asyncio.run(scenario()) == (0, 0)


def test_stats_report_strategy_subscribers_and_counters() -> None:
    async def scenario() -> tuple[dict[str, Any], dict[str, Any]]:
        hub = Hub("queued", queue_size=7)
        sock = FakeSocket("browser")
        async with connected(hub, sock):
            await publish_n(hub, 1, 3)
            await wait_for(lambda: len(sock.sent) == 3)
            hub.poll_json("orders", 1)
            return hub.stats(), sock.sent[0]

    stats, first = asyncio.run(scenario())
    assert stats["strategy"] == "queued"
    assert stats["queue_size"] == 7
    assert stats["channels"] == {"orders": {"seq": 3, "subscribers": 1}}
    (sub,) = stats["subscribers"]
    assert sub["channel"] == "orders"
    assert sub["client"] == "browser"
    assert (sub["dropped"], sub["queued"], sub["sent"]) == (0, 0, 3)
    assert stats["ws_messages_sent"] == 3
    assert stats["ws_bytes_sent"] >= 3 * len(json.dumps(first, separators=(",", ":"))) - 30
    assert stats["http_poll_requests"] == 1
    assert stats["http_poll_bytes"] > 0
    assert stats["rss_bytes"] > 1_000_000
