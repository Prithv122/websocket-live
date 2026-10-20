"""The fan-out hub: per-channel sequence numbers, a replay ring, and two delivery strategies.

`naive` awaits every subscriber's socket in turn, inside `publish`. `queued` gives each subscriber
a bounded queue and a writer task, so `publish` never waits on a socket. The difference between
them under a stalled client is what this project measures; see PROTOCOL.md for the wire format.

All state is touched from one event loop. The only places that must not `await` are the ones that
decide what a new subscriber has missed and register it (`Hub._subscribe`, `Hub.publish` up to
the fan-out): between those steps nothing else can run, which is what makes resume hole-free.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import json
import logging
import re
import secrets
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import psutil

from wslive.models import CHANNEL_PATTERN, Envelope, iso_ms

log = logging.getLogger(__name__)

Strategy = Literal["naive", "queued"]


class SocketLike(Protocol):
    """What the hub needs from a WebSocket. Starlette's `WebSocket` satisfies it."""

    async def send_text(self, data: str) -> None: ...

    async def receive_text(self) -> str: ...


@dataclass(slots=True)
class _Item:
    seq: int
    text: str


class _Channel:
    def __init__(self, name: str, ring_size: int) -> None:
        self.name = name
        self.seq = 0
        self.ring: deque[Envelope] = deque(maxlen=ring_size)
        self.subs: list[_Subscriber] = []
        # Naive fan-out holds this while it walks the subscribers, so publishes stay in order.
        self.fanout = asyncio.Lock()


class _Subscriber:
    def __init__(
        self,
        hub: Hub,
        channel: _Channel,
        socket: SocketLike,
        client: str | None,
        start_seq: int,
        backlog: list[Envelope],
    ) -> None:
        self.id = f"s{next(hub._ids)}"
        self.hub = hub
        self.channel = channel
        self.socket = socket
        self.client = client
        # `backlog` covers everything up to `start_seq` (or the peer did not ask); live follows.
        self.start_seq = start_seq
        self.backlog = backlog
        self.sent = 0
        self.dropped = 0
        self.closed = asyncio.Event()

    @property
    def queued(self) -> int:
        return 0

    async def run(self) -> None:
        raise NotImplementedError

    def close(self) -> None:
        self.closed.set()
        with contextlib.suppress(ValueError):
            self.channel.subs.remove(self)

    async def _write(self, text: str) -> None:
        await self.socket.send_text(text)
        self.sent += 1
        self.hub._ws_messages += 1
        self.hub._ws_bytes += len(text.encode())

    def _heartbeat_text(self) -> str:
        return self.hub._heartbeat(self.channel.name, self.channel.seq).to_json()


class _NaiveSubscriber(_Subscriber):
    """Sent to by `publish` itself, one socket at a time."""

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        # Orders the replay, live events and heartbeats that all write to this one socket.
        self._lock = asyncio.Lock()
        self._replaying = bool(self.backlog)
        self._pending: deque[str] = deque()

    @property
    def queued(self) -> int:
        return len(self._pending)

    async def _send(self, text: str) -> None:
        async with self._lock:
            await self._write(text)

    async def deliver(self, text: str) -> None:
        if self._replaying:
            # Published while the replay is still going out; run() flushes it afterwards.
            self._pending.append(text)
            return
        try:
            await self._send(text)
        except Exception:
            log.debug("send to %s failed; dropping subscriber", self.id, exc_info=True)
            self.close()

    async def run(self) -> None:
        try:
            for envelope in self.backlog:
                await self._send(envelope.to_json())
            while self._pending:
                await self._send(self._pending.popleft())
            # No await between the empty check above and this line, so nothing is left behind.
            self._replaying = False
            while True:
                try:
                    await asyncio.wait_for(self.closed.wait(), self.hub.heartbeat_s)
                except TimeoutError:
                    await self._send(self._heartbeat_text())
                else:
                    return
        except Exception:
            log.debug("writer for %s stopped", self.id, exc_info=True)
        finally:
            self.close()


class _QueuedSubscriber(_Subscriber):
    """A bounded queue and one writer task; `offer` never waits."""

    def __init__(self, *args: Any) -> None:
        super().__init__(*args)
        self._queue: asyncio.Queue[_Item] = asyncio.Queue(maxsize=self.hub.queue_size)
        self._last_sent = self.start_seq

    @property
    def queued(self) -> int:
        return self._queue.qsize()

    def offer(self, seq: int, text: str) -> None:
        if self._queue.full():
            self._queue.get_nowait()
            self.dropped += 1
        self._queue.put_nowait(_Item(seq, text))

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            for envelope in self.backlog:
                await self._write(envelope.to_json())
            next_beat = loop.time() + self.hub.heartbeat_s
            while True:
                wait = next_beat - loop.time()
                if wait <= 0:
                    await self._write(self._heartbeat_text())
                    next_beat = loop.time() + self.hub.heartbeat_s
                    continue
                try:
                    item = await asyncio.wait_for(self._queue.get(), wait)
                except TimeoutError:
                    continue
                if item.seq != self._last_sent + 1:
                    first, last = self._last_sent + 1, item.seq - 1
                    gap = self.hub._envelope(
                        self.channel.name,
                        last,
                        "gap",
                        {"from": first, "to": last, "reason": "slow_consumer"},
                    )
                    await self._write(gap.to_json())
                await self._write(item.text)
                self._last_sent = item.seq
        except Exception:
            log.debug("writer for %s stopped", self.id, exc_info=True)
        finally:
            self.close()


class Hub:
    def __init__(
        self,
        strategy: Strategy = "queued",
        queue_size: int = 64,
        ring_size: int = 1000,
        heartbeat_s: float = 10.0,
    ) -> None:
        if strategy not in ("naive", "queued"):
            raise ValueError(f"unknown strategy: {strategy!r}")
        if queue_size < 1 or ring_size < 1 or heartbeat_s <= 0:
            raise ValueError("queue_size and ring_size must be >= 1 and heartbeat_s > 0")
        self.strategy = strategy
        self.queue_size = queue_size
        self.ring_size = ring_size
        self.heartbeat_s = heartbeat_s
        # Changes on restart, so a reconnecting client can tell its saved seq is from another run.
        self.epoch = secrets.token_hex(4)
        self._channels: dict[str, _Channel] = {}
        self._ids = itertools.count(1)
        self._ws_messages = 0
        self._ws_bytes = 0
        self._poll_requests = 0
        self._poll_bytes = 0
        self._process = psutil.Process()

    def _channel(self, name: str) -> _Channel:
        channel = self._channels.get(name)
        if channel is None:
            if not re.fullmatch(CHANNEL_PATTERN, name):
                raise ValueError(f"invalid channel name: {name!r}")
            channel = self._channels[name] = _Channel(name, self.ring_size)
        return channel

    @staticmethod
    def _envelope(channel: str, seq: int, type: str, data: dict[str, Any]) -> Envelope:
        return Envelope.model_validate(
            {"channel": channel, "seq": seq, "ts": iso_ms(time.time()), "type": type, "data": data}
        )

    def _heartbeat(self, channel: str, seq: int) -> Envelope:
        return self._envelope(channel, seq, "heartbeat", {"epoch": self.epoch})

    async def publish(self, channel: str, type: str, data: dict[str, Any]) -> Envelope:
        """Stamp, store and fan out one event. Only `event` can be published."""
        if type != "event":
            raise ValueError("only type 'event' can be published")
        ch = self._channel(channel)
        envelope = self._envelope(channel, ch.seq + 1, type, data)
        ch.seq = envelope.seq
        ch.ring.append(envelope)
        text = envelope.to_json()
        if self.strategy == "queued":
            for sub in ch.subs:
                assert isinstance(sub, _QueuedSubscriber)
                sub.offer(envelope.seq, text)
        else:
            # Fixed before waiting for the lock: a subscriber that joins while this publish is
            # queued behind another gets this event from its backlog, not from here.
            targets = [s for s in ch.subs if isinstance(s, _NaiveSubscriber)]
            async with ch.fanout:
                for sub in targets:
                    if not sub.closed.is_set():
                        await sub.deliver(text)
        return envelope

    def _subscribe(
        self, socket: SocketLike, channel: str, since: int | None, client: str | None
    ) -> _Subscriber:
        ch = self._channel(channel)
        latest = ch.seq
        # Always first: lets the client check the epoch before it trusts any replay.
        backlog: list[Envelope] = [self._heartbeat(channel, latest)]
        if since is not None and since < latest:
            oldest = ch.ring[0].seq
            if oldest <= since + 1:
                backlog += [e for e in ch.ring if e.seq > since]
            else:
                events = [e.model_dump() for e in ch.ring]
                backlog.append(
                    self._envelope(
                        channel, latest, "snapshot", {"oldest": oldest, "events": events}
                    )
                )
        cls = _QueuedSubscriber if self.strategy == "queued" else _NaiveSubscriber
        sub = cls(self, ch, socket, client, latest, backlog)
        ch.subs.append(sub)
        return sub

    async def serve_socket(
        self, ws: SocketLike, channel: str, since: int | None, client: str | None
    ) -> None:
        """Serve one accepted socket until the peer goes away or sending to it fails."""
        sub = self._subscribe(ws, channel, since, client)
        writer = asyncio.create_task(sub.run(), name=f"writer-{sub.id}")
        reader = asyncio.create_task(_discard_inbound(ws), name=f"reader-{sub.id}")
        try:
            await asyncio.wait({writer, reader}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            sub.close()
            for task in (writer, reader):
                task.cancel()
            # Not gather(return_exceptions=True): it would swallow a cancellation aimed at us.
            await asyncio.wait({writer, reader})

    def poll(self, channel: str, since: int | None) -> dict[str, Any]:
        """Polling baseline: the ring's events after `since`. No `since` means "just the seq"."""
        ch = self._channels.get(channel)
        if ch is None:
            return {"seq": 0, "events": []}
        events = [] if since is None else [e.model_dump() for e in ch.ring if e.seq > since]
        return {"seq": ch.seq, "events": events}

    def poll_json(self, channel: str, since: int | None) -> bytes:
        """`poll` as the response body, counted in the stats."""
        body = json.dumps(self.poll(channel, since), separators=(",", ":")).encode()
        self._poll_requests += 1
        self._poll_bytes += len(body)
        return body

    def stats(self) -> dict[str, Any]:
        return {
            "strategy": self.strategy,
            "queue_size": self.queue_size,
            "channels": {
                name: {"seq": ch.seq, "subscribers": len(ch.subs)}
                for name, ch in self._channels.items()
            },
            "subscribers": [
                {
                    "id": s.id,
                    "channel": s.channel.name,
                    "client": s.client,
                    "dropped": s.dropped,
                    "queued": s.queued,
                    "sent": s.sent,
                }
                for ch in self._channels.values()
                for s in ch.subs
            ],
            "ws_messages_sent": self._ws_messages,
            "ws_bytes_sent": self._ws_bytes,
            "http_poll_requests": self._poll_requests,
            "http_poll_bytes": self._poll_bytes,
            "rss_bytes": self._process.memory_info().rss,
        }


async def _discard_inbound(ws: SocketLike) -> None:
    """Read and ignore client messages; this is how a disconnect is noticed."""
    while True:
        try:
            await ws.receive_text()
        except KeyError:
            continue  # a binary frame: nothing in it for us
        except Exception:
            return
