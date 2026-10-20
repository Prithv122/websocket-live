"""Load-generator side of the benchmark: client slots, worker processes, stalled clients.

Healthy clients live in worker processes (one Python process cannot keep up with hundreds of
sockets and measure latency honestly at the same time). Each worker also runs an event-loop lag
probe: if the load generator itself stalls, receive timestamps are late and the latency numbers
are contaminated, so the lag is recorded per run as a validity control.

Parent <-> worker protocol over one Pipe, all messages tuples:
  worker -> ("up", pid)                          imports done, event loop running
  parent -> ("connect", t0)                      slot at position p connects at t0 + p * gap
  worker -> ("connected", ok, failed, errors)    every client either connected or failed
  parent -> ("go", ws, seconds, drain)           measurement window starts at epoch time ws
  worker -> ("result", WorkerResult)             sent at ws + seconds + drain
  worker -> ("error", traceback)                 anything unexpected
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import time
import traceback
from array import array
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from typing import Any
from urllib.parse import quote

from websockets.asyncio.client import connect

from wslive.bench import stats
from wslive.models import parse_iso_ms
from wslive.rawws import StalledSocket, open_stalled

# Events whose t_sched is this close to the window are kept, so the parent decides membership
# exactly (by sequence number) without float edge effects at the window boundaries.
MARGIN_S = 0.5
LAG_INTERVAL_S = 0.010
MAX_MESSAGE_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class Slot:
    """One client in connection order; `position` is also the order of subscription."""

    kind: str  # "healthy" | "stalled"
    label: str
    position: int


def interleave_slots(n: int, stalled: int) -> list[Slot]:
    """Spread `stalled` stalled clients evenly through n connection slots, deterministically.

    Slot i is stalled when the running count floor(i * stalled / n + 1/2) steps up there, which
    puts exactly `stalled` of them in, centred in their stride (n=10, stalled=1 -> slot 4).
    """
    if not 0 <= stalled < n:
        raise ValueError("need 0 <= stalled < n (at least one healthy client)")

    def count(i: int) -> int:
        return (2 * i * stalled + n) // (2 * n)

    slots: list[Slot] = []
    kinds = {"healthy": 0, "stalled": 0}
    for i in range(n):
        kind = "stalled" if count(i + 1) > count(i) else "healthy"
        slots.append(Slot(kind, f"{kind}-{kinds[kind]}", i))
        kinds[kind] += 1
    return slots


@dataclass(frozen=True)
class WorkerSpec:
    worker_id: int
    host: str
    port: int
    channel: str
    mode: str  # "push" | "poll"
    slots: tuple[Slot, ...]
    connect_gap_s: float
    poll_interval_s: float
    seed: int


@dataclass
class ClientRecord:
    """What one healthy client saw. Latency arrays are parallel to `seq` and in seconds."""

    label: str
    position: int
    connected: bool = False
    error: str | None = None
    closed_early: bool = False
    seq: array = field(default_factory=lambda: array("q"))
    lat_sched: array = field(default_factory=lambda: array("d"))
    lat_ts: array = field(default_factory=lambda: array("d"))
    messages: int = 0  # push: messages received inside the window
    requests: int = 0  # poll: requests answered inside the window
    wire_bytes: int = 0  # push: payload bytes, poll: response bytes, inside the window
    gaps: int = 0
    gap_events: int = 0
    anomalies: int = 0  # sequence jumps with no gap envelope
    duplicates: int = 0
    no_t_sched: int = 0
    last_seq: int | None = None


@dataclass
class WorkerResult:
    worker_id: int
    records: list[ClientRecord]
    lag_ms: dict[str, Any]


class Window:
    """The measurement window as one worker knows it (unset until the "go" message)."""

    def __init__(self) -> None:
        self.start = float("inf")
        self.end = float("inf")

    def set(self, start: float, seconds: float) -> None:
        self.start, self.end = start, start + seconds

    def contains(self, t: float) -> bool:
        return self.start <= t < self.end

    def keeps_sched(self, t_sched: float) -> bool:
        return self.start - MARGIN_S <= t_sched < self.end + MARGIN_S


def ingest(rec: ClientRecord, win: Window, recv: float, envelope: dict[str, Any]) -> None:
    """Record one envelope received at `recv` (epoch seconds, taken before any parsing)."""
    kind = envelope.get("type")
    seq = envelope.get("seq")
    if kind == "gap":
        data = envelope.get("data", {})
        rec.gaps += 1
        rec.gap_events += int(data.get("to", seq)) - int(data.get("from", seq)) + 1
        rec.last_seq = seq
        return
    if kind != "event" or not isinstance(seq, int):
        return
    if rec.last_seq is not None and seq != rec.last_seq + 1:
        if seq <= rec.last_seq:
            rec.duplicates += 1
            return
        rec.anomalies += 1
    rec.last_seq = seq
    t_sched = envelope.get("data", {}).get("t_sched")
    if t_sched is None:
        rec.no_t_sched += 1
        return
    if win.keeps_sched(t_sched):
        rec.seq.append(seq)
        rec.lat_sched.append(recv - t_sched)
        rec.lat_ts.append(recv - parse_iso_ms(envelope["ts"]))


async def sleep_until(epoch: float) -> None:
    await asyncio.sleep(max(0.0, epoch - time.time()))


async def _push_client(
    rec: ClientRecord, spec: WorkerSpec, win: Window, t0: float, ready: asyncio.Event
) -> None:
    uri = f"ws://{spec.host}:{spec.port}/ws/{spec.channel}?client={quote(rec.label)}"
    await sleep_until(t0 + rec.position * spec.connect_gap_s)
    try:
        # No keepalive pings and no compression: the benchmark measures the server's fan-out,
        # not the library's housekeeping, and compression would add CPU cost on both sides.
        async with connect(
            uri,
            max_queue=None,
            max_size=MAX_MESSAGE_BYTES,
            compression=None,
            ping_interval=None,
            close_timeout=1,
            open_timeout=15,
        ) as ws:
            rec.connected = True
            ready.set()
            async for message in ws:
                recv = time.time()
                size = len(message) if isinstance(message, bytes) else len(message.encode())
                if win.contains(recv):
                    rec.messages += 1
                    rec.wire_bytes += size
                ingest(rec, win, recv, json.loads(message))
            rec.closed_early = True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        rec.closed_early = rec.connected
        rec.error = f"{type(exc).__name__}: {exc}"
    finally:
        ready.set()


class HttpConnection:
    """Minimal HTTP/1.1 keep-alive GET client (Content-Length responses only)."""

    def __init__(self, host: str, port: int) -> None:
        self.host, self.port = host, port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def get(self, path: str) -> tuple[int, bytes, int]:
        """Return (status, body, bytes on the wire for the response). Retries once on a stale
        keep-alive connection."""
        for attempt in (0, 1):
            try:
                if self._writer is None:
                    self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
                return await self._once(path)
            except (ConnectionError, asyncio.IncompleteReadError, OSError):
                await self.close()
                if attempt:
                    raise
        raise AssertionError("unreachable")

    async def _once(self, path: str) -> tuple[int, bytes, int]:
        assert self._reader is not None and self._writer is not None
        request = f"GET {path} HTTP/1.1\r\nHost: {self.host}:{self.port}\r\n\r\n"
        self._writer.write(request.encode())
        await self._writer.drain()
        head = await self._reader.readuntil(b"\r\n\r\n")
        lines = head.split(b"\r\n")
        status = int(lines[0].split()[1])
        length = None
        for line in lines[1:]:
            name, _, value = line.partition(b":")
            if name.strip().lower() == b"content-length":
                length = int(value)
        if length is None:
            raise ConnectionError("response without Content-Length")
        body = await self._reader.readexactly(length)
        return status, body, len(head) + length

    async def close(self) -> None:
        writer, self._writer, self._reader = self._writer, None, None
        if writer is not None:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()


async def _poll_client(
    rec: ClientRecord, spec: WorkerSpec, win: Window, t0: float, ready: asyncio.Event
) -> None:
    http = HttpConnection(spec.host, spec.port)
    offset = random.Random(spec.seed * 100_003 + rec.position).uniform(0, spec.poll_interval_s)
    await sleep_until(t0 + rec.position * spec.connect_gap_s)
    try:
        # since far past the head: learn the latest seq, receive nothing old.
        status, body, _ = await http.get(f"/poll/{spec.channel}?since={2**31 - 1}")
        if status != 200:
            raise ConnectionError(f"poll returned HTTP {status}")
        rec.last_seq = json.loads(body)["seq"]
        rec.connected = True
        ready.set()
        next_at = time.time() + offset
        while True:
            await sleep_until(next_at)
            next_at += spec.poll_interval_s  # fixed grid: a slow reply does not shift later polls
            status, body, wire = await http.get(f"/poll/{spec.channel}?since={rec.last_seq}")
            recv = time.time()
            if status != 200:
                raise ConnectionError(f"poll returned HTTP {status}")
            if win.contains(recv):
                rec.requests += 1
                rec.wire_bytes += wire
            for envelope in json.loads(body)["events"]:
                ingest(rec, win, recv, envelope)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        rec.closed_early = rec.connected
        rec.error = f"{type(exc).__name__}: {exc}"
    finally:
        ready.set()
        await http.close()


async def _lag_probe(start: float, end: float, out: list[float]) -> None:
    """Sleep LAG_INTERVAL_S repeatedly and record how late each wake-up was."""
    loop = asyncio.get_running_loop()
    await sleep_until(start)
    expected = loop.time() + LAG_INTERVAL_S
    while time.time() < end:
        await asyncio.sleep(max(0.0, expected - loop.time()))
        now = loop.time()
        out.append(max(0.0, now - expected))
        expected = now + LAG_INTERVAL_S


def lag_summary(lags_s: list[float]) -> dict[str, Any]:
    if not lags_s:
        return {"samples": 0, "p50": None, "p99": None, "max": None}
    ms = sorted(x * 1000 for x in lags_s)
    return {
        "samples": len(ms),
        "p50": stats.percentile(ms, 50, presorted=True),
        "p99": stats.percentile(ms, 99, presorted=True),
        "max": ms[-1],
    }


async def _worker(spec: WorkerSpec, conn: Connection) -> None:
    loop = asyncio.get_running_loop()

    async def recv() -> tuple[Any, ...]:
        return await loop.run_in_executor(None, conn.recv)

    conn.send(("up", os.getpid()))
    _, t0 = await recv()
    win = Window()
    run_client = _push_client if spec.mode == "push" else _poll_client
    records = [ClientRecord(s.label, s.position) for s in spec.slots]
    ready = [asyncio.Event() for _ in records]
    tasks = [
        asyncio.create_task(run_client(rec, spec, win, t0, ev))
        for rec, ev in zip(records, ready, strict=True)
    ]
    await asyncio.gather(*(ev.wait() for ev in ready))
    ok = sum(r.connected for r in records)
    conn.send(("connected", ok, len(records) - ok, [r.error for r in records if r.error][:3]))

    _, ws, seconds, drain = await recv()
    win.set(ws, seconds)
    lags: list[float] = []
    probe = asyncio.create_task(_lag_probe(ws, ws + seconds + drain, lags))
    await sleep_until(ws + seconds + drain)
    for task in (*tasks, probe):
        task.cancel()
    await asyncio.gather(*tasks, probe, return_exceptions=True)
    conn.send(("result", WorkerResult(spec.worker_id, records, lag_summary(lags))))


def worker_entry(spec: WorkerSpec, conn: Connection) -> None:
    """Process target (module level so the spawn start method can import it)."""
    try:
        asyncio.run(_worker(spec, conn))
    except BaseException:
        conn.send(("error", traceback.format_exc()))
        raise SystemExit(1) from None


async def open_stalled_clients(
    host: str,
    port: int,
    channel: str,
    slots: list[Slot],
    t0: float,
    gap_s: float,
    rcvbuf: int | None,
) -> tuple[list[StalledSocket], list[str]]:
    """Connect the stalled clients on the same schedule as the healthy ones; return the open
    sockets (to be held for the whole run) and one message per failure."""

    async def one(slot: Slot) -> StalledSocket:
        await sleep_until(t0 + slot.position * gap_s)
        return await open_stalled(host, port, f"/ws/{channel}?client={slot.label}", rcvbuf=rcvbuf)

    results = await asyncio.gather(*(one(s) for s in slots), return_exceptions=True)
    sockets = [r for r in results if isinstance(r, StalledSocket)]
    errors = [f"{type(r).__name__}: {r}" for r in results if isinstance(r, BaseException)]
    return sockets, errors
