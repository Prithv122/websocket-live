"""`wslive bench`: one load-generator run against a freshly started server.

Question per run: with N clients, `stalled` of which never read their socket, what is the
delivery latency the healthy clients see, measured from the feed's scheduled publish time?

How the expected message count is defined (the denominator of delivery ratio, and the number of
samples behind every percentile): the feed publishes on a fixed grid, event `seq` at
`t_ref + (seq - seq_ref) / rate`. After the run the harness reads the server's ring buffer (sized
to cover the whole run), takes one event as the grid reference, and counts the grid slots whose
scheduled time lies in [window_start, window_start + seconds). Every healthy client is expected
to receive each of them. An event the server never published (a publisher stuck behind a stalled
socket) still counts as expected, so a blocked publisher cannot shrink its own denominator. If
the events do not sit on a grid, the harness falls back to the ring's own count, provided the
ring covers the window, and says so in the result (`window.method`).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json
import math
import multiprocessing
import os
import platform
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib import metadata
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

import psutil

import wslive
from wslive.bench import stats
from wslive.bench.loadgen import (
    MARGIN_S,
    Slot,
    WorkerResult,
    WorkerSpec,
    interleave_slots,
    open_stalled_clients,
    sleep_until,
    worker_entry,
)

STRATEGIES = ("naive", "queued")
MODES = ("push", "poll")
RSS_INTERVAL_S = 0.5
SCHEMA_VERSION = 1
PACKAGES = ("fastapi", "uvicorn", "websockets", "starlette", "psutil", "pydantic")


class ConfigError(ValueError):
    """Bad options: the run was not attempted."""


class HarnessError(RuntimeError):
    """The harness itself failed (server did not start, worker died, no usable feed)."""


@dataclass(frozen=True)
class BenchConfig:
    strategy: str = "queued"
    n: int = 50
    stalled: int = 0
    rate: float = 20.0
    seconds: float = 60.0
    warmup: float = 3.0
    drain: float = 2.0
    pad_bytes: int = 0
    rcvbuf: int | None = None  # SO_RCVBUF of the stalled clients only; None = OS default
    queue_size: int = 64
    procs: int = 1
    mode: str = "push"
    port: int = 0
    out: str = "results/runs"
    seed: int = 7
    repeat_index: int = 0
    poll_interval: float = 1.0
    host: str = "127.0.0.1"
    channel: str = "orders"
    heartbeat: float = 10.0
    connect_gap_s: float = 0.005

    def validate(self) -> None:
        problems = []
        if self.strategy not in STRATEGIES:
            problems.append(f"strategy must be one of {STRATEGIES}")
        if self.mode not in MODES:
            problems.append(f"mode must be one of {MODES}")
        if self.n < 2:
            problems.append("n must be at least 2")
        elif not 0 <= self.stalled < self.n:
            problems.append("stalled must be in [0, n - 1]: at least one healthy client")
        if self.rate <= 0:
            problems.append("rate must be positive")
        if self.seconds <= 0:
            problems.append("seconds must be positive")
        if self.warmup < 2 * MARGIN_S:
            problems.append(f"warmup must be at least {2 * MARGIN_S} s")
        if self.drain < 0:
            problems.append("drain must not be negative")
        if self.pad_bytes < 0:
            problems.append("pad_bytes must not be negative")
        if self.rcvbuf is not None and self.rcvbuf <= 0:
            problems.append("rcvbuf must be positive (omit it for the OS default)")
        if self.queue_size < 1:
            problems.append("queue_size must be at least 1")
        if self.procs < 1:
            problems.append("procs must be at least 1")
        if not 0 <= self.port <= 65535:
            problems.append("port must be in [0, 65535]")
        if self.poll_interval <= 0:
            problems.append("poll_interval must be positive")
        if problems:
            raise ConfigError("; ".join(problems))

    @property
    def healthy(self) -> int:
        return self.n - self.stalled

    @property
    def stalled_share(self) -> float:
        return self.stalled / self.n


def stalled_count(n: int, share: float) -> int:
    """Number of stalled clients for a share of n, rounded half up."""
    if not 0 <= share < 1:
        raise ConfigError("stalled share must be in [0, 1)")
    return math.floor(n * share + 0.5)


def ring_size_for(cfg: BenchConfig) -> int:
    """A ring big enough to hold every event of the run (connect phase, window, drain)."""
    run_s = cfg.n * cfg.connect_gap_s + cfg.warmup + cfg.seconds + cfg.drain + 30
    return max(1000, math.ceil(cfg.rate * run_s))


def result_filename(started: datetime, cfg: BenchConfig) -> str:
    stamp = started.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    suffix = "" if cfg.mode == "push" else f"-{cfg.mode}"
    return f"{stamp}-{cfg.strategy}-n{cfg.n}-s{cfg.stalled}-r{cfg.repeat_index}{suffix}.json"


@dataclass(frozen=True)
class ExpectedWindow:
    seq_range: range
    scheduled: int  # grid slots (or ring events) with t_sched in the window
    published: int  # of those, how many the server's ring actually holds
    method: str  # "grid" | "ring"
    t_ref: float
    seq_ref: int
    grid_max_error_s: float


def window_seq_range(
    t_ref: float, seq_ref: int, rate: float, window_start: float, seconds: float
) -> range:
    """Sequence numbers whose grid time t_ref + (seq - seq_ref) / rate is in the window."""
    eps = 1e-9
    lo = math.ceil(seq_ref + (window_start - t_ref) * rate - eps)
    hi = math.ceil(seq_ref + (window_start + seconds - t_ref) * rate - eps)
    return range(lo, max(lo, hi))


def expected_window(
    ring: list[tuple[int, float]], rate: float, window_start: float, seconds: float
) -> ExpectedWindow:
    """Decide which sequence numbers every healthy client should have received.

    `ring` is (seq, t_sched) for each event in the server's ring, oldest first.
    """
    if not ring:
        raise HarnessError("the server's ring is empty: the feed published nothing")
    seq_ref, t_ref = ring[0]
    max_err = max(abs(t - (t_ref + (s - seq_ref) / rate)) for s, t in ring)
    held = {s for s, _ in ring}
    if max_err < 0.25 / rate:
        seqs = window_seq_range(t_ref, seq_ref, rate, window_start, seconds)
        method = "grid"
    elif ring[0][1] <= window_start:
        in_window = [s for s, t in ring if window_start <= t < window_start + seconds]
        seqs = range(in_window[0], in_window[-1] + 1) if in_window else range(0)
        method = "ring"
    else:
        raise HarnessError("events are off the schedule grid and the ring misses the window start")
    if len(seqs) == 0:
        raise HarnessError("no events are scheduled inside the measurement window")
    if seqs.start < 1:
        raise HarnessError("the measurement window starts before the feed did")
    return ExpectedWindow(
        seqs, len(seqs), sum(s in held for s in seqs), method, t_ref, seq_ref, max_err
    )


def collect_latencies(
    records: list[Any], seqs: range
) -> tuple[list[float], list[float], list[int]]:
    """Latency samples in ms for every (healthy client, expected seq) pair.

    A pair with no delivery contributes +inf. Returns (from t_sched, from the server ts, number
    received inside the window per client).
    """
    sched: list[float] = []
    stamp: list[float] = []
    received: list[int] = []
    for rec in records:
        got = 0
        for seq, a, b in zip(rec.seq, rec.lat_sched, rec.lat_ts, strict=True):
            if seq in seqs:
                sched.append(a * 1000)
                stamp.append(b * 1000)
                got += 1
        missing = len(seqs) - got
        sched.extend([math.inf] * missing)
        stamp.extend([math.inf] * missing)
        received.append(got)
    return sched, stamp, received


def _latency_block(samples: list[float]) -> dict[str, Any]:
    return {
        f"{k}_ms" if k not in ("count", "finite") else k: v
        for k, v in stats.summarize(samples).items()
    }


def _rate(after: dict[str, Any], before: dict[str, Any], key: str, seconds: float) -> float:
    return (after.get(key, 0) - before.get(key, 0)) / seconds


def subscriber_order_inversions(subscribers: list[dict[str, Any]], slots: list[Slot]) -> int:
    """How often consecutive subscribers in the server's list appear out of connection order."""
    position = {s.label: s.position for s in slots}
    seen = [position[s["client"]] for s in subscribers if s.get("client") in position]
    return sum(b < a for a, b in itertools.pairwise(seen))


def analyze_run(
    cfg: BenchConfig,
    slots: list[Slot],
    results: list[WorkerResult],
    expected: ExpectedWindow,
    snapshots: dict[str, dict[str, Any]],
    timing: dict[str, float],
    rss_samples: list[int],
    stalled_open: int,
    stalled_errors: list[str],
) -> dict[str, Any]:
    """Turn raw observations into the metrics sections of the result file."""
    records = sorted((r for res in results for r in res.records), key=lambda r: r.position)
    sched, stamp, received = collect_latencies(records, expected.seq_range)
    total_expected = expected.scheduled * len(records)
    got = sum(received)
    window_s = timing["window_s"]
    start, end, final = snapshots["start"], snapshots["end"], snapshots["final"]
    subs = final.get("subscribers", [])
    stalled_subs = {s["client"]: s for s in subs if str(s.get("client", "")).startswith("stalled-")}
    healthy_dropped = sum(
        s.get("dropped", 0) for s in subs if str(s.get("client", "")).startswith("healthy-")
    )
    lag = {res.worker_id: res.lag_ms for res in sorted(results, key=lambda r: r.worker_id)}
    p99s = [v["p99"] for v in lag.values() if v["p99"] is not None]
    wire_total = sum(r.wire_bytes for r in records)

    healthy: dict[str, Any] = {
        "count": len(records),
        "connected": sum(r.connected for r in records),
        "connect_failures": sum(not r.connected for r in records),
        "closed_early": sum(r.closed_early for r in records),
        "errors": sorted({r.error for r in records if r.error})[:5],
        "expected_per_client": expected.scheduled,
        "expected_total": total_expected,
        "received_total": got,
        "delivery_ratio": stats.delivery_ratio(got, total_expected),
        "min_client_delivery_ratio": stats.delivery_ratio(min(received), expected.scheduled),
        "gaps_seen": sum(r.gaps for r in records),
        "gap_events": sum(r.gap_events for r in records),
        "seq_anomalies": sum(r.anomalies for r in records),
        "duplicates": sum(r.duplicates for r in records),
        "events_without_t_sched": sum(r.no_t_sched for r in records),
        "dropped_by_server": healthy_dropped,
        "latency_sched": _latency_block(sched),
        "latency_ts": _latency_block(stamp),
        "histogram_sched_ms": stats.histogram(sched),
    }
    stalled: dict[str, Any] = {
        "count": cfg.stalled,
        "connected": stalled_open,
        "connect_errors": stalled_errors[:5],
        "dropped": {k: v.get("dropped", 0) for k, v in sorted(stalled_subs.items())},
        "dropped_total": sum(v.get("dropped", 0) for v in stalled_subs.values()),
        "queued_total": sum(v.get("queued", 0) for v in stalled_subs.values()),
    }
    server: dict[str, Any] = {
        "rss_idle_bytes": rss_samples[0] if rss_samples else None,
        "rss_max_bytes": max(rss_samples, default=None),
        "rss_final_bytes": rss_samples[-1] if rss_samples else None,
        "rss_samples": len(rss_samples),
        "rss_reported_final_bytes": final.get("rss_bytes"),
        "cpu_cores_used": timing.get("server_cpu_s", 0.0) / window_s,
        "system_cpu_percent": timing.get("system_cpu_percent"),
        "stats_final": final,
        "subscriber_order_inversions": subscriber_order_inversions(subs, slots),
    }
    traffic: dict[str, Any] = {
        "window_s": window_s,
        "server_ws_messages_per_s": _rate(end, start, "ws_messages_sent", window_s),
        "server_ws_bytes_per_s": _rate(end, start, "ws_bytes_sent", window_s),
        "server_http_poll_requests_per_s": _rate(end, start, "http_poll_requests", window_s),
        "server_http_poll_bytes_per_s": _rate(end, start, "http_poll_bytes", window_s),
        "client_messages_per_s": sum(r.messages for r in records) / cfg.seconds,
        "client_requests_per_s": sum(r.requests for r in records) / cfg.seconds,
        "client_bytes_per_s": wire_total / cfg.seconds,
    }
    return {
        "window": {
            "start": timing["ws"],
            "seconds": cfg.seconds,
            "method": expected.method,
            "seq_first": expected.seq_range.start,
            "seq_last": expected.seq_range.stop - 1,
            "scheduled": expected.scheduled,
            "published": expected.published,
            "t_ref": expected.t_ref,
            "seq_ref": expected.seq_ref,
            "grid_max_error_ms": expected.grid_max_error_s * 1000,
        },
        "healthy": healthy,
        "stalled": stalled,
        "server": server,
        "traffic": traffic,
        "loadgen": {
            "workers": len(results),
            "loop_lag_ms": lag,
            "loop_lag_p99_ms_max": max(p99s, default=None),
        },
    }


def summary_metrics(result: dict[str, Any]) -> dict[str, Any]:
    """The flat subset of a result that the sweep keeps and the report reads."""
    h, s, srv, tr = result["healthy"], result["stalled"], result["server"], result["traffic"]
    return {
        "p50_ms": h["latency_sched"]["p50_ms"],
        "p95_ms": h["latency_sched"]["p95_ms"],
        "p99_ms": h["latency_sched"]["p99_ms"],
        "max_ms": h["latency_sched"]["max_ms"],
        "p95_ts_ms": h["latency_ts"]["p95_ms"],
        "delivery_ratio": h["delivery_ratio"],
        "min_client_delivery_ratio": h["min_client_delivery_ratio"],
        "gaps_seen": h["gaps_seen"],
        "connect_failures": h["connect_failures"],
        "closed_early": h["closed_early"],
        "stalled_dropped_total": s["dropped_total"],
        "rss_max_bytes": srv["rss_max_bytes"],
        "loop_lag_p99_ms": result["loadgen"]["loop_lag_p99_ms_max"],
        "server_ws_messages_per_s": tr["server_ws_messages_per_s"],
        "server_ws_bytes_per_s": tr["server_ws_bytes_per_s"],
        "server_http_poll_requests_per_s": tr["server_http_poll_requests_per_s"],
        "server_http_poll_bytes_per_s": tr["server_http_poll_bytes_per_s"],
    }


# --- environment ------------------------------------------------------------------------------


def _cpu_model() -> str:
    try:
        if sys.platform == "win32":
            import winreg

            key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0"
            )
            return str(winreg.QueryValueEx(key, "ProcessorNameString")[0]).strip()
        if sys.platform == "darwin":
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except (OSError, subprocess.SubprocessError, ImportError):
        pass
    return platform.processor() or "unknown"


def _git(*args: str) -> str | None:
    root = Path(__file__).resolve().parents[3]
    try:
        out = subprocess.run(
            ["git", "-C", str(root), *args], capture_output=True, text=True, timeout=10, check=True
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip()


def collect_environment() -> dict[str, Any]:
    versions = {}
    for name in PACKAGES:
        with contextlib.suppress(metadata.PackageNotFoundError):
            versions[name] = metadata.version(name)
    dirty = _git("status", "--porcelain", "--", ".", ":(exclude)results")
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_model": _cpu_model(),
        "cpu_count_logical": os.cpu_count(),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "memory_total_bytes": psutil.virtual_memory().total,
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "packages": versions,
        "wslive_version": wslive.__version__,
        "git_sha": _git("rev-parse", "HEAD"),
        "git_dirty": None if dirty is None else bool(dirty),
        "wall_clock_resolution_s": time.get_clock_info("time").resolution,
    }


# --- server subprocess ------------------------------------------------------------------------


def free_port(host: str) -> int:
    with socket.socket() as s:
        s.bind((host, 0))
        return s.getsockname()[1]


class ServerProcess:
    """`wslive serve` as a child process, with RSS/CPU probes that look at its whole process tree
    (a virtual-environment launcher on Windows is a separate parent process)."""

    def __init__(self, cfg: BenchConfig, port: int) -> None:
        self.cfg, self.port = cfg, port
        self._log = tempfile.TemporaryFile()  # noqa: SIM115 - lives as long as the server
        self._proc: subprocess.Popen[bytes] | None = None
        self._tree: list[psutil.Process] = []

    def command(self) -> list[str]:
        c = self.cfg
        return [
            sys.executable, "-m", "wslive.cli", "serve",
            "--host", c.host, "--port", str(self.port),
            "--strategy", c.strategy, "--queue-size", str(c.queue_size),
            "--ring-size", str(ring_size_for(c)), "--heartbeat", str(c.heartbeat),
            "--feed-rate", str(c.rate), "--feed-channel", c.channel,
            "--feed-seed", str(c.seed), "--pad-bytes", str(c.pad_bytes),
            "--log-level", "warning",
        ]  # fmt: skip

    def start(self) -> None:
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        src = str(Path(wslive.__file__).resolve().parents[1])
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [src, env.get("PYTHONPATH")]))
        self._proc = subprocess.Popen(
            self.command(), stdin=subprocess.DEVNULL, stdout=self._log, stderr=self._log, env=env
        )

    def alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def log_tail(self, limit: int = 2000) -> str:
        self._log.seek(0)
        return self._log.read().decode(errors="replace")[-limit:]

    def _processes(self, refresh: bool = False) -> list[psutil.Process]:
        if refresh or not self._tree:
            assert self._proc is not None
            try:
                root = psutil.Process(self._proc.pid)
                self._tree = [root, *root.children(recursive=True)]
            except psutil.NoSuchProcess:
                self._tree = []
        return self._tree

    def rss(self) -> int:
        """Largest resident set among the server's processes (the launcher is a few MB)."""
        sizes = []
        for p in self._processes():
            with contextlib.suppress(psutil.Error):
                sizes.append(p.memory_info().rss)
        return max(sizes, default=0)

    def cpu_seconds(self) -> float:
        total = 0.0
        for p in self._processes():
            with contextlib.suppress(psutil.Error):
                t = p.cpu_times()
                total += t.user + t.system
        return total

    def stop(self) -> None:
        if self._proc is None:
            return
        procs = self._processes(refresh=True)
        for p in procs:
            with contextlib.suppress(psutil.Error):
                p.terminate()
        _, alive = psutil.wait_procs(procs, timeout=5)
        for p in alive:
            with contextlib.suppress(psutil.Error):
                p.kill()
        psutil.wait_procs(alive, timeout=5)
        with contextlib.suppress(subprocess.TimeoutExpired):
            self._proc.wait(timeout=5)
        self._log.close()


def _fetch_json_sync(host: str, port: int, path: str, timeout: float = 15.0) -> Any:
    with urllib.request.urlopen(f"http://{host}:{port}{path}", timeout=timeout) as resp:
        return json.loads(resp.read())


async def fetch_json(host: str, port: int, path: str, timeout: float = 15.0) -> Any:
    return await asyncio.to_thread(_fetch_json_sync, host, port, path, timeout)


async def wait_healthy(server: ServerProcess, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not server.alive():
            raise HarnessError(f"server exited during start-up:\n{server.log_tail()}")
        try:
            await fetch_json(server.cfg.host, server.port, "/health", timeout=1.0)
            return
        except OSError:
            await asyncio.sleep(0.1)
    raise HarnessError(f"server not healthy after {timeout:.0f} s:\n{server.log_tail()}")


# --- orchestration ----------------------------------------------------------------------------


def _recv_or_none(conn: Connection, timeout: float) -> tuple[Any, ...] | None:
    return conn.recv() if conn.poll(timeout) else None


async def expect(
    proc: multiprocessing.process.BaseProcess, conn: Connection, kind: str, timeout: float
) -> tuple[Any, ...]:
    """Wait for a worker message of the given kind; surface worker errors and deaths."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            msg = await asyncio.to_thread(_recv_or_none, conn, 0.25)
        except (EOFError, OSError):
            msg = None
            if not proc.is_alive():
                raise HarnessError(
                    f"worker exited (code {proc.exitcode}) before '{kind}'"
                ) from None
        if msg is None:
            if not proc.is_alive():
                raise HarnessError(f"worker exited (code {proc.exitcode}) before '{kind}'")
            continue
        if msg[0] == "error":
            raise HarnessError(f"worker failed:\n{msg[1]}")
        if msg[0] != kind:
            raise HarnessError(f"expected '{kind}' from worker, got '{msg[0]}'")
        return msg
    raise HarnessError(f"timed out waiting for worker message '{kind}'")


async def _sample_rss(server: ServerProcess, out: list[int]) -> None:
    while True:
        out.append(server.rss())
        await asyncio.sleep(RSS_INTERVAL_S)


def _ring_events(payload: dict[str, Any]) -> list[tuple[int, float]]:
    events = []
    for env in payload.get("events", []):
        t = env.get("data", {}).get("t_sched")
        if t is not None:
            events.append((int(env["seq"]), float(t)))
    if payload.get("events") and not events:
        raise HarnessError("feed events carry no t_sched; latency cannot be measured")
    return events


async def execute(cfg: BenchConfig) -> dict[str, Any]:
    """Run once and return the full result document (not yet written to disk)."""
    cfg.validate()
    started = datetime.now(UTC)
    env_task = asyncio.create_task(asyncio.to_thread(collect_environment))
    port = cfg.port or free_port(cfg.host)
    slots = interleave_slots(cfg.n, cfg.stalled)
    healthy_slots = [s for s in slots if s.kind == "healthy"]
    stalled_slots = [s for s in slots if s.kind == "stalled"]
    procs = min(cfg.procs, len(healthy_slots))

    ctx = multiprocessing.get_context("spawn")
    workers: list[tuple[multiprocessing.process.BaseProcess, Connection]] = []
    server = ServerProcess(cfg, port)
    sockets: list[Any] = []
    rss: list[int] = []
    sampler: asyncio.Task[None] | None = None
    try:
        for w in range(procs):
            spec = WorkerSpec(
                worker_id=w, host=cfg.host, port=port, channel=cfg.channel, mode=cfg.mode,
                slots=tuple(healthy_slots[w::procs]), connect_gap_s=cfg.connect_gap_s,
                poll_interval_s=cfg.poll_interval, seed=cfg.seed,
            )  # fmt: skip
            parent_conn, child_conn = ctx.Pipe()
            proc = ctx.Process(target=worker_entry, args=(spec, child_conn), daemon=True)
            proc.start()
            child_conn.close()
            workers.append((proc, parent_conn))
        server.start()
        await wait_healthy(server)
        await asyncio.gather(*(expect(p, c, "up", 60) for p, c in workers))
        psutil.cpu_percent(interval=None)
        sampler = asyncio.create_task(_sample_rss(server, rss))

        t0 = time.time() + 0.3
        for _, conn in workers:
            conn.send(("connect", t0))
        stalled_task = asyncio.create_task(
            open_stalled_clients(
                cfg.host, port, cfg.channel, stalled_slots, t0, cfg.connect_gap_s, cfg.rcvbuf
            )
        )
        connect_budget = cfg.n * cfg.connect_gap_s + 60
        await asyncio.gather(*(expect(p, c, "connected", connect_budget) for p, c in workers))
        sockets, stalled_errors = await stalled_task

        ws = time.time() + cfg.warmup
        for _, conn in workers:
            conn.send(("go", ws, cfg.seconds, cfg.drain))
        snapshots: dict[str, dict[str, Any]] = {}
        await sleep_until(ws)
        snapshots["start"] = await fetch_json(cfg.host, port, "/stats")
        t_a, cpu_a = time.time(), server.cpu_seconds()
        psutil.cpu_percent(interval=None)
        await sleep_until(ws + cfg.seconds)
        snapshots["end"] = await fetch_json(cfg.host, port, "/stats")
        t_b, cpu_b = time.time(), server.cpu_seconds()
        system_cpu = psutil.cpu_percent(interval=None)
        timing = {
            "ws": ws, "window_s": t_b - t_a, "server_cpu_s": cpu_b - cpu_a,
            "system_cpu_percent": system_cpu,
        }  # fmt: skip

        wait = ws + cfg.seconds + cfg.drain - time.time() + 60
        done = await asyncio.gather(*(expect(p, c, "result", wait) for p, c in workers))
        results = [msg[1] for msg in done]
        snapshots["final"] = await fetch_json(cfg.host, port, "/stats")
        ring = await fetch_json(cfg.host, port, f"/poll/{cfg.channel}?since=0")
        expected = expected_window(_ring_events(ring), cfg.rate, ws, cfg.seconds)
        sampler.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sampler
        rss.append(server.rss())

        analysis = analyze_run(
            cfg, slots, results, expected, snapshots, timing, rss, len(sockets), stalled_errors
        )
    finally:
        if sampler is not None:
            sampler.cancel()
        for s in sockets:
            await s.close()
        for proc, conn in workers:
            if proc.is_alive():
                proc.terminate()
            conn.close()
        for proc, _ in workers:
            proc.join(timeout=5)
        server.stop()

    return {
        "schema_version": SCHEMA_VERSION,
        "started_utc": started.isoformat(timespec="seconds"),
        "config": {
            **asdict(cfg),
            "stalled_share": cfg.stalled_share,
            "ring_size": ring_size_for(cfg),
            "procs_used": procs,
        },
        "environment": await env_task,
        **analysis,
    }


def write_result(result: dict[str, Any], out_dir: str | Path) -> Path:
    """Write the result as strict JSON under out_dir and return the path."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    started = datetime.fromisoformat(result["started_utc"])
    cfg = BenchConfig(**{k: result["config"][k] for k in BenchConfig.__dataclass_fields__})
    base = result_filename(started, cfg)
    path = out / base
    counter = 0
    while path.exists():
        counter += 1
        path = out / base.replace(".json", f"-{counter}.json")
    path.write_text(json.dumps(stats.json_safe(result), indent=2, allow_nan=False) + "\n")
    return path


def run_bench(cfg: BenchConfig) -> tuple[dict[str, Any], Path]:
    result = asyncio.run(execute(cfg))
    return result, write_result(result, cfg.out)


# --- command line -----------------------------------------------------------------------------


def add_bench_args(p: argparse.ArgumentParser) -> None:
    d = BenchConfig()
    p.add_argument("--strategy", choices=STRATEGIES, default=d.strategy)
    p.add_argument("--n", type=int, default=d.n, help="total clients, stalled included")
    group = p.add_mutually_exclusive_group()
    group.add_argument("--stalled", type=int, help="number of stalled clients (default 0)")
    group.add_argument("--stalled-share", type=float, help="fraction of n that is stalled")
    p.add_argument("--rate", type=float, default=d.rate, help="feed events per second")
    p.add_argument("--seconds", type=float, default=d.seconds, help="measurement window")
    p.add_argument("--warmup", type=float, default=d.warmup, help="seconds before the window")
    p.add_argument("--drain", type=float, default=d.drain, help="seconds of late delivery allowed")
    p.add_argument("--pad-bytes", type=int, default=d.pad_bytes)
    p.add_argument("--rcvbuf", type=int, default=None, help="SO_RCVBUF of stalled clients")
    p.add_argument("--queue-size", type=int, default=d.queue_size)
    p.add_argument("--procs", type=int, default=min(4, os.cpu_count() or 1))
    p.add_argument("--mode", choices=MODES, default=d.mode)
    p.add_argument("--poll-interval", type=float, default=d.poll_interval)
    p.add_argument("--port", type=int, default=0, help="0 picks a free port")
    p.add_argument("--out", default=d.out, help="directory for the result JSON")
    p.add_argument("--seed", type=int, default=d.seed, help="feed seed")
    p.add_argument("--repeat-index", type=int, default=0)
    p.set_defaults(func=bench_main)


def config_from_args(args: argparse.Namespace) -> BenchConfig:
    if args.stalled_share is not None:
        stalled = stalled_count(args.n, args.stalled_share)
    else:
        stalled = args.stalled or 0
    return BenchConfig(
        strategy=args.strategy, n=args.n, stalled=stalled, rate=args.rate, seconds=args.seconds,
        warmup=args.warmup, drain=args.drain, pad_bytes=args.pad_bytes, rcvbuf=args.rcvbuf,
        queue_size=args.queue_size, procs=args.procs, mode=args.mode, port=args.port,
        out=args.out, seed=args.seed, repeat_index=args.repeat_index,
        poll_interval=args.poll_interval,
    )  # fmt: skip


def describe(result: dict[str, Any]) -> list[str]:
    h, m = result["healthy"], summary_metrics(result)

    def ms(x: float | None) -> str:
        return "inf" if x is None else f"{x:.1f} ms"

    return [
        f"healthy latency from schedule: p50 {ms(m['p50_ms'])}, p95 {ms(m['p95_ms'])}, "
        f"p99 {ms(m['p99_ms'])}",
        f"delivery ratio {h['delivery_ratio']:.4f} ({h['received_total']}/{h['expected_total']}), "
        f"gaps seen {h['gaps_seen']}, stalled dropped {m['stalled_dropped_total']}",
        f"server RSS max {(m['rss_max_bytes'] or 0) / 2**20:.1f} MiB, "
        f"load-generator loop lag p99 {ms(m['loop_lag_p99_ms'])}",
    ]


def bench_main(args: argparse.Namespace) -> int:
    try:
        cfg = config_from_args(args)
        cfg.validate()
    except ConfigError as exc:
        print(f"wslive bench: {exc}", file=sys.stderr)
        return 2
    try:
        result, path = run_bench(cfg)
    except HarnessError as exc:
        print(f"wslive bench: harness failure: {exc}", file=sys.stderr)
        return 1
    for line in describe(result):
        print(line)
    print(path.resolve())
    return 0
