"""Pure parts of the runner and load generator: no sockets, no subprocesses."""

import argparse
import itertools
import json
import math
from array import array
from datetime import UTC, datetime

import pytest

from wslive.bench import runner, stats
from wslive.bench.loadgen import (
    ClientRecord,
    Window,
    WorkerResult,
    ingest,
    interleave_slots,
    lag_summary,
)

RATE = 20.0


def test_stalled_count_rounds_half_up() -> None:
    assert runner.stalled_count(200, 0.1) == 20
    assert runner.stalled_count(50, 0.1) == 5
    assert runner.stalled_count(500, 0.3) == 150
    assert runner.stalled_count(50, 0.0) == 0
    assert runner.stalled_count(5, 0.1) == 1  # 0.5 rounds up
    with pytest.raises(runner.ConfigError):
        runner.stalled_count(10, 1.0)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("strategy", "fifo"),
        ("mode", "pull"),
        ("n", 1),
        ("stalled", 50),
        ("rate", 0),
        ("seconds", 0),
        ("warmup", 0.5),
        ("drain", -1),
        ("rcvbuf", 0),
        ("queue_size", 0),
        ("procs", 0),
        ("port", 70000),
    ],
)
def test_config_validation_rejects(field: str, value: object) -> None:
    with pytest.raises(runner.ConfigError, match=field.split("_")[0]):
        runner.BenchConfig(**{field: value}).validate()


def test_config_defaults_are_valid() -> None:
    runner.BenchConfig().validate()
    cfg = runner.BenchConfig(n=200, stalled=20)
    assert cfg.healthy == 180
    assert cfg.stalled_share == 0.1


def test_interleave_spreads_stalled_clients_evenly_and_deterministically() -> None:
    slots = interleave_slots(10, 1)
    assert [s.position for s in slots if s.kind == "stalled"] == [4]
    for n, stalled in [(50, 5), (200, 20), (200, 60), (500, 150), (7, 3), (50, 0)]:
        slots = interleave_slots(n, stalled)
        assert slots == interleave_slots(n, stalled)
        assert [s.position for s in slots] == list(range(n))
        positions = [s.position for s in slots if s.kind == "stalled"]
        assert len(positions) == stalled
        if stalled > 1:
            gaps = {b - a for a, b in itertools.pairwise(positions)}
            assert max(gaps) - min(gaps) <= 1  # even to within one slot
    labels = sorted(s.label for s in interleave_slots(6, 2))
    assert labels == ["healthy-0", "healthy-1", "healthy-2", "healthy-3", "stalled-0", "stalled-1"]


def test_interleave_requires_a_healthy_client() -> None:
    with pytest.raises(ValueError):
        interleave_slots(5, 5)


def test_ring_size_covers_the_whole_run() -> None:
    cfg = runner.BenchConfig(n=500, rate=20, seconds=60, warmup=3, drain=2)
    assert runner.ring_size_for(cfg) >= 20 * (60 + 3 + 2 + 30)
    assert runner.ring_size_for(runner.BenchConfig(seconds=1, warmup=1, drain=0, rate=1)) == 1000


def test_result_filename() -> None:
    started = datetime(2026, 10, 20, 12, 34, 56, tzinfo=UTC)
    cfg = runner.BenchConfig(strategy="naive", n=200, stalled=20, repeat_index=2)
    assert runner.result_filename(started, cfg) == "20261020T123456Z-naive-n200-s20-r2.json"
    poll = runner.BenchConfig(mode="poll")
    assert runner.result_filename(started, poll).endswith("-r0-poll.json")


def test_window_seq_range_counts_grid_slots() -> None:
    # slot s is scheduled at 100 + (s - 1) / 20: seq 41 at 102.0, seq 101 at 105.0
    r = runner.window_seq_range(100.0, 1, RATE, 102.0, 3.0)
    assert (r.start, r.stop) == (41, 101)
    assert len(r) == 60
    r2 = runner.window_seq_range(100.0, 1, RATE, 102.01, 3.0)
    assert (r2.start, r2.stop) == (42, 102)
    assert len(r2) == 60
    assert len(runner.window_seq_range(100.0, 1, RATE, 102.0, 0.0)) == 0


def grid_ring(first: int, last: int, t_ref: float = 100.0) -> list[tuple[int, float]]:
    return [(s, t_ref + (s - 1) / RATE) for s in range(first, last + 1)]


def test_expected_window_from_a_complete_ring() -> None:
    e = runner.expected_window(grid_ring(1, 150), RATE, 102.0, 3.0)
    assert (e.scheduled, e.published) == (60, 60)
    assert e.grid_max_error_s == 0.0


def test_expected_window_counts_events_the_publisher_never_published() -> None:
    # The ring stops at seq 80 (publisher stuck): 20 scheduled events were never published.
    e = runner.expected_window(grid_ring(1, 80), RATE, 102.0, 3.0)
    assert (e.scheduled, e.published) == (60, 40)


def test_expected_window_works_when_the_ring_has_wrapped() -> None:
    e = runner.expected_window(grid_ring(30, 150), RATE, 102.0, 3.0)
    assert (e.scheduled, e.seq_range.start) == (60, 41)


def test_events_off_the_grid_are_a_harness_error_not_a_smaller_denominator() -> None:
    jitter = [(s, t + (0.02 if s % 2 else 0.0)) for s, t in grid_ring(1, 150)]
    with pytest.raises(runner.HarnessError, match="off the schedule grid"):
        runner.expected_window(jitter, RATE, 102.0, 3.0)


def test_expected_window_failures() -> None:
    with pytest.raises(runner.HarnessError, match="empty"):
        runner.expected_window([], RATE, 1.0, 3.0)
    with pytest.raises(runner.HarnessError, match="no events"):
        runner.expected_window(grid_ring(1, 10), RATE, 105.0, 0.0)
    with pytest.raises(runner.HarnessError, match="before the feed"):
        runner.expected_window(grid_ring(1, 10), RATE, 90.0, 3.0)


def record(label: str, seqs: list[int], lat_s: float = 0.010, lat_ts_s: float = 0.002):
    rec = ClientRecord(label, int(label.split("-")[1]), connected=True)
    rec.seq = array("q", seqs)
    rec.lat_sched = array("d", [lat_s] * len(seqs))
    rec.lat_ts = array("d", [lat_ts_s] * len(seqs))
    return rec


def test_collect_latencies_marks_missing_as_infinite_and_ignores_out_of_window() -> None:
    window = range(10, 20)
    full = record("healthy-0", list(range(9, 21)))  # 9 and 20 are margin events, not counted
    half = record("healthy-1", list(range(10, 15)))
    sched, stamp, received = runner.collect_latencies([full, half], window)
    assert received == [10, 5]
    assert len(sched) == len(stamp) == 20
    assert sum(math.isinf(x) for x in sched) == 5
    assert sorted(x for x in sched if math.isfinite(x)) == [10.0] * 15  # seconds -> ms
    assert stamp.count(2.0) == 15


def test_ingest_tracks_gaps_duplicates_anomalies_and_the_window() -> None:
    win = Window()
    win.set(100.0, 10.0)
    rec = ClientRecord("healthy-0", 0)

    def event(seq: int, t_sched: float | None = 101.0) -> dict:
        data = {} if t_sched is None else {"t_sched": t_sched}
        return {"type": "event", "seq": seq, "ts": "2026-10-20T00:00:00.000Z", "data": data}

    ingest(rec, win, 101.5, event(1))
    ingest(rec, win, 101.6, event(2, 99.6))  # inside the half-second margin
    ingest(rec, win, 101.7, event(3, 50.0))  # far before the window: counted, not recorded
    ingest(rec, win, 101.8, event(3))  # duplicate
    ingest(rec, win, 101.9, event(5))  # hole with no gap envelope
    ingest(rec, win, 102.0, {"type": "gap", "seq": 9, "data": {"from": 6, "to": 9}})
    ingest(rec, win, 102.1, event(10))
    ingest(rec, win, 102.2, event(11, None))
    ingest(rec, win, 102.3, {"type": "heartbeat", "seq": 11, "data": {}})
    assert list(rec.seq) == [1, 2, 5, 10]
    assert rec.lat_sched[0] == pytest.approx(0.5)
    assert (rec.gaps, rec.gap_events) == (1, 4)
    assert (rec.duplicates, rec.anomalies, rec.no_t_sched) == (1, 1, 1)


def test_window_membership() -> None:
    win = Window()
    assert not win.contains(0.0)  # unset window contains nothing
    win.set(10.0, 5.0)
    assert win.contains(10.0)
    assert win.contains(14.99)
    assert not win.contains(15.0)


def test_lag_summary() -> None:
    s = lag_summary([0.001] * 98 + [0.050, 0.100])
    assert s["samples"] == 100
    assert s["p50"] == pytest.approx(1.0)
    assert s["p99"] == pytest.approx(50.0)
    assert s["max"] == pytest.approx(100.0)
    assert lag_summary([])["p99"] is None


def test_subscriber_order_inversions() -> None:
    slots = interleave_slots(6, 2)
    by_pos = {s.position: s.label for s in slots}
    ordered = [{"client": by_pos[i]} for i in range(6)]
    assert runner.subscriber_order_inversions(ordered, slots) == 0
    ordered[0], ordered[1] = ordered[1], ordered[0]
    assert runner.subscriber_order_inversions(ordered, slots) == 1


def synthetic_analysis(missing_for_second: int = 0) -> tuple[runner.BenchConfig, dict]:
    cfg = runner.BenchConfig(strategy="queued", n=3, stalled=1, seconds=3, warmup=1, drain=1)
    slots = interleave_slots(3, 1)
    healthy = [s for s in slots if s.kind == "healthy"]
    expected = runner.expected_window(grid_ring(1, 90), RATE, 101.0, 3.0)
    seqs = list(expected.seq_range)
    recs = [
        record(healthy[0].label, seqs),
        record(healthy[1].label, seqs[: len(seqs) - missing_for_second]),
    ]
    results = [WorkerResult(0, recs, {"samples": 300, "p50": 5.0, "p99": 8.0, "max": 9.0})]
    snap = {
        "ws_messages_sent": 0,
        "ws_bytes_sent": 0,
        "http_poll_requests": 0,
        "http_poll_bytes": 0,
        "subscribers": [],
    }
    end_subs = [{"id": 2, "client": "stalled-0", "dropped": 5, "queued": 64}]
    end = {**snap, "ws_messages_sent": 180, "ws_bytes_sent": 36_000, "subscribers": end_subs}
    final = {
        **end,
        "rss_bytes": 50_000_000,
        "subscribers": [
            {"id": 1, "client": healthy[0].label, "dropped": 0, "queued": 0},
            {"id": 2, "client": "stalled-0", "dropped": 17, "queued": 64},
        ],
    }
    timing = {"ws": 101.0, "window_s": 3.0, "server_cpu_s": 0.3, "system_cpu_percent": 12.0}
    snapshots = {"start": snap, "end": end, "final": final}
    rss = [50_000_000, 52_000_000, 51_000_000]
    analysis = runner.analyze_run(cfg, slots, results, expected, snapshots, timing, rss, 1, [])
    return cfg, analysis


def test_analyze_run_schema_and_values() -> None:
    _, a = synthetic_analysis()
    assert set(a) == {
        "invalid_reasons",
        "window",
        "healthy",
        "stalled",
        "server",
        "traffic",
        "loadgen",
    }
    assert a["invalid_reasons"] == []
    h = a["healthy"]
    assert (h["expected_total"], h["received_total"], h["delivery_ratio"]) == (120, 120, 1.0)
    assert h["latency_sched"]["p95_ms"] == 10.0
    assert h["latency_ts"]["p95_ms"] == 2.0
    assert h["histogram_sched_ms"]["total"] == 120
    assert a["stalled"]["dropped"] == {"stalled-0": 17}
    assert a["stalled"]["dropped_total"] == 17
    assert a["server"]["rss_max_bytes"] == 52_000_000
    assert a["server"]["rss_final_bytes"] == 51_000_000
    assert a["traffic"]["server_ws_messages_per_s"] == 60.0
    assert a["loadgen"]["loop_lag_p99_ms_max"] == 8.0
    assert a["window"]["scheduled"] == 60
    json.dumps(stats.json_safe(a), allow_nan=False)  # strict JSON


def test_integrity_reasons_for_missing_stalled_and_failed_healthy_clients() -> None:
    cfg = runner.BenchConfig(n=4, stalled=2)
    good = [record("healthy-0", [1]), record("healthy-1", [1])]
    listed = {"subscribers": [{"client": "stalled-0"}, {"client": "stalled-1"}, {"client": "x"}]}
    assert runner.integrity_reasons(cfg, good, 2, [], listed) == []

    reasons = runner.integrity_reasons(cfg, good, 1, ["ConnectionError: refused"], listed)
    assert reasons == ["1 of 2 stalled handshakes completed (ConnectionError: refused)"]

    one_listed = {"subscribers": [{"client": "stalled-0"}, {"client": "healthy-0"}]}
    reasons = runner.integrity_reasons(cfg, good, 2, [], one_listed)
    assert reasons == ["server lists 1 of 2 stalled subscribers at window end"]

    bad = [record("healthy-0", [1]), record("healthy-1", [1])]
    bad[0].connected = False
    bad[1].closed_early = True
    reasons = runner.integrity_reasons(cfg, bad, 2, [], listed)
    assert reasons == ["1 healthy clients failed to connect", "1 healthy clients closed early"]

    reasons = runner.integrity_reasons(cfg, good[:1], 2, [], listed)
    assert reasons == ["1 of 2 healthy clients reported"]


def test_analyze_run_records_why_a_run_is_invalid() -> None:
    cfg, _ = synthetic_analysis()
    slots = interleave_slots(3, 1)
    expected = runner.expected_window(grid_ring(1, 90), RATE, 101.0, 3.0)
    recs = [record(s.label, list(expected.seq_range)) for s in slots if s.kind == "healthy"]
    recs[0].closed_early = True
    results = [WorkerResult(0, recs, {"samples": 1, "p50": 1.0, "p99": 2.0, "max": 3.0})]
    snap = {"subscribers": []}
    snapshots = {"start": snap, "end": snap, "final": snap}
    timing = {"ws": 101.0, "window_s": 3.0}
    out = runner.analyze_run(cfg, slots, results, expected, snapshots, timing, [1], 0, ["boom"])
    assert out["invalid_reasons"] == [
        "0 of 1 stalled handshakes completed (boom)",
        "server lists 0 of 1 stalled subscribers at window end",
        "1 healthy clients closed early",
    ]


def test_missing_messages_push_p95_to_infinity_and_to_null_in_json() -> None:
    _, a = synthetic_analysis(missing_for_second=10)  # 10 of 120 missing = 8.3%
    h = a["healthy"]
    assert h["delivery_ratio"] == pytest.approx(110 / 120)
    assert h["min_client_delivery_ratio"] == pytest.approx(50 / 60)
    assert math.isinf(h["latency_sched"]["p95_ms"])
    assert h["latency_sched"]["p50_ms"] == 10.0
    assert h["histogram_sched_ms"]["missing"] == 10
    dumped = json.loads(json.dumps(stats.json_safe(a), allow_nan=False))
    assert dumped["healthy"]["latency_sched"]["p95_ms"] is None


def test_summary_metrics_and_result_file(tmp_path) -> None:
    cfg, a = synthetic_analysis()
    result = {
        "schema_version": runner.SCHEMA_VERSION,
        "started_utc": "2026-10-20T12:34:56+00:00",
        "config": {**cfg.__dict__, "stalled_share": cfg.stalled_share, "ring_size": 1000},
        "environment": {"platform": "test"},
        **a,
    }
    m = runner.summary_metrics(result)
    assert m["p95_ms"] == 10.0
    assert m["delivery_ratio"] == 1.0
    assert m["loop_lag_p99_ms"] == 8.0
    assert m["stalled_dropped_total"] == 17
    assert m["gaps_seen"] == 0
    path = runner.write_result(result, tmp_path / "runs")
    assert path.name == "20261020T123456Z-queued-n3-s1-r0.json"
    again = runner.write_result(result, tmp_path / "runs")
    assert again != path
    assert again.exists()  # never overwrites
    loaded = json.loads(path.read_text())
    assert loaded["healthy"]["delivery_ratio"] == 1.0
    assert loaded["config"]["strategy"] == "queued"
    assert runner.describe(result)[0].startswith("healthy latency")


def parse(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    runner.add_bench_args(p)
    return p.parse_args(argv)


def test_arguments_to_config() -> None:
    cfg = runner.config_from_args(
        parse(["--n", "200", "--stalled-share", "0.1", "--strategy", "naive"])
    )
    assert (cfg.n, cfg.stalled, cfg.strategy, cfg.rcvbuf) == (200, 20, "naive", None)
    cfg = runner.config_from_args(parse(["--n", "3", "--stalled", "1", "--rcvbuf", "4096"]))
    assert (cfg.stalled, cfg.rcvbuf) == (1, 4096)
    assert runner.config_from_args(parse([])).stalled == 0


def test_stalled_and_share_are_mutually_exclusive(capsys) -> None:
    with pytest.raises(SystemExit):
        parse(["--stalled", "1", "--stalled-share", "0.1"])
    assert "not allowed" in capsys.readouterr().err


def test_bench_main_reports_bad_options_without_running(capsys) -> None:
    assert runner.bench_main(parse(["--n", "3", "--stalled", "3"])) == 2
    assert "healthy" in capsys.readouterr().err
