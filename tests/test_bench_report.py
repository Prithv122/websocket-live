"""Plan loading, the mechanical verdict, the report and the resumable sweep, on synthetic data."""

import argparse
import json
from pathlib import Path

import pytest

from wslive.bench import report, sweep
from wslive.bench.runner import stalled_count

PLAN_FILE = Path(__file__).resolve().parents[1] / "eval_plan.toml"


@pytest.fixture
def plan() -> report.Plan:
    return report.load_plan(PLAN_FILE)


def edited_plan(tmp_path: Path, old: str, new: str) -> Path:
    text = PLAN_FILE.read_text(encoding="utf-8")
    assert old in text
    path = tmp_path / "plan.toml"
    path.write_text(text.replace(old, new), encoding="utf-8")
    return path


def make_line(
    plan: report.Plan,
    stage: str,
    strategy: str,
    p95: float | None,
    *,
    delivery: float = 1.0,
    lag: float | None = 5.0,
    repeat: int = 0,
    n: int | None = None,
    stalled: int | None = None,
    plan_hash: str | None = None,
) -> dict:
    return {
        "plan_hash": plan_hash or plan.hash,
        "stage": stage,
        "strategy": strategy,
        "n": n or plan.primary_n,
        "stalled": plan.primary_stalled if stalled is None else stalled,
        "repeat": repeat,
        "run_file": f"runs/{stage}-{strategy}-{repeat}.json",
        "metrics": {
            "p50_ms": 4.0,
            "p95_ms": p95,
            "p99_ms": p95,
            "p95_ts_ms": 2.0,
            "delivery_ratio": delivery,
            "gaps_seen": 0,
            "stalled_dropped_total": 3,
            "rss_max_bytes": 60 * 2**20,
            "loop_lag_p99_ms": lag,
        },
        "env": {"cpu_model": "Test CPU", "cpu_count_logical": 8, "platform": "Test OS",
                "python": "3.13", "packages": {"websockets": "17"}, "git_sha": "abcdef0123456789",
                "git_dirty": False},
    }  # fmt: skip


def stage_lines(plan: report.Plan, stage: str, naive: float | None, queued: float | None, **kw):
    return [
        make_line(plan, stage, arm, p95, repeat=r, **kw)
        for arm, p95 in (("naive", naive), ("queued", queued))
        for r in range(plan.repeats)
    ]


# --- the plan -----------------------------------------------------------------------------------


def test_committed_plan_loads_with_the_owner_numbers(plan: report.Plan) -> None:
    assert plan.arms == ("naive", "queued")
    assert plan.n_values == (50, 200, 500)
    assert plan.shares == (0.0, 0.1, 0.3)
    assert (plan.rate, plan.seconds, plan.warmup, plan.drain, plan.repeats) == (20, 60, 3, 2, 3)
    assert (plan.queue_size, plan.pad_bytes, plan.rcvbuf) == (64, 0, 0)
    assert (plan.primary_n, plan.primary_stalled) == (200, 20)
    assert (plan.p95_budget_ms, plan.min_delivery_ratio, plan.loop_lag_limit_ms) == (100, 0.99, 20)
    assert plan.order_seed == 20261020
    assert [(s.pad_bytes, s.rcvbuf) for s in plan.ladder] == [(4096, 0), (4096, 4096)]
    assert plan.stages() == ["matrix", "ladder-1", "ladder-2"]
    assert plan.stage_params("ladder-2") == (4096, 4096)
    assert plan.stage_params("matrix") == (0, 0)


def test_plan_hash_is_stable_and_changes_with_any_value(tmp_path: Path, plan: report.Plan) -> None:
    assert report.load_plan(PLAN_FILE).hash == plan.hash
    changed = report.load_plan(edited_plan(tmp_path, "p95_budget_ms = 100", "p95_budget_ms = 101"))
    assert changed.hash != plan.hash


def test_plan_with_a_placeholder_is_refused(tmp_path: Path) -> None:
    path = edited_plan(tmp_path, "p95_budget_ms = 100", 'p95_budget_ms = "TBD"')
    with pytest.raises(report.PlanError, match=r"TBD.*primary\.p95_budget_ms"):
        report.load_plan(path)
    nested = edited_plan(
        tmp_path, "pad_bytes = 4096\nrcvbuf = 4096", 'pad_bytes = "TBD"\nrcvbuf = 1'
    )
    with pytest.raises(report.PlanError, match=r"escalation\[1\]\.pad_bytes"):
        report.load_plan(nested)


def test_malformed_plans_are_refused(tmp_path: Path) -> None:
    with pytest.raises(report.PlanError, match="cannot read"):
        report.load_plan(tmp_path / "missing.toml")
    with pytest.raises(report.PlanError, match="primary cell"):
        report.load_plan(edited_plan(tmp_path, "[primary]\nn = 200", "[primary]\nn = 201"))
    with pytest.raises(report.PlanError, match="min_delivery_ratio"):
        report.load_plan(
            edited_plan(tmp_path, "min_delivery_ratio = 0.99", "min_delivery_ratio = 2")
        )
    with pytest.raises(report.PlanError, match="p95_budget_ms must be a number"):
        report.load_plan(edited_plan(tmp_path, "p95_budget_ms = 100", 'p95_budget_ms = "fast"'))


# --- verdict ------------------------------------------------------------------------------------


def test_arm_result_uses_medians_of_valid_runs_only(plan: report.Plan) -> None:
    runs = [
        make_line(plan, "matrix", "queued", 50.0, repeat=0),
        make_line(plan, "matrix", "queued", 60.0, repeat=1),
        make_line(plan, "matrix", "queued", 9999.0, repeat=2, lag=25.0),  # invalid: lag > 20 ms
    ]
    r = report.arm_result(runs, plan)
    assert (r.runs, r.valid, r.p95_median, r.passes) == (3, 2, 55.0, True)
    assert (r.p95_min, r.p95_max) == (50.0, 60.0)


def test_budget_and_delivery_floor_are_inclusive(plan: report.Plan) -> None:
    at_limit = [make_line(plan, "matrix", "queued", 100.0, delivery=0.99)] * 3
    assert report.arm_result(at_limit, plan).passes is True
    over = [make_line(plan, "matrix", "queued", 100.1)] * 3
    assert report.arm_result(over, plan).passes is False
    lossy = [make_line(plan, "matrix", "queued", 10.0, delivery=0.9899)] * 3
    assert report.arm_result(lossy, plan).passes is False


def test_a_null_p95_is_infinite_and_fails(plan: report.Plan) -> None:
    runs = [make_line(plan, "matrix", "naive", None, delivery=0.8)] * 3
    r = report.arm_result(runs, plan)
    assert r.p95_median == float("inf")
    assert r.passes is False


def test_all_runs_invalid_gives_no_pass_fail(plan: report.Plan) -> None:
    runs = [make_line(plan, "matrix", "naive", 10.0, lag=21.0)] * 3
    assert report.arm_result(runs, plan).passes is None
    assert (
        report.arm_result([make_line(plan, "matrix", "naive", 10.0, lag=None)], plan).passes is None
    )


def test_verdict_supported(plan: report.Plan) -> None:
    lines = stage_lines(plan, "matrix", naive=900.0, queued=40.0)
    assert report.primary_verdict(lines, plan).label == "SUPPORTED"


def test_verdict_not_supported_when_queued_misses_the_budget(plan: report.Plan) -> None:
    lines = stage_lines(plan, "matrix", naive=900.0, queued=140.0)
    assert report.primary_verdict(lines, plan).label == "NOT SUPPORTED"


def test_verdict_goes_to_the_ladder_when_naive_passes(plan: report.Plan) -> None:
    lines = stage_lines(plan, "matrix", naive=30.0, queued=30.0)
    v = report.primary_verdict(lines, plan)
    assert v.label == "INCOMPLETE"
    assert "ladder-1" in v.text


def test_verdict_supported_under_escalation(plan: report.Plan) -> None:
    lines = stage_lines(plan, "matrix", 30.0, 30.0) + stage_lines(plan, "ladder-1", 30.0, 30.0)
    lines += stage_lines(plan, "ladder-2", naive=500.0, queued=40.0)
    v = report.primary_verdict(lines, plan)
    assert v.label == "SUPPORTED UNDER ESCALATION"
    assert "ladder-2" in v.text


def test_verdict_null_when_naive_never_breaks(plan: report.Plan) -> None:
    lines = [ln for s in plan.stages() for ln in stage_lines(plan, s, naive=30.0, queued=30.0)]
    v = report.primary_verdict(lines, plan)
    assert v.label == "NULL"
    assert v.text == "naive did not break within the tested conditions"


def test_verdict_invalid_and_incomplete(plan: report.Plan) -> None:
    bad = stage_lines(plan, "matrix", naive=900.0, queued=40.0, lag=50.0)
    assert report.primary_verdict(bad, plan).label == "INVALID"
    partial = stage_lines(plan, "matrix", naive=900.0, queued=40.0)[:-1]
    assert report.primary_verdict(partial, plan).label == "INCOMPLETE"
    assert report.primary_verdict([], plan).label == "INCOMPLETE"


def test_verdict_needs_a_majority_of_valid_runs(plan: report.Plan) -> None:
    lines = stage_lines(plan, "matrix", naive=900.0, queued=40.0)
    queued = [ln for ln in lines if ln["strategy"] == "queued"]
    queued[0]["metrics"]["loop_lag_p99_ms"] = 50.0
    assert report.primary_verdict(lines, plan).label == "SUPPORTED"  # 2 of 3 valid
    queued[1]["metrics"]["loop_lag_p99_ms"] = 50.0
    assert report.primary_verdict(lines, plan).label == "INVALID"  # a lone valid run


def test_other_cells_do_not_affect_the_primary_verdict(plan: report.Plan) -> None:
    lines = stage_lines(plan, "matrix", naive=900.0, queued=40.0)
    lines += stage_lines(plan, "matrix", naive=1.0, queued=1.0, n=50, stalled=0)
    assert report.primary_verdict(lines, plan).label == "SUPPORTED"


# --- report -------------------------------------------------------------------------------------


def args_for(tmp_path: Path, plan_path: Path = PLAN_FILE) -> argparse.Namespace:
    return argparse.Namespace(
        plan=str(plan_path), sweep=str(tmp_path / "sweep.jsonl"), out=str(tmp_path / "REPORT.md")
    )


def write_sweep(tmp_path: Path, lines: list[dict]) -> None:
    (tmp_path / "sweep.jsonl").write_text("".join(json.dumps(ln) + "\n" for ln in lines))


def test_report_renders_from_the_sweep_and_plan_alone(tmp_path: Path, plan: report.Plan) -> None:
    lines = stage_lines(plan, "matrix", naive=None, queued=40.0, delivery=0.9)
    lines[0]["metrics"]["loop_lag_p99_ms"] = 31.0
    write_sweep(tmp_path, lines)
    assert report.report_main(args_for(tmp_path)) == 0
    text = (tmp_path / "REPORT.md").read_text(encoding="utf-8")
    assert f"`{plan.hash}`" in text
    assert "## Verdict: " in text
    assert "| matrix | 0 | default | naive |" in text
    assert "inf" in text  # null p95 renders as infinite
    assert "## Invalid runs" in text
    assert "31.0" in text
    assert "Test CPU" in text
    assert "`abcdef0123`" in text


def test_report_refuses_a_sweep_from_another_plan(
    tmp_path: Path, plan: report.Plan, capsys
) -> None:
    lines = stage_lines(plan, "matrix", 900.0, 40.0)
    lines.append(make_line(plan, "matrix", "naive", 1.0, plan_hash="deadbeef0000"))
    write_sweep(tmp_path, lines)
    assert report.report_main(args_for(tmp_path)) == 2
    assert "deadbeef0000" in capsys.readouterr().err
    assert not (tmp_path / "REPORT.md").exists()


def test_report_refuses_a_plan_with_placeholders(tmp_path: Path, plan: report.Plan, capsys) -> None:
    write_sweep(tmp_path, stage_lines(plan, "matrix", 900.0, 40.0))
    tbd = edited_plan(tmp_path, "min_delivery_ratio = 0.99", 'min_delivery_ratio = "TBD"')
    assert report.report_main(args_for(tmp_path, tbd)) == 2
    assert "TBD" in capsys.readouterr().err


def test_report_refuses_an_empty_sweep(tmp_path: Path) -> None:
    assert report.report_main(args_for(tmp_path)) == 2


# --- sweep --------------------------------------------------------------------------------------


def test_matrix_has_every_cell_once_in_a_fixed_shuffled_order(plan: report.Plan) -> None:
    cells = sweep.stage_cells(plan, "matrix")
    assert len(cells) == 2 * 3 * 3 * 3
    assert len({c.key for c in cells}) == len(cells)
    assert cells == sweep.stage_cells(plan, "matrix")
    assert [c.strategy for c in cells] != sorted(c.strategy for c in cells)  # arms interleaved
    assert {(c.n, c.stalled) for c in cells} == {
        (n, stalled_count(n, s)) for n in plan.n_values for s in plan.shares
    }
    ladder = sweep.stage_cells(plan, "ladder-2")
    assert len(ladder) == 2 * plan.repeats
    assert {(c.n, c.stalled, c.pad_bytes, c.rcvbuf) for c in ladder} == {(200, 20, 4096, 4096)}


def test_cell_config_applies_the_stage_conditions(plan: report.Plan, tmp_path: Path) -> None:
    cell = sweep.stage_cells(plan, "ladder-2")[0]
    cfg = sweep.cell_config(plan, cell, tmp_path, procs=2)
    cfg.validate()
    assert (cfg.pad_bytes, cfg.rcvbuf, cfg.queue_size) == (4096, 4096, 64)
    assert (cfg.seconds, cfg.warmup, cfg.drain, cfg.rate) == (60, 3, 2, 20)
    base = sweep.cell_config(plan, sweep.stage_cells(plan, "matrix")[0], tmp_path, procs=2)
    assert base.rcvbuf is None  # 0 in the plan means the OS default
    assert base.out == str(tmp_path / "runs")


class FakeRunner:
    """Stands in for run_bench: naive degrades only when the stage says so."""

    def __init__(self, tmp_path: Path, plan: report.Plan, naive_breaks_at: str | None) -> None:
        self.tmp_path, self.plan, self.breaks_at = tmp_path, plan, naive_breaks_at
        self.calls: list[tuple[str, int, int, int | None]] = []

    def __call__(self, cfg):
        self.calls.append((cfg.strategy, cfg.n, cfg.stalled, cfg.rcvbuf))
        # Identify the stage from the conditions the plan gave this cell.
        stage = {(0, None): "matrix", (4096, None): "ladder-1", (4096, 4096): "ladder-2"}[
            (cfg.pad_bytes, cfg.rcvbuf)
        ]
        broken = cfg.strategy == "naive" and stage == self.breaks_at and cfg.stalled > 0
        p95 = 900.0 if broken else 20.0
        path = self.tmp_path / "runs" / f"{len(self.calls)}.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text("{}")
        result = {
            "started_utc": "2026-10-20T00:00:00+00:00",
            "environment": {"platform": "fake"},
            "config": {
                "rate": 20,
                "seconds": 60,
                "warmup": 3,
                "drain": 2,
                "queue_size": 64,
                "procs_used": 1,
            },
            "healthy": {
                "latency_sched": {"p50_ms": 5.0, "p95_ms": p95, "p99_ms": p95, "max_ms": p95},
                "latency_ts": {"p95_ms": 3.0},
                "delivery_ratio": 1.0,
                "min_client_delivery_ratio": 1.0,
                "gaps_seen": 0,
                "connect_failures": 0,
                "closed_early": 0,
            },
            "stalled": {"dropped_total": 0},
            "server": {"rss_max_bytes": 1},
            "traffic": {
                "server_ws_messages_per_s": 1,
                "server_ws_bytes_per_s": 1,
                "server_http_poll_requests_per_s": 0,
                "server_http_poll_bytes_per_s": 0,
            },
            "loadgen": {"loop_lag_p99_ms_max": 4.0},
        }
        return result, path


def test_sweep_runs_the_matrix_and_stops_when_naive_fails(
    tmp_path: Path, plan: report.Plan
) -> None:
    fake = FakeRunner(tmp_path, plan, naive_breaks_at="matrix")
    lines = sweep.run_sweep(plan, tmp_path, run_fn=fake, log=lambda _: None)
    assert len(lines) == len(fake.calls) == 54  # no ladder: naive already failed the primary cell
    assert report.primary_verdict(lines, plan).label == "SUPPORTED"
    on_disk = report.read_sweep(tmp_path / "sweep.jsonl")
    assert len(on_disk) == 54
    assert {ln["plan_hash"] for ln in on_disk} == {plan.hash}
    assert on_disk[0]["run_file"].startswith("runs/")


def test_sweep_is_resumable(tmp_path: Path, plan: report.Plan) -> None:
    first = FakeRunner(tmp_path, plan, naive_breaks_at="matrix")
    done = []
    for cell in sweep.stage_cells(plan, "matrix")[:10]:  # an interrupted sweep
        result, path = first(sweep.cell_config(plan, cell, tmp_path, 1))
        sweep.append_line(
            tmp_path / "sweep.jsonl", sweep.sweep_line(plan, cell, result, path, tmp_path)
        )
        done.append(cell.key)
    second = FakeRunner(tmp_path, plan, naive_breaks_at="matrix")
    sweep.run_sweep(plan, tmp_path, run_fn=second, log=lambda _: None)
    assert len(second.calls) == 54 - 10
    keys = [(ln["stage"], ln["strategy"], ln["n"], ln["stalled"], ln["repeat"])
            for ln in report.read_sweep(tmp_path / "sweep.jsonl")]  # fmt: skip
    assert len(keys) == len(set(keys)) == 54
    third = FakeRunner(tmp_path, plan, naive_breaks_at="matrix")
    sweep.run_sweep(plan, tmp_path, run_fn=third, log=lambda _: None)
    assert third.calls == []


def test_sweep_walks_the_ladder_only_while_naive_passes(tmp_path: Path, plan: report.Plan) -> None:
    fake = FakeRunner(tmp_path, plan, naive_breaks_at="ladder-2")
    lines = sweep.run_sweep(plan, tmp_path, run_fn=fake, log=lambda _: None)
    assert len(lines) == 54 + 6 + 6
    assert {ln["stage"] for ln in lines} == {"matrix", "ladder-1", "ladder-2"}
    assert report.primary_verdict(lines, plan).label == "SUPPORTED UNDER ESCALATION"
    # Naive broke at step 1 instead: step 2 is never run.
    other = tmp_path / "other"
    other.mkdir()
    fake1 = FakeRunner(other, plan, naive_breaks_at="ladder-1")
    lines = sweep.run_sweep(plan, other, run_fn=fake1, log=lambda _: None)
    assert {ln["stage"] for ln in lines} == {"matrix", "ladder-1"}


def test_null_result_is_reported_after_the_full_ladder(tmp_path: Path, plan: report.Plan) -> None:
    lines = sweep.run_sweep(
        plan, tmp_path, run_fn=FakeRunner(tmp_path, plan, None), log=lambda _: None
    )
    assert len(lines) == 54 + 12
    assert report.primary_verdict(lines, plan).label == "NULL"


def sweep_args(tmp_path: Path, plan_path: Path = PLAN_FILE, *, dry_run: bool = False):
    return argparse.Namespace(
        plan=str(plan_path), out=str(tmp_path), cooldown=0.0, procs=1, dry_run=dry_run
    )


def test_sweep_refuses_a_plan_with_placeholders(tmp_path: Path, capsys) -> None:
    tbd = edited_plan(tmp_path, "seconds = 60", 'seconds = "TBD"')
    assert sweep.sweep_main(sweep_args(tmp_path / "out", tbd)) == 2
    err = capsys.readouterr().err
    assert "TBD" in err
    assert "run.seconds" in err
    assert not (tmp_path / "out" / "sweep.jsonl").exists()


def test_sweep_refuses_to_append_to_another_plans_results(
    tmp_path: Path, plan: report.Plan, capsys
) -> None:
    write_sweep(tmp_path, [make_line(plan, "matrix", "naive", 1.0, plan_hash="deadbeef0000")])
    assert sweep.sweep_main(sweep_args(tmp_path)) == 2
    assert "deadbeef0000" in capsys.readouterr().err


def test_dry_run_lists_cells_and_runs_nothing(tmp_path: Path, plan: report.Plan, capsys) -> None:
    sweep.append_line(
        tmp_path / "sweep.jsonl", make_line(plan, "matrix", "naive", 1.0, n=50, stalled=0)
    )
    assert sweep.sweep_main(sweep_args(tmp_path, dry_run=True)) == 0
    out = capsys.readouterr().out
    assert out.count("pending") == 53 + 6 + 6
    assert out.count("done") == 1
    assert "only while naive passes" in out
    assert "matrix: 54 cells" in out
