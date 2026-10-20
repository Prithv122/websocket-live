"""`wslive sweep`: run the pre-registered matrix from `eval_plan.toml`.

Run order: the cells of a stage are listed in canonical order (arm, n, stalled, repeat) and then
shuffled with `random.Random(order_seed + stage index)`, so slow drift of the machine (thermals,
background load) is spread across arms instead of landing on whichever arm ran last. The order is
a pure function of the plan, which is what makes the sweep resumable: a cell is skipped when
`results/sweep.jsonl` already holds a line for it under the same plan hash.

Stages: `matrix` first. Each escalation step (`ladder-k`) re-runs only the primary cell, both
arms, and happens only while naive passed the previous stage; the first step where naive fails
ends the ladder.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from wslive.bench.report import (
    DEFAULT_PLAN,
    MATRIX_STAGE,
    Plan,
    PlanError,
    foreign_hashes,
    load_plan,
    read_sweep,
    stage_results,
)
from wslive.bench.runner import BenchConfig, HarnessError, run_bench, stalled_count, summary_metrics

RunFn = Callable[[BenchConfig], tuple[dict[str, Any], Path]]


@dataclass(frozen=True)
class Cell:
    stage: str
    strategy: str
    n: int
    stalled: int
    repeat: int
    pad_bytes: int
    rcvbuf: int  # 0 = OS default

    @property
    def key(self) -> tuple[str, str, int, int, int]:
        return (self.stage, self.strategy, self.n, self.stalled, self.repeat)

    def label(self) -> str:
        return (
            f"{self.stage} {self.strategy} n={self.n} stalled={self.stalled} "
            f"repeat={self.repeat} pad={self.pad_bytes} rcvbuf={self.rcvbuf or 'default'}"
        )


def stage_cells(plan: Plan, stage: str) -> list[Cell]:
    """Cells of a stage in the deterministic shuffled run order."""
    pad, rcvbuf = plan.stage_params(stage)
    if stage == MATRIX_STAGE:
        grid = [(n, stalled_count(n, share)) for n in plan.n_values for share in plan.shares]
    else:
        grid = [(plan.primary_n, plan.primary_stalled)]
    cells = [
        Cell(stage, arm, n, stalled, repeat, pad, rcvbuf)
        for arm in sorted(plan.arms)
        for n, stalled in grid
        for repeat in range(plan.repeats)
    ]
    cells.sort(key=lambda c: c.key)
    random.Random(plan.order_seed + plan.stages().index(stage)).shuffle(cells)
    return cells


def cell_config(plan: Plan, cell: Cell, out_dir: Path, procs: int) -> BenchConfig:
    return BenchConfig(
        strategy=cell.strategy,
        n=cell.n,
        stalled=cell.stalled,
        rate=plan.rate,
        seconds=plan.seconds,
        warmup=plan.warmup,
        drain=plan.drain,
        pad_bytes=cell.pad_bytes,
        rcvbuf=cell.rcvbuf or None,
        queue_size=plan.queue_size,
        procs=procs,
        out=str(out_dir / "runs"),
        seed=plan.feed_seed,
        repeat_index=cell.repeat,
    )


def sweep_line(
    plan: Plan, cell: Cell, result: dict[str, Any], path: Path, out_dir: Path
) -> dict[str, Any]:
    env = result["environment"]
    cfg = result["config"]
    return {
        "plan_hash": plan.hash,
        "stage": cell.stage,
        "strategy": cell.strategy,
        "n": cell.n,
        "stalled": cell.stalled,
        "stalled_share": cell.stalled / cell.n,
        "repeat": cell.repeat,
        "pad_bytes": cell.pad_bytes,
        "rcvbuf": cell.rcvbuf,
        "started_utc": result["started_utc"],
        "run_file": path.resolve().relative_to(out_dir.resolve()).as_posix(),
        "metrics": summary_metrics(result),
        "config": {
            k: cfg[k] for k in ("rate", "seconds", "warmup", "drain", "queue_size", "procs_used")
        },
        "env": {
            k: env.get(k)
            for k in (
                "cpu_model",
                "cpu_count_logical",
                "platform",
                "python",
                "packages",
                "git_sha",
                "git_dirty",
            )
        },
    }


def append_line(sweep_path: Path, line: dict[str, Any]) -> None:
    from wslive.bench.stats import json_safe

    sweep_path.parent.mkdir(parents=True, exist_ok=True)
    with sweep_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(json_safe(line), allow_nan=False, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def run_stage(
    plan: Plan,
    stage: str,
    lines: list[dict[str, Any]],
    out_dir: Path,
    *,
    run_fn: RunFn = run_bench,
    cooldown: float = 0.0,
    procs: int = 1,
    state: dict[str, bool] | None = None,
) -> int:
    """Run the stage's missing cells; append each finished run to sweep.jsonl. Returns runs done."""
    state = state if state is not None else {"ran": False}
    done_keys = {
        (ln["stage"], ln["strategy"], ln["n"], ln["stalled"], ln["repeat"]) for ln in lines
    }
    pending = [c for c in stage_cells(plan, stage) if c.key not in done_keys]
    for i, cell in enumerate(pending, 1):
        if state["ran"] and cooldown > 0:
            time.sleep(cooldown)
        print(f"[{stage} {i}/{len(pending)}] {cell.label()}", flush=True)
        result, path = run_fn(cell_config(plan, cell, out_dir, procs))
        line = sweep_line(plan, cell, result, path, out_dir)
        append_line(out_dir / "sweep.jsonl", line)
        lines.append(line)
        state["ran"] = True
    return len(pending)


def run_sweep(
    plan: Plan,
    out_dir: Path,
    *,
    run_fn: RunFn = run_bench,
    cooldown: float = 0.0,
    procs: int = 1,
    log: Callable[[str], None] = print,
) -> list[dict[str, Any]]:
    """The matrix, then escalation steps while naive keeps passing. Returns all lines."""
    lines = read_sweep(out_dir / "sweep.jsonl")
    state = {"ran": False}
    kw = {"run_fn": run_fn, "cooldown": cooldown, "procs": procs, "state": state}
    run_stage(plan, MATRIX_STAGE, lines, out_dir, **kw)
    stages = plan.stages()
    for previous, stage in itertools.pairwise(stages):
        naive = stage_results(lines, plan, previous)["naive"]
        if naive.passes is not True:
            why = "had no valid run" if naive.passes is None else "failed"
            log(f"naive {why} at {previous}: escalation ends")
            break
        log(f"naive passed {previous}: running {stage}")
        run_stage(plan, stage, lines, out_dir, **kw)
    return lines


def add_sweep_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--plan", default=DEFAULT_PLAN, help="pre-registered plan (TOML)")
    p.add_argument("--out", default="results", help="holds sweep.jsonl and runs/")
    p.add_argument("--cooldown", type=float, default=5.0, help="seconds between runs")
    p.add_argument("--procs", type=int, default=min(4, os.cpu_count() or 1))
    p.add_argument("--dry-run", action="store_true", help="list the cells, run nothing")
    p.set_defaults(func=sweep_main)


def sweep_main(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    try:
        plan = load_plan(args.plan)
    except PlanError as exc:
        print(f"wslive sweep: {exc}", file=sys.stderr)
        return 2
    lines = read_sweep(out_dir / "sweep.jsonl")
    other = foreign_hashes(lines, plan)
    if other:
        print(
            f"wslive sweep: {out_dir / 'sweep.jsonl'} holds runs from plan(s) "
            f"{sorted(map(str, other))}; this plan hashes to {plan.hash}. Move that file away "
            "to start a new sweep.",
            file=sys.stderr,
        )
        return 2
    if args.dry_run:
        done = {(ln["stage"], ln["strategy"], ln["n"], ln["stalled"], ln["repeat"]) for ln in lines}
        print(f"plan {plan.hash}: {len(lines)} runs already recorded")
        for stage in plan.stages():
            cells = stage_cells(plan, stage)
            note = "" if stage == MATRIX_STAGE else " (only while naive passes the previous stage)"
            print(f"{stage}: {len(cells)} cells{note}")
            for c in cells:
                print(f"  {'done   ' if c.key in done else 'pending'} {c.label()}")
        return 0
    try:
        run_sweep(plan, out_dir, cooldown=args.cooldown, procs=args.procs)
    except HarnessError as exc:
        print(f"wslive sweep: harness failure: {exc}", file=sys.stderr)
        return 1
    print(f"sweep finished; render with: wslive report --sweep {out_dir / 'sweep.jsonl'}")
    return 0
