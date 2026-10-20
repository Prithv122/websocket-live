"""`wslive sweep`: run the pre-registered matrix from `eval_plan.toml`.

Run order: the cells of a stage are listed in canonical order (arm, n, stalled, repeat) and then
shuffled with `random.Random(order_seed + stage index)`, so slow drift of the machine (thermals,
background load) is spread across arms instead of landing on whichever arm ran last. The order is
a pure function of the plan, which is what makes the sweep resumable: a cell is skipped when
`results/sweep.jsonl` already holds a line for it under the same plan hash.

Stages: `matrix` first. Each escalation step (`ladder-k`) re-runs only the primary cell, both
arms, and happens only while BOTH arms passed the previous stage; the first step where naive fails
(or queued does not pass) ends the ladder.

A run that ends in a harness error leaves a line with `status: "harness_error"`. It counts as an
invalid run for its cell and repeat and is not re-run on resume.

Resume safety: the sweep refuses to start on a dirty tree (changes under `results/` do not
count) and refuses to resume when the recorded runs were made at another commit, on a dirty tree,
or with another `--procs`; a mixed sweep could not be attributed to one version of the code.
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
from datetime import UTC, datetime
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
from wslive.bench.runner import (
    BenchConfig,
    HarnessError,
    collect_environment,
    git_state,
    run_bench,
    stalled_count,
    summary_metrics,
)

RunFn = Callable[[BenchConfig], tuple[dict[str, Any], Path]]
ENV_KEYS = (
    "cpu_model",
    "cpu_count_logical",
    "platform",
    "python",
    "packages",
    "git_sha",
    "git_dirty",
)


class SweepRefused(RuntimeError):
    """The sweep must not run (or continue) in the current state; the message says why."""


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
        "status": "ok",
        "invalid_reasons": result.get("invalid_reasons", []),
        "run_file": path.resolve().relative_to(out_dir.resolve()).as_posix(),
        "metrics": summary_metrics(result),
        "config": {
            k: cfg[k]
            for k in ("rate", "seconds", "warmup", "drain", "queue_size", "procs", "procs_used")
        },
        "env": {k: env.get(k) for k in ENV_KEYS},
    }


def error_line(plan: Plan, cell: Cell, exc: Exception, procs: int) -> dict[str, Any]:
    """The trace of a run that ended in a harness error: invalid for its cell, never re-run."""
    text = str(exc)
    env = collect_environment()
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
        "started_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "status": "harness_error",
        "error": text,
        "invalid_reasons": ["harness error: " + (text.splitlines() or [""])[0][:300]],
        "metrics": {},
        "config": {"procs": procs},
        "env": {k: env.get(k) for k in ENV_KEYS},
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
    state: dict[str, Any] | None = None,
    guard: Callable[[], None] | None = None,
) -> int:
    """Run the stage's missing cells; append each finished run to sweep.jsonl. Returns runs done.

    `guard`, when given, is called before every cell and may raise SweepRefused.
    """
    state = state if state is not None else {"ran": False, "errors": 0}
    done_keys = {
        (ln["stage"], ln["strategy"], ln["n"], ln["stalled"], ln["repeat"]) for ln in lines
    }
    pending = [c for c in stage_cells(plan, stage) if c.key not in done_keys]
    for i, cell in enumerate(pending, 1):
        if guard is not None:
            guard()
        if state["ran"] and cooldown > 0:
            time.sleep(cooldown)
        print(f"[{stage} {i}/{len(pending)}] {cell.label()}", flush=True)
        try:
            result, path = run_fn(cell_config(plan, cell, out_dir, procs))
            line = sweep_line(plan, cell, result, path, out_dir)
        except HarnessError as exc:
            print(f"harness error, recorded as an invalid run: {exc}", file=sys.stderr)
            line = error_line(plan, cell, exc, procs)
            state["errors"] = state.get("errors", 0) + 1
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
    guard: Callable[[], None] | None = None,
    state: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """The matrix, then escalation steps while both arms keep passing. Returns all lines."""
    lines = read_sweep(out_dir / "sweep.jsonl")
    state = state if state is not None else {"ran": False, "errors": 0}
    kw = {"run_fn": run_fn, "cooldown": cooldown, "procs": procs, "state": state, "guard": guard}
    run_stage(plan, MATRIX_STAGE, lines, out_dir, **kw)
    stages = plan.stages()
    for previous, stage in itertools.pairwise(stages):
        res = stage_results(lines, plan, previous)
        if not all(r.passes is True for r in res.values()):
            how = ", ".join(f"{arm} {_describe(r.passes)}" for arm, r in sorted(res.items()))
            log(f"escalation ends at {previous} ({how}); it needs both arms to pass")
            break
        log(f"both arms passed {previous}: running {stage}")
        run_stage(plan, stage, lines, out_dir, **kw)
    return lines


def _describe(passes: bool | None) -> str:
    return "has no valid run" if passes is None else "passed" if passes else "failed"


def resume_problems(
    lines: list[dict[str, Any]], procs: int, sha: str | None, dirty: bool | None
) -> list[str]:
    """Reasons the sweep may not start or resume in this state (empty list: fine)."""
    problems = []
    if dirty is None or sha is None:
        problems.append("cannot read the git state (is this a git checkout?)")
    elif dirty:
        problems.append("the working tree has uncommitted changes (results/ is ignored)")
    shas = {(ln.get("env") or {}).get("git_sha") for ln in lines}
    if lines and shas != {sha}:
        found = ", ".join(sorted(str(s)[:10] for s in shas))
        problems.append(
            f"recorded runs were made at commit {found}, the tree is at {str(sha)[:10]}"
        )
    if any((ln.get("env") or {}).get("git_dirty") is not False for ln in lines):
        problems.append("some recorded runs were made on a dirty tree")
    used = {(ln.get("config") or {}).get("procs") for ln in lines}
    if lines and used != {procs}:
        problems.append(f"recorded runs used procs={sorted(map(str, used))}, this run has {procs}")
    return problems


def make_guard(sha: str | None) -> Callable[[], None]:
    """A check, run before every cell, that the tree is still clean and at the starting commit."""

    def guard() -> None:
        now_sha, dirty = git_state()
        if now_sha != sha or dirty is not False:
            raise SweepRefused(
                "the working tree changed during the sweep (commit moved or uncommitted changes); "
                "runs recorded so far are kept"
            )

    return guard


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
            note = (
                "" if stage == MATRIX_STAGE else " (only while both arms pass the previous stage)"
            )
            print(f"{stage}: {len(cells)} cells{note}")
            for c in cells:
                print(f"  {'done   ' if c.key in done else 'pending'} {c.label()}")
        return 0
    sha, dirty = git_state()
    problems = resume_problems(lines, args.procs, sha, dirty)
    if problems:
        print("wslive sweep: refusing to run:\n  " + "\n  ".join(problems), file=sys.stderr)
        return 2
    state = {"ran": False, "errors": 0}
    try:
        run_sweep(
            plan,
            out_dir,
            cooldown=args.cooldown,
            procs=args.procs,
            guard=make_guard(sha),
            state=state,
        )
    except SweepRefused as exc:
        print(f"wslive sweep: {exc}", file=sys.stderr)
        return 2
    if state["errors"]:
        print(
            f"wslive sweep: {state['errors']} run(s) ended in harness errors; they are recorded "
            "as invalid runs and will not be re-run",
            file=sys.stderr,
        )
        return 1
    print(f"sweep finished; render with: wslive report --sweep {out_dir / 'sweep.jsonl'}")
    return 0
