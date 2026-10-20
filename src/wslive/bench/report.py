"""The pre-registered plan, its mechanical verdict, and `wslive report`.

`eval_plan.toml` is parsed into a `Plan`; `primary_verdict` applies the plan's rule to the sweep
lines and nothing else, so the same lines and plan always give the same verdict and the report
renders without any other data.

Rule, per stage (the matrix first, then each escalation step in order), at the primary cell:
  * a run is valid when every worker's event-loop lag p99 is within the plan's limit;
  * an arm passes when the median p95 (from the schedule) of its valid runs is within the budget
    and the median delivery ratio is at least the minimum; a missing message counts as an
    infinite latency, so p95 is infinite once more than 5% are missing.
  * queued fails -> NOT SUPPORTED; queued passes and naive fails -> SUPPORTED (under escalation
    when the stage is a ladder step); both pass -> go to the next stage; no stage left -> NULL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from wslive.bench.runner import stalled_count

PLACEHOLDER = "TBD"
MATRIX_STAGE = "matrix"
DEFAULT_PLAN = "eval_plan.toml"
DEFAULT_SWEEP = "results/sweep.jsonl"
DEFAULT_REPORT = "results/REPORT.md"


class PlanError(ValueError):
    """The plan file is unreadable, incomplete or still has placeholders."""


@dataclass(frozen=True)
class LadderStep:
    pad_bytes: int
    rcvbuf: int  # 0 = OS default


@dataclass(frozen=True)
class Plan:
    raw: dict[str, Any]
    hash: str
    arms: tuple[str, ...]
    n_values: tuple[int, ...]
    shares: tuple[float, ...]
    repeats: int
    order_seed: int
    feed_seed: int
    rate: float
    seconds: float
    warmup: float
    drain: float
    pad_bytes: int
    rcvbuf: int  # 0 = OS default; applies to the stalled clients only
    queue_size: int
    primary_n: int
    primary_share: float
    p95_budget_ms: float
    min_delivery_ratio: float
    loop_lag_limit_ms: float
    ladder: tuple[LadderStep, ...]

    @property
    def primary_stalled(self) -> int:
        return stalled_count(self.primary_n, self.primary_share)

    def stages(self) -> list[str]:
        return [MATRIX_STAGE, *(f"ladder-{i}" for i in range(1, len(self.ladder) + 1))]

    def stage_params(self, stage: str) -> tuple[int, int]:
        """(pad_bytes, rcvbuf) used by a stage."""
        if stage == MATRIX_STAGE:
            return self.pad_bytes, self.rcvbuf
        step = self.ladder[int(stage.split("-")[1]) - 1]
        return step.pad_bytes, step.rcvbuf


def plan_hash(raw: dict[str, Any]) -> str:
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:12]


def find_placeholders(value: Any, path: str = "") -> list[str]:
    """Paths of every string in the plan that is still the TBD placeholder."""
    found: list[str] = []
    if isinstance(value, dict):
        for k, v in value.items():
            found += find_placeholders(v, f"{path}.{k}" if path else k)
    elif isinstance(value, list):
        for i, v in enumerate(value):
            found += find_placeholders(v, f"{path}[{i}]")
    elif isinstance(value, str) and value.strip().upper().startswith(PLACEHOLDER):
        found.append(path)
    return found


def _num(raw: dict[str, Any], section: str, key: str, *, integer: bool = False) -> Any:
    try:
        value = raw[section][key]
    except KeyError:
        raise PlanError(f"missing [{section}] {key}") from None
    kinds = (int,) if integer else (int, float)
    if isinstance(value, bool) or not isinstance(value, kinds):
        raise PlanError(f"[{section}] {key} must be a number, got {value!r}")
    return value


def load_plan(path: str | Path) -> Plan:
    try:
        raw = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PlanError(f"cannot read plan {path}: {exc}") from exc
    placeholders = find_placeholders(raw)
    if placeholders:
        raise PlanError(
            f"{path} still has {PLACEHOLDER} placeholders: {', '.join(placeholders)}. "
            "The owner must fill them before any run."
        )
    try:
        arms = tuple(raw["matrix"]["arms"])
        n_values = tuple(raw["matrix"]["n"])
        shares = tuple(raw["matrix"]["stalled_shares"])
        ladder = tuple(
            LadderStep(int(s["pad_bytes"]), int(s["rcvbuf"])) for s in raw.get("escalation", [])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise PlanError(f"malformed plan: {exc!r}") from exc
    plan = Plan(
        raw=raw,
        hash=plan_hash(raw),
        arms=arms,
        n_values=n_values,
        shares=shares,
        repeats=_num(raw, "matrix", "repeats", integer=True),
        order_seed=_num(raw, "matrix", "order_seed", integer=True),
        feed_seed=_num(raw, "run", "feed_seed", integer=True),
        rate=_num(raw, "run", "rate"),
        seconds=_num(raw, "run", "seconds"),
        warmup=_num(raw, "run", "warmup"),
        drain=_num(raw, "run", "drain"),
        pad_bytes=_num(raw, "run", "pad_bytes", integer=True),
        rcvbuf=_num(raw, "run", "rcvbuf", integer=True),
        queue_size=_num(raw, "run", "queue_size", integer=True),
        primary_n=_num(raw, "primary", "n", integer=True),
        primary_share=_num(raw, "primary", "stalled_share"),
        p95_budget_ms=_num(raw, "primary", "p95_budget_ms"),
        min_delivery_ratio=_num(raw, "primary", "min_delivery_ratio"),
        loop_lag_limit_ms=_num(raw, "primary", "loop_lag_limit_ms"),
        ladder=ladder,
    )
    problems = []
    if sorted(plan.arms) != ["naive", "queued"]:
        problems.append("arms must be exactly naive and queued")
    if plan.repeats < 1:
        problems.append("repeats must be at least 1")
    if plan.primary_n not in plan.n_values or plan.primary_share not in plan.shares:
        problems.append("the primary cell must be one of the matrix cells")
    if not 0 < plan.min_delivery_ratio <= 1:
        problems.append("min_delivery_ratio must be in (0, 1]")
    if plan.p95_budget_ms <= 0 or plan.loop_lag_limit_ms <= 0:
        problems.append("p95_budget_ms and loop_lag_limit_ms must be positive")
    if problems:
        raise PlanError("; ".join(problems))
    return plan


# --- sweep lines ------------------------------------------------------------------------------


def read_sweep(path: str | Path) -> list[dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def foreign_hashes(lines: list[dict[str, Any]], plan: Plan) -> set[str]:
    return {ln.get("plan_hash") for ln in lines} - {plan.hash}


def _inf(x: float | None) -> float:
    return math.inf if x is None else x


def is_harness_error(line: dict[str, Any]) -> bool:
    return line.get("status") == "harness_error"


def invalid_reasons(line: dict[str, Any], plan: Plan) -> list[str]:
    """Every reason a run is excluded: its recorded integrity failures plus loop lag."""
    reasons = list(line.get("invalid_reasons") or [])
    if is_harness_error(line):
        return reasons or ["harness error"]
    lag = line["metrics"].get("loop_lag_p99_ms")
    if lag is None:
        reasons.append("no loop-lag samples")
    elif lag > plan.loop_lag_limit_ms:
        reasons.append(f"loop-lag p99 {lag:.1f} ms exceeds {plan.loop_lag_limit_ms:g} ms")
    return reasons


def is_valid(line: dict[str, Any], plan: Plan) -> bool:
    return not invalid_reasons(line, plan)


@dataclass(frozen=True)
class ArmResult:
    runs: int
    valid: int
    p95_median: float
    p95_min: float
    p95_max: float
    delivery_median: float
    passes: bool | None  # None: valid runs are not a majority, so no verdict


def arm_result(runs: list[dict[str, Any]], plan: Plan) -> ArmResult:
    valid = [r for r in runs if is_valid(r, plan)]
    if not valid:
        nan = math.nan
        return ArmResult(len(runs), 0, nan, nan, nan, nan, None)
    p95 = [_inf(r["metrics"]["p95_ms"]) for r in valid]
    delivery = [r["metrics"]["delivery_ratio"] for r in valid]
    med_p95, med_dr = statistics.median(p95), statistics.median(delivery)
    passes = med_p95 <= plan.p95_budget_ms and med_dr >= plan.min_delivery_ratio
    # Pre-registered: a verdict needs a majority of valid runs (2 of 3), never a lone survivor.
    if 2 * len(valid) <= len(runs):
        passes = None
    return ArmResult(len(runs), len(valid), med_p95, min(p95), max(p95), med_dr, passes)


def cell_runs(
    lines: list[dict[str, Any]], stage: str, n: int, stalled: int, strategy: str
) -> list[dict[str, Any]]:
    return [
        ln
        for ln in lines
        if ln["stage"] == stage
        and ln["n"] == n
        and ln["stalled"] == stalled
        and ln["strategy"] == strategy
    ]


def stage_results(lines: list[dict[str, Any]], plan: Plan, stage: str) -> dict[str, ArmResult]:
    return {
        arm: arm_result(cell_runs(lines, stage, plan.primary_n, plan.primary_stalled, arm), plan)
        for arm in plan.arms
    }


@dataclass(frozen=True)
class Verdict:
    label: str
    text: str


def primary_verdict(lines: list[dict[str, Any]], plan: Plan) -> Verdict:
    """Apply the plan's rule (module docstring) to the sweep lines."""
    for stage in plan.stages():
        res = stage_results(lines, plan, stage)
        if any(r.runs < plan.repeats for r in res.values()):
            return Verdict(
                "INCOMPLETE", f"stage '{stage}' has fewer than {plan.repeats} runs per arm"
            )
        if any(r.passes is None for r in res.values()):
            return Verdict(
                "INVALID", f"stage '{stage}': an arm has too few valid runs at the primary cell"
            )
        if not res["queued"].passes:
            return Verdict("NOT SUPPORTED", f"queued missed the budget or delivery floor ({stage})")
        if not res["naive"].passes:
            if stage == MATRIX_STAGE:
                return Verdict("SUPPORTED", "queued passed and naive failed at the primary cell")
            return Verdict(
                "SUPPORTED UNDER ESCALATION",
                f"naive passed the primary cell but failed at {stage}; queued passed",
            )
    return Verdict("NULL", "naive did not break within the tested conditions")


# --- rendering --------------------------------------------------------------------------------


def _ms(x: float) -> str:
    return "n/a" if math.isnan(x) else "inf" if math.isinf(x) else f"{x:.1f}"


def _yn(passes: bool | None) -> str:
    return "n/a" if passes is None else "pass" if passes else "FAIL"


def _median(values: list[float]) -> float:
    return statistics.median(values) if values else math.nan


def _row_stats(runs: list[dict[str, Any]], plan: Plan) -> dict[str, Any]:
    valid = [r for r in runs if is_valid(r, plan)]

    def med(key: str) -> float:
        return _median([_inf(r["metrics"][key]) for r in valid])

    arm = arm_result(runs, plan)
    return {
        "valid": f"{len(valid)}/{len(runs)}",
        "p50": med("p50_ms"),
        "p95": arm.p95_median,
        "range": f"{_ms(arm.p95_min)}-{_ms(arm.p95_max)}",
        "p99": med("p99_ms"),
        "p95_ts": med("p95_ts_ms"),
        "delivery": arm.delivery_median,
        "gaps": med("gaps_seen"),
        "dropped": med("stalled_dropped_total"),
        "rss": med("rss_max_bytes") / 2**20,
        "lag": max(
            (_inf(r["metrics"]["loop_lag_p99_ms"]) for r in runs if r["metrics"]),
            default=math.nan,
        ),
    }


def render_report(lines: list[dict[str, Any]], plan: Plan) -> str:
    verdict = primary_verdict(lines, plan)
    out: list[str] = ["# Slow-consumer sweep report", ""]
    out += [
        f"Plan hash `{plan.hash}` - {len(lines)} runs - results are properties of a simulator on "
        "the machine below, not of production.",
        "",
        f"## Verdict: {verdict.label}",
        "",
        f"{verdict.text}.",
        "",
        f"Rule: at the primary cell (n={plan.primary_n}, {plan.primary_stalled} stalled = "
        f"{plan.primary_share:.0%}) an arm passes when the median p95 delivery latency from the "
        f"scheduled publish time is at most {plan.p95_budget_ms:g} ms and the median delivery "
        f"ratio is at least {plan.min_delivery_ratio:g}. Missing messages count as infinite "
        f"latency. A run is invalid when a load-generator worker's event-loop lag p99 exceeds "
        f"{plan.loop_lag_limit_ms:g} ms, when it did not have the stalled and healthy clients it "
        "intended, or when it ended in a harness error; invalid runs are excluded from the "
        "medians.",
        "",
        "## Primary cell",
        "",
        "| stage | pad bytes | stalled rcvbuf | arm | valid | p95 ms (min-max) | delivery "
        "| result |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for stage in plan.stages():
        pad, rcv = plan.stage_params(stage)
        res = stage_results(lines, plan, stage)
        if all(r.runs == 0 for r in res.values()):
            out.append(f"| {stage} | {pad} | {rcv or 'default'} | - | not run | - | - | - |")
            continue
        for arm in plan.arms:
            r = res[arm]
            out.append(
                f"| {stage} | {pad} | {rcv or 'default'} | {arm} | {r.valid}/{r.runs} | "
                f"{_ms(r.p95_median)} ({_ms(r.p95_min)}-{_ms(r.p95_max)}) | "
                f"{r.delivery_median:.4f} | {_yn(r.passes)} |"
            )
    out += [
        "",
        f"## Matrix ({MATRIX_STAGE} stage)",
        "",
        "Medians over valid runs; `lag` is the worst worker loop-lag p99 over all runs of the row.",
        "",
        "| n | stalled | arm | valid | p50 ms | p95 ms | p95 range | p99 ms | delivery | gaps | "
        "stalled dropped | server RSS MiB | lag ms |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for n in plan.n_values:
        for share in plan.shares:
            stalled = stalled_count(n, share)
            for arm in plan.arms:
                runs = cell_runs(lines, MATRIX_STAGE, n, stalled, arm)
                if not runs:
                    out.append(f"| {n} | {stalled} | {arm} | 0/0 |" + " - |" * 9)
                    continue
                s = _row_stats(runs, plan)
                out.append(
                    f"| {n} | {stalled} | {arm} | {s['valid']} | {_ms(s['p50'])} | "
                    f"{_ms(s['p95'])} | "
                    f"{s['range']} | {_ms(s['p99'])} | {s['delivery']:.4f} | {s['gaps']:g} | "
                    f"{s['dropped']:g} | {s['rss']:.1f} | {_ms(s['lag'])} |"
                )
    out += [
        "",
        "## Secondary: p95 from the server's own stamp (descriptive)",
        "",
        "Latency from the envelope `ts` instead of the schedule. A publisher that falls behind "
        "stamps late, so this clock hides its delay; the verdict never uses it.",
        "",
        "| n | stalled | naive p95 ms | queued p95 ms |",
        "|---|---|---|---|",
    ]
    for n in plan.n_values:
        for share in plan.shares:
            stalled = stalled_count(n, share)
            cols = []
            for arm in plan.arms:
                runs = cell_runs(lines, MATRIX_STAGE, n, stalled, arm)
                cols.append(_ms(_row_stats(runs, plan)["p95_ts"]) if runs else "-")
            out.append(f"| {n} | {stalled} | {' | '.join(cols)} |")
    invalid = [ln for ln in lines if not is_valid(ln, plan)]
    out += ["", "## Invalid runs", ""]
    if invalid:
        out += [
            "| stage | arm | n | stalled | repeat | reasons | file |",
            "|---|---|---|---|---|---|---|",
        ]
        for ln in invalid:
            reasons = "; ".join(invalid_reasons(ln, plan)).replace("|", "/").replace(chr(10), " ")
            out.append(
                f"| {ln['stage']} | {ln['strategy']} | {ln['n']} | {ln['stalled']} | "
                f"{ln['repeat']} | {reasons} | {ln.get('run_file', '')} |"
            )
    else:
        out.append("None.")
    out += ["", "## Environment", ""]
    envs = {json.dumps(ln.get("env", {}), sort_keys=True) for ln in lines}
    for text in sorted(envs):
        env = json.loads(text)
        pk = ", ".join(f"{k} {v}" for k, v in env.get("packages", {}).items())
        out.append(
            f"- {env.get('cpu_model', '?')} ({env.get('cpu_count_logical', '?')} logical cores), "
            f"{env.get('platform', '?')}, Python {env.get('python', '?')}; {pk}; commit "
            f"`{str(env.get('git_sha'))[:10]}`{' (dirty tree)' if env.get('git_dirty') else ''}"
        )
    if not envs:
        out.append("No runs yet.")
    return "\n".join(out) + "\n"


# --- command line -----------------------------------------------------------------------------


def add_report_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--plan", default=DEFAULT_PLAN)
    p.add_argument("--sweep", default=DEFAULT_SWEEP)
    p.add_argument("--out", default=DEFAULT_REPORT)
    p.set_defaults(func=report_main)


def report_main(args: argparse.Namespace) -> int:
    try:
        plan = load_plan(args.plan)
    except PlanError as exc:
        print(f"wslive report: {exc}", file=sys.stderr)
        return 2
    lines = read_sweep(args.sweep)
    other = foreign_hashes(lines, plan)
    if other:
        print(
            f"wslive report: {args.sweep} has runs from plan(s) {sorted(map(str, other))}, "
            f"but the plan file hashes to {plan.hash}. Refusing to mix them.",
            file=sys.stderr,
        )
        return 2
    if not lines:
        print(f"wslive report: no runs in {args.sweep}", file=sys.stderr)
        return 2
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_report(lines, plan), encoding="utf-8")
    print(f"{primary_verdict(lines, plan).label}\n{out.resolve()}")
    return 0
