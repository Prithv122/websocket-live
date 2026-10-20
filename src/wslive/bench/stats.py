"""Pure statistics for the benchmark: nearest-rank percentiles, a log histogram, delivery ratio.

Samples are plain floats in one unit (the runner uses milliseconds). A message that never
arrived is `math.inf`, so a percentile is infinite as soon as more than (100 - q)% of the
expected messages are missing. JSON has no infinity: `json_safe` turns inf and nan into None,
and readers treat a missing percentile as infinite.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from typing import Any

HIST_LO = 0.1
HIST_HI = 120_000.0
HIST_GROWTH = 1.02


def percentile(samples: Sequence[float], q: float, *, presorted: bool = False) -> float:
    """Nearest-rank percentile: the value at 1-based rank ceil(q/100 * n) of the sorted sample.

    Always returns an element of the sample, never an interpolation, so an infinite (missing)
    sample is reported as such instead of being averaged away. q is in [0, 100]; q = 0 gives the
    minimum. An empty sample or a NaN raises ValueError.
    """
    if not 0 <= q <= 100:
        raise ValueError(f"q must be in [0, 100], got {q}")
    if not samples:
        raise ValueError("percentile of an empty sample")
    ordered = samples if presorted else sorted(samples)
    if math.isnan(ordered[0]) or math.isnan(ordered[-1]):
        raise ValueError("NaN in sample")
    rank = max(1, math.ceil(q / 100 * len(ordered)))
    return ordered[rank - 1]


def summarize(samples: Sequence[float]) -> dict[str, Any]:
    """count, finite count, min, p50/p95/p99, max and the mean of the finite samples."""
    if not samples:
        return {"count": 0, "finite": 0, "min": None, "p50": None, "p95": None, "p99": None,
                "max": None, "mean_finite": None}  # fmt: skip
    ordered = sorted(samples)
    finite = [x for x in ordered if math.isfinite(x)]
    return {
        "count": len(ordered),
        "finite": len(finite),
        "min": ordered[0],
        "p50": percentile(ordered, 50, presorted=True),
        "p95": percentile(ordered, 95, presorted=True),
        "p99": percentile(ordered, 99, presorted=True),
        "max": ordered[-1],
        "mean_finite": math.fsum(finite) / len(finite) if finite else None,
    }


def delivery_ratio(received: int, expected: int) -> float:
    """received / expected; expected must be positive (a run that expects nothing is invalid)."""
    if expected <= 0:
        raise ValueError("expected must be positive")
    if received < 0:
        raise ValueError("received must not be negative")
    return received / expected


def _edge(hist: dict[str, Any], index: int) -> float:
    return hist["lo"] * hist["growth"] ** index


def _bucket_index(hist: dict[str, Any], x: float) -> int:
    """Bucket i covers [lo * g^i, lo * g^(i+1)); the log estimate is corrected against the edges."""
    i = int(math.log(x / hist["lo"]) / math.log(hist["growth"]))
    while _edge(hist, i + 1) <= x:
        i += 1
    while i > 0 and _edge(hist, i) > x:
        i -= 1
    return i


def histogram(
    samples: Iterable[float],
    *,
    lo: float = HIST_LO,
    hi: float = HIST_HI,
    growth: float = HIST_GROWTH,
) -> dict[str, Any]:
    """Log-spaced histogram, JSON-ready.

    Regular buckets are stored sparsely as {index: count} with bucket i covering
    [lo * growth^i, lo * growth^(i+1)). Values below `lo` go to `below`, values at or above `hi`
    to `overflow`, and infinite (never delivered) samples to `missing`.
    """
    if not (0 < lo < hi and growth > 1):
        raise ValueError("need 0 < lo < hi and growth > 1")
    hist: dict[str, Any] = {
        "lo": lo,
        "hi": hi,
        "growth": growth,
        "buckets": math.ceil(math.log(hi / lo) / math.log(growth)),
        "counts": {},
        "below": 0,
        "overflow": 0,
        "missing": 0,
        "total": 0,
    }
    counts: dict[int, int] = {}
    for x in samples:
        hist["total"] += 1
        if math.isinf(x) and x > 0:
            hist["missing"] += 1
        elif x < lo:
            hist["below"] += 1
        elif x >= hi:
            hist["overflow"] += 1
        else:
            i = _bucket_index(hist, x)
            counts[i] = counts.get(i, 0) + 1
    hist["counts"] = {str(i): counts[i] for i in sorted(counts)}
    return hist


def histogram_percentile(hist: dict[str, Any], q: float) -> float:
    """Approximate nearest-rank percentile from a histogram: the upper edge of the bucket
    holding that rank. At most `growth` times the exact value (2% by default) and never below it.
    Ranks in `overflow` or `missing` come back as inf."""
    total = hist["total"]
    if total <= 0:
        raise ValueError("percentile of an empty histogram")
    rank = max(1, math.ceil(q / 100 * total))
    seen = hist["below"]
    if rank <= seen:
        return hist["lo"]
    for index in sorted(int(k) for k in hist["counts"]):
        seen += hist["counts"][str(index)]
        if rank <= seen:
            return _edge(hist, index + 1)
    return math.inf


def json_safe(value: Any) -> Any:
    """Recursively replace inf and nan by None so the result serialises as strict JSON."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return value
