import math
import random

import pytest

from wslive.bench import stats

INF = math.inf


def test_percentile_is_nearest_rank_not_interpolated() -> None:
    sample = [float(x) for x in range(1, 11)]
    assert stats.percentile(sample, 50) == 5.0  # rank ceil(0.5 * 10) = 5
    assert stats.percentile(sample, 90) == 9.0
    assert stats.percentile(sample, 95) == 10.0  # rank ceil(9.5) = 10
    assert stats.percentile(sample, 100) == 10.0
    assert stats.percentile(sample, 0) == 1.0
    assert stats.percentile(sample, 10) == 1.0


def test_percentile_sorts_unless_told_otherwise() -> None:
    assert stats.percentile([3.0, 1.0, 2.0], 50) == 2.0
    assert stats.percentile([3.0, 1.0, 2.0], 50, presorted=True) == 1.0  # caller's problem


def test_percentile_single_sample_and_bad_input() -> None:
    assert stats.percentile([7.0], 99) == 7.0
    with pytest.raises(ValueError, match="empty"):
        stats.percentile([], 50)
    with pytest.raises(ValueError, match="q must"):
        stats.percentile([1.0], 101)
    with pytest.raises(ValueError, match="NaN"):
        stats.percentile([1.0, math.nan], 50)


def test_missing_messages_make_the_percentile_infinite_past_the_cutoff() -> None:
    finite = [float(x) for x in range(1, 96)]
    assert stats.percentile(finite + [INF] * 5, 95) == 95.0  # exactly 5% missing: still finite
    assert stats.percentile(finite[:-1] + [INF] * 6, 95) == INF  # 6% missing
    assert stats.percentile(finite + [INF] * 5, 99) == INF
    assert stats.percentile([INF] * 3, 0) == INF


def test_summarize_counts_and_means_only_the_finite() -> None:
    s = stats.summarize([1.0, 2.0, 3.0, INF])
    assert (s["count"], s["finite"]) == (4, 3)
    assert s["mean_finite"] == 2.0
    assert (s["min"], s["max"], s["p50"], s["p99"]) == (1.0, INF, 2.0, INF)
    assert stats.summarize([])["count"] == 0
    assert stats.summarize([INF])["mean_finite"] is None


def test_delivery_ratio() -> None:
    assert stats.delivery_ratio(99, 100) == 0.99
    assert stats.delivery_ratio(0, 5) == 0.0
    with pytest.raises(ValueError):
        stats.delivery_ratio(1, 0)
    with pytest.raises(ValueError):
        stats.delivery_ratio(-1, 5)


def test_histogram_bucket_bounds() -> None:
    h = stats.histogram([0.05, 0.1, 0.1019, 0.1021, 119_999.0, 120_000.0, 1e9, INF])
    assert h["below"] == 1  # 0.05 < lo
    assert h["overflow"] == 2  # at hi and above
    assert h["missing"] == 1
    assert h["total"] == 8
    assert h["counts"]["0"] == 2  # [0.1, 0.102) holds 0.1 and 0.1019
    assert h["counts"]["1"] == 1
    assert sum(h["counts"].values()) + h["below"] + h["overflow"] + h["missing"] == 8
    assert h["buckets"] == 707


def test_every_sample_lies_inside_its_bucket() -> None:
    rng = random.Random(1)
    samples = [10 ** rng.uniform(-0.9, 5.07) for _ in range(5000)]
    h = stats.histogram(samples)
    lo, g = h["lo"], h["growth"]
    # Re-bucket independently and compare the counts bucket by bucket.
    expected: dict[int, int] = {}
    for x in samples:
        if x < lo or x >= h["hi"]:
            continue
        i = math.floor(math.log(x / lo, g))
        # float log can be off by one at an edge; fix against the exact edges
        while lo * g ** (i + 1) <= x:
            i += 1
        while lo * g**i > x:
            i -= 1
        expected[i] = expected.get(i, 0) + 1
    assert {int(k): v for k, v in h["counts"].items()} == expected


@pytest.mark.parametrize("q", [1, 50, 90, 95, 99, 99.9])
def test_histogram_rederives_percentiles_within_two_percent(q: float) -> None:
    rng = random.Random(7)
    samples = [rng.lognormvariate(3.0, 1.2) for _ in range(20_000)] + [INF] * 30
    exact = stats.percentile(samples, q)
    approx = stats.histogram_percentile(stats.histogram(samples), q)
    if math.isinf(exact):
        assert math.isinf(approx)
    else:
        assert exact <= approx <= exact * 1.02 * (1 + 1e-12)


def test_histogram_percentile_reports_infinity_for_missing_and_overflow() -> None:
    h = stats.histogram([1.0] * 90 + [INF] * 10)
    assert stats.histogram_percentile(h, 90) < 1.03
    assert math.isinf(stats.histogram_percentile(h, 91))
    h2 = stats.histogram([1.0] * 50 + [500_000.0] * 50)
    assert math.isinf(stats.histogram_percentile(h2, 75))
    h3 = stats.histogram([0.01, 0.01, 5.0])
    assert stats.histogram_percentile(h3, 50) == h3["lo"]  # below the first edge
    with pytest.raises(ValueError):
        stats.histogram_percentile(stats.histogram([]), 50)


def test_histogram_rejects_bad_parameters() -> None:
    with pytest.raises(ValueError):
        stats.histogram([1.0], lo=10, hi=1)
    with pytest.raises(ValueError):
        stats.histogram([1.0], growth=1.0)


def test_json_safe_turns_inf_and_nan_into_none() -> None:
    out = stats.json_safe({"a": INF, "b": [1.0, math.nan], "c": {"d": 2}, "e": (3.0,)})
    assert out == {"a": None, "b": [1.0, None], "c": {"d": 2}, "e": [3.0]}
