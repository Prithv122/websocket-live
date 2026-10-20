"""Short real-socket run of the whole harness: server subprocess, worker process, stalled client."""

import argparse
import json
from pathlib import Path

import pytest

pytest.importorskip("wslive.app")

from wslive.bench import runner

pytestmark = pytest.mark.bench_smoke


@pytest.mark.parametrize("strategy", ["queued", "naive"])
def test_three_second_run_writes_a_well_formed_result(
    strategy: str, tmp_path: Path, capsys
) -> None:
    parser = argparse.ArgumentParser()
    runner.add_bench_args(parser)
    args = parser.parse_args(
        [
            *("--strategy", strategy, "--n", "3", "--stalled", "1"),
            *("--seconds", "3", "--procs", "1", "--warmup", "1", "--drain", "0.5"),
            *("--out", str(tmp_path / "runs")),
        ]
    )

    assert runner.bench_main(args) == 0

    path = Path(capsys.readouterr().out.strip().splitlines()[-1])
    assert path.parent == (tmp_path / "runs").resolve()
    result = json.loads(path.read_text())
    assert result["config"]["strategy"] == strategy
    healthy = result["healthy"]
    assert healthy["connected"] == 2
    assert healthy["expected_per_client"] == 60  # 3 s at 20 events/s
    assert healthy["delivery_ratio"] > 0.9
    assert healthy["gaps_seen"] == 0
    assert healthy["latency_sched"]["p50_ms"] > 0
    assert healthy["latency_ts"]["p50_ms"] > 0
    assert sum(healthy["histogram_sched_ms"]["counts"].values()) > 0
    assert result["stalled"]["connected"] == 1
    assert "stalled-0" in result["stalled"]["dropped"]
    assert result["server"]["rss_max_bytes"] > 0
    assert result["loadgen"]["loop_lag_p99_ms_max"] is not None
    assert result["traffic"]["server_ws_messages_per_s"] > 0
    assert result["environment"]["python"]
