"""`wslive serve | feed | bench | sweep | report`.

Each subcommand's options and entry point live next to the code they drive; this module only
wires them together, importing lazily so `wslive --help` stays fast.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    from wslive.app import add_serve_args
    from wslive.bench.report import add_report_args
    from wslive.bench.runner import add_bench_args
    from wslive.bench.sweep import add_sweep_args
    from wslive.feed import add_feed_args

    parser = argparse.ArgumentParser(prog="wslive", description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    add_serve_args(sub.add_parser("serve", help="run the push server and dashboard"))
    add_feed_args(sub.add_parser("feed", help="publish seeded orders to a running server"))
    add_bench_args(sub.add_parser("bench", help="one load-generator run"))
    add_sweep_args(sub.add_parser("sweep", help="the pre-registered matrix (EVAL_PLAN.md)"))
    add_report_args(sub.add_parser("report", help="summarise sweep results"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    sys.exit(main())
