"""Seeded order feed: an in-process publisher for the hub and an HTTP publisher for `wslive feed`.

Both follow one schedule: event `i` is due at `t0 + i / rate`, and its `t_sched` is that due
time, not the moment `publish` happened to run. A publisher that falls behind (the naive
strategy blocks it) therefore publishes the overdue events back to back with their original
`t_sched`; it never skips one and never moves the schedule. Latency measured from `t_sched`
then includes the publisher's own delay instead of hiding it.
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import Any, Protocol

from wslive.models import CHANNEL_PATTERN, STATUSES, ZONES, OrderEvent

# Uneven on purpose, so the dashboard's per-zone bars have something to show.
ZONE_WEIGHTS = (30, 22, 18, 14, 10, 6)
STATUS_WEIGHTS = (35, 25, 35, 5)
MIN_AMOUNT = 49.0


class Publisher(Protocol):
    async def publish(self, channel: str, type: str, data: dict[str, Any]) -> Any: ...


def generate_orders(seed: int) -> Iterator[OrderEvent]:
    """Deterministic, endless stream of orders: the same seed always gives the same orders."""
    rng = random.Random(seed)
    for n in itertools.count(1):
        zone = rng.choices(ZONES, weights=ZONE_WEIGHTS)[0]
        status = rng.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
        # Log-normal: a few large baskets, most orders a few hundred rupees.
        amount = round(max(MIN_AMOUNT, rng.lognormvariate(6.0, 0.6)), 2)
        yield OrderEvent(order_id=f"ord-{n:06d}", zone=zone, amount=amount, status=status)


def event_data(order: OrderEvent, t_sched: float, pad_bytes: int = 0) -> dict[str, Any]:
    data = {**order.model_dump(), "t_sched": t_sched}
    if pad_bytes > 0:
        data["pad"] = "x" * pad_bytes
    return data


async def run_feed(
    hub: Publisher,
    channel: str = "orders",
    rate: float = 20.0,
    seed: int = 7,
    pad_bytes: int = 0,
    count: int | None = None,
) -> int:
    """Publish `count` orders (forever when None) at `rate` per second. Returns how many."""
    t0 = time.time()
    published = 0
    for i, order in enumerate(itertools.islice(generate_orders(seed), count)):
        t_sched = t0 + i / rate
        while (delay := t_sched - time.time()) > 0:
            await asyncio.sleep(delay)
        await hub.publish(channel, "event", event_data(order, t_sched, pad_bytes))
        published += 1
        # A queued publish never suspends; yield so a feed that is behind cannot starve the loop.
        await asyncio.sleep(0)
    return published


def add_feed_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("--url", default="http://127.0.0.1:8000", help="server base URL")
    p.add_argument("--channel", default="orders")
    p.add_argument("--rate", type=float, default=20.0, help="events per second")
    p.add_argument("--seed", type=int, default=7)
    p.add_argument(
        "--count", type=int, default=None, help="stop after N events (default: run until Ctrl-C)"
    )
    p.set_defaults(func=feed_main)


def _post(url: str, body: dict[str, Any], token: str | None) -> None:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, json.dumps(body).encode(), headers, method="POST")
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def feed_main(args: argparse.Namespace) -> int:
    if not re.fullmatch(CHANNEL_PATTERN, args.channel):
        print(f"invalid channel name: {args.channel!r}", file=sys.stderr)
        return 2
    if not args.url.startswith(("http://", "https://")):
        print("--url must start with http:// or https://", file=sys.stderr)
        return 2
    if args.rate <= 0:
        print("--rate must be positive", file=sys.stderr)
        return 2

    url = f"{args.url.rstrip('/')}/publish/{args.channel}"
    token = os.environ.get("PUBLISH_TOKEN")
    t0 = time.time()
    published = 0
    try:
        for i, order in enumerate(itertools.islice(generate_orders(args.seed), args.count)):
            t_sched = t0 + i / args.rate
            while (delay := t_sched - time.time()) > 0:
                time.sleep(delay)
            _post(url, {"type": "event", "data": event_data(order, t_sched)}, token)
            published += 1
    except KeyboardInterrupt:
        pass
    except urllib.error.HTTPError as exc:
        print(f"server rejected the event: HTTP {exc.code}", file=sys.stderr)
        return 1
    except (urllib.error.URLError, OSError) as exc:
        print(f"cannot reach {url}: {exc}", file=sys.stderr)
        return 1
    print(f"published {published} events to {url}")
    return 0
