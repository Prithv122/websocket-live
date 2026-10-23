# Build Notes — websocket-live

Working notes: what broke, what was tried, why X over Y.

---

## Log

### 2026-10-20 — design frozen before any code

- **Own hub over socket.io or Redis pub/sub.** The fan-out and slow-consumer logic is the thing
  being measured; a library would hide it.
- **Resume from seq with a 1,000-event ring buffer.** About forty lines, and the streaming project
  (#34) gets lossless reconnects for free. Older than the ring → one snapshot, then live.
- **`gap` envelope carries `seq = b`.** The stream a client sees stays continuous
  (… a-1, gap(a..b), b+1 …), so "seq must be last + 1" stays the only rule a client needs.
- **Stalled clients are raw TCP sockets that never read** (`rawws.py`). A library client keeps
  draining the socket into its own queue, which would hide the stall from the server.
- **Added `GET /poll/{channel}`** (not in the original brief's route list) so the push-vs-poll
  comparison measures a real polling client instead of an estimate.

### 2026-10-20 — pre-registration (owner decisions)

Set before any timing run; see `EVAL_PLAN.md` and `eval_plan.toml`.

- p95 budget 100 ms for healthy clients at N=200 with 10% stalled; `queued` must pass.
- **Latency from the scheduled publish time, not the server stamp.** A naive publisher blocked
  behind a stalled socket stamps its events late, so stamp-based latency would hide most of the
  delay being measured (coordinated omission). Stamp-based latency is kept as a secondary column.
- 60 s window, 3 s warmup, 3 repeats. Healthy delivery ≥ 99%; missing messages count as infinite
  latency. A run is invalid if the load generator's own event-loop lag p99 exceeds 20 ms.
- Escalation ladder if `naive` passes (fixed now): 4 KB payload padding, then 4 KB padding plus
  a 4 KB `SO_RCVBUF` on stalled clients. Stop at the first break; otherwise report that naive did
  not break within the tested conditions.
- Rejected: 50 ms budget and 99.9% / 10 ms validity. The question is head-of-line blocking, not
  whether Windows can hold tight scheduler jitter.

### 2026-10-20 — build, parallel parts against the protocol

- **Backpressure is real with every uvicorn WebSocket implementation installed.** `websockets`
  (legacy) awaits `drain()`; `websockets-sansio` and `wsproto` wait on a `writable` event cleared
  by `pause_writing()`. None buffers without bound. Pinned `websockets-sansio` so `auto` can't
  silently change what is measured. A real-socket test proves naive blocks (stalled client with
  a 4 KB receive buffer, 64 KB events: the 6th publish blocks) and queued doesn't.
- **uvicorn pings every 20 s by default and drops a client that doesn't answer.** A stalled
  client would have been disconnected about 40 s into a 60 s run, ending the stall mid-measurement.
  Pings are off by default (`--ws-ping-interval 0`).
- **`asyncio.gather(..., return_exceptions=True)` swallowed a cancellation** aimed at the socket
  handler; WebSocket tests failed 10–17% of the time. Replaced with `asyncio.wait`.
- **Server restart broke resume.** The browser's `since=` seq meant nothing to a restarted server:
  if it was behind, new events looked like duplicates until the next heartbeat; if ahead, it
  replayed unrelated events. Fix: every connection opens with a heartbeat carrying the server's
  `epoch`; a new epoch makes the client adopt that heartbeat's seq. Additive, so `v` stays 1.
- **A lone valid run must not decide a verdict.** Added before any run: an arm is judged only if a
  majority of its runs (2 of 3) pass the loop-lag validity check, otherwise INVALID.
- **Windows timer floor:** a 3-second smoke shows latency from schedule p50 ≈ 8.5 ms vs ≈ 1 ms from
  the server stamp. The feed wakes late by about the timer granularity; both arms get the same
  offset, and it is far below the 100 ms budget.
- **Browser check (in-app browser, 280 px wide):** live updates, no console errors from page code,
  no horizontal scroll. Killed and restarted the server at seq ≈ 125: the page backed off,
  reconnected, counted 1 server restart, adopted the new seq (82 fifteen seconds later) with
  0 gaps and 0 duplicates. Found: at narrow widths the bar chart's SVG scales its 12 px labels down
  to about 7 px.

### 2026-10-20 — independent review before any measurement

A fresh-context review found 12 issues; all fixed except the dashboard keeping since-page-load
totals across a server restart (deliberate: they describe what this browser received).

- **The "stalled" client was reading.** `asyncio.open_connection` attached a StreamReader that
  pulled about 131 KB into user space before pausing, even with a 4 KB `SO_RCVBUF`. At the default
  payload naive would have frozen only after about 99 s, longer than a run: a false null. Fixed by
  never wrapping the socket in a transport (byte-by-byte handshake); `test_rawws.py` peeks the
  kernel buffer to prove every later byte is still there. Re-probe: a naive publisher now freezes
  after about 265 KB at pad 0 (about 66 s at 20 events/s, so close to one run's length) and about
  146 KB with a 4 KB receive buffer. That remainder is kernel and server-side buffering, the same
  for both arms.
- **Amendment 1 to the plan:** run integrity (all stalled clients connected and still subscribed at
  window end, no healthy connect failures), harness errors recorded instead of silently re-run,
  sweep refuses to resume across commits, dirty trees or a different worker count, no fallback
  that could shrink the denominator, ladder only when both arms pass.
- Naive no longer buffers during a resuming client's replay (the publish waits, as naive means).

### 2026-10-20 — local CI replay (hosted Actions unavailable until about 2026-11-02)

Fresh clone of `683e82e`, fresh `uv sync --all-groups --locked` and `npm ci`, Windows 11, Python
3.13.9, Node 24.19 (the workflow pins Node 22 on Ubuntu), `--basetemp` in a scratch directory:
ruff check and format clean; pytest 205 passed, coverage 90%; `wslive bench --n 3 --stalled 1
--seconds 3 --procs 1` wrote a result file; `npm test` 64 passed; `html-validate` clean.

### 2026-10-23 — window renderer for the streaming pipeline

- **The two projects disagreed on the contract, and only a real run showed it.** The
  streaming-pipeline bridge posted `{"type": "window"}`; this hub accepts only `type: "event"`
  (the other types are its own control messages) and answered 422. The bridge's tests used a
  stub hub that accepted anything. Fixed on the bridge side: it now posts `type: "event"` to
  `/publish/windows`, and the channel says what the payload is, as `orders` does. Letting the hub
  accept new types would have meant a protocol bump, and this client would have read each one as
  a seq hole and reconnected.
- **Renderer:** `latestWindows` keeps the highest `revision` per `(zone, window_start)`, because
  revise mode sends a window several times and a late lower revision must not overwrite a final
  one. Revenue comes from `revenue_cents`, so it is exact to the paisa. Times are stream (event)
  time. `late_dropped` is the zone's running total, not a per-window count, and the panel says so.
- **Checked in a browser through Redpanda:** a 600 s seeded log went produce → consume (revise,
  `L = 30`) → bridge → hub. 176 records were forwarded and received, with no gaps or duplicates.
  All 18 rows shown matched the simulator's records for the same log. A paced run with 90 s delays
  showed provisional rows turning final, and non-zero late drops.
- **Known limit:** rerunning the pipeline into an open page restarts revisions at 0 for the
  same window times, so the older run's higher revisions win until they leave the 200-event ring.
  Reload the page between runs. A run id in the record would fix it; the pipeline has none today.

### 2026-10-24 — the registered sweep

- **Conditions.** Started 02:13 IST after a background LLM evaluation had been idle for 17 minutes;
  no browser, music or other work running. A 10-second sample just before the start showed no
  process above 2% of the machine (the busiest was an idle editor session). The sweep ran on a
  clean tree at `401cd9b` and finished at 03:26 (73 minutes, 60 runs).
- **Result: SUPPORTED UNDER ESCALATION.** Both arms passed the primary cell at the default message
  size (p95 naive 21.4 ms, queued 24.0 ms, 100% delivered). Ladder step 1 (4 KB padding) froze
  naive completely: 0% delivered in all three repeats, while queued held 23.6 ms. Step 2 was not
  needed. No invalid runs.
- **Reading the freeze.** In the naive ladder runs the server sent about 20 messages per second of
  about 114 bytes: that is the heartbeat (200 clients, every 10 s), so not one padded event left
  the server after the publisher blocked. This matches the pre-sweep probe above (naive needed
  about 265 KB in front of a stalled socket before it blocked; at 4 KB × 20/s that is about 3 s,
  inside the warm-up).
- **Validity margin was thin.** The highest loop-lag p99 of any run was 19.9 ms against the 20 ms
  limit. The Windows timer floor (about 15 ms per 10 ms tick) uses most of that budget; the limit
  was set knowing this, and no run crossed it.
- **One outlier.** A `queued` run at N = 500 with 30% stalled had p95 365.9 ms; its two repeats
  were 35.8 and 36.2 ms. The median rule absorbs it; it is in the report's range column.
- **Push vs 1 s polling** (descriptive, not in the verdict): one run each at N = 200. The same
  bytes (every poll has news at 20 events/s), 200 requests/s, p50 502 ms vs 13 ms.

---

## Rejected approaches

| Approach | Why rejected |
|---|---|
| `python-socketio` | Hides the fan-out being measured |
| Redis pub/sub fan-out | Adds a service without changing the per-socket problem |
| Latency from server `ts` as primary | Coordinated omission: hides a blocked publisher's delay |

## Open questions

- [x] Does the chosen uvicorn WebSocket implementation apply backpressure (send waits for drain)?
  Yes, all three (see the build log).
