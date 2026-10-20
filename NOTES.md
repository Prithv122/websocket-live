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
