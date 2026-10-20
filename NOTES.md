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

---

## Rejected approaches

| Approach | Why rejected |
|---|---|
| `python-socketio` | Hides the fan-out being measured |
| Redis pub/sub fan-out | Adds a service without changing the per-socket problem |
| Latency from server `ts` as primary | Coordinated omission: hides a blocked publisher's delay |

## Open questions

- [ ] Does the chosen uvicorn WebSocket implementation apply backpressure (send waits for drain)?
