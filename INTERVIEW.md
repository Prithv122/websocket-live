# Interview Prep — websocket-live

**Five questions, five answers.** An unanswered question means this project is not shipped.

---

### Q1. Walk me through the architecture in 90 seconds.

_A:_ A seeded feed (or anyone calling `POST /publish`) hands events to a `Hub`. The hub stamps
each with a per-channel sequence number, keeps the last 1,000 in a ring buffer, and fans them out
over WebSockets. Fan-out has two strategies: `naive` awaits each socket's send in turn; `queued`
gives each client a bounded queue and its own writer task, and when the queue is full it drops the
oldest message and later tells the client with a `gap` message. The browser client tracks the
sequence number: a missing number means it lost something, so it reconnects with `?since=<last>`
and the server replays from the ring. Every connection starts with a heartbeat carrying the
server's epoch, so a client can tell a restarted server from the one it was talking to. The
dashboard is vanilla JS with hand-built SVG and redraws at most once per animation frame.

### Q2. Why drop the oldest message instead of buffering everything or disconnecting the client?

_A:_ An unbounded buffer moves the problem from latency to memory: one frozen tab grows the
server's memory until it falls over. Disconnecting is harsh for a brief stall (a laptop lid, a
tunnel). For a live dashboard the newest value is the one that matters, so dropping the oldest
keeps the client current, and the `gap` message (with the exact range lost) lets it recover the
missing events from the ring buffer if it cares. The queue size (64) bounds memory per client.

### Q3. What's the weakest part of this, and what would break first under load?

_A:_ It is one Python process: one event loop serialises JSON encoding and every socket write, so
CPU on that core is the ceiling, and the ring buffer and subscriber list live in that process's
memory, so there is no horizontal scaling and a restart loses history (the epoch heartbeat makes
that visible to clients, but doesn't prevent it). The benchmark also runs the load generator on
the same laptop, so above a few hundred clients the measurement competes with the thing measured;
a loop-lag control flags runs where the load generator itself was stalling.

### Q4. How do you know it works? What did you measure, and against what baseline?

_A:_ I wrote the question, the budget (p95 ≤ 100 ms from the scheduled time, ≥ 99% delivered, at
200 clients with 20 stalled) and an escalation ladder into `EVAL_PLAN.md` before any run, and
`wslive report` prints the verdict. At the default ~190-byte messages both strategies passed:
naive's p95 was 21.4 ms, queued's 24.0 ms. That was the null I had planned for: at 20 small events a
second the buffers in front of a stalled socket don't fill within a 65-second run. Step 1 of the
ladder, 4 KB messages, filled them during warm-up, and naive delivered **0%** of events to the 180
healthy clients in every repeat, while queued stayed at 23.6 ms and 100%. So the verdict is
"supported under escalation", 60 runs, none invalid. The baseline is naive itself, same harness and
same machine, and the cost of the fix is visible too: with no stalled clients queued is a few
milliseconds slower (24.6 vs 20.4 ms at N = 200).

### Q5. Why measure latency from the scheduled publish time and not the server's timestamp?

_A:_ Coordinated omission. When `naive` is blocked behind a stalled socket, the publisher can't
stamp the next event until the send returns, so an event that should have gone out at t = 10 s
might be stamped at t = 40 s and then delivered 2 ms later. Latency from the stamp says 2 ms; the
user waited 30 s. Measuring from when the event was *scheduled* charges the system for its own
delay. The stamp-based number is kept as a secondary column, which shows how much it would hide.

### Q6. What was the bug you're most glad you caught before measuring?

_A:_ The "stalled" test client wasn't stalled. I opened it with `asyncio.open_connection`, which
attaches a reader that keeps pulling data off the socket into Python memory — about 131 KB —
before it pauses. So the server saw a client that was reading. At the default message size the
naive broadcast would have needed about 99 s to block, longer than a run, and the experiment
would have reported "naive is fine". An independent review caught it; the fix reads the handshake
response one byte at a time on the raw socket and never reads again, and a test peeks at the
kernel buffer to prove every byte after the handshake is still there.

---

## 30-second pitch

Live dashboards push updates over WebSockets, and the quiet failure is one frozen viewer: a
broadcast loop that awaits each send makes everyone wait behind it. I built the push server, a
vanilla-JS dashboard with lossless reconnect, and a load generator with clients that never read
their socket, then pre-registered the test. With small messages a naive broadcast survived a
one-minute run; with 4 KB messages it delivered nothing at all to 180 healthy clients, while a
bounded drop-oldest queue per client held p95 at about 24 ms with everything delivered. The latency
is measured from the scheduled publish time, so a stuck publisher can't hide its own delay.
