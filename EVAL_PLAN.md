# Evaluation plan: slow consumers and fan-out strategy

Pre-registered. Decided by the owner on 2026-10-20 and committed before any timing run. The
machine-readable copy is `eval_plan.toml`; its hash is stamped on every sweep line, and the sweep
and the report refuse to mix results from different hashes. If this file and the TOML ever
disagree, the TOML is what runs.

## Question

With N clients connected, of which a share never read their socket, does the `queued` strategy
(a bounded drop-oldest queue per client) keep the **healthy** clients' p95 delivery latency under
budget where the `naive` strategy (the publisher awaits each send in turn) does not?

Everything here is a property of this simulator on one machine, not of production.

## Metric

**Delivery latency of one message to one healthy client** = the time the client's receive
returned (`time.time()` taken when the consumer coroutine gets the frame, before the JSON is
parsed) minus `t_sched`, the time the feed was scheduled to publish that event. Client and server
share one machine and one clock, so there is no skew to correct.

- **Why the schedule and not the server's stamp.** The envelope `ts` is written when the server
  builds the message. A publisher stuck behind a stalled socket builds it late, and a latency
  measured from `ts` would hide exactly the delay the experiment is about (coordinated omission).
  `t_sched` is when the event should have gone out, whatever the publisher managed. Latency from
  `ts` is recorded as a secondary, descriptive column and never decides anything.
- **Missing means infinite.** Every healthy client is expected to receive every event scheduled in
  the measurement window. An event that never arrives (not by the end of the drain period) counts
  as an infinite latency, so a percentile is infinite as soon as the missing share passes the
  complement of the percentile: p95 is infinite when more than 5% of messages are missing. This also
  covers events the server never published because the publisher was stuck.
- **Percentile.** Nearest rank on the exact sample: the value at rank `ceil(q/100 * n)` of the
  sorted values. No interpolation. A log-bucketed histogram (about 2% wide buckets) is stored with
  each run so the percentiles can be re-derived approximately from the committed files.
- **Delivery ratio** = messages received in the window / messages expected, over all healthy
  clients. Heartbeats are ignored. `gap` envelopes received by healthy clients are counted (they
  should be zero).

## Design

| | |
|---|---|
| Arms | `naive`, `queued` (queue size 64) |
| Clients N | 50, 200, 500 (stalled clients included) |
| Stalled share | 0.0, 0.1, 0.3 (a stalled client completes the handshake and never reads) |
| Feed | 20 events per second, seeded, `pad_bytes` 0 |
| Per run | 3 s warm-up, 60 s measurement window, 2 s drain for late deliveries |
| Stalled client socket | OS default receive buffer |
| Repeats | 3 per cell |
| Cells | 2 arms x 3 N x 3 shares x 3 repeats = 54 runs |
| Run order | shuffled with `random.Random(20261020 + stage index)`, so drift of the machine is spread across arms |

Stalled clients are interleaved evenly through the connection order, so no arm gets a lucky
position in the server's subscriber list. The measurement window starts once every client has
connected and the warm-up has passed. The server is a fresh process for every run; its memory is
sampled every 0.5 s.

How many messages are expected: the feed publishes on a fixed grid, so after the run the harness
reads the server's ring buffer (sized to hold the whole run), takes one event as the grid
reference, and counts the grid slots whose scheduled time lies inside the window. That count does
not shrink when the publisher falls behind. If the published events are not on that grid the run is
a harness error: there is no fallback that counts only what was published, because that would let a
stuck publisher shrink its own denominator.

## Primary hypothesis and the mechanical verdict

Primary cell: **N = 200, 10% stalled (20 clients)**.

- **Budget:** p95 delivery latency from the schedule of at most **100 ms**.
- **Delivery floor:** delivery ratio of at least **0.99** for healthy clients.
- An **arm passes** when the median over its valid runs (a majority must be valid) of p95 is within the budget **and** the
  median delivery ratio meets the floor.
- Expectation: `queued` passes, `naive` fails.

| Outcome at the primary cell | Verdict |
|---|---|
| `queued` passes, `naive` fails | **SUPPORTED** |
| `queued` fails | **NOT SUPPORTED** |
| both pass | go to the escalation ladder (below) |
| an arm has fewer than a majority of valid runs (under 2 of 3); see the validity control and Amendment 1 | **INVALID** |
| fewer than 3 runs for an arm in a stage that has to be judged | **INCOMPLETE** |

`wslive report` computes this from the committed sweep lines and prints it; nothing is judged by
hand. The other cells are reported as descriptive tables (medians with min-max across the repeats,
delivery ratio, gaps, drops of stalled clients, server memory).

## Validity control

The load generator runs in several processes. In each, a 10 ms ticker records how late it wakes,
from the start of the window to the end of the drain (latencies are still recorded during the
drain). If any worker's p99 lag exceeds **20 ms**, the load generator itself was stalling, the
receive timestamps are late, and the run is flagged **invalid** and excluded from the medians.
Invalid runs are listed in the report, not hidden and not silently re-run. An arm is judged only
when a majority of its runs in the judged stage are valid (2 of 3); otherwise the verdict is
**INVALID**.

**Measured floors on this machine.** Windows timer granularity makes `asyncio.sleep(0.01)` take
about 15.5 ms, so the loop-lag p50 sits around 5 ms even when idle, and the latency from the
schedule has a floor of up to about 15.6 ms in both arms (the feed's own sleep overshoots by the
same granularity). The 20 ms lag limit and the 100 ms budget were set knowing this.

## Amendment 1 (2026-10-20, before any timing run)

Run integrity. No owner number changes. A run is **INVALID**, excluded from the medians exactly like
a loop-lag failure and listed in the report with its reasons (`invalid_reasons` on the run file and
on the sweep line), if any of these holds:

1. the number of stalled handshakes that completed differs from the intended stalled count;
2. the server's `/stats`, read at the end of the window before anything is closed, lists fewer
   `stalled-*` subscribers than intended;
3. any healthy client failed to connect, closed early, or did not report;
4. the run ended in a harness error (the sweep records it as a line with `status: "harness_error"`
   and the error text; it counts as an invalid run for that cell and repeat and is not re-run on
   resume).

Why: a run with fewer stalled clients than intended would judge `naive` under easier conditions than
the plan names, which can only help `naive` pass. Healthy clients that failed to connect would
become infinite latencies and missing messages for reasons unrelated to fan-out, and would punish
whichever arm they happened to land in.

The sweep also protects the attribution of its results: it refuses to start on a tree with
uncommitted changes (changes under `results/` do not count), and refuses to resume when the recorded
runs were made at another commit, on a dirty tree, or with another `--procs`.

## Escalation ladder (only if both arms pass the primary cell)

A step is run only if both arms passed the previous stage; if `queued` does not pass, the verdict is
already **NOT SUPPORTED** and the ladder ends. A `naive` that never degrades on this machine would be an uninteresting experiment, so the
conditions that make a stalled socket bite sooner are fixed here, in advance, and tried in order:

1. `pad_bytes` 4096, default receive buffer.
2. `pad_bytes` 4096 and a 4096-byte `SO_RCVBUF` on the **stalled** clients only.

A step re-runs only the primary cell, both arms, 3 repeats. Stop at the first step where `naive`
fails. If `queued` then passes, the verdict is **SUPPORTED UNDER ESCALATION**. If `naive` passes
every step the result is a **NULL**: "naive did not break within the tested conditions".

## Null-result rule

A null is publishable and is published as a null. The harness, the thresholds and the ladder are
not tuned after seeing results to make the outcome flip. A change to any of them is a new plan with
a new hash, and the old results stay in the repository as a record.

## Reproduce

```
uv run wslive sweep            # runs the plan, resumable, results/sweep.jsonl + results/runs/*.json
uv run wslive report           # results/REPORT.md
```

Timing runs need a quiet machine; record what else was running in `NOTES.md`.
