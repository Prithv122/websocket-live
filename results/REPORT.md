# Slow-consumer sweep report

Plan hash `ef0405288c9b` - 60 runs - results are properties of a simulator on the machine below, not of production.

## Verdict: SUPPORTED UNDER ESCALATION

naive passed the primary cell but failed at ladder-1; queued passed.

Rule: at the primary cell (n=200, 20 stalled = 10%) an arm passes when the median p95 delivery latency from the scheduled publish time is at most 100 ms and the median delivery ratio is at least 0.99. Missing messages count as infinite latency. A run is invalid when a load-generator worker's event-loop lag p99 exceeds 20 ms, when it did not have the stalled and healthy clients it intended, or when it ended in a harness error; invalid runs are excluded from the medians.

## Primary cell

| stage | pad bytes | stalled rcvbuf | arm | valid | p95 ms (min-max) | delivery | result |
|---|---|---|---|---|---|---|---|
| matrix | 0 | default | naive | 3/3 | 21.4 (18.6-22.4) | 1.0000 | pass |
| matrix | 0 | default | queued | 3/3 | 24.0 (22.8-24.9) | 1.0000 | pass |
| ladder-1 | 4096 | default | naive | 3/3 | inf (inf-inf) | 0.0000 | FAIL |
| ladder-1 | 4096 | default | queued | 3/3 | 23.6 (23.2-24.2) | 1.0000 | pass |
| ladder-2 | 4096 | 4096 | - | not run | - | - | - |

## Matrix (matrix stage)

Medians over valid runs; `lag` is the worst worker loop-lag p99 over all runs of the row.

| n | stalled | arm | valid | p50 ms | p95 ms | p95 range | p99 ms | delivery | gaps | stalled dropped | server RSS MiB | lag ms |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 50 | 0 | naive | 3/3 | 9.2 | 17.6 | 16.6-20.3 | 21.7 | 1.0000 | 0 | 0 | 60.5 | 19.4 |
| 50 | 0 | queued | 3/3 | 9.8 | 18.3 | 15.8-18.4 | 21.8 | 1.0000 | 0 | 0 | 59.7 | 17.8 |
| 50 | 5 | naive | 3/3 | 9.0 | 17.3 | 17.2-18.0 | 22.9 | 1.0000 | 0 | 0 | 61.0 | 19.2 |
| 50 | 5 | queued | 3/3 | 9.8 | 18.8 | 17.5-19.0 | 26.1 | 1.0000 | 0 | 0 | 60.1 | 19.0 |
| 50 | 15 | naive | 3/3 | 7.9 | 16.7 | 14.2-17.0 | 21.1 | 1.0000 | 0 | 0 | 61.8 | 18.5 |
| 50 | 15 | queued | 3/3 | 8.8 | 17.9 | 15.0-18.0 | 22.9 | 1.0000 | 0 | 0 | 60.9 | 19.2 |
| 200 | 0 | naive | 3/3 | 11.3 | 20.4 | 19.6-24.1 | 22.3 | 1.0000 | 0 | 0 | 75.8 | 18.6 |
| 200 | 0 | queued | 3/3 | 14.6 | 24.6 | 22.8-24.7 | 27.5 | 1.0000 | 0 | 0 | 76.2 | 19.1 |
| 200 | 20 | naive | 3/3 | 12.0 | 21.4 | 18.6-22.4 | 24.2 | 1.0000 | 0 | 0 | 77.7 | 19.5 |
| 200 | 20 | queued | 3/3 | 13.5 | 24.0 | 22.8-24.9 | 27.2 | 1.0000 | 0 | 0 | 78.2 | 17.7 |
| 200 | 60 | naive | 3/3 | 11.6 | 20.4 | 19.9-20.5 | 22.2 | 1.0000 | 0 | 0 | 80.5 | 18.5 |
| 200 | 60 | queued | 3/3 | 14.1 | 23.4 | 22.5-24.5 | 27.7 | 1.0000 | 0 | 0 | 81.3 | 18.8 |
| 500 | 0 | naive | 3/3 | 16.9 | 30.6 | 27.3-30.8 | 35.4 | 1.0000 | 0 | 0 | 107.2 | 19.1 |
| 500 | 0 | queued | 3/3 | 22.4 | 38.6 | 38.6-39.0 | 47.1 | 1.0000 | 0 | 0 | 108.8 | 16.0 |
| 500 | 50 | naive | 3/3 | 16.3 | 27.8 | 26.4-29.6 | 31.9 | 1.0000 | 0 | 0 | 111.1 | 18.5 |
| 500 | 50 | queued | 3/3 | 22.5 | 37.7 | 37.5-42.3 | 44.0 | 1.0000 | 0 | 0 | 113.3 | 16.1 |
| 500 | 150 | naive | 3/3 | 16.7 | 28.6 | 26.6-28.9 | 33.2 | 1.0000 | 0 | 0 | 119.5 | 17.9 |
| 500 | 150 | queued | 3/3 | 21.0 | 36.2 | 35.8-365.9 | 43.2 | 1.0000 | 0 | 0 | 123.1 | 16.4 |

## Secondary: p95 from the server's own stamp (descriptive)

Latency from the envelope `ts` instead of the schedule. A publisher that falls behind stamps late, so this clock hides its delay; the verdict never uses it.

| n | stalled | naive p95 ms | queued p95 ms |
|---|---|---|---|
| 50 | 0 | 3.4 | 3.7 |
| 50 | 5 | 3.1 | 3.8 |
| 50 | 15 | 2.8 | 3.5 |
| 200 | 0 | 8.6 | 11.8 |
| 200 | 20 | 8.2 | 10.7 |
| 200 | 60 | 7.7 | 10.2 |
| 500 | 0 | 18.8 | 27.2 |
| 500 | 50 | 17.9 | 26.6 |
| 500 | 150 | 16.7 | 26.3 |

## Invalid runs

None.

## Environment

- Intel(R) Core(TM) i7-8850H CPU @ 2.60GHz (12 logical cores), Windows-11-10.0.26200-SP0, Python 3.13.13; fastapi 0.153.0, psutil 7.2.2, pydantic 2.14.1, starlette 1.9.0, uvicorn 0.54.0, websockets 17.3; commit `401cd9ba4e`
