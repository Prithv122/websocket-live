# websocket-live — C4

**Tier:** 2 · **Category:** C — Web / APIs · **Wave:** 4

The project's own engineering guide. Root rules in `../GUIDELINES.md` apply.

## What this is

A WebSocket push server with a live browser dashboard, built to answer one measured question:
when 10% of clients stop reading their socket, does a per-client bounded queue (drop-oldest) keep
the healthy clients' p95 delivery latency under budget where a naive sequential broadcast does not?
The dashboard is feed-agnostic: the streaming-pipeline project (#34) publishes into it later.

## Stack

- **Server:** Python 3.13, FastAPI/Starlette WebSockets, own `Hub` (no socket.io, no Redis).
- **Load generator:** asyncio + `websockets`; stalled clients are raw TCP sockets that never read.
- **Client:** vanilla ES modules in `web/`, hand-built SVG, no chart library, no build step.
- **Checks:** `pytest` + `ruff`, `node --test`, `html-validate`.

## Non-negotiables

1. `PROTOCOL.md` is the contract. Server, client, load generator and tests follow it; changing it
   means a version bump.
2. Thresholds in `EVAL_PLAN.md` are set by the owner and committed **before** the sweep runs.
   Never tuned after seeing results. A null result is reported as a null.
3. Every number in the README comes from a committed results file produced by `wslive bench`,
   and is described as a property of the simulator on one laptop, not of production.
4. Latency is measured on one machine (no clock skew) from the scheduled publish time, so a
   publisher that falls behind cannot hide its own delay (coordinated omission).
5. No secrets in the repo. `PUBLISH_TOKEN` comes from the environment only.

## Acceptance criteria

- [ ] Envelope shape, gap-free seq, resume via `since`, drop-oldest, publish auth on/off: tested
- [ ] `wslive bench` sweep per `EVAL_PLAN.md`, results committed, verdict stated mechanically
- [ ] Dashboard reconnects (backoff + jitter) and resumes without a hole; checked in a browser
- [ ] Runs from a clean clone using only the documented steps
- [ ] Tests, lint, `node --test`, `html-validate` and CI green

## Commands

```
uv run wslive serve --strategy queued --feed-rate 20      # dashboard on http://127.0.0.1:8000
uv run wslive bench --n 3 --stalled 1 --seconds 3          # smoke run, prints a results path
uv run pytest ; npm test ; npm run validate
```

## Project-specific notes

- Local pytest needs `--basetemp=<scratch dir>` on this machine; never put it in `pyproject.toml`.
- Timing runs only on a quiet machine; record what else was running in `NOTES.md`.
