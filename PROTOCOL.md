# Wire protocol (v1)

The contract between the server, the browser client, the load generator and any upstream
publisher. Change it only with a version bump.

## Envelope (server → client)

Every message on a WebSocket is one JSON object:

```json
{"v": 1, "channel": "orders", "seq": 42, "ts": "2026-10-20T12:34:56.789Z", "type": "event", "data": {}}
```

| Field | Meaning |
|---|---|
| `v` | Protocol version, always `1`. |
| `channel` | Channel name, `[a-z0-9_-]{1,32}`. |
| `seq` | Per-channel sequence number. Events are numbered 1, 2, 3, … with no holes. |
| `ts` | Server stamp when the envelope was created, ISO-8601 UTC with milliseconds and a `Z`. |
| `type` | `event`, `snapshot`, `heartbeat` or `gap`. Clients must ignore types they do not know. |
| `data` | Type-specific object. |

### Types

- **`event`** — one published item. `seq` is the new channel sequence number.
- **`heartbeat`** — the first message on every connection, then every 10 s on an idle or busy
  connection. `seq` is the channel's latest event seq at that moment (it does not advance the
  sequence). `data` is `{"epoch": "<id>"}`: a short random id the server picks when it starts, so
  it changes on every restart. On connect the heartbeat comes before any replay, snapshot or
  live event.
- **`snapshot`** — sent once, right after connect, when the client asked to resume from a `since`
  that is older than the ring buffer. `seq` is the latest event seq included. `data` is
  `{"oldest": <seq of first event>, "events": [<event envelopes, oldest first>]}` holding the
  whole ring buffer. Live events follow from `seq + 1`.
- **`gap`** — the server dropped events for this client (slow consumer, `queued` strategy).
  `data` is `{"from": a, "to": b, "reason": "slow_consumer"}` and `seq` is `b`, so the stream the
  client sees stays continuous: …, event(a-1), gap(a..b), event(b+1), …
  A client that needs the missing events reconnects with `since=a-1`.

**Epoch.** A client remembers the last epoch it saw. If a heartbeat carries a different epoch
(the server restarted, or this is the first connection), the `last` seq the client holds
belongs to another run and means nothing. It sets `last` to that heartbeat's `seq` and carries on
live; events replayed after it have `seq <= last` and are ignored as duplicates, so a client never
shows events from a previous run or mistakes new ones for duplicates. With an unchanged epoch the
heartbeat changes nothing.

The `epoch` field is additive (heartbeat `data` used to be `{}`) and clients ignore fields they do
not know, so the protocol version stays `1`.

A client tracks the last seq it saw. For `event`, the next seq must be `last + 1`; anything else
is a gap the client did not get a `gap` message for (a bug or a lost connection) and it should
reconnect with `since=last`.

## Event payload: `OrderEvent`

`data` of an `event` on the `orders` channel (shared with the streaming-pipeline project):

| Field | Type | Notes |
|---|---|---|
| `order_id` | str | `ord-000001` style, unique per feed run |
| `zone` | str | one of `north`, `south`, `east`, `west`, `central`, `airport` |
| `amount` | float | rupees, 2 decimals, > 0 |
| `status` | str | `placed`, `picked`, `delivered` or `cancelled` |

The built-in feed adds `t_sched` (float, Unix seconds: when the event was *scheduled* to be
published) and, when asked for larger payloads, `pad` (a filler string). Consumers must ignore
fields they do not know.

## Endpoints

| Route | Purpose |
|---|---|
| `WS /ws/{channel}?since=<seq>&client=<id>` | Subscribe. No `since`: live only. `since` inside the ring buffer: the missed events (`seq > since`) are replayed, then live, with no hole or duplicate at the boundary. `since` older than the ring: one `snapshot`, then live. `since` at or past the latest seq: live only. `client` is an optional label shown in `/stats`. |
| `POST /publish/{channel}` | Body `{"type": str, "data": {}}` → `202 {"seq": n}`. The server stamps `seq` and `ts`. If the env var `PUBLISH_TOKEN` is set, `Authorization: Bearer <token>` is required (401 otherwise); unset means open, for local development only. |
| `GET /poll/{channel}?since=<seq>` | Polling baseline for comparison: `{"seq": latest, "events": [envelopes with seq > since, from the ring]}`. |
| `GET /health` | `{"status": "ok"}` |
| `GET /stats` | Strategy, per-channel seq and subscriber count, per-subscriber `{id, channel, client, dropped, queued}`, byte and message counters, server RSS. |
| `GET /` | The dashboard (static files from `web/`). |

## Fan-out strategies

- **`naive`** — the publisher awaits `send()` to each subscriber in turn. One client that stops
  reading eventually fills the socket buffers, and from then on every later client waits behind it.
- **`queued`** — each subscriber has its own bounded queue (`maxsize=K`) and writer task. The
  publisher never awaits a socket. When a queue is full the oldest queued envelope is dropped and
  the subscriber's drop counter goes up; the writer sends a `gap` before the next envelope it sends.
