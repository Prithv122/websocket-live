// Resilient WebSocket client for the wire protocol in PROTOCOL.md.
// Everything environmental (socket class, timers, randomness, clock) is injectable so the
// reconnect and sequence logic runs under Node with fakes.

export const DEFAULT_BACKOFF = Object.freeze({ baseMs: 500, maxMs: 15000, factor: 2 });

/** Full-jitter backoff: a uniform draw from [0, min(maxMs, baseMs * factor^attempt)). */
export function backoffDelay(attempt, backoff = DEFAULT_BACKOFF, random = Math.random) {
  const { baseMs, maxMs, factor } = backoff;
  return random() * Math.min(maxMs, baseMs * factor ** attempt);
}

/** `ws://host:port` (or `wss://`) for the page's own origin. */
export function originFromLocation(loc) {
  return `${loc.protocol === "https:" ? "wss:" : "ws:"}//${loc.host}`;
}

/** `since` is omitted until the client has seen a sequence number. */
export function buildUrl({ url, channel, since = null, clientId = "" }) {
  const query = new URLSearchParams();
  if (since !== null) query.set("since", String(since));
  if (clientId) query.set("client", clientId);
  const qs = query.toString();
  return `${url.replace(/\/+$/, "")}/ws/${encodeURIComponent(channel)}${qs ? `?${qs}` : ""}`;
}

/**
 * @param {object} options
 * @param {string} options.url      WebSocket origin, e.g. `ws://127.0.0.1:8000`
 * @param {string} options.channel
 * @returns {{start(): void, stop(): void, on(event: string, fn: Function): () => void, getState(): object}}
 *
 * Events: `event` (envelope), `snapshot` ({oldest, events}), `gap` ({from, to, reason}),
 * `reset` ({from, to}) and `status` ({state, attempt, delayMs, reason}).
 */
export function createClient(options) {
  const {
    url,
    channel,
    WebSocketImpl = globalThis.WebSocket,
    backoff = DEFAULT_BACKOFF,
    staleMs = 25000,
  } = options;
  const setTimer = options.setTimeout ?? globalThis.setTimeout.bind(globalThis);
  const clearTimer = options.clearTimeout ?? globalThis.clearTimeout.bind(globalThis);
  const random = options.random ?? Math.random;
  const now = options.now ?? Date.now;
  const clientId = options.clientId ?? `c-${random().toString(36).slice(2, 10)}`;

  const listeners = new Map();
  const counters = { reconnects: 0, gaps: 0, droppedByServer: 0, duplicates: 0, malformed: 0, resets: 0 };

  let socket = null;
  let last = null; // last seq seen; null until the first sequenced message
  let attempt = 0;
  let healthy = false; // true once the current connection has delivered a message
  let stopped = true;
  let retryTimer = null;
  let watchdog = null;
  let lastMessageAt = null;
  let status = { state: "idle", attempt: 0, delayMs: null, reason: null };

  function emit(event, payload) {
    for (const fn of [...(listeners.get(event) ?? [])]) fn(payload);
  }

  function setStatus(state, extra = {}) {
    status = { state, attempt, delayMs: null, reason: null, ...extra };
    emit("status", { ...status });
  }

  function disarmWatchdog() {
    if (watchdog !== null) clearTimer(watchdog);
    watchdog = null;
  }

  function armWatchdog() {
    disarmWatchdog();
    watchdog = setTimer(() => {
      watchdog = null;
      dropAndRetry("stale");
    }, staleMs);
  }

  function connect() {
    retryTimer = null;
    healthy = false;
    setStatus("connecting");
    const ws = new WebSocketImpl(buildUrl({ url, channel, since: last, clientId }));
    socket = ws;
    armWatchdog();
    // Handlers from a socket we already abandoned must not touch current state.
    ws.onopen = () => {
      if (socket === ws) setStatus("open");
    };
    ws.onmessage = (message) => {
      if (socket === ws) handleMessage(message.data);
    };
    ws.onclose = () => {
      if (socket !== ws) return;
      socket = null;
      disarmWatchdog();
      scheduleReconnect("closed");
    };
    ws.onerror = () => {}; // a close event always follows
  }

  function scheduleReconnect(reason) {
    if (stopped) return;
    const delayMs = backoffDelay(attempt, backoff, random);
    attempt += 1;
    counters.reconnects += 1;
    setStatus("reconnecting", { delayMs, reason });
    retryTimer = setTimer(connect, delayMs);
  }

  function dropAndRetry(reason) {
    const ws = socket;
    socket = null;
    disarmWatchdog();
    if (ws) {
      try {
        ws.close();
      } catch {
        // already closing
      }
    }
    scheduleReconnect(reason);
  }

  function markAlive() {
    lastMessageAt = now();
    if (!healthy) {
      healthy = true;
      attempt = 0;
    }
    armWatchdog();
  }

  function handleMessage(raw) {
    let env;
    try {
      env = JSON.parse(raw);
    } catch {
      counters.malformed += 1;
      return;
    }
    if (env === null || typeof env !== "object" || typeof env.type !== "string" || !Number.isInteger(env.seq)) {
      counters.malformed += 1;
      return;
    }
    markAlive();

    switch (env.type) {
      case "event":
        handleEvent(env);
        break;
      case "gap":
        handleServerGap(env);
        break;
      case "snapshot":
        last = env.seq;
        emit("snapshot", { oldest: env.data?.oldest ?? null, events: env.data?.events ?? [] });
        break;
      case "heartbeat":
        // A heartbeat carries the channel's latest seq. Seeing it below ours means the
        // server lost its history (restart); without this, every new event would look
        // like a duplicate forever.
        if (last !== null && env.seq < last) {
          counters.resets += 1;
          emit("reset", { from: last, to: env.seq });
          last = env.seq;
        }
        break;
      default:
        break; // unknown types are ignored, per the protocol
    }
  }

  function handleEvent(env) {
    if (last === null || env.seq === last + 1) {
      last = env.seq;
      emit("event", env);
    } else if (env.seq <= last) {
      counters.duplicates += 1;
    } else {
      counters.gaps += 1;
      emit("gap", { from: last + 1, to: env.seq - 1, reason: "client_detected" });
      dropAndRetry("gap"); // reconnect with since=last; the server replays the hole
    }
  }

  function handleServerGap(env) {
    const { from, to, reason = "slow_consumer" } = env.data ?? {};
    counters.gaps += 1;
    if (Number.isInteger(from) && Number.isInteger(to) && to >= from) {
      counters.droppedByServer += to - from + 1;
    }
    if (last === null || env.seq > last) last = env.seq;
    emit("gap", { from, to, reason });
  }

  return {
    start() {
      if (!stopped) return;
      stopped = false;
      attempt = 0;
      connect();
    },
    stop() {
      stopped = true;
      if (retryTimer !== null) clearTimer(retryTimer);
      retryTimer = null;
      disarmWatchdog();
      const ws = socket;
      socket = null;
      if (ws) ws.close();
      setStatus("closed");
    },
    on(event, fn) {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event).add(fn);
      return () => listeners.get(event).delete(fn);
    },
    getState() {
      return { status: { ...status }, lastSeq: last, lastMessageAt, clientId, counters: { ...counters } };
    },
  };
}
