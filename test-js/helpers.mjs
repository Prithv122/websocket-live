// Fakes shared by the client tests: a manual clock and a scriptable WebSocket class.

export function makeClock(start = 1_700_000_000_000) {
  let time = start;
  let nextId = 1;
  const timers = new Map();
  return {
    now: () => time,
    setTimeout(fn, ms) {
      const id = nextId++;
      timers.set(id, { fn, at: time + ms });
      return id;
    },
    clearTimeout(id) {
      timers.delete(id);
    },
    pending: () => timers.size,
    /** Advance time, firing due timers in order (timers may schedule more timers). */
    advance(ms) {
      const target = time + ms;
      for (;;) {
        const due = [...timers].filter(([, t]) => t.at <= target).sort((a, b) => a[1].at - b[1].at)[0];
        if (!due) break;
        timers.delete(due[0]);
        time = Math.max(time, due[1].at);
        due[1].fn();
      }
      time = target;
    },
  };
}

export function makeFakeWebSocket() {
  class FakeWebSocket {
    static instances = [];
    static get last() {
      return FakeWebSocket.instances.at(-1);
    }

    constructor(url) {
      this.url = url;
      this.closed = false;
      FakeWebSocket.instances.push(this);
    }

    close() {
      this.closed = true;
    }

    // Test-side controls.
    open() {
      this.onopen?.({});
    }
    receive(message) {
      this.onmessage?.({ data: typeof message === "string" ? message : JSON.stringify(message) });
    }
    serverClose() {
      this.onclose?.({});
    }
  }
  return FakeWebSocket;
}

export const envelope = (seq, type = "event", data = {}, channel = "orders") => ({
  v: 1,
  channel,
  seq,
  ts: "2026-10-20T12:00:00.000Z",
  type,
  data,
});

/** A client wired to fakes, with every emitted event recorded. */
export async function harness(overrides = {}) {
  const { createClient } = await import("../web/js/ws-client.js");
  const clock = makeClock();
  const WebSocketImpl = makeFakeWebSocket();
  const client = createClient({
    url: "ws://localhost:8000",
    channel: "orders",
    WebSocketImpl,
    setTimeout: clock.setTimeout,
    clearTimeout: clock.clearTimeout,
    now: clock.now,
    random: () => 0.5,
    clientId: "test",
    ...overrides,
  });
  const log = [];
  for (const name of ["event", "gap", "snapshot", "status", "reset"]) client.on(name, (p) => log.push([name, p]));
  const of = (name) => log.filter(([n]) => n === name).map(([, p]) => p);
  return { client, clock, WebSocketImpl, log, of };
}
