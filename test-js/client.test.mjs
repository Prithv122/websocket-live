import { test } from "node:test";
import assert from "node:assert/strict";
import { harness, envelope } from "./helpers.mjs";

const connectAndOpen = async (overrides) => {
  const h = await harness(overrides);
  h.client.start();
  h.WebSocketImpl.last.open();
  return h;
};

test("start connects without since and reports connecting then open", async () => {
  const h = await harness();
  h.client.start();
  assert.equal(h.WebSocketImpl.last.url, "ws://localhost:8000/ws/orders?client=test");
  h.WebSocketImpl.last.open();
  assert.deepEqual(h.of("status").map((s) => s.state), ["connecting", "open"]);
});

test("in-order events are emitted and advance lastSeq", async () => {
  const h = await connectAndOpen();
  for (const seq of [1, 2, 3]) h.WebSocketImpl.last.receive(envelope(seq));
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2, 3]);
  assert.equal(h.client.getState().lastSeq, 3);
});

test("the first event may start anywhere (live-only connect to a busy channel)", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(envelope(501));
  h.WebSocketImpl.last.receive(envelope(502));
  assert.deepEqual(h.of("event").map((e) => e.seq), [501, 502]);
  assert.equal(h.of("gap").length, 0);
});

test("duplicates and stale seqs are ignored and counted", async () => {
  const h = await connectAndOpen();
  for (const seq of [1, 2, 2, 1, 3]) h.WebSocketImpl.last.receive(envelope(seq));
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2, 3]);
  assert.equal(h.client.getState().counters.duplicates, 2);
});

test("a hole in the sequence emits a client_detected gap and reconnects with since=last", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(envelope(1));
  h.WebSocketImpl.last.receive(envelope(2));
  const first = h.WebSocketImpl.last;
  first.receive(envelope(5)); // 3 and 4 never arrived

  assert.deepEqual(h.of("gap"), [{ from: 3, to: 4, reason: "client_detected" }]);
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2], "the out-of-order event is not delivered");
  assert.ok(first.closed, "old socket is closed");
  assert.equal(h.client.getState().status.state, "reconnecting");

  h.clock.advance(500); // random()=0.5 and attempt 0 -> 250 ms
  assert.equal(h.WebSocketImpl.instances.length, 2);
  assert.equal(h.WebSocketImpl.last.url, "ws://localhost:8000/ws/orders?since=2&client=test");

  // The server replays 3.. and the stream continues without a hole.
  h.WebSocketImpl.last.open();
  for (const seq of [3, 4, 5]) h.WebSocketImpl.last.receive(envelope(seq));
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2, 3, 4, 5]);
  assert.equal(h.client.getState().counters.gaps, 1);
});

test("a late message from the abandoned socket is ignored", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(envelope(1));
  const old = h.WebSocketImpl.last;
  old.receive(envelope(9)); // triggers the reconnect
  old.receive(envelope(2));
  old.serverClose();
  assert.deepEqual(h.of("event").map((e) => e.seq), [1]);
  assert.equal(h.client.getState().counters.reconnects, 1, "the stale close must not schedule a second reconnect");
});

test("a server gap is emitted, counted, and keeps the stream continuous", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(envelope(1));
  h.WebSocketImpl.last.receive(envelope(7, "gap", { from: 2, to: 7, reason: "slow_consumer" }));
  h.WebSocketImpl.last.receive(envelope(8));
  assert.deepEqual(h.of("gap"), [{ from: 2, to: 7, reason: "slow_consumer" }]);
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 8]);
  const { counters } = h.client.getState();
  assert.equal(counters.droppedByServer, 6);
  assert.equal(counters.gaps, 1);
  assert.equal(h.WebSocketImpl.instances.length, 1, "no reconnect for a server-announced gap");
});

test("a snapshot is emitted whole and sets lastSeq", async () => {
  const h = await connectAndOpen();
  const events = [envelope(10), envelope(11), envelope(12)];
  h.WebSocketImpl.last.receive(envelope(12, "snapshot", { oldest: 10, events }));
  h.WebSocketImpl.last.receive(envelope(13));
  assert.deepEqual(h.of("snapshot"), [{ oldest: 10, events }]);
  assert.deepEqual(h.of("event").map((e) => e.seq), [13]);
  assert.equal(h.client.getState().lastSeq, 13);
});

test("heartbeats are liveness only", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(envelope(1));
  h.WebSocketImpl.last.receive(envelope(1, "heartbeat"));
  assert.equal(h.of("event").length, 1);
  assert.equal(h.client.getState().lastSeq, 1);
});

test("heartbeat without an epoch (older server): a seq behind ours still resets", async () => {
  const h = await connectAndOpen();
  for (const seq of [1, 2, 3]) h.WebSocketImpl.last.receive(envelope(seq));
  h.WebSocketImpl.last.receive(envelope(0, "heartbeat"));
  assert.deepEqual(h.of("reset"), [{ from: 3, to: 0, epoch: null }]);
  h.WebSocketImpl.last.receive(envelope(1));
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2, 3, 1]);
});

test("unknown types are ignored but still count as liveness", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(envelope(1, "telemetry-from-the-future"));
  assert.equal(h.log.filter(([n]) => n === "event" || n === "gap").length, 0);
  assert.equal(h.client.getState().counters.malformed, 0);
  assert.equal(h.client.getState().lastSeq, null);
});

test("malformed JSON and malformed envelopes are ignored and counted", async () => {
  const h = await connectAndOpen();
  const ws = h.WebSocketImpl.last;
  ws.receive("{not json");
  ws.receive("null");
  ws.receive(JSON.stringify({ type: "event" })); // no seq
  ws.receive(envelope(1));
  assert.equal(h.client.getState().counters.malformed, 3);
  assert.deepEqual(h.of("event").map((e) => e.seq), [1]);
});

test("a dropped connection reconnects with since and a jittered delay", async () => {
  const h = await connectAndOpen({ random: () => 0.5 });
  h.WebSocketImpl.last.receive(envelope(1));
  h.WebSocketImpl.last.serverClose();
  const [status] = h.of("status").slice(-1);
  assert.equal(status.state, "reconnecting");
  assert.equal(status.delayMs, 250);
  h.clock.advance(249);
  assert.equal(h.WebSocketImpl.instances.length, 1);
  h.clock.advance(1);
  assert.equal(h.WebSocketImpl.instances.length, 2);
  assert.match(h.WebSocketImpl.last.url, /since=1&client=test/);
});

test("delays grow while connections never become healthy", async () => {
  const h = await harness({ random: () => 0.999999 });
  h.client.start();
  const delays = [];
  for (let i = 0; i < 4; i += 1) {
    h.WebSocketImpl.last.serverClose(); // accepted then dropped without a message
    delays.push(Math.round(h.of("status").at(-1).delayMs));
    h.clock.advance(20000);
  }
  assert.deepEqual(delays, [500, 1000, 2000, 4000]);
});

test("the attempt counter resets once a connection has delivered a message", async () => {
  const h = await harness({ random: () => 0.999999 });
  h.client.start();
  h.WebSocketImpl.last.serverClose();
  h.clock.advance(20000);
  h.WebSocketImpl.last.serverClose();
  assert.equal(Math.round(h.of("status").at(-1).delayMs), 1000);
  h.clock.advance(20000);

  h.WebSocketImpl.last.open();
  h.WebSocketImpl.last.receive(envelope(1, "heartbeat")); // healthy
  h.WebSocketImpl.last.serverClose();
  assert.equal(Math.round(h.of("status").at(-1).delayMs), 500);
});

test("opening alone is not healthy: a server that accepts then closes keeps backing off", async () => {
  const h = await harness({ random: () => 0.999999 });
  h.client.start();
  h.WebSocketImpl.last.open();
  h.WebSocketImpl.last.serverClose();
  h.clock.advance(20000);
  h.WebSocketImpl.last.open();
  h.WebSocketImpl.last.serverClose();
  assert.equal(Math.round(h.of("status").at(-1).delayMs), 1000);
});

test("watchdog: silence for staleMs closes the socket and reconnects", async () => {
  const h = await connectAndOpen({ staleMs: 25000 });
  h.WebSocketImpl.last.receive(envelope(1));
  const first = h.WebSocketImpl.last;
  h.clock.advance(24999);
  assert.equal(first.closed, false);
  h.clock.advance(1);
  assert.equal(first.closed, true);
  assert.equal(h.of("status").at(-1).reason, "stale");
  h.clock.advance(500);
  assert.equal(h.WebSocketImpl.instances.length, 2);
  assert.match(h.WebSocketImpl.last.url, /since=1/);
});

test("watchdog: any message, including a heartbeat, pushes the deadline back", async () => {
  const h = await connectAndOpen({ staleMs: 25000 });
  for (let i = 0; i < 5; i += 1) {
    h.clock.advance(20000);
    h.WebSocketImpl.last.receive(envelope(0, "heartbeat"));
  }
  assert.equal(h.WebSocketImpl.instances.length, 1);
  assert.equal(h.WebSocketImpl.last.closed, false);
});

test("stop closes the socket, cancels timers and never reconnects", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.serverClose(); // a retry timer is now pending
  h.client.stop();
  assert.equal(h.clock.pending(), 0);
  h.clock.advance(60000);
  assert.equal(h.WebSocketImpl.instances.length, 1);
  assert.equal(h.of("status").at(-1).state, "closed");

  const live = await connectAndOpen();
  const ws = live.WebSocketImpl.last;
  live.client.stop();
  assert.ok(ws.closed);
  ws.serverClose();
  assert.equal(live.clock.pending(), 0, "a late close after stop() does not schedule anything");
});

test("on() returns an unsubscribe function", async () => {
  const h = await connectAndOpen();
  const seen = [];
  const off = h.client.on("event", (e) => seen.push(e.seq));
  h.WebSocketImpl.last.receive(envelope(1));
  off();
  h.WebSocketImpl.last.receive(envelope(2));
  assert.deepEqual(seen, [1]);
});

const hb = (seq, epoch) => envelope(seq, "heartbeat", epoch === undefined ? {} : { epoch });

test("the epoch is remembered from the first heartbeat and a repeat changes nothing", async () => {
  const h = await connectAndOpen();
  const ws = h.WebSocketImpl.last;
  ws.receive(hb(0, "A"));
  ws.receive(envelope(1));
  ws.receive(envelope(2));
  ws.receive(hb(2, "A"));
  ws.receive(hb(1, "A")); // same epoch: seq alone never causes a reset
  assert.equal(h.of("reset").length, 0);
  assert.equal(h.client.getState().lastSeq, 2);
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2]);
});

test("same-epoch reconnect resumes with since and the replay is delivered as before", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(hb(0, "A"));
  h.WebSocketImpl.last.receive(envelope(1));
  h.WebSocketImpl.last.receive(envelope(2));
  h.WebSocketImpl.last.serverClose();
  h.clock.advance(500);
  const second = h.WebSocketImpl.last;
  assert.match(second.url, /since=2/);
  second.open();
  second.receive(hb(4, "A")); // first message: latest seq at subscribe time
  second.receive(envelope(3));
  second.receive(envelope(4));
  second.receive(envelope(5));
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2, 3, 4, 5]);
  assert.equal(h.of("reset").length, 0);
});

test("restart with the new server behind: reset, then its live events are accepted", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(hb(0, "A"));
  for (let seq = 1; seq <= 50; seq += 1) h.WebSocketImpl.last.receive(envelope(seq));
  h.WebSocketImpl.last.serverClose();
  h.clock.advance(500);
  const second = h.WebSocketImpl.last;
  assert.match(second.url, /since=50/);
  second.open();
  second.receive(hb(3, "B")); // new instance, only 3 events so far
  assert.deepEqual(h.of("reset"), [{ from: 50, to: 3, epoch: "B" }]);
  assert.equal(h.client.getState().lastSeq, 3);
  second.receive(envelope(4));
  second.receive(envelope(5));
  assert.deepEqual(h.of("event").slice(-2).map((e) => e.seq), [4, 5]);
  assert.equal(h.client.getState().counters.resets, 1);
});

test("restart with the new server ahead: its replay is ignored as duplicates, live is accepted", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(hb(0, "A"));
  for (let seq = 1; seq <= 10; seq += 1) h.WebSocketImpl.last.receive(envelope(seq));
  h.WebSocketImpl.last.serverClose();
  h.clock.advance(500);
  const second = h.WebSocketImpl.last;
  second.open();
  second.receive(hb(30, "B")); // the new server is already at 30
  assert.deepEqual(h.of("reset"), [{ from: 10, to: 30, epoch: "B" }]);
  for (let seq = 11; seq <= 30; seq += 1) second.receive(envelope(seq)); // replay from its ring
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  assert.equal(h.client.getState().counters.duplicates, 20);
  second.receive(envelope(31));
  assert.equal(h.of("event").at(-1).seq, 31);
  assert.equal(h.of("gap").length, 0);
});

test("restart followed by a snapshot from the new server: not emitted, seq adopted", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(hb(0, "A"));
  h.WebSocketImpl.last.receive(envelope(1));
  h.WebSocketImpl.last.serverClose();
  h.clock.advance(500);
  const second = h.WebSocketImpl.last;
  second.open();
  second.receive(hb(500, "B"));
  second.receive(envelope(500, "snapshot", { oldest: 400, events: [envelope(400), envelope(500)] }));
  second.receive(envelope(501));
  assert.equal(h.of("snapshot").length, 0);
  assert.deepEqual(h.of("event").map((e) => e.seq), [1, 501]);
});

test("a first-ever heartbeat with an epoch is not a restart, even when events preceded it", async () => {
  const h = await connectAndOpen();
  h.WebSocketImpl.last.receive(envelope(1));
  h.WebSocketImpl.last.receive(hb(1, "A"));
  assert.equal(h.of("reset").length, 0);
});
