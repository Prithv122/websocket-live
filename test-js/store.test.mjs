import { test } from "node:test";
import assert from "node:assert/strict";
import { createStore, percentile, RingBuffer } from "../web/js/store.js";
import { makeClock } from "./helpers.mjs";

const T0 = Date.parse("2026-10-20T12:00:00.000Z");
const order = (seq, zone = "north", amount = 100, status = "placed", ts = new Date(T0).toISOString()) => ({
  seq,
  ts,
  type: "event",
  data: { order_id: `ord-${seq}`, zone, amount, status },
});

function setup(options = {}) {
  const clock = makeClock(T0);
  return { clock, store: createStore({ now: clock.now, ...options }) };
}

test("RingBuffer overwrites the oldest and returns oldest-first", () => {
  const ring = new RingBuffer(3);
  for (let i = 1; i <= 5; i += 1) ring.push(i);
  assert.deepEqual(ring.toArray(), [3, 4, 5]);
  assert.equal(ring.size, 3);
  assert.throws(() => new RingBuffer(0), RangeError);
});

test("memory stays bounded across 100k events", () => {
  const { clock, store } = setup();
  const zones = ["north", "south", "east", "west", "central", "airport"];
  for (let i = 1; i <= 100_000; i += 1) {
    store.add(order(i, zones[i % zones.length], 10), {});
    if (i % 20 === 0) clock.advance(1);
  }
  const snap = store.snapshot();
  assert.equal(snap.total, 100_000);
  assert.equal(snap.events.length, 200);
  assert.equal(snap.events[0].seq, 100_000 - 199, "keeps the newest 200");
  assert.equal(snap.events.at(-1).seq, 100_000);
  assert.equal(snap.rate.series.length, 60);
  assert.equal(snap.latency.samples, 200);
  assert.equal(snap.zones.length, 6);
});

test("zone names from the wire cannot grow the map without bound", () => {
  const { store } = setup();
  for (let i = 0; i < 1000; i += 1) store.add(order(i + 1, `zone-${i}`, 1));
  const zones = store.snapshot().zones;
  assert.equal(zones.length, 65, "64 named zones plus the overflow bucket");
  assert.equal(zones.find((z) => z.zone === "other").count, 1000 - 64);
});

test("per-zone counts and revenue sum correctly, largest revenue first", () => {
  const { store } = setup();
  store.add(order(1, "north", 100.5));
  store.add(order(2, "north", 50.25));
  store.add(order(3, "south", 300));
  store.add(order(4, "east", 10));
  const { zones } = store.snapshot();
  assert.deepEqual(zones, [
    { zone: "south", count: 1, revenue: 300 },
    { zone: "north", count: 2, revenue: 150.75 },
    { zone: "east", count: 1, revenue: 10 },
  ]);
});

test("cancelled orders are counted but earn no revenue", () => {
  const { store } = setup();
  store.add(order(1, "north", 100, "delivered"));
  store.add(order(2, "north", 999, "cancelled"));
  assert.deepEqual(store.snapshot().zones, [{ zone: "north", count: 2, revenue: 100 }]);
});

test("events without a zone or amount are stored but not aggregated", () => {
  const { store } = setup();
  store.add({ seq: 1, ts: new Date(T0).toISOString(), data: { kind: "other" } });
  store.add({ seq: 2, ts: new Date(T0).toISOString(), data: { zone: "north", amount: "NaN" } });
  const snap = store.snapshot();
  assert.equal(snap.total, 2);
  assert.deepEqual(snap.zones, []);
});

test("events/s buckets by receive time over a 60 s window", () => {
  const { clock, store } = setup();
  for (let i = 0; i < 5; i += 1) store.add(order(i + 1)); // second S
  clock.advance(1000);
  for (let i = 0; i < 3; i += 1) store.add(order(i + 10)); // second S+1
  const { series } = store.snapshot().rate;
  assert.equal(series.length, 60);
  assert.deepEqual(series.slice(-2), [5, 3]);
  assert.equal(series.slice(0, -2).reduce((a, b) => a + b, 0), 0);
});

test("events fall out of the window after 60 s and buckets are reused cleanly", () => {
  const { clock, store } = setup();
  store.add(order(1));
  clock.advance(59_000);
  assert.equal(store.snapshot().rate.series[0], 1, "still the oldest bucket");
  clock.advance(1000);
  assert.equal(store.snapshot().rate.series.reduce((a, b) => a + b, 0), 0);
  store.add(order(2)); // lands in the bucket the first event used
  assert.equal(store.snapshot().rate.series.at(-1), 1);
});

test("perSecond averages the last 5 complete seconds and peak tracks the window", () => {
  const { clock, store } = setup();
  for (const n of [10, 10, 10, 10, 10, 2]) {
    for (let i = 0; i < n; i += 1) store.add(order(1));
    clock.advance(1000);
  }
  const { rate } = store.snapshot();
  // Window now ends in an empty current second; the 5 before it are 10,10,10,10,2.
  assert.equal(rate.perSecond, 42 / 5);
  assert.equal(rate.peak, 10);
});

test("latency is receive time minus the server stamp; p50/p95 are nearest-rank", () => {
  const { clock, store } = setup();
  for (let i = 1; i <= 100; i += 1) {
    clock.advance(0);
    store.add(order(i, "north", 1, "placed", new Date(T0 - i).toISOString())); // i ms old
  }
  const { latency } = store.snapshot();
  assert.equal(latency.latest, 100);
  assert.equal(latency.p50, 50);
  assert.equal(latency.p95, 95);
  assert.equal(latency.samples, 100);
});

test("latency percentiles are over the last 200 samples only", () => {
  const { store } = setup();
  for (let i = 0; i < 500; i += 1) {
    const age = i < 300 ? 5000 : 10; // old slow samples then fast ones
    store.add(order(i + 1, "north", 1, "placed", new Date(T0 - age).toISOString()));
  }
  const { latency } = store.snapshot();
  assert.equal(latency.samples, 200);
  assert.equal(latency.p95, 10);
});

test("percentile helper edge cases", () => {
  assert.equal(percentile([], 50), null);
  assert.equal(percentile([7], 95), 7);
  assert.equal(percentile([1, 2, 3, 4], 50), 2);
  assert.equal(percentile([1, 2, 3, 4], 100), 4);
});

test("an unparseable ts yields no latency sample but the event still counts", () => {
  const { store } = setup();
  store.add({ seq: 1, ts: "garbage", data: { zone: "north", amount: 5 } });
  const snap = store.snapshot();
  assert.equal(snap.latency.latest, null);
  assert.equal(snap.latency.p50, null);
  assert.equal(snap.total, 1);
});

test("replayed (snapshot) events count towards totals but not throughput or latency", () => {
  const { store } = setup();
  store.add(order(1, "north", 10, "placed", "2020-01-01T00:00:00.000Z"), { replay: true });
  const snap = store.snapshot();
  assert.equal(snap.total, 1);
  assert.equal(snap.zones[0].revenue, 10);
  assert.equal(snap.latency.samples, 0);
  assert.equal(snap.rate.series.at(-1), 0);
});
