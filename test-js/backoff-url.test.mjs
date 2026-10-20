import { test } from "node:test";
import assert from "node:assert/strict";
import { backoffDelay, buildUrl, originFromLocation, DEFAULT_BACKOFF } from "../web/js/ws-client.js";

test("backoff ceiling doubles per attempt: 500, 1000, 2000, ...", () => {
  const max = () => 0.999999999;
  const ceilings = [0, 1, 2, 3].map((a) => backoffDelay(a, DEFAULT_BACKOFF, max));
  ceilings.forEach((d, i) => assert.ok(Math.abs(d - 500 * 2 ** i) < 1e-3, `attempt ${i}: ${d}`));
});

test("backoff is capped at maxMs however many attempts pile up", () => {
  const max = () => 0.999999999;
  for (const attempt of [6, 10, 50, 1000]) {
    assert.ok(backoffDelay(attempt, DEFAULT_BACKOFF, max) <= 15000);
  }
  assert.ok(backoffDelay(50, DEFAULT_BACKOFF, max) > 14999);
});

test("full jitter: the draw spans [0, ceiling) rather than adding a small wobble", () => {
  assert.equal(backoffDelay(3, DEFAULT_BACKOFF, () => 0), 0);
  assert.equal(backoffDelay(3, DEFAULT_BACKOFF, () => 0.5), 2000);
  assert.equal(backoffDelay(0, DEFAULT_BACKOFF, () => 0.25), 125);
});

test("backoff honours custom parameters", () => {
  assert.equal(backoffDelay(2, { baseMs: 100, maxMs: 1000, factor: 3 }, () => 0.5), 450);
});

test("first connect omits since", () => {
  assert.equal(
    buildUrl({ url: "ws://h:8000", channel: "orders", clientId: "abc" }),
    "ws://h:8000/ws/orders?client=abc",
  );
});

test("resume includes since before client", () => {
  assert.equal(
    buildUrl({ url: "ws://h:8000", channel: "orders", since: 42, clientId: "abc" }),
    "ws://h:8000/ws/orders?since=42&client=abc",
  );
});

test("since=0 is a real value, not 'absent'", () => {
  assert.match(buildUrl({ url: "ws://h", channel: "c", since: 0 }), /since=0/);
});

test("no query string at all when there is nothing to say", () => {
  assert.equal(buildUrl({ url: "ws://h/", channel: "orders" }), "ws://h/ws/orders");
});

test("origin follows the page protocol: https gives wss, http gives ws", () => {
  assert.equal(originFromLocation({ protocol: "https:", host: "example.com" }), "wss://example.com");
  assert.equal(originFromLocation({ protocol: "http:", host: "127.0.0.1:8000" }), "ws://127.0.0.1:8000");
});
