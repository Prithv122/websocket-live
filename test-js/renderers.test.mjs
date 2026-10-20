import { test } from "node:test";
import assert from "node:assert/strict";
import {
  connectionPanel,
  defaultRenderers,
  esc,
  eventsPerSecond,
  formatAge,
  formatRupees,
  latestOrders,
  revenueByZone,
  statusAnnouncement,
  statusLabel,
} from "../web/js/renderers.js";
import { createRenderScheduler } from "../web/js/dashboard.js";
import { createStore } from "../web/js/store.js";

const NOW = Date.parse("2026-10-20T12:00:00.000Z");

function viewWith(orders = [], conn = {}) {
  const store = createStore({ now: () => NOW });
  orders.forEach((data, i) => store.add({ seq: i + 1, ts: new Date(NOW - 20).toISOString(), data }));
  return {
    channel: "orders",
    now: NOW + 3000,
    store: store.snapshot(),
    conn: {
      status: { state: "open", attempt: 0, delayMs: null, reason: null },
      lastSeq: orders.length || null,
      counters: { reconnects: 0, gaps: 0, droppedByServer: 0, duplicates: 0, malformed: 0, resets: 0 },
      ...conn,
    },
  };
}

const order = (n, zone, amount, status = "placed") => ({ order_id: `ord-${String(n).padStart(6, "0")}`, zone, amount, status });

/** Count opening and closing tags so we notice unbalanced markup without a DOM. */
function assertBalanced(html, tags) {
  for (const tag of tags) {
    const open = (html.match(new RegExp(`<${tag}[\\s>]`, "g")) ?? []).length;
    const close = (html.match(new RegExp(`</${tag}>`, "g")) ?? []).length;
    assert.equal(open, close, `<${tag}> unbalanced`);
  }
}

test("Indian digit grouping and the rupee sign", () => {
  assert.equal(formatRupees(1234567), "₹12,34,567");
  assert.equal(formatRupees(999.6), "₹1,000");
});

test("esc neutralises markup", () => {
  assert.equal(esc(`<img src=x onerror="a('b')">&`), "&lt;img src=x onerror=&quot;a(&#39;b&#39;)&quot;&gt;&amp;");
});

test("formatAge", () => {
  assert.deepEqual([0, 999, 1000, 59_999, 60_000, 3_600_000, -5].map(formatAge), ["0s", "0s", "1s", "59s", "1m", "1h", "0s"]);
});

test("revenue chart: one bar per zone, a labelled value each, and an aria-label with the numbers", () => {
  const view = viewWith([order(1, "north", 1234567), order(2, "south", 500), order(3, "south", 250)]);
  const { html, title } = revenueByZone(view);
  assert.equal(title, "Revenue by zone");
  assert.match(html, /^<svg [^>]*role="img"/);
  assert.equal((html.match(/<rect /g) ?? []).length, 2);
  assert.match(html, /aria-label="Revenue by zone: north ₹12,34,567, south ₹750"/);
  assert.match(html, />₹12,34,567</);
  assertBalanced(html, ["svg"]);
});

test("revenue chart: the widest bar fits inside its lane and is never zero-width", () => {
  const view = viewWith([order(1, "north", 1_000_000), order(2, "south", 0.01)]);
  const widths = [...revenueByZone(view).html.matchAll(/<rect [^>]*width="([\d.]+)"/g)].map((m) => Number(m[1]));
  assert.equal(widths[0], 360 - 64 - 100);
  assert.ok(widths[1] >= 1);
});

test("revenue chart: empty state is a plain message, not an empty svg", () => {
  assert.doesNotMatch(revenueByZone(viewWith()).html, /<svg/);
});

test("wire strings are escaped in the chart labels and the table", () => {
  const evil = `<script>alert(1)</script>`;
  const view = viewWith([order(1, evil, 5)]);
  view.store.events[0].data.order_id = evil;
  for (const { html } of [revenueByZone(view), latestOrders(view)]) {
    assert.doesNotMatch(html, /<script>/);
    assert.match(html, /&lt;script&gt;/);
  }
});

test("sparkline: 60 points, role=img and an aria-label that states the numbers", () => {
  const view = viewWith([order(1, "north", 1)]);
  const { html } = eventsPerSecond(view);
  const points = html.match(/<polyline class="spark-line" points="([^"]+)"/)[1].split(" ");
  assert.equal(points.length, 60);
  for (const p of points) {
    const [x, y] = p.split(",").map(Number);
    assert.ok(x >= 0 && x <= 360 && y >= 0 && y <= 72, p);
  }
  assert.match(html, /role="img" aria-label="Events per second over the last 60 seconds: about [\d.]+ per second, peak 1"/);
  assert.doesNotMatch(html, /NaN|Infinity/);
});

test("sparkline survives an all-zero window", () => {
  const { html } = eventsPerSecond(viewWith());
  assert.doesNotMatch(html, /NaN|Infinity/);
});

test("orders table: a real table, newest first, capped at 15 rows, ages from receive time", () => {
  const orders = Array.from({ length: 20 }, (_, i) => order(i + 1, "east", 100 + i, i % 2 ? "delivered" : "placed"));
  const { html, wide } = latestOrders(viewWith(orders));
  assert.ok(wide);
  assert.match(html, /<table>/);
  assert.equal((html.match(/<tr>/g) ?? []).length, 1 + 15, "header row + 15");
  assert.ok(html.indexOf("ord-000020") < html.indexOf("ord-000006"), "newest first");
  assert.doesNotMatch(html, /ord-000005</);
  assert.match(html, /<td class="num">3s<\/td>/);
  assert.match(html, /<td class="num">119\.00<\/td>/);
  assert.match(html, /<th scope="col" class="num">Amount \(₹\)<\/th>/);
  assertBalanced(html, ["table", "tr", "thead", "tbody", "td", "span"]);
});

test("connection panel shows status, last seq, counters and latency", () => {
  const view = viewWith([order(1, "north", 1)], {
    lastSeq: 1234567,
    counters: { reconnects: 2, gaps: 3, droppedByServer: 40, duplicates: 1, malformed: 0, resets: 0 },
  });
  const { html } = connectionPanel(view);
  for (const expected of ["Connected", "12,34,567", "<dd>2</dd>", "<dd>3</dd>", "<dd>40</dd>", "20 ms / 20 ms"]) {
    assert.ok(html.includes(expected), `missing ${expected}`);
  }
  assertBalanced(html, ["dl", "div", "dt", "dd"]);
});

test("connection panel before any data shows dashes, not NaN", () => {
  const { html } = connectionPanel(viewWith());
  assert.match(html, /<dt>Last seq<\/dt><dd>–<\/dd>/);
  assert.doesNotMatch(html, /NaN|null|undefined/);
});

test("status labels and announcements", () => {
  assert.equal(statusLabel({ state: "reconnecting", delayMs: 1234, attempt: 3 }), "Reconnecting in 1.2 s (attempt 3)");
  assert.equal(statusLabel({ state: "open" }), "Connected");
  assert.equal(statusAnnouncement("orders", "open"), "orders channel: connected");
  assert.equal(statusAnnouncement("orders", "reconnecting"), "orders channel: connection lost, reconnecting");
});

test("every default renderer returns {id, title, html} and the ids are unique", () => {
  const view = viewWith([order(1, "north", 10)]);
  const outs = defaultRenderers.orders.map((fn) => fn(view));
  for (const out of outs) {
    assert.equal(typeof out.id, "string");
    assert.equal(typeof out.title, "string");
    assert.equal(typeof out.html, "string");
  }
  assert.equal(new Set(outs.map((o) => o.id)).size, outs.length);
  assert.ok(Array.isArray(defaultRenderers["*"]), "fallback list for channels without renderers");
});

test("scheduler: many requests in one frame render once, and nothing renders while hidden", () => {
  const frames = [];
  let hidden = false;
  let renders = 0;
  const s = createRenderScheduler(() => renders++, { raf: (fn) => frames.push(fn), isHidden: () => hidden });

  s.request();
  s.request();
  s.request();
  assert.equal(frames.length, 1, "one frame requested");
  frames.shift()();
  assert.equal(renders, 1);

  s.request(); // a new frame is allowed once the previous one ran
  assert.equal(frames.length, 1);
  hidden = true;
  frames.shift()();
  assert.equal(renders, 1, "hidden: no render");

  hidden = false;
  s.request();
  frames.shift()();
  assert.equal(renders, 2);
});
