// Panel renderers. Each is a pure function `view -> {id, title, html, wide?}` where
//   view = {channel, now, store: <store.snapshot()>, conn: <client.getState()>}
// and `html` is a string injected with innerHTML, so every value that came off the wire
// goes through esc() first. Charts are hand-built SVG.

const intFmt = new Intl.NumberFormat("en-IN");
const rupeeFmt = new Intl.NumberFormat("en-IN", { maximumFractionDigits: 0 });
const amountFmt = new Intl.NumberFormat("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const oneDecimal = new Intl.NumberFormat("en-IN", { minimumFractionDigits: 1, maximumFractionDigits: 1 });

const ESCAPES = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
export const esc = (value) => String(value).replace(/[&<>"']/g, (c) => ESCAPES[c]);

export const formatInt = (n) => intFmt.format(n);
export const formatRupees = (n) => `\u20B9${rupeeFmt.format(n)}`;
export const formatAmount = (n) => amountFmt.format(n);
export const formatMs = (ms) => (ms === null || ms === undefined ? "\u2013" : `${intFmt.format(Math.round(ms))} ms`);

export function formatAge(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  return `${Math.floor(s / 3600)}h`;
}

const empty = (text) => `<p class="empty">${esc(text)}</p>`;

/* 1. Revenue per zone: labelled horizontal bars. Labels are HTML text so they stay readable
   at any width; only the bar itself is SVG (width as an attribute, no inline styles). */

export function revenueByZone({ store }) {
  const { zones } = store;
  const title = "Revenue by zone";
  if (zones.length === 0) return { id: "revenue-by-zone", title, html: empty("Waiting for orders\u2026") };

  const max = Math.max(...zones.map((z) => z.revenue), 1);
  const label = `${title}: ${zones.map((z) => `${z.zone} ${formatRupees(z.revenue)}`).join(", ")}`;
  const rows = zones
    .map((z) => {
      const width = Math.max(1, (z.revenue / max) * 100).toFixed(1);
      return (
        `<div class="bar-row"><span class="bar-label">${esc(z.zone)}</span>` +
        `<span class="bar-value">${formatRupees(z.revenue)}</span>` +
        `<svg class="bar-track" viewBox="0 0 100 6" preserveAspectRatio="none" aria-hidden="true">` +
        `<rect class="bar" width="${width}" height="6"/></svg></div>`
      );
    })
    .join("");
  return { id: "revenue-by-zone", title, html: `<div class="bars" role="img" aria-label="${esc(label)}">${rows}</div>` };
}

/* 2. Events per second over the last 60 s. */

const SPARK = { width: 360, height: 72, pad: 6 };

export function eventsPerSecond({ store }) {
  const { series, perSecond, peak } = store.rate;
  const title = "Events per second";
  const n = series.length;
  const max = Math.max(peak, 1);
  const x = (i) => ((i / (n - 1)) * SPARK.width).toFixed(1);
  const y = (v) => (SPARK.height - SPARK.pad - (v / max) * (SPARK.height - 2 * SPARK.pad)).toFixed(1);
  const points = series.map((v, i) => `${x(i)},${y(v)}`).join(" ");
  const base = SPARK.height - SPARK.pad;
  const label = `${title} over the last ${n} seconds: about ${oneDecimal.format(perSecond)} per second, peak ${formatInt(peak)}`;
  const html =
    `<p class="stat"><strong>${oneDecimal.format(perSecond)}</strong> events/s ` +
    `<span class="muted">peak ${formatInt(peak)} \u00B7 last ${n} s</span></p>` +
    `<svg class="chart" viewBox="0 0 ${SPARK.width} ${SPARK.height}" role="img" aria-label="${esc(label)}">` +
    `<line class="axis" x1="0" y1="${base}" x2="${SPARK.width}" y2="${base}"/>` +
    `<polygon class="spark-area" points="0,${base} ${points} ${SPARK.width},${base}"/>` +
    `<polyline class="spark-line" points="${points}"/></svg>`;
  return { id: "events-per-second", title, html };
}

/* 3. The latest 15 orders as a real table. */

export function latestOrders({ store, now }) {
  const title = "Latest orders";
  const rows = store.events.slice(-15).reverse();
  if (rows.length === 0) return { id: "latest-orders", title, html: empty("Waiting for orders\u2026"), wide: true };

  const body = rows
    .map(({ data, receivedAt }) => {
      const status = String(data.status ?? "");
      const amount = Number.isFinite(data.amount) ? formatAmount(data.amount) : "\u2013";
      return (
        `<tr><th scope="row">${esc(data.order_id ?? "\u2013")}</th><td>${esc(data.zone ?? "\u2013")}</td>` +
        `<td class="num">${amount}</td><td><span class="status status--${esc(status.replace(/[^a-z]/g, ""))}">${esc(status || "\u2013")}</span></td>` +
        `<td class="num">${formatAge(now - receivedAt)}</td></tr>`
      );
    })
    .join("");
  const html =
    `<div class="table-wrap"><table><caption class="visually-hidden">The ${rows.length} most recent orders, newest first</caption>` +
    `<thead><tr><th scope="col">Order</th><th scope="col">Zone</th><th scope="col" class="num">Amount (\u20B9)</th>` +
    `<th scope="col">Status</th><th scope="col" class="num">Age</th></tr></thead><tbody>${body}</tbody></table></div>`;
  return { id: "latest-orders", title, html, wide: true };
}

/* 4. Connection health. */

const STATE_LABEL = { idle: "Idle", connecting: "Connecting", open: "Connected", closed: "Disconnected" };

export function statusLabel({ state, delayMs, attempt }) {
  if (state === "reconnecting") return `Reconnecting in ${oneDecimal.format((delayMs ?? 0) / 1000)} s (attempt ${attempt})`;
  return STATE_LABEL[state] ?? state;
}

/** What the polite live region says; used only when the connection state changes. */
export function statusAnnouncement(channel, state) {
  const words = { connecting: "connecting", open: "connected", reconnecting: "connection lost, reconnecting", closed: "disconnected" };
  return `${channel} channel: ${words[state] ?? state}`;
}

export function connectionPanel({ store, conn }) {
  const { counters, status, lastSeq } = conn;
  const { latency } = store;
  const dd = (term, value) => `<div><dt>${term}</dt><dd>${value}</dd></div>`;
  const html =
    `<dl class="kv">` +
    dd("Status", `<span class="dot dot--${esc(status.state)}" aria-hidden="true"></span>${esc(statusLabel(status))}`) +
    dd("Last seq", lastSeq === null ? "\u2013" : formatInt(lastSeq)) +
    dd("Events received", formatInt(store.total)) +
    dd("Reconnects", formatInt(counters.reconnects)) +
    dd("Server restarts", formatInt(counters.resets)) +
    dd("Gaps", formatInt(counters.gaps)) +
    dd("Dropped by server", formatInt(counters.droppedByServer)) +
    dd("Duplicates ignored", formatInt(counters.duplicates)) +
    dd("Latency (latest)", formatMs(latency.latest)) +
    dd("Latency p50 / p95", `${formatMs(latency.p50)} / ${formatMs(latency.p95)}`) +
    `</dl><p class="note">Latency is this browser's clock minus the server stamp, so it is only meaningful when the clocks agree.</p>`;
  return { id: "connection", title: "Connection", html };
}

/** Panels for the built-in orders channel, and for any channel with no renderers of its own. */
export const defaultRenderers = {
  orders: [revenueByZone, eventsPerSecond, latestOrders, connectionPanel],
  "*": [eventsPerSecond, connectionPanel],
};
