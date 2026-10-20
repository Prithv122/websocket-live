// Bounded rolling state for one channel. Memory is O(maxEvents + latencySamples + zones +
// windowSeconds) no matter how many events arrive.

const MAX_ZONES = 64; // zone names come off the wire; cap them so a bad feed cannot grow the map
const OVERFLOW_ZONE = "other";

export class RingBuffer {
  #items;
  #start = 0;
  #size = 0;

  constructor(capacity) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new RangeError("capacity must be a positive integer");
    this.#items = new Array(capacity);
  }

  get size() {
    return this.#size;
  }

  push(item) {
    const cap = this.#items.length;
    if (this.#size < cap) {
      this.#items[(this.#start + this.#size) % cap] = item;
      this.#size += 1;
    } else {
      this.#items[this.#start] = item; // overwrite the oldest
      this.#start = (this.#start + 1) % cap;
    }
  }

  /** Oldest first. */
  toArray() {
    const cap = this.#items.length;
    return Array.from({ length: this.#size }, (_, i) => this.#items[(this.#start + i) % cap]);
  }
}

/** Nearest-rank percentile of an ascending array; null when empty. */
export function percentile(sorted, p) {
  if (sorted.length === 0) return null;
  const rank = Math.max(1, Math.ceil((p / 100) * sorted.length));
  return sorted[rank - 1];
}

export function createStore({ maxEvents = 200, latencySamples = 200, windowSeconds = 60, now = Date.now } = {}) {
  const events = new RingBuffer(maxEvents);
  const latencies = new RingBuffer(latencySamples);
  const zones = new Map(); // zone -> {count, revenue}
  // One bucket per wall-clock second; `stamps` says which second a bucket currently holds.
  const bucketCounts = new Array(windowSeconds).fill(0);
  const bucketStamps = new Array(windowSeconds).fill(-1);
  let total = 0;
  let latestLatency = null;

  function countInBucket(nowMs) {
    const second = Math.floor(nowMs / 1000);
    const i = second % windowSeconds;
    if (bucketStamps[i] !== second) {
      bucketStamps[i] = second;
      bucketCounts[i] = 0;
    }
    bucketCounts[i] += 1;
  }

  function addToZone(data) {
    if (typeof data.zone !== "string" || !Number.isFinite(data.amount)) return;
    const key = zones.has(data.zone) || zones.size < MAX_ZONES ? data.zone : OVERFLOW_ZONE;
    const entry = zones.get(key) ?? { count: 0, revenue: 0 };
    entry.count += 1;
    if (data.status !== "cancelled") entry.revenue += data.amount; // cancelled orders earn nothing
    zones.set(key, entry);
  }

  return {
    /**
     * @param envelope an `event` envelope
     * @param opts.replay true for events delivered inside a snapshot: they are history, so
     *   they count towards totals but not towards throughput or latency.
     */
    add(envelope, { replay = false } = {}) {
      const receivedAt = now();
      const data = envelope.data ?? {};
      total += 1;
      events.push({ seq: envelope.seq, ts: envelope.ts, receivedAt, data });
      addToZone(data);
      if (replay) return;
      countInBucket(receivedAt);
      const sentAt = Date.parse(envelope.ts);
      if (Number.isFinite(sentAt)) {
        latestLatency = receivedAt - sentAt;
        latencies.push(latestLatency);
      }
    },

    snapshot() {
      const nowMs = now();
      const second = Math.floor(nowMs / 1000);
      const series = [];
      for (let k = windowSeconds - 1; k >= 0; k -= 1) {
        const s = second - k;
        const i = ((s % windowSeconds) + windowSeconds) % windowSeconds;
        series.push(bucketStamps[i] === s ? bucketCounts[i] : 0);
      }
      // Average over the last 5 *complete* seconds; the current one is still filling.
      const recent = series.slice(-6, -1);
      const sorted = latencies.toArray().sort((a, b) => a - b);

      return {
        total,
        events: events.toArray(),
        zones: [...zones]
          .map(([zone, v]) => ({ zone, ...v }))
          .sort((a, b) => b.revenue - a.revenue || a.zone.localeCompare(b.zone)),
        rate: {
          series,
          perSecond: recent.reduce((a, b) => a + b, 0) / recent.length,
          peak: Math.max(...series),
        },
        latency: {
          latest: latestLatency,
          p50: percentile(sorted, 50),
          p95: percentile(sorted, 95),
          samples: sorted.length,
        },
      };
    },
  };
}
