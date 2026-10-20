/** In-process pipeline metrics. Latency uses a fixed-size reservoir of the most recent samples. */
class Reservoir {
  private buf: Float64Array;
  private idx = 0;
  private filled = 0;
  constructor(size: number) {
    this.buf = new Float64Array(size);
  }
  add(v: number) {
    this.buf[this.idx] = v;
    this.idx = (this.idx + 1) % this.buf.length;
    if (this.filled < this.buf.length) this.filled++;
  }
  percentiles(ps: number[]): { p50?: number | null; p95?: number | null; p99?: number | null } & Record<string, number | null> {
    const out: Record<string, number | null> = {};
    if (this.filled === 0) {
      for (const p of ps) out[`p${p}`] = null;
      return out;
    }
    const sorted = Array.from(this.buf.subarray(0, this.filled)).sort((a, b) => a - b);
    for (const p of ps) {
      const i = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
      out[`p${p}`] = Math.round(sorted[Math.max(0, i)] * 10) / 10;
    }
    return out;
  }
  get count() {
    return this.filled;
  }
}

/** Per-second counter over a sliding minute, for rates. */
class RateCounter {
  private buckets = new Map<number, number>();
  add(n = 1) {
    const s = Math.floor(Date.now() / 1000);
    this.buckets.set(s, (this.buckets.get(s) ?? 0) + n);
    if (this.buckets.size > 120) {
      for (const k of this.buckets.keys()) if (k < s - 60) this.buckets.delete(k);
    }
  }
  /** Average per second over the last `window` complete seconds. */
  rate(window = 10): number {
    const now = Math.floor(Date.now() / 1000);
    let sum = 0;
    for (let s = now - window; s < now; s++) sum += this.buckets.get(s) ?? 0;
    return Math.round((sum / window) * 10) / 10;
  }
}

export const metrics = {
  startedAt: Date.now(),
  ingested: 0,
  ingestInvalid: 0,
  ingestRate: new RateCounter(),
  persistedTxn: 0,
  persistedHb: 0,
  persistedAck: 0,
  persistRate: new RateCounter(),
  duplicatesSuppressed: 0,
  batches: 0,
  batchRetries: 0,
  reclaimed: 0,
  chaosSkippedAcks: 0,
  lastBatchSize: 0,
  lastBatchMs: 0,
  /** Device publish -> Postgres commit, per transaction (ms). */
  publishToPersist: new Reservoir(20_000),
  wsClients: 0,

  snapshot() {
    return {
      uptimeS: Math.round((Date.now() - this.startedAt) / 1000),
      ingest: { total: this.ingested, invalid: this.ingestInvalid, perSec: this.ingestRate.rate() },
      persist: {
        txn: this.persistedTxn,
        heartbeats: this.persistedHb,
        acks: this.persistedAck,
        perSec: this.persistRate.rate(),
        batches: this.batches,
        lastBatchSize: this.lastBatchSize,
        lastBatchMs: this.lastBatchMs,
        retries: this.batchRetries,
      },
      idempotency: {
        duplicatesSuppressed: this.duplicatesSuppressed,
        reclaimed: this.reclaimed,
        chaosSkippedAcks: this.chaosSkippedAcks,
      },
      latencyMs: { samples: this.publishToPersist.count, ...this.publishToPersist.percentiles([50, 95, 99]) },
      wsClients: this.wsClients,
    };
  },
};
