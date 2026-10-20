import type { Redis } from 'ioredis';
import { config } from './config.js';
import { pool } from './db.js';
import { log } from './log.js';
import { publishEvent, type AnomalyItem } from './events.js';

const HEARTBEAT_INTERVAL_S = Number(process.env.HEARTBEAT_INTERVAL_S || 10);

/**
 * Cluster-wide singleton jobs. Each run first takes a Redis lock (SET NX PX), so with several
 * API/jobs replicas exactly one of them runs a given tick.
 */
async function withLock(redis: Redis, name: string, ttlMs: number, fn: () => Promise<void>) {
  const key = `tp:lock:${name}`;
  const got = await redis.set(key, config.instanceId, 'PX', ttlMs, 'NX');
  if (!got) return;
  try {
    await fn();
  } catch (err) {
    log.error({ err: (err as Error).message, job: name }, 'job failed');
  }
}

export type FeatureRow = {
  terminalId: string;
  features: {
    txn_per_min: number;
    decline_rate: number;
    error_rate: number;
    avg_latency_ms: number | null;
    hb_missing_ratio: number;
    avg_battery: number | null;
    avg_signal_dbm: number | null;
    avg_temperature_c: number | null;
    avg_queue_depth: number | null;
    reboots_per_hour: number;
  };
};

/** Per-terminal feature vector over the scoring window. Must match services/ml/app/features.py. */
export async function computeFeatures(windowS = config.scoreWindowS, minHistoryS = config.minHistoryS): Promise<FeatureRow[]> {
  const res = await pool.query(
    `WITH tx AS (
       SELECT terminal_id, count(*)::float8 AS n,
              avg((status = 'declined')::int)::float8 AS decline_rate,
              avg((status = 'error')::int)::float8 AS error_rate,
              avg(auth_latency_ms)::float8 AS avg_latency_ms
         FROM transactions WHERE device_ts > now() - make_interval(secs => $1)
        GROUP BY terminal_id
     ), hb AS (
       SELECT terminal_id, count(*)::float8 AS hb_count,
              avg(battery)::float8 AS avg_battery, avg(signal_dbm)::float8 AS avg_signal_dbm,
              avg(temperature_c)::float8 AS avg_temperature_c, avg(queue_depth)::float8 AS avg_queue_depth,
              (max(boot_count) - min(boot_count))::float8 AS reboots
         FROM heartbeats WHERE device_ts > now() - make_interval(secs => $1)
        GROUP BY terminal_id
     )
     SELECT t.id AS terminal_id,
            least($1::float8, extract(epoch from now() - t.first_seen)) AS span_s,
            COALESCE(tx.n, 0) AS n, COALESCE(tx.decline_rate, 0) AS decline_rate,
            COALESCE(tx.error_rate, 0) AS error_rate, tx.avg_latency_ms,
            COALESCE(hb.hb_count, 0) AS hb_count,
            COALESCE(hb.avg_battery, t.battery) AS avg_battery,
            COALESCE(hb.avg_signal_dbm, t.signal_dbm) AS avg_signal_dbm,
            COALESCE(hb.avg_temperature_c, t.temperature_c) AS avg_temperature_c,
            COALESCE(hb.avg_queue_depth, t.queue_depth) AS avg_queue_depth,
            COALESCE(hb.reboots, 0) AS reboots
       FROM terminals t
       LEFT JOIN tx ON tx.terminal_id = t.id
       LEFT JOIN hb ON hb.terminal_id = t.id
      WHERE t.first_seen < now() - make_interval(secs => $2)`,
    [windowS, minHistoryS],
  );
  return res.rows.map((r) => {
    const span = Math.max(Number(r.span_s), 1);
    const expectedHb = span / HEARTBEAT_INTERVAL_S;
    return {
      terminalId: r.terminal_id,
      features: {
        txn_per_min: r.n / (span / 60),
        decline_rate: r.decline_rate,
        error_rate: r.error_rate,
        avg_latency_ms: r.avg_latency_ms,
        hb_missing_ratio: Math.min(1, Math.max(0, 1 - r.hb_count / expectedHb)),
        avg_battery: r.avg_battery,
        avg_signal_dbm: r.avg_signal_dbm,
        avg_temperature_c: r.avg_temperature_c,
        avg_queue_depth: r.avg_queue_depth,
        reboots_per_hour: r.reboots / (span / 3600),
      },
    };
  });
}

async function scoreFleet(events: Redis) {
  const rows = await computeFeatures();
  if (!rows.length) return;
  const res = await fetch(`${config.mlUrl.replace(/\/$/, '')}/score`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ terminals: rows }),
    signal: AbortSignal.timeout(60_000), // free-tier ML hosts may be cold-starting
  });
  if (!res.ok) throw new Error(`ml service returned ${res.status}`);
  const body = (await res.json()) as { results: AnomalyItem[]; modelVersion?: string };
  const byId = new Map(rows.map((r) => [r.terminalId, r.features]));
  const items = body.results;
  await pool.query(
    `INSERT INTO anomalies (terminal_id, score, flagged, reasons, features, scored_at)
     SELECT u.id, u.score, u.flagged, u.reasons::jsonb, u.features::jsonb, now()
       FROM unnest($1::text[], $2::float8[], $3::bool[], $4::text[], $5::text[]) AS u(id, score, flagged, reasons, features)
     ON CONFLICT (terminal_id) DO UPDATE SET score = EXCLUDED.score, flagged = EXCLUDED.flagged,
       reasons = EXCLUDED.reasons, features = EXCLUDED.features, scored_at = EXCLUDED.scored_at`,
    [
      items.map((i) => i.terminalId),
      items.map((i) => i.score),
      items.map((i) => i.flagged),
      items.map((i) => JSON.stringify(i.reasons)),
      items.map((i) => JSON.stringify(byId.get(i.terminalId) ?? {})),
    ],
  );
  publishEvent(events, { t: 'anomalies', items, scoredAt: Date.now() });
  log.info({ scored: items.length, flagged: items.filter((i) => i.flagged).length, model: body.modelVersion }, 'fleet scored');
}

async function prune() {
  const cutoff = new Date(Date.now() - config.retentionHours * 3600_000);
  const hbCutoff = new Date(Date.now() - config.heartbeatRetentionHours * 3600_000);
  let deleted = 0;
  // Small batches keep lock times short while ingest keeps writing.
  for (;;) {
    const r = await pool.query(
      `DELETE FROM transactions WHERE id IN (SELECT id FROM transactions WHERE persisted_at < $1 LIMIT 20000)`,
      [cutoff],
    );
    deleted += r.rowCount ?? 0;
    if ((r.rowCount ?? 0) < 20000) break;
  }
  const hb = await pool.query(`DELETE FROM heartbeats WHERE device_ts < $1`, [hbCutoff]);
  await pool.query(`DELETE FROM commands WHERE created_at < now() - interval '7 days'`);
  if (deleted || hb.rowCount) log.info({ transactions: deleted, heartbeats: hb.rowCount }, 'retention pruned');
}

export function startJobs(redis: Redis, events: Redis): { stop: () => void } {
  const timers: NodeJS.Timeout[] = [];
  if (config.mlUrl) {
    timers.push(
      setInterval(() => withLock(redis, 'score', config.scoreIntervalS * 1000 - 500, () => scoreFleet(events)), config.scoreIntervalS * 1000),
    );
    log.info({ mlUrl: config.mlUrl, everyS: config.scoreIntervalS }, 'anomaly scoring enabled');
  } else {
    log.warn('ML_URL not set; anomaly scoring disabled');
  }
  timers.push(setInterval(() => withLock(redis, 'retention', 290_000, prune), 300_000));
  setTimeout(() => withLock(redis, 'retention', 290_000, prune), 10_000);
  return { stop: () => timers.forEach(clearInterval) };
}
