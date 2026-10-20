import { config } from './config.js';
import { pool } from './db.js';

/** Derived status: offline if no heartbeat recently, flagged if the anomaly model says so, else online. */
const STATUS_SQL = `
  CASE
    WHEN t.last_seen IS NULL OR t.last_seen < now() - make_interval(secs => $1) THEN 'offline'
    WHEN a.flagged THEN 'flagged'
    ELSE 'online'
  END`;

const TERMINAL_COLUMNS = `
  t.id, t.merchant_id AS "merchantId", t.model, t.firmware, t.city, t.lat, t.lng,
  t.first_seen AS "firstSeen", t.last_seen AS "lastSeen", t.battery, t.signal_dbm AS "signalDbm",
  t.temperature_c AS "temperatureC", t.queue_depth AS "queueDepth", t.boot_count AS "bootCount",
  ${STATUS_SQL} AS status,
  a.score AS "anomalyScore", COALESCE(a.flagged, false) AS flagged, COALESCE(a.reasons, '[]'::jsonb) AS reasons`;

export type TerminalFilter = {
  status?: 'online' | 'offline' | 'flagged';
  city?: string;
  merchantId?: string;
  firmware?: string;
  q?: string;
  limit?: number;
  offset?: number;
};

export async function listTerminals(f: TerminalFilter = {}) {
  const params: unknown[] = [config.offlineAfterS];
  const where: string[] = [];
  if (f.status) {
    params.push(f.status);
    where.push(`${STATUS_SQL} = $${params.length}`);
  }
  if (f.city) {
    params.push(f.city);
    where.push(`t.city ILIKE $${params.length}`);
  }
  if (f.merchantId) {
    params.push(f.merchantId);
    where.push(`t.merchant_id = $${params.length}`);
  }
  if (f.firmware) {
    params.push(f.firmware);
    where.push(`t.firmware = $${params.length}`);
  }
  if (f.q) {
    params.push(`%${f.q}%`);
    where.push(`(t.id ILIKE $${params.length} OR t.merchant_id ILIKE $${params.length})`);
  }
  const limit = Math.min(Math.max(f.limit ?? 2000, 1), 5000);
  const offset = Math.max(f.offset ?? 0, 0);
  const sql = `
    SELECT ${TERMINAL_COLUMNS}
      FROM terminals t LEFT JOIN anomalies a ON a.terminal_id = t.id
     ${where.length ? 'WHERE ' + where.join(' AND ') : ''}
     ORDER BY COALESCE(a.flagged, false) DESC, a.score DESC NULLS LAST, t.id
     LIMIT ${limit} OFFSET ${offset}`;
  return (await pool.query(sql, params)).rows;
}

export async function getTerminal(id: string) {
  const [terminal, stats, heartbeats, commands] = await Promise.all([
    pool.query(
      `SELECT ${TERMINAL_COLUMNS}, a.features, a.scored_at AS "scoredAt"
         FROM terminals t LEFT JOIN anomalies a ON a.terminal_id = t.id WHERE t.id = $2`,
      [config.offlineAfterS, id],
    ),
    pool.query(
      `SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status = 'approved')::int AS approved,
              count(*) FILTER (WHERE status = 'declined')::int AS declined,
              count(*) FILTER (WHERE status = 'error')::int AS errors,
              COALESCE(sum(amount_minor) FILTER (WHERE status = 'approved'), 0) AS "approvedAmountMinor",
              round(avg(auth_latency_ms))::int AS "avgAuthLatencyMs"
         FROM transactions WHERE terminal_id = $1 AND device_ts > now() - interval '1 hour'`,
      [id],
    ),
    pool.query(
      `SELECT device_ts AS ts, battery, signal_dbm AS "signalDbm", temperature_c AS "temperatureC",
              queue_depth AS "queueDepth", boot_count AS "bootCount"
         FROM heartbeats WHERE terminal_id = $1 ORDER BY device_ts DESC LIMIT 60`,
      [id],
    ),
    pool.query(
      `SELECT id, command, status, result, requested_by AS "requestedBy", created_at AS "createdAt", acked_at AS "ackedAt"
         FROM commands WHERE terminal_id = $1 ORDER BY created_at DESC LIMIT 10`,
      [id],
    ),
  ]);
  if (!terminal.rows[0]) return null;
  return {
    ...terminal.rows[0],
    lastHour: stats.rows[0],
    heartbeats: heartbeats.rows.reverse(),
    commands: commands.rows,
  };
}

export async function terminalTransactions(id: string, opts: { limit?: number; status?: string } = {}) {
  const params: unknown[] = [id];
  let filter = '';
  if (opts.status) {
    params.push(opts.status);
    filter = `AND status = $2`;
  }
  const limit = Math.min(Math.max(opts.limit ?? 50, 1), 500);
  return (
    await pool.query(
      `SELECT idempotency_key AS "idempotencyKey", terminal_id AS "terminalId", merchant_id AS "merchantId",
              amount_minor AS "amountMinor", currency, method, status, error_code AS "errorCode",
              auth_latency_ms AS "authLatencyMs", device_ts AS "deviceTs", persisted_at AS "persistedAt"
         FROM transactions WHERE terminal_id = $1 ${filter}
        ORDER BY device_ts DESC LIMIT ${limit}`,
      params,
    )
  ).rows;
}

export async function recentTransactions(limit = 30) {
  return (
    await pool.query(
      `SELECT idempotency_key AS key, terminal_id AS "terminalId", amount_minor AS "amountMinor", currency,
              method, status, error_code AS "errorCode", (extract(epoch from device_ts) * 1000)::bigint AS ts
         FROM transactions ORDER BY persisted_at DESC LIMIT $1`,
      [Math.min(Math.max(limit, 1), 200)],
    )
  ).rows;
}

export async function flaggedTerminals(limit = 50) {
  return (
    await pool.query(
      `SELECT ${TERMINAL_COLUMNS}, a.features, a.scored_at AS "scoredAt"
         FROM anomalies a JOIN terminals t ON t.id = a.terminal_id
        WHERE a.flagged
        ORDER BY a.score DESC LIMIT ${Math.min(Math.max(limit, 1), 500)}`,
      [config.offlineAfterS],
    )
  ).rows;
}

export async function overview() {
  const [fleet, txn, perMinute, byCity] = await Promise.all([
    pool.query(
      `SELECT count(*)::int AS total,
              count(*) FILTER (WHERE s = 'online')::int AS online,
              count(*) FILTER (WHERE s = 'offline')::int AS offline,
              count(*) FILTER (WHERE s = 'flagged')::int AS flagged,
              round(avg(battery))::int AS "avgBattery"
         FROM (SELECT ${STATUS_SQL} AS s, t.battery
                 FROM terminals t LEFT JOIN anomalies a ON a.terminal_id = t.id) x`,
      [config.offlineAfterS],
    ),
    pool.query(
      `SELECT count(*)::int AS total,
              count(*) FILTER (WHERE status = 'approved')::int AS approved,
              count(*) FILTER (WHERE status = 'declined')::int AS declined,
              count(*) FILTER (WHERE status = 'error')::int AS errors,
              COALESCE(sum(amount_minor) FILTER (WHERE status = 'approved'), 0) AS "approvedAmountMinor",
              percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch from persisted_at - device_ts) * 1000) AS "p50Ms",
              percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch from persisted_at - device_ts) * 1000) AS "p99Ms"
         FROM transactions WHERE persisted_at > now() - interval '5 minutes'`,
    ),
    pool.query(
      `SELECT (extract(epoch from date_trunc('minute', device_ts)) * 1000)::bigint AS minute, count(*)::int AS count,
              count(*) FILTER (WHERE status <> 'approved')::int AS failed
         FROM transactions WHERE device_ts > now() - interval '30 minutes'
        GROUP BY 1 ORDER BY 1`,
    ),
    pool.query(
      `SELECT COALESCE(city, 'unknown') AS city, count(*)::int AS terminals,
              count(*) FILTER (WHERE ${STATUS_SQL} = 'flagged')::int AS flagged,
              count(*) FILTER (WHERE ${STATUS_SQL} = 'offline')::int AS offline
         FROM terminals t LEFT JOIN anomalies a ON a.terminal_id = t.id
        GROUP BY 1 ORDER BY 2 DESC`,
      [config.offlineAfterS],
    ),
  ]);
  const t = txn.rows[0];
  return {
    fleet: fleet.rows[0],
    last5Min: {
      ...t,
      approvalRate: t.total ? Math.round((t.approved / t.total) * 1000) / 10 : null,
      p50Ms: t.p50Ms === null ? null : Math.round(t.p50Ms),
      p99Ms: t.p99Ms === null ? null : Math.round(t.p99Ms),
    },
    perMinute: perMinute.rows,
    byCity: byCity.rows,
    offlineAfterS: config.offlineAfterS,
  };
}

/** Publish-to-persist latency computed from stored rows; covers every gateway pod, used by the load test. */
export async function latencyStats(sinceMs: number) {
  const res = await pool.query(
    `SELECT count(*)::int AS count,
            min(device_ts) AS "firstTs", max(persisted_at) AS "lastPersisted",
            percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99,
            max(ms) AS max
       FROM (SELECT device_ts, persisted_at, extract(epoch from persisted_at - device_ts) * 1000 AS ms
               FROM transactions WHERE device_ts >= to_timestamp($1 / 1000.0)) x`,
    [sinceMs],
  );
  const r = res.rows[0];
  const round = (v: number | null) => (v === null ? null : Math.round(v * 10) / 10);
  return { count: r.count, firstTs: r.firstTs, lastPersisted: r.lastPersisted, p50: round(r.p50), p95: round(r.p95), p99: round(r.p99), max: round(r.max) };
}
