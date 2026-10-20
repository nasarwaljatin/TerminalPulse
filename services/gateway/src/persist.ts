import type { Redis } from 'ioredis';
import type { PoolClient } from 'pg';
import { config } from './config.js';
import { pool } from './db.js';
import { log } from './log.js';
import { metrics } from './metrics.js';
import { parsePayload, type AckMessage, type HeartbeatMessage, type Kind, type TxnMessage } from './messages.js';
import { createRedis } from './redis.js';
import { publishEvent, type BatchEvent } from './events.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

type Entry = { id: string; kind: Kind; raw: string; receivedAt: number };
type StreamEntries = [string, string[] | null][];

/**
 * Redis Streams consumer group -> PostgreSQL.
 *
 * Workers read up to BATCH_SIZE entries, write them in one transaction using multi-row
 * UNNEST inserts, and XACK only after COMMIT. A crash between COMMIT and XACK leaves the
 * entries pending; another consumer claims them with XAUTOCLAIM and writes them again,
 * which the UNIQUE(idempotency_key) constraint turns into a no-op.
 */
export async function startPersist(events: Redis): Promise<{ stop: () => Promise<void> }> {
  const admin = createRedis('persist-admin');
  try {
    await admin.xgroup('CREATE', config.streamKey, config.consumerGroup, '0', 'MKSTREAM');
    log.info({ group: config.consumerGroup }, 'created consumer group');
  } catch (err) {
    if (!String((err as Error).message).includes('BUSYGROUP')) throw err;
  }

  let running = true;
  const workers: Promise<void>[] = [];
  const conns: Redis[] = [admin];
  for (let i = 0; i < Math.max(1, config.persistConcurrency); i++) {
    const redis = createRedis(`persist-${i}`, { blocking: true });
    conns.push(redis);
    workers.push(workerLoop(redis, events, `${config.instanceId}-${i}`, () => running));
  }
  log.info({ workers: workers.length, batchSize: config.batchSize }, 'persist workers started');

  return {
    stop: async () => {
      running = false;
      await Promise.race([Promise.all(workers), sleep(3000)]);
      conns.forEach((c) => c.disconnect());
    },
  };
}

async function workerLoop(redis: Redis, events: Redis, consumer: string, isRunning: () => boolean) {
  let lastClaim = 0;
  while (isRunning()) {
    try {
      // Periodically pick up entries left pending by crashed consumers (or by chaos mode).
      if (Date.now() - lastClaim > 5000) {
        lastClaim = Date.now();
        const claimed = await claimStale(redis, consumer);
        if (claimed.length) {
          metrics.reclaimed += claimed.length;
          await processBatch(redis, events, claimed, { reclaimed: true });
        }
      }

      const args: (string | number)[] = ['GROUP', config.consumerGroup, consumer, 'COUNT', config.batchSize];
      if (config.batchBlockMs > 0) args.push('BLOCK', config.batchBlockMs);
      args.push('STREAMS', config.streamKey, '>');
      const res = (await redis.call('XREADGROUP', ...args)) as [string, StreamEntries][] | null;
      const entries = res ? toEntries(res[0][1]) : [];
      if (entries.length === 0) {
        if (config.batchBlockMs <= 0) await sleep(20);
        continue;
      }
      await processBatch(redis, events, entries, { reclaimed: false });
    } catch (err) {
      if (!isRunning()) break;
      log.error({ err: (err as Error).message }, 'persist loop error');
      await sleep(500);
    }
  }
}

function toEntries(raw: StreamEntries): Entry[] {
  const out: Entry[] = [];
  for (const [id, fields] of raw) {
    if (!fields) {
      out.push({ id, kind: 'txn', raw: '', receivedAt: 0 }); // trimmed/deleted entry: ack and skip
      continue;
    }
    const f: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) f[fields[i]] = fields[i + 1];
    out.push({ id, kind: f.k as Kind, raw: f.p ?? '', receivedAt: Number(f.r) || Date.now() });
  }
  return out;
}

async function claimStale(redis: Redis, consumer: string): Promise<Entry[]> {
  const res = (await redis.call(
    'XAUTOCLAIM',
    config.streamKey,
    config.consumerGroup,
    consumer,
    config.claimIdleMs,
    '0-0',
    'COUNT',
    config.batchSize,
  )) as [string, StreamEntries, string[]?];
  return toEntries(res[1] ?? []);
}

async function processBatch(redis: Redis, events: Redis, entries: Entry[], opts: { reclaimed: boolean }) {
  const started = Date.now();
  const txns = new Map<string, TxnMessage & { receivedAt: number }>();
  const latestHb = new Map<string, HeartbeatMessage>();
  const hbs: HeartbeatMessage[] = [];
  const acks: AckMessage[] = [];
  let inBatchDups = 0;

  for (const e of entries) {
    if (!e.raw) continue;
    const parsed = parsePayload(e.kind, e.raw);
    if (!parsed) continue;
    if (parsed.kind === 'txn') {
      if (txns.has(parsed.msg.idempotencyKey)) inBatchDups++;
      else txns.set(parsed.msg.idempotencyKey, { ...parsed.msg, receivedAt: e.receivedAt });
    } else if (parsed.kind === 'hb') {
      hbs.push(parsed.msg);
      const prev = latestHb.get(parsed.msg.terminalId);
      if (!prev || prev.ts < parsed.msg.ts) latestHb.set(parsed.msg.terminalId, parsed.msg);
    } else {
      acks.push(parsed.msg);
    }
  }

  let inserted: string[] = [];
  for (let attempt = 0; ; attempt++) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await upsertTerminals(client, [...latestHb.values()], [...txns.values()]);
      inserted = await insertTransactions(client, [...txns.values()]);
      await insertHeartbeats(client, hbs);
      await applyAcks(client, acks);
      await client.query('COMMIT');
      client.release();
      break;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      client.release();
      const code = (err as { code?: string }).code;
      // Deadlocks/serialization failures are transient; anything else bubbles up and the
      // entries stay pending, to be reclaimed and retried later.
      if ((code === '40P01' || code === '40001') && attempt < 4) {
        metrics.batchRetries++;
        await sleep(20 * (attempt + 1));
        continue;
      }
      throw err;
    }
  }

  const committedAt = Date.now();
  const ids = entries.map((e) => e.id);
  if (!opts.reclaimed && config.chaosNoAckRate > 0 && Math.random() < config.chaosNoAckRate) {
    metrics.chaosSkippedAcks += ids.length; // forced redelivery: XAUTOCLAIM will hand these out again
  } else {
    await redis.xack(config.streamKey, config.consumerGroup, ...ids);
  }

  const insertedSet = new Set(inserted);
  const insertedTxns = [...txns.values()].filter((t) => insertedSet.has(t.idempotencyKey));
  for (const t of insertedTxns) metrics.publishToPersist.add(committedAt - t.ts);
  const dups = inBatchDups + (txns.size - inserted.length);
  metrics.duplicatesSuppressed += dups;
  metrics.persistedTxn += inserted.length;
  metrics.persistedHb += hbs.length;
  metrics.persistedAck += acks.length;
  metrics.persistRate.add(entries.length);
  metrics.batches++;
  metrics.lastBatchSize = entries.length;
  metrics.lastBatchMs = committedAt - started;

  let approved = 0;
  let declined = 0;
  let errored = 0;
  let amountMinor = 0;
  for (const t of insertedTxns) {
    if (t.status === 'approved') {
      approved++;
      amountMinor += t.amountMinor;
    } else if (t.status === 'declined') declined++;
    else errored++;
  }

  const event: BatchEvent = {
    t: 'batch',
    hb: [...latestHb.values()].map((h) => ({
      id: h.terminalId,
      merchantId: h.merchantId,
      city: h.city,
      model: h.model,
      firmware: h.firmware,
      lat: h.lat,
      lng: h.lng,
      battery: h.battery,
      signalDbm: h.signalDbm,
      temperatureC: h.temperatureC,
      queueDepth: h.queueDepth,
      lastSeen: h.ts,
    })),
    tx: {
      count: insertedTxns.length,
      approved,
      declined,
      error: errored,
      amountMinor,
      recent: insertedTxns.slice(-10).map((t) => ({
        key: t.idempotencyKey,
        terminalId: t.terminalId,
        amountMinor: t.amountMinor,
        currency: t.currency,
        method: t.method,
        status: t.status,
        errorCode: t.errorCode ?? null,
        ts: t.ts,
      })),
      latencies: insertedTxns.slice(0, 50).map((t) => committedAt - t.ts),
    },
    dups,
    acks: acks.map((a) => ({ commandId: a.commandId, terminalId: a.terminalId, ok: a.ok, result: a.result ?? null })),
  };
  publishEvent(events, event);
}

const knownTerminals = new Set<string>();

async function upsertTerminals(client: PoolClient, hbs: HeartbeatMessage[], txns: TxnMessage[]) {
  // Sorted ids give every transaction the same lock order, so concurrent batches can't deadlock.
  if (hbs.length) {
    hbs.sort((a, b) => (a.terminalId < b.terminalId ? -1 : 1));
    await client.query(
      `INSERT INTO terminals (id, merchant_id, model, firmware, city, lat, lng, last_seen,
                              battery, signal_dbm, temperature_c, queue_depth, boot_count)
       SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::float8[],
                            $7::float8[], $8::timestamptz[], $9::int2[], $10::int2[], $11::float4[],
                            $12::int4[], $13::int4[])
       ON CONFLICT (id) DO UPDATE SET
         merchant_id = EXCLUDED.merchant_id,
         model = COALESCE(EXCLUDED.model, terminals.model),
         firmware = COALESCE(EXCLUDED.firmware, terminals.firmware),
         city = COALESCE(EXCLUDED.city, terminals.city),
         lat = COALESCE(EXCLUDED.lat, terminals.lat),
         lng = COALESCE(EXCLUDED.lng, terminals.lng),
         last_seen = EXCLUDED.last_seen,
         battery = EXCLUDED.battery,
         signal_dbm = EXCLUDED.signal_dbm,
         temperature_c = EXCLUDED.temperature_c,
         queue_depth = EXCLUDED.queue_depth,
         boot_count = EXCLUDED.boot_count,
         updated_at = now()
       WHERE terminals.last_seen IS NULL OR EXCLUDED.last_seen > terminals.last_seen`,
      [
        hbs.map((h) => h.terminalId),
        hbs.map((h) => h.merchantId),
        hbs.map((h) => h.model ?? null),
        hbs.map((h) => h.firmware ?? null),
        hbs.map((h) => h.city ?? null),
        hbs.map((h) => h.lat ?? null),
        hbs.map((h) => h.lng ?? null),
        hbs.map((h) => new Date(h.ts)),
        hbs.map((h) => Math.round(h.battery)),
        hbs.map((h) => Math.round(h.signalDbm)),
        hbs.map((h) => h.temperatureC),
        hbs.map((h) => h.queueDepth),
        hbs.map((h) => h.bootCount),
      ],
    );
    for (const h of hbs) knownTerminals.add(h.terminalId);
  }
  // Transactions can arrive before a terminal's first heartbeat; make sure the row exists.
  const unseen = new Map<string, string>();
  for (const t of txns) if (!knownTerminals.has(t.terminalId)) unseen.set(t.terminalId, t.merchantId);
  if (unseen.size) {
    const ids = [...unseen.keys()].sort();
    await client.query(
      `INSERT INTO terminals (id, merchant_id) SELECT * FROM unnest($1::text[], $2::text[]) ON CONFLICT (id) DO NOTHING`,
      [ids, ids.map((id) => unseen.get(id)!)],
    );
    ids.forEach((id) => knownTerminals.add(id));
  }
}

async function insertTransactions(client: PoolClient, txns: (TxnMessage & { receivedAt: number })[]): Promise<string[]> {
  if (!txns.length) return [];
  txns.sort((a, b) => (a.idempotencyKey < b.idempotencyKey ? -1 : 1));
  const res = await client.query<{ idempotency_key: string }>(
    `INSERT INTO transactions (idempotency_key, terminal_id, merchant_id, amount_minor, currency, method,
                               status, error_code, auth_latency_ms, device_ts, received_at)
     SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::int8[], $5::text[], $6::text[],
                          $7::text[], $8::text[], $9::int4[], $10::timestamptz[], $11::timestamptz[])
     ON CONFLICT ON CONSTRAINT transactions_idempotency_key_uq DO NOTHING
     RETURNING idempotency_key`,
    [
      txns.map((t) => t.idempotencyKey),
      txns.map((t) => t.terminalId),
      txns.map((t) => t.merchantId),
      txns.map((t) => t.amountMinor),
      txns.map((t) => t.currency.toUpperCase()),
      txns.map((t) => t.method),
      txns.map((t) => t.status),
      txns.map((t) => t.errorCode ?? null),
      txns.map((t) => t.authLatencyMs),
      txns.map((t) => new Date(t.ts)),
      txns.map((t) => new Date(t.receivedAt)),
    ],
  );
  return res.rows.map((r) => r.idempotency_key);
}

async function insertHeartbeats(client: PoolClient, hbs: HeartbeatMessage[]) {
  if (!hbs.length) return;
  await client.query(
    `INSERT INTO heartbeats (terminal_id, device_ts, battery, signal_dbm, temperature_c, queue_depth, boot_count)
     SELECT * FROM unnest($1::text[], $2::timestamptz[], $3::int2[], $4::int2[], $5::float4[], $6::int4[], $7::int4[])
     ON CONFLICT DO NOTHING`,
    [
      hbs.map((h) => h.terminalId),
      hbs.map((h) => new Date(h.ts)),
      hbs.map((h) => Math.round(h.battery)),
      hbs.map((h) => Math.round(h.signalDbm)),
      hbs.map((h) => h.temperatureC),
      hbs.map((h) => h.queueDepth),
      hbs.map((h) => h.bootCount),
    ],
  );
}

async function applyAcks(client: PoolClient, acks: AckMessage[]) {
  if (!acks.length) return;
  await client.query(
    `UPDATE commands c
        SET status = CASE WHEN u.ok THEN 'acked' ELSE 'failed' END, result = u.result, acked_at = u.ts
       FROM unnest($1::uuid[], $2::bool[], $3::text[], $4::timestamptz[]) AS u(id, ok, result, ts)
      WHERE c.id = u.id AND c.status = 'sent'`,
    [acks.map((a) => a.commandId), acks.map((a) => a.ok), acks.map((a) => a.result ?? null), acks.map((a) => new Date(a.ts))],
  );
}
