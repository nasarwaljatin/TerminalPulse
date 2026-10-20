import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import express, { type NextFunction, type Request, type Response } from 'express';
import type { Redis } from 'ioredis';
import { z } from 'zod';
import { config } from './config.js';
import { pool } from './db.js';
import { log } from './log.js';
import { metrics } from './metrics.js';
import { COMMANDS, sendCommand } from './commands.js';
import { ingestReady, ingestState } from './ingest.js';
import {
  flaggedTerminals,
  getTerminal,
  latencyStats,
  listTerminals,
  overview,
  recentTransactions,
  terminalTransactions,
} from './queries.js';

type Handler = (req: Request, res: Response) => Promise<unknown>;
const route = (fn: Handler) => (req: Request, res: Response, next: NextFunction) => fn(req, res).catch(next);

function requireAdmin(req: Request, res: Response, next: NextFunction) {
  if (!config.adminApiKey) return next();
  const header = req.header('x-api-key') ?? req.header('authorization')?.replace(/^Bearer\s+/i, '') ?? '';
  const a = Buffer.from(header);
  const b = Buffer.from(config.adminApiKey);
  if (a.length === b.length && crypto.timingSafeEqual(a, b)) return next();
  res.status(401).json({ error: 'admin API key required (x-api-key header)' });
}

const str = (v: unknown) => (typeof v === 'string' && v.length ? v : undefined);
const num = (v: unknown) => (typeof v === 'string' && v !== '' && Number.isFinite(Number(v)) ? Number(v) : undefined);

async function streamInfo(redis: Redis) {
  try {
    const groups = (await redis.call('XINFO', 'GROUPS', config.streamKey)) as unknown[][];
    const length = await redis.xlen(config.streamKey);
    const out = groups.map((g) => {
      const o: Record<string, unknown> = {};
      for (let i = 0; i < g.length; i += 2) o[String(g[i])] = g[i + 1];
      return { name: o.name, consumers: o.consumers, pending: o.pending, lag: o.lag ?? null };
    });
    return { key: config.streamKey, length, groups: out };
  } catch (err) {
    return { key: config.streamKey, error: (err as Error).message };
  }
}

export function createApp(redis: Redis, events: Redis) {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '64kb' }));
  app.use((req, res, next) => {
    res.setHeader('Access-Control-Allow-Origin', config.corsOrigin);
    res.setHeader('Access-Control-Allow-Headers', 'content-type, x-api-key, authorization');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return void res.sendStatus(204);
    next();
  });

  app.get('/healthz', (_req, res) => {
    res.json({ ok: true, instance: config.instanceId, roles: [...config.roles] });
  });

  app.get(
    '/readyz',
    route(async (_req, res) => {
      await pool.query('SELECT 1');
      await redis.ping();
      if (config.roles.has('ingest') && !ingestReady()) {
        return res.status(503).json({ ok: false, mqttSubscribed: `${ingestState.subscribed.size}/${ingestState.expected}` });
      }
      res.json({ ok: true });
    }),
  );

  app.get('/api/overview', route(async (_req, res) => res.json(await overview())));

  app.get(
    '/api/terminals',
    route(async (req, res) => {
      const status = str(req.query.status);
      if (status && !['online', 'offline', 'flagged'].includes(status)) {
        return res.status(400).json({ error: 'status must be online, offline or flagged' });
      }
      res.json(
        await listTerminals({
          status: status as 'online' | 'offline' | 'flagged' | undefined,
          city: str(req.query.city),
          merchantId: str(req.query.merchantId),
          firmware: str(req.query.firmware),
          q: str(req.query.q),
          limit: num(req.query.limit),
          offset: num(req.query.offset),
        }),
      );
    }),
  );

  app.get(
    '/api/terminals/:id',
    route(async (req, res) => {
      const t = await getTerminal(String(req.params.id));
      if (!t) return res.status(404).json({ error: 'terminal not found' });
      res.json(t);
    }),
  );

  app.get(
    '/api/terminals/:id/transactions',
    route(async (req, res) => {
      res.json(
        await terminalTransactions(String(req.params.id), { limit: num(req.query.limit), status: str(req.query.status) }),
      );
    }),
  );

  const CommandBody = z.object({ command: z.enum(COMMANDS), requestedBy: z.string().max(64).optional() });
  app.post(
    '/api/terminals/:id/commands',
    requireAdmin,
    route(async (req, res) => {
      const body = CommandBody.safeParse(req.body);
      if (!body.success) return res.status(400).json({ error: `command must be one of ${COMMANDS.join(', ')}` });
      const result = await sendCommand(events, String(req.params.id), body.data.command, body.data.requestedBy ?? 'api');
      if (!result) return res.status(404).json({ error: 'terminal not found' });
      res.status(202).json(result);
    }),
  );

  app.get('/api/anomalies', route(async (req, res) => res.json(await flaggedTerminals(num(req.query.limit)))));
  app.get('/api/transactions/recent', route(async (req, res) => res.json(await recentTransactions(num(req.query.limit)))));

  app.get(
    '/api/metrics',
    route(async (_req, res) => {
      res.json({ instance: config.instanceId, roles: [...config.roles], ...metrics.snapshot(), stream: await streamInfo(redis) });
    }),
  );

  app.get(
    '/api/metrics/latency',
    route(async (req, res) => res.json(await latencyStats(num(req.query.sinceMs) ?? Date.now() - 5 * 60_000))),
  );

  /** Prometheus text format, for scraping in Kubernetes. */
  app.get('/metrics', (_req, res) => {
    const m = metrics.snapshot();
    const lines = [
      `tp_ingested_total ${m.ingest.total}`,
      `tp_ingest_invalid_total ${m.ingest.invalid}`,
      `tp_persisted_txn_total ${m.persist.txn}`,
      `tp_persisted_heartbeats_total ${m.persist.heartbeats}`,
      `tp_duplicates_suppressed_total ${m.idempotency.duplicatesSuppressed}`,
      `tp_reclaimed_total ${m.idempotency.reclaimed}`,
      `tp_publish_to_persist_ms{quantile="0.5"} ${m.latencyMs.p50 ?? 'NaN'}`,
      `tp_publish_to_persist_ms{quantile="0.99"} ${m.latencyMs.p99 ?? 'NaN'}`,
      `tp_ws_clients ${m.wsClients}`,
    ];
    res.type('text/plain').send(lines.join('\n') + '\n');
  });

  // Serve the built dashboard when it ships in the same image (single-service deploys).
  if (config.staticDir && fs.existsSync(config.staticDir)) {
    const dir = path.resolve(config.staticDir);
    app.use(express.static(dir, { maxAge: '1h', index: false }));
    app.get(/^\/(?!api\/|ws$|mqtt$).*/, (_req, res) => res.sendFile(path.join(dir, 'index.html')));
    log.info({ dir }, 'serving dashboard');
  }

  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    log.error({ err: err.message }, 'request failed');
    res.status(500).json({ error: 'internal error' });
  });

  return app;
}
