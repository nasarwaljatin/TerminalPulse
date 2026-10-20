#!/usr/bin/env node
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';

/**
 * TerminalPulse MCP server: six fleet-triage tools for Claude, backed by the gateway REST API.
 *
 * Env:
 *   TERMINALPULSE_API_URL   gateway base URL (default http://localhost:8080)
 *   TERMINALPULSE_API_KEY   admin key, needed only for send_terminal_command when the gateway sets ADMIN_API_KEY
 */
const API = (process.env.TERMINALPULSE_API_URL ?? 'http://localhost:8080').replace(/\/$/, '');
const API_KEY = process.env.TERMINALPULSE_API_KEY ?? '';

async function call(path: string, init: RequestInit = {}): Promise<unknown> {
  const res = await fetch(API + path, {
    ...init,
    headers: { 'content-type': 'application/json', ...(API_KEY ? { 'x-api-key': API_KEY } : {}), ...init.headers },
    signal: AbortSignal.timeout(60_000), // free-tier hosts can take a while to wake up
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`TerminalPulse API ${res.status}: ${text.slice(0, 300)}`);
  return text ? JSON.parse(text) : null;
}

const ok = (data: unknown) => ({ content: [{ type: 'text' as const, text: JSON.stringify(data, null, 2) }] });
const fail = (err: unknown) => ({ isError: true, content: [{ type: 'text' as const, text: (err as Error).message }] });
const run = async (fn: () => Promise<unknown>) => {
  try {
    return ok(await fn());
  } catch (err) {
    return fail(err);
  }
};
const qs = (params: Record<string, unknown>) => {
  const s = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== '') s.set(k, String(v));
  const str = s.toString();
  return str ? `?${str}` : '';
};
const rupees = (minor: number) => `INR ${(minor / 100).toLocaleString('en-IN', { maximumFractionDigits: 2 })}`;

const server = new McpServer({ name: 'terminalpulse', version: '1.0.0' });

server.registerTool(
  'get_fleet_overview',
  {
    title: 'Fleet overview',
    description:
      'Snapshot of the whole POS terminal fleet: online/offline/flagged counts, last-5-minute transaction volume, approval rate and publish-to-persist latency, plus per-city breakdown. Start triage here.',
    inputSchema: {},
    annotations: { readOnlyHint: true },
  },
  async () =>
    run(async () => {
      const o = (await call('/api/overview')) as any;
      return {
        fleet: o.fleet,
        last5Min: { ...o.last5Min, approvedVolume: rupees(o.last5Min.approvedAmountMinor) },
        byCity: o.byCity,
        offlineAfterSeconds: o.offlineAfterS,
      };
    }),
);

server.registerTool(
  'list_flagged_terminals',
  {
    title: 'List flagged terminals',
    description:
      'Terminals the Isolation Forest model currently flags as anomalous, most anomalous first, with the score and the human-readable reasons (e.g. "temperature high: 71C").',
    inputSchema: { limit: z.number().int().min(1).max(200).default(20).describe('Maximum terminals to return') },
    annotations: { readOnlyHint: true },
  },
  async ({ limit }) =>
    run(async () => {
      const rows = (await call(`/api/anomalies${qs({ limit })}`)) as any[];
      return rows.map((t) => ({
        id: t.id,
        merchantId: t.merchantId,
        city: t.city,
        status: t.status,
        anomalyScore: t.anomalyScore,
        reasons: t.reasons,
        lastSeen: t.lastSeen,
      }));
    }),
);

server.registerTool(
  'search_terminals',
  {
    title: 'Search terminals',
    description: 'Find terminals by status, city, merchant, firmware version or a partial id match.',
    inputSchema: {
      status: z.enum(['online', 'offline', 'flagged']).optional(),
      city: z.string().optional().describe('City name, e.g. Mumbai'),
      merchantId: z.string().optional(),
      firmware: z.string().optional().describe('Exact firmware version, e.g. 4.2.1'),
      q: z.string().optional().describe('Partial terminal or merchant id'),
      limit: z.number().int().min(1).max(500).default(50),
    },
    annotations: { readOnlyHint: true },
  },
  async (args) =>
    run(async () => {
      const rows = (await call(`/api/terminals${qs(args)}`)) as any[];
      return {
        count: rows.length,
        terminals: rows.map((t) => ({
          id: t.id,
          merchantId: t.merchantId,
          city: t.city,
          status: t.status,
          firmware: t.firmware,
          battery: t.battery,
          signalDbm: t.signalDbm,
          temperatureC: t.temperatureC,
          anomalyScore: t.anomalyScore,
        })),
      };
    }),
);

server.registerTool(
  'get_terminal_health',
  {
    title: 'Terminal health',
    description:
      'Full health record for one terminal: latest telemetry, anomaly verdict with reasons and feature values, last-hour transaction stats, recent heartbeat trend and recent remote commands.',
    inputSchema: { terminalId: z.string().describe('Terminal id, e.g. T00042') },
    annotations: { readOnlyHint: true },
  },
  async ({ terminalId }) =>
    run(async () => {
      const t = (await call(`/api/terminals/${encodeURIComponent(terminalId)}`)) as any;
      const hb = t.heartbeats as any[];
      const trend = (key: string) => (hb.length ? { first: hb[0][key], last: hb[hb.length - 1][key] } : null);
      return {
        id: t.id,
        merchantId: t.merchantId,
        city: t.city,
        model: t.model,
        firmware: t.firmware,
        status: t.status,
        lastSeen: t.lastSeen,
        telemetry: {
          battery: t.battery,
          signalDbm: t.signalDbm,
          temperatureC: t.temperatureC,
          queueDepth: t.queueDepth,
          bootCount: t.bootCount,
        },
        anomaly: { flagged: t.flagged, score: t.anomalyScore, reasons: t.reasons, features: t.features, scoredAt: t.scoredAt },
        lastHour: t.lastHour,
        heartbeatTrend: {
          samples: hb.length,
          battery: trend('battery'),
          temperatureC: trend('temperatureC'),
          signalDbm: trend('signalDbm'),
          bootCount: trend('bootCount'),
        },
        recentCommands: t.commands,
      };
    }),
);

server.registerTool(
  'get_terminal_transactions',
  {
    title: 'Terminal transactions',
    description: 'Recent transactions for one terminal, newest first, optionally filtered by status (approved, declined, error).',
    inputSchema: {
      terminalId: z.string(),
      status: z.enum(['approved', 'declined', 'error']).optional(),
      limit: z.number().int().min(1).max(200).default(25),
    },
    annotations: { readOnlyHint: true },
  },
  async ({ terminalId, status, limit }) =>
    run(async () => {
      const rows = (await call(`/api/terminals/${encodeURIComponent(terminalId)}/transactions${qs({ status, limit })}`)) as any[];
      return rows.map((r) => ({ ...r, amount: rupees(r.amountMinor) }));
    }),
);

server.registerTool(
  'send_terminal_command',
  {
    title: 'Send terminal command',
    description:
      'Send a remote command to a terminal over MQTT (QoS 1): "reboot" (clears many transient faults; terminal is offline ~10 s), "ping" (liveness check), or "clear_queue" (flush the offline transaction queue). Returns the command id; the terminal acknowledges asynchronously, visible later via get_terminal_health.',
    inputSchema: {
      terminalId: z.string(),
      command: z.enum(['reboot', 'ping', 'clear_queue']),
      reason: z.string().max(60).optional().describe('Short note recorded with the command'),
    },
    annotations: { destructiveHint: true, idempotentHint: false },
  },
  async ({ terminalId, command, reason }) =>
    run(async () =>
      call(`/api/terminals/${encodeURIComponent(terminalId)}/commands`, {
        method: 'POST',
        body: JSON.stringify({ command, requestedBy: `claude-mcp${reason ? `: ${reason}` : ''}`.slice(0, 64) }),
      }),
    ),
);

await server.connect(new StdioServerTransport());
console.error(`terminalpulse MCP server ready (api: ${API})`);
