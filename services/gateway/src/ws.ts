import type http from 'node:http';
import type { Redis } from 'ioredis';
import { WebSocketServer, WebSocket } from 'ws';
import { config } from './config.js';
import { log } from './log.js';
import { metrics } from './metrics.js';
import type { AnomalyItem, FleetEvent, RecentTxn, TerminalDelta } from './events.js';

/**
 * Fans fleet events out to dashboards. Every API pod subscribes to the Redis channel, folds
 * events into a pending delta, and flushes one "tick" per WS_TICK_MS to its sockets, so a
 * heartbeat reaches the browser within (persist batch + tick) of being published.
 */
export function startWsHub(server: http.Server, sub: Redis): { stop: () => Promise<void> } {
  const wss = new WebSocketServer({ noServer: true });
  server.on('upgrade', (req, socket, head) => {
    if (req.url?.split('?')[0] !== '/ws') return;
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
  });

  let terminals = new Map<string, TerminalDelta>();
  let anomalies = new Map<string, AnomalyItem>();
  let recent: RecentTxn[] = [];
  let commands: unknown[] = [];
  let tx = { count: 0, approved: 0, declined: 0, error: 0, amountMinor: 0, dups: 0 };
  let latencies: number[] = [];
  let hbAges: number[] = [];

  sub.subscribe(config.eventsChannel).catch((err) => log.error({ err }, 'subscribe events failed'));
  sub.on('message', (_channel, raw) => {
    let ev: FleetEvent;
    try {
      ev = JSON.parse(raw);
    } catch {
      return;
    }
    const now = Date.now();
    if (ev.t === 'batch') {
      for (const h of ev.hb) {
        terminals.set(h.id, h);
        if (h.lastSeen) hbAges.push(now - h.lastSeen);
      }
      tx.count += ev.tx.count;
      tx.approved += ev.tx.approved;
      tx.declined += ev.tx.declined;
      tx.error += ev.tx.error;
      tx.amountMinor += ev.tx.amountMinor;
      tx.dups += ev.dups;
      recent.push(...ev.tx.recent);
      if (recent.length > 20) recent = recent.slice(-20);
      latencies.push(...ev.tx.latencies);
      for (const a of ev.acks) commands.push({ ...a, status: a.ok ? 'acked' : 'failed' });
    } else if (ev.t === 'anomalies') {
      for (const a of ev.items) anomalies.set(a.terminalId, a);
    } else if (ev.t === 'command') {
      commands.push(ev);
    }
  });

  wss.on('connection', (ws) => {
    metrics.wsClients = wss.clients.size;
    ws.send(JSON.stringify({ type: 'hello', serverTs: Date.now(), offlineAfterS: config.offlineAfterS, tickMs: config.wsTickMs }));
    ws.on('message', (data) => {
      // Clock-offset probe: the dashboard estimates server time to measure true push latency.
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === 'ping') ws.send(JSON.stringify({ type: 'pong', t0: msg.t0, serverTs: Date.now() }));
      } catch {
        /* ignore */
      }
    });
    ws.on('close', () => (metrics.wsClients = wss.clients.size));
  });

  const pct = (arr: number[], p: number) => {
    if (!arr.length) return null;
    const s = [...arr].sort((a, b) => a - b);
    return s[Math.min(s.length - 1, Math.ceil(p * s.length) - 1)];
  };

  const timer = setInterval(() => {
    const hasData = terminals.size || anomalies.size || tx.count || commands.length;
    if (!hasData) return;
    const payload = JSON.stringify({
      type: 'tick',
      serverTs: Date.now(),
      terminals: [...terminals.values()],
      anomalies: [...anomalies.values()],
      tx,
      recent,
      commands,
      latency: { p50: pct(latencies, 0.5), p99: pct(latencies, 0.99), n: latencies.length },
      heartbeatAge: { p50: pct(hbAges, 0.5), p99: pct(hbAges, 0.99) },
    });
    terminals = new Map();
    anomalies = new Map();
    commands = [];
    tx = { count: 0, approved: 0, declined: 0, error: 0, amountMinor: 0, dups: 0 };
    latencies = [];
    hbAges = [];
    for (const client of wss.clients) {
      if (client.readyState === WebSocket.OPEN && client.bufferedAmount < 4_000_000) client.send(payload);
    }
  }, config.wsTickMs);

  return {
    stop: async () => {
      clearInterval(timer);
      for (const c of wss.clients) c.terminate();
      wss.close();
      sub.disconnect();
    },
  };
}
