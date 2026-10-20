import { useCallback, useEffect, useRef, useState } from 'react';
import { api, toTerminal, wsUrl, type Overview, type RecentTxn, type Status, type Terminal } from './api';

export type SecondBucket = { t: number; approved: number; failed: number };
export type CommandUpdate = { id?: string; commandId?: string; terminalId: string; status: string; result?: string | null; command?: string };

type Tick = {
  type: 'tick';
  serverTs: number;
  terminals: Partial<Terminal & { lastSeen: number }>[];
  anomalies: { terminalId: string; score: number; flagged: boolean; reasons: string[] }[];
  tx: { count: number; approved: number; declined: number; error: number; amountMinor: number; dups: number };
  recent: RecentTxn[];
  commands: CommandUpdate[];
  latency: { p50: number | null; p99: number | null; n: number };
  heartbeatAge: { p50: number | null; p99: number | null };
};

const HISTORY_S = 120;

/**
 * Live fleet state: a REST snapshot on load (and every 30 s to resync), with WebSocket ticks
 * applied on top. Also estimates the server clock offset so push latency is measured end to end.
 */
export function useFleet() {
  const terminals = useRef(new Map<string, Terminal>());
  const [version, setVersion] = useState(0);
  const [overview, setOverview] = useState<Overview | null>(null);
  const [recent, setRecent] = useState<RecentTxn[]>([]);
  const [series, setSeries] = useState<SecondBucket[]>([]);
  const [connected, setConnected] = useState(false);
  const [offlineAfterS, setOfflineAfterS] = useState(35);
  const [pushLatency, setPushLatency] = useState<{ p50: number | null; p99: number | null }>({ p50: null, p99: null });
  const [persistLatency, setPersistLatency] = useState<{ p50: number | null; p99: number | null }>({ p50: null, p99: null });
  const [dups, setDups] = useState(0);
  const [lastCommand, setLastCommand] = useState<CommandUpdate | null>(null);
  const [error, setError] = useState<string | null>(null);
  const clockOffset = useRef(0); // serverTime - clientTime
  const buckets = useRef(new Map<number, SecondBucket>());
  const [now, setNow] = useState(Date.now());

  const refresh = useCallback(async () => {
    try {
      const [rows, ov, txns] = await Promise.all([
        api<any[]>('/api/terminals?limit=5000'),
        api<Overview>('/api/overview'),
        api<RecentTxn[]>('/api/transactions/recent?limit=20'),
      ]);
      const map = terminals.current;
      for (const r of rows) {
        const t = toTerminal(r);
        const prev = map.get(t.id);
        // Keep a fresher WS heartbeat if the snapshot is older.
        if (prev?.lastSeen && t.lastSeen && prev.lastSeen > t.lastSeen) t.lastSeen = prev.lastSeen;
        map.set(t.id, t);
      }
      setOverview(ov);
      setRecent((cur) => (cur.length ? cur : txns.reverse()));
      setVersion((v) => v + 1);
      setError(null);
    } catch (e) {
      setError((e as Error).message);
    }
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 30_000);
    const clock = setInterval(() => setNow(Date.now()), 1000);
    return () => {
      clearInterval(id);
      clearInterval(clock);
    };
  }, [refresh]);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let closed = false;
    let retry: ReturnType<typeof setTimeout>;
    let ping: ReturnType<typeof setInterval>;

    const connect = () => {
      ws = new WebSocket(wsUrl());
      ws.onopen = () => {
        setConnected(true);
        const probe = () => ws?.readyState === WebSocket.OPEN && ws.send(JSON.stringify({ type: 'ping', t0: Date.now() }));
        probe();
        ping = setInterval(probe, 15_000);
      };
      ws.onclose = () => {
        setConnected(false);
        clearInterval(ping);
        if (!closed) retry = setTimeout(connect, 2000);
      };
      ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.type === 'hello') setOfflineAfterS(msg.offlineAfterS);
        else if (msg.type === 'pong') {
          const t1 = Date.now();
          clockOffset.current = msg.serverTs - (msg.t0 + t1) / 2;
        } else if (msg.type === 'tick') applyTick(msg as Tick);
      };
    };

    const applyTick = (tick: Tick) => {
      const map = terminals.current;
      const serverNow = Date.now() + clockOffset.current;
      const ages: number[] = [];
      for (const d of tick.terminals) {
        if (!d.id) continue;
        const prev = map.get(d.id);
        map.set(d.id, {
          ...(prev ?? toTerminal({ id: d.id, merchantId: d.merchantId, reasons: [] })),
          ...(Object.fromEntries(Object.entries(d).filter(([, v]) => v !== undefined)) as Partial<Terminal>),
        });
        if (d.lastSeen) ages.push(serverNow - d.lastSeen);
      }
      for (const a of tick.anomalies) {
        const t = map.get(a.terminalId);
        if (t) map.set(a.terminalId, { ...t, flagged: a.flagged, anomalyScore: a.score, reasons: a.reasons });
      }
      if (ages.length) {
        ages.sort((a, b) => a - b);
        setPushLatency({ p50: ages[Math.floor(ages.length / 2)], p99: ages[Math.min(ages.length - 1, Math.ceil(ages.length * 0.99) - 1)] });
      }
      if (tick.latency.n) setPersistLatency({ p50: tick.latency.p50, p99: tick.latency.p99 });
      if (tick.tx.dups) setDups((d) => d + tick.tx.dups);
      if (tick.recent.length) setRecent((cur) => [...cur, ...tick.recent].slice(-30));
      if (tick.commands.length) setLastCommand(tick.commands[tick.commands.length - 1]);

      const sec = Math.floor(Date.now() / 1000);
      const b = buckets.current.get(sec) ?? { t: sec, approved: 0, failed: 0 };
      b.approved += tick.tx.approved;
      b.failed += tick.tx.declined + tick.tx.error;
      buckets.current.set(sec, b);
      for (const k of buckets.current.keys()) if (k < sec - HISTORY_S) buckets.current.delete(k);
      setVersion((v) => v + 1);
    };

    connect();
    return () => {
      closed = true;
      clearTimeout(retry);
      clearInterval(ping);
      ws?.close();
    };
  }, []);

  // Rebuild the per-second throughput series once a second (including empty seconds).
  useEffect(() => {
    const sec = Math.floor(now / 1000);
    const out: SecondBucket[] = [];
    for (let s = sec - HISTORY_S; s < sec; s++) out.push(buckets.current.get(s) ?? { t: s, approved: 0, failed: 0 });
    setSeries(out);
  }, [now]);

  const statusOf = useCallback(
    (t: Terminal): Status => {
      const serverNow = now + clockOffset.current;
      if (!t.lastSeen || serverNow - t.lastSeen > offlineAfterS * 1000) return 'offline';
      return t.flagged ? 'flagged' : 'online';
    },
    [now, offlineAfterS],
  );

  return {
    terminals: terminals.current,
    version,
    overview,
    recent,
    series,
    connected,
    pushLatency,
    persistLatency,
    dups,
    lastCommand,
    error,
    statusOf,
    refresh,
  };
}
