import { memo, useMemo } from 'react';
import { compactInr, inr, type RecentTxn, type Status, type Terminal } from './api';
import type { SecondBucket } from './useFleet';

export const STATUS_COLOR: Record<Status, string> = {
  online: 'bg-ok',
  flagged: 'bg-warn',
  offline: 'bg-idle',
};

export const STATUS_TEXT: Record<Status, string> = {
  online: 'text-ok',
  flagged: 'text-warn',
  offline: 'text-slate-400',
};

export function Kpi({ label, value, sub, tone }: { label: string; value: string; sub?: string; tone?: 'ok' | 'warn' | 'bad' }) {
  const color = tone === 'ok' ? 'text-ok' : tone === 'warn' ? 'text-warn' : tone === 'bad' ? 'text-bad' : 'text-slate-50';
  return (
    <div className="card px-4 py-3 min-w-0">
      <div className="text-[11px] uppercase tracking-wider text-slate-400 truncate">{label}</div>
      <div className={`mt-1 text-2xl font-semibold tabular ${color}`}>{value}</div>
      {sub && <div className="mt-0.5 text-xs text-slate-500 truncate tabular">{sub}</div>}
    </div>
  );
}

/** Stacked per-second bars: approved (green) and declined+error (red) for the last two minutes. */
export function Throughput({ series }: { series: SecondBucket[] }) {
  const max = Math.max(5, ...series.map((b) => b.approved + b.failed));
  const w = 720;
  const h = 120;
  const bw = w / Math.max(series.length, 1);
  const last10 = series.slice(-10);
  const rate = last10.reduce((s, b) => s + b.approved + b.failed, 0) / Math.max(last10.length, 1);
  return (
    <div className="card p-4">
      <div className="flex items-baseline justify-between">
        <div>
          <div className="text-sm font-medium text-slate-200">Transactions persisted / second</div>
          <div className="text-xs text-slate-500">Live from WebSocket ticks · last 2 minutes</div>
        </div>
        <div className="text-right">
          <div className="text-xl font-semibold tabular text-slate-50">{rate.toFixed(1)}/s</div>
          <div className="text-xs text-slate-500">10 s average</div>
        </div>
      </div>
      <svg viewBox={`0 0 ${w} ${h}`} className="mt-3 w-full h-28" preserveAspectRatio="none" role="img" aria-label="Transactions per second">
        {[0.25, 0.5, 0.75].map((f) => (
          <line key={f} x1={0} x2={w} y1={h * f} y2={h * f} stroke="#1b2740" strokeWidth={1} />
        ))}
        {series.map((b, i) => {
          const ah = (b.approved / max) * (h - 4);
          const fh = (b.failed / max) * (h - 4);
          return (
            <g key={b.t}>
              <rect x={i * bw + 0.5} y={h - ah} width={Math.max(bw - 1, 1)} height={ah} fill="#34d399" opacity={0.85} />
              <rect x={i * bw + 0.5} y={h - ah - fh} width={Math.max(bw - 1, 1)} height={fh} fill="#f87171" opacity={0.9} />
            </g>
          );
        })}
      </svg>
      <div className="mt-2 flex gap-4 text-xs text-slate-400">
        <span className="flex items-center gap-1.5"><i className="inline-block w-2.5 h-2.5 rounded-sm bg-ok" />approved</span>
        <span className="flex items-center gap-1.5"><i className="inline-block w-2.5 h-2.5 rounded-sm bg-bad" />declined / error</span>
        <span className="ml-auto tabular">peak {max}/s</span>
      </div>
    </div>
  );
}

const Cell = memo(function Cell({ t, status, beat, onSelect }: { t: Terminal; status: Status; beat: boolean; onSelect: (id: string) => void }) {
  return (
    <button
      onClick={() => onSelect(t.id)}
      title={`${t.id} · ${t.city ?? ''} · ${status}${t.reasons[0] ? ' · ' + t.reasons[0] : ''}`}
      aria-label={`${t.id} ${status}`}
      className={`h-3.5 w-3.5 rounded-[3px] ${STATUS_COLOR[status]} ${beat ? 'beat' : ''} hover:scale-150 hover:z-10 transition-transform focus:outline focus:outline-2 focus:outline-accent`}
    />
  );
});

export function FleetGrid({
  terminals,
  statusOf,
  now,
  onSelect,
}: {
  terminals: Terminal[];
  statusOf: (t: Terminal) => Status;
  now: number;
  onSelect: (id: string) => void;
}) {
  return (
    <div className="flex flex-wrap gap-[3px]">
      {terminals.map((t) => (
        <Cell key={t.id} t={t} status={statusOf(t)} beat={!!t.lastSeen && now - t.lastSeen < 1200} onSelect={onSelect} />
      ))}
      {terminals.length === 0 && <div className="text-sm text-slate-500 py-8">No terminals match. Start the simulator to bring the fleet online.</div>}
    </div>
  );
}

export function FlaggedList({ terminals, statusOf, onSelect }: { terminals: Terminal[]; statusOf: (t: Terminal) => Status; onSelect: (id: string) => void }) {
  const flagged = useMemo(
    // Offline takes precedence over flagged (same rule as the KPI tiles and the grid colours).
    () => terminals.filter((t) => statusOf(t) === 'flagged').sort((a, b) => (b.anomalyScore ?? 0) - (a.anomalyScore ?? 0)),
    [terminals, statusOf],
  );
  return (
    <div className="card p-4 flex flex-col min-h-0">
      <div className="flex items-baseline justify-between">
        <div className="text-sm font-medium text-slate-200">Needs attention</div>
        <div className="text-xs text-slate-500">Isolation Forest · {flagged.length} flagged</div>
      </div>
      <div className="mt-3 space-y-2 overflow-y-auto max-h-[420px] pr-1">
        {flagged.length === 0 && <div className="text-sm text-slate-500">Nothing flagged. Scoring starts once terminals have 5 minutes of history.</div>}
        {flagged.map((t) => (
          <button
            key={t.id}
            onClick={() => onSelect(t.id)}
            className="w-full text-left rounded-lg border border-ink-700 bg-ink-900/60 hover:border-warn/60 px-3 py-2 transition-colors"
          >
            <div className="flex items-center gap-2">
              <span className="font-mono text-sm text-slate-100">{t.id}</span>
              <span className="text-xs text-slate-500">{t.city}</span>
              <span className={`ml-auto text-xs ${STATUS_TEXT[statusOf(t)]}`}>{statusOf(t)}</span>
              <span className="text-xs tabular text-warn">{t.anomalyScore?.toFixed(2)}</span>
            </div>
            {t.reasons.length > 0 && <div className="mt-1 text-xs text-slate-400 leading-snug">{t.reasons.join(' · ')}</div>}
          </button>
        ))}
      </div>
    </div>
  );
}

const TXN_STYLE: Record<string, string> = {
  approved: 'text-ok',
  declined: 'text-warn',
  error: 'text-bad',
};

export function TxnFeed({ txns, onSelect }: { txns: RecentTxn[]; onSelect: (id: string) => void }) {
  const rows = [...txns].reverse().slice(0, 14);
  return (
    <div className="card p-4">
      <div className="flex items-baseline justify-between">
        <div className="text-sm font-medium text-slate-200">Live transactions</div>
        <div className="text-xs text-slate-500">idempotent · exactly one row per key</div>
      </div>
      <div className="mt-3 divide-y divide-ink-700">
        {rows.length === 0 && <div className="text-sm text-slate-500 py-2">Waiting for transactions…</div>}
        {rows.map((r) => (
          <div key={r.key} className="flex items-center gap-2 py-1.5 text-xs">
            <button onClick={() => onSelect(r.terminalId)} className="font-mono text-slate-300 hover:text-accent">
              {r.terminalId}
            </button>
            <span className="text-slate-500 uppercase">{r.method}</span>
            <span className={`${TXN_STYLE[r.status]} ml-auto`}>{r.status}{r.errorCode ? ` ${r.errorCode}` : ''}</span>
            <span className="tabular text-slate-200 w-24 text-right">{inr(r.amountMinor)}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function CityTable({ terminals, statusOf }: { terminals: Terminal[]; statusOf: (t: Terminal) => Status }) {
  const rows = useMemo(() => {
    const m = new Map<string, { city: string; total: number; flagged: number; offline: number }>();
    for (const t of terminals) {
      const city = t.city ?? 'unknown';
      const r = m.get(city) ?? { city, total: 0, flagged: 0, offline: 0 };
      r.total++;
      const s = statusOf(t);
      if (s === 'flagged') r.flagged++;
      if (s === 'offline') r.offline++;
      m.set(city, r);
    }
    return [...m.values()].sort((a, b) => b.total - a.total);
  }, [terminals, statusOf]);
  return (
    <div className="card p-4">
      <div className="text-sm font-medium text-slate-200">By city</div>
      <table className="mt-2 w-full text-xs tabular">
        <thead className="text-slate-500">
          <tr>
            <th className="text-left font-normal py-1">City</th>
            <th className="text-right font-normal">Terminals</th>
            <th className="text-right font-normal">Flagged</th>
            <th className="text-right font-normal">Offline</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.city} className="border-t border-ink-700">
              <td className="py-1.5 text-slate-300">{r.city}</td>
              <td className="text-right text-slate-200">{r.total}</td>
              <td className={`text-right ${r.flagged ? 'text-warn' : 'text-slate-500'}`}>{r.flagged}</td>
              <td className={`text-right ${r.offline ? 'text-slate-300' : 'text-slate-500'}`}>{r.offline}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function MiniLine({ values, color, label, unit }: { values: number[]; color: string; label: string; unit: string }) {
  const w = 200;
  const h = 40;
  if (values.length < 2) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const pts = values.map((v, i) => `${(i / (values.length - 1)) * w},${h - 3 - ((v - min) / span) * (h - 6)}`).join(' ');
  return (
    <div className="rounded-lg bg-ink-900/70 border border-ink-700 p-2">
      <div className="flex justify-between text-[11px] text-slate-400">
        <span>{label}</span>
        <span className="tabular text-slate-200">
          {values[values.length - 1].toFixed(0)}
          {unit}
        </span>
      </div>
      <svg viewBox={`0 0 ${w} ${h}`} className="w-full h-10" preserveAspectRatio="none" role="img" aria-label={`${label} trend`}>
        <polyline points={pts} fill="none" stroke={color} strokeWidth={1.8} vectorEffect="non-scaling-stroke" />
      </svg>
    </div>
  );
}

export { compactInr };
