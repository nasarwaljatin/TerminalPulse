import { useCallback, useMemo, useState } from 'react';
import { compactInr, type Status } from './api';
import { CityTable, FlaggedList, FleetGrid, Kpi, Throughput, TxnFeed } from './components';
import { TerminalDrawer } from './TerminalDrawer';
import { useFleet } from './useFleet';

const FILTERS: (Status | 'all')[] = ['all', 'online', 'flagged', 'offline'];
const ms = (v: number | null) => (v === null ? '–' : v >= 1000 ? `${(v / 1000).toFixed(1)} s` : `${Math.round(v)} ms`);

export default function App() {
  const fleet = useFleet();
  const [selected, setSelected] = useState<string | null>(null);
  const [filter, setFilter] = useState<Status | 'all'>('all');
  const [city, setCity] = useState('');
  const [query, setQuery] = useState('');
  const onSelect = useCallback((id: string) => setSelected(id), []);
  const now = Date.now();

  const all = useMemo(
    () => [...fleet.terminals.values()].sort((a, b) => a.id.localeCompare(b.id)),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [fleet.version],
  );
  const counts = useMemo(() => {
    const c = { online: 0, flagged: 0, offline: 0 };
    for (const t of all) c[fleet.statusOf(t)]++;
    return c;
  }, [all, fleet.statusOf]);
  const cities = useMemo(() => [...new Set(all.map((t) => t.city).filter(Boolean) as string[])].sort(), [all]);
  const visible = useMemo(
    () =>
      all.filter(
        (t) =>
          (filter === 'all' || fleet.statusOf(t) === filter) &&
          (!city || t.city === city) &&
          (!query || t.id.toLowerCase().includes(query.toLowerCase()) || t.merchantId?.toLowerCase().includes(query.toLowerCase())),
      ),
    [all, filter, city, query, fleet.statusOf],
  );

  const ov = fleet.overview?.last5Min;
  const recentStats = useMemo(() => {
    const s = fleet.series.slice(-60);
    const total = s.reduce((a, b) => a + b.approved + b.failed, 0);
    const ok = s.reduce((a, b) => a + b.approved, 0);
    return { tps: total / 60, approval: total ? (ok / total) * 100 : null };
  }, [fleet.series]);

  return (
    <div className="min-h-full">
      <header className="sticky top-0 z-30 border-b border-ink-700 bg-ink-950/90 backdrop-blur">
        <div className="mx-auto max-w-[1440px] px-4 sm:px-6 py-3 flex items-center gap-3">
          <svg viewBox="0 0 32 32" className="h-7 w-7" aria-hidden>
            <rect width="32" height="32" rx="8" fill="#111a2c" />
            <path d="M4 17h6l3-8 5 15 3-7h7" fill="none" stroke="#34d399" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          <div>
            <div className="font-semibold text-slate-50 leading-tight">TerminalPulse</div>
            <div className="text-[11px] text-slate-500 leading-tight">IoT payment terminal fleet</div>
          </div>
          <div className="ml-auto flex items-center gap-2 text-xs">
            <span className={`h-2 w-2 rounded-full ${fleet.connected ? 'bg-ok live-dot' : 'bg-bad'}`} />
            <span className="text-slate-300">{fleet.connected ? 'Live' : 'Reconnecting…'}</span>
            <span className="hidden sm:inline text-slate-500 tabular">· device → browser p50 {ms(fleet.pushLatency.p50)}</span>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-[1440px] px-4 sm:px-6 py-5 space-y-4">
        {fleet.error && (
          <div className="card border-bad/50 px-4 py-2 text-sm text-red-200">
            API unreachable ({fleet.error}). Free-tier hosts can take ~1 minute to wake up; retrying automatically.
          </div>
        )}

        <section className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3" aria-label="Key metrics">
          <Kpi label="Terminals online" value={`${counts.online + counts.flagged}`} sub={`of ${all.length} · ${counts.offline} offline`} tone="ok" />
          <Kpi label="Flagged by model" value={`${counts.flagged}`} sub="Isolation Forest" tone={counts.flagged ? 'warn' : undefined} />
          <Kpi label="Throughput" value={`${recentStats.tps.toFixed(1)}/s`} sub={ov ? `${ov.total.toLocaleString()} txns in 5 min` : '—'} />
          <Kpi
            label="Approval rate"
            value={recentStats.approval === null ? '–' : `${recentStats.approval.toFixed(1)}%`}
            sub={ov ? `${compactInr(ov.approvedAmountMinor)} approved (5 min)` : '—'}
          />
          <Kpi label="Publish → persist p99" value={ms(fleet.persistLatency.p99)} sub={`p50 ${ms(fleet.persistLatency.p50)} · MQTT→Redis→Postgres`} />
          <Kpi label="Duplicates suppressed" value={fleet.dups.toLocaleString()} sub="since page load · idempotency keys" />
        </section>

        <section className="grid grid-cols-1 xl:grid-cols-3 gap-4">
          <div className="xl:col-span-2 space-y-4 min-w-0">
            <Throughput series={fleet.series} />
            <div className="card p-4">
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-sm font-medium text-slate-200 mr-2">Fleet</div>
                <div className="flex rounded-lg border border-ink-700 overflow-hidden" role="tablist">
                  {FILTERS.map((f) => (
                    <button
                      key={f}
                      role="tab"
                      aria-selected={filter === f}
                      onClick={() => setFilter(f)}
                      className={`px-3 py-1 text-xs capitalize ${filter === f ? 'bg-ink-700 text-slate-50' : 'text-slate-400 hover:text-slate-200'}`}
                    >
                      {f}
                      {f !== 'all' && <span className="ml-1 tabular text-slate-500">{counts[f]}</span>}
                    </button>
                  ))}
                </div>
                <select
                  value={city}
                  onChange={(e) => setCity(e.target.value)}
                  className="rounded-lg border border-ink-700 bg-ink-900 px-2 py-1 text-xs text-slate-200"
                  aria-label="City"
                >
                  <option value="">All cities</option>
                  {cities.map((c) => (
                    <option key={c}>{c}</option>
                  ))}
                </select>
                <input
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search terminal / merchant"
                  className="w-full sm:w-auto sm:ml-auto rounded-lg border border-ink-700 bg-ink-900 px-3 py-1 text-xs text-slate-200 placeholder:text-slate-600"
                />
              </div>
              <div className="mt-2 mb-3 flex gap-4 text-[11px] text-slate-500">
                <span className="flex items-center gap-1.5"><i className="inline-block h-2.5 w-2.5 rounded-sm bg-ok" />online</span>
                <span className="flex items-center gap-1.5"><i className="inline-block h-2.5 w-2.5 rounded-sm bg-warn" />flagged</span>
                <span className="flex items-center gap-1.5"><i className="inline-block h-2.5 w-2.5 rounded-sm bg-idle" />offline</span>
                <span className="ml-auto">{visible.length} shown · click a terminal for details</span>
              </div>
              <FleetGrid terminals={visible} statusOf={fleet.statusOf} now={now} onSelect={onSelect} />
            </div>
          </div>
          <div className="space-y-4 min-w-0">
            <FlaggedList terminals={all} statusOf={fleet.statusOf} onSelect={onSelect} />
            <TxnFeed txns={fleet.recent} onSelect={onSelect} />
            <CityTable terminals={all} statusOf={fleet.statusOf} />
          </div>
        </section>
      </main>

      {selected && <TerminalDrawer id={selected} onClose={() => setSelected(null)} lastCommand={fleet.lastCommand} />}
    </div>
  );
}
