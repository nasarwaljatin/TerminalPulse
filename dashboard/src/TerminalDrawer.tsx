import { useCallback, useEffect, useState } from 'react';
import { api, HttpError, inr, type TerminalDetail, type TxnRow } from './api';
import { MiniLine, STATUS_TEXT } from './components';
import type { CommandUpdate } from './useFleet';

const KEY_STORAGE = 'tp.adminKey';

function readKey(): string {
  try {
    return localStorage.getItem(KEY_STORAGE) ?? '';
  } catch {
    return '';
  }
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg bg-ink-900/70 border border-ink-700 px-3 py-2">
      <div className="text-[11px] text-slate-400">{label}</div>
      <div className="text-sm font-medium tabular text-slate-100">{value}</div>
    </div>
  );
}

export function TerminalDrawer({ id, onClose, lastCommand }: { id: string; onClose: () => void; lastCommand: CommandUpdate | null }) {
  const [detail, setDetail] = useState<TerminalDetail | null>(null);
  const [txns, setTxns] = useState<TxnRow[]>([]);
  const [busy, setBusy] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [adminKey, setAdminKey] = useState(readKey);
  const [needKey, setNeedKey] = useState(false);

  const load = useCallback(async () => {
    try {
      const [d, t] = await Promise.all([
        api<TerminalDetail>(`/api/terminals/${id}`),
        api<TxnRow[]>(`/api/terminals/${id}/transactions?limit=12`),
      ]);
      setDetail(d);
      setTxns(t);
    } catch (e) {
      setNotice((e as Error).message);
    }
  }, [id]);

  useEffect(() => {
    setDetail(null);
    setNotice(null);
    load();
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, [load]);

  // Refresh as soon as this terminal acknowledges a command.
  useEffect(() => {
    if (lastCommand?.terminalId === id) load();
  }, [lastCommand, id, load]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === 'Escape' && onClose();
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const send = async (command: string) => {
    setBusy(command);
    setNotice(null);
    try {
      await api(`/api/terminals/${id}/commands`, {
        method: 'POST',
        body: JSON.stringify({ command, requestedBy: 'dashboard' }),
        headers: adminKey ? { 'x-api-key': adminKey } : {},
      });
      setNotice(`${command} sent over MQTT; waiting for the terminal to acknowledge…`);
      setNeedKey(false);
      load();
    } catch (e) {
      if (e instanceof HttpError && e.status === 401) {
        setNeedKey(true);
        setNotice('This deployment requires the admin API key to send commands.');
      } else setNotice((e as Error).message);
    } finally {
      setBusy(null);
    }
  };

  const saveKey = (k: string) => {
    setAdminKey(k);
    try {
      localStorage.setItem(KEY_STORAGE, k);
    } catch {
      /* storage unavailable */
    }
  };

  const hb = detail?.heartbeats ?? [];
  return (
    <div className="fixed inset-0 z-40 flex justify-end" role="dialog" aria-modal="true" aria-label={`Terminal ${id}`}>
      <div className="absolute inset-0 bg-black/50" onClick={onClose} />
      <aside className="drawer relative h-full w-full max-w-[480px] overflow-y-auto bg-ink-900 border-l border-ink-700 p-5">
        <div className="flex items-start justify-between gap-3">
          <div>
            <div className="font-mono text-lg text-slate-50">{id}</div>
            {detail && (
              <div className="text-xs text-slate-400 mt-0.5">
                {detail.merchantId} · {detail.city} · {detail.model} · fw {detail.firmware}
              </div>
            )}
          </div>
          <button onClick={onClose} className="rounded-md px-2 py-1 text-slate-400 hover:text-slate-100 hover:bg-ink-700" aria-label="Close">
            ✕
          </button>
        </div>

        {!detail && !notice && <div className="mt-6 text-sm text-slate-500">Loading…</div>}
        {detail && (
          <>
            <div className="mt-4 flex items-center gap-2 text-sm">
              <span className={`font-medium ${STATUS_TEXT[detail.status]}`}>{detail.status}</span>
              <span className="text-slate-500">· last seen {detail.lastSeen ? new Date(detail.lastSeen).toLocaleTimeString() : 'never'}</span>
              {detail.anomalyScore !== null && <span className="ml-auto text-xs tabular text-slate-400">anomaly score {detail.anomalyScore.toFixed(3)}</span>}
            </div>

            {detail.flagged && detail.reasons.length > 0 && (
              <div className="mt-3 rounded-lg border border-warn/40 bg-warn/10 px-3 py-2 text-xs text-amber-100 space-y-1">
                {detail.reasons.map((r) => (
                  <div key={r}>• {r}</div>
                ))}
              </div>
            )}

            <div className="mt-4 grid grid-cols-3 gap-2">
              <Stat label="Battery" value={detail.battery !== null ? `${detail.battery}%` : '–'} />
              <Stat label="Signal" value={detail.signalDbm !== null ? `${detail.signalDbm} dBm` : '–'} />
              <Stat label="Temp" value={detail.temperatureC !== null ? `${detail.temperatureC.toFixed(1)}°C` : '–'} />
              <Stat label="Queue" value={String(detail.queueDepth ?? '–')} />
              <Stat label="Boots" value={String(detail.bootCount ?? '–')} />
              <Stat label="Auth latency" value={detail.lastHour.avgAuthLatencyMs ? `${detail.lastHour.avgAuthLatencyMs} ms` : '–'} />
            </div>

            <div className="mt-3 grid grid-cols-2 gap-2">
              <MiniLine values={hb.map((h) => h.battery)} color="#34d399" label="Battery" unit="%" />
              <MiniLine values={hb.map((h) => h.temperatureC)} color="#fbbf24" label="Temperature" unit="°C" />
              <MiniLine values={hb.map((h) => h.signalDbm)} color="#60a5fa" label="Signal" unit=" dBm" />
              <MiniLine values={hb.map((h) => h.queueDepth)} color="#f87171" label="Offline queue" unit="" />
            </div>

            <div className="mt-4 text-xs text-slate-400 tabular">
              Last hour: {detail.lastHour.total} txns · {detail.lastHour.approved} approved · {detail.lastHour.declined} declined ·{' '}
              {detail.lastHour.errors} errors · {inr(detail.lastHour.approvedAmountMinor)}
            </div>

            <div className="mt-5">
              <div className="text-sm font-medium text-slate-200">Remote commands</div>
              <div className="mt-2 flex gap-2">
                {['reboot', 'ping', 'clear_queue'].map((c) => (
                  <button
                    key={c}
                    disabled={!!busy}
                    onClick={() => send(c)}
                    className="rounded-md border border-ink-600 bg-ink-800 px-3 py-1.5 text-xs text-slate-100 hover:border-accent disabled:opacity-50"
                  >
                    {busy === c ? 'sending…' : c.replace('_', ' ')}
                  </button>
                ))}
              </div>
              {needKey && (
                <input
                  type="password"
                  placeholder="Admin API key"
                  value={adminKey}
                  onChange={(e) => saveKey(e.target.value)}
                  className="mt-2 w-full rounded-md border border-ink-600 bg-ink-950 px-3 py-1.5 text-xs text-slate-100"
                />
              )}
              {notice && <div className="mt-2 text-xs text-slate-400">{notice}</div>}
              <div className="mt-2 space-y-1">
                {detail.commands.map((c) => (
                  <div key={c.id} className="flex gap-2 text-xs">
                    <span className="text-slate-300">{c.command}</span>
                    <span className={c.status === 'acked' ? 'text-ok' : c.status === 'failed' ? 'text-bad' : 'text-slate-400'}>{c.status}</span>
                    <span className="text-slate-500 truncate">{c.result}</span>
                    <span className="ml-auto text-slate-600 tabular">{new Date(c.createdAt).toLocaleTimeString()}</span>
                  </div>
                ))}
              </div>
            </div>

            <div className="mt-5">
              <div className="text-sm font-medium text-slate-200">Recent transactions</div>
              <table className="mt-2 w-full text-xs tabular">
                <tbody>
                  {txns.map((t) => (
                    <tr key={t.idempotencyKey} className="border-t border-ink-700">
                      <td className="py-1.5 text-slate-500">{new Date(t.deviceTs).toLocaleTimeString()}</td>
                      <td className="text-slate-400 uppercase">{t.method}</td>
                      <td className={t.status === 'approved' ? 'text-ok' : t.status === 'declined' ? 'text-warn' : 'text-bad'}>
                        {t.status}
                        {t.errorCode ? ` ${t.errorCode}` : ''}
                      </td>
                      <td className="text-right text-slate-500">{t.authLatencyMs} ms</td>
                      <td className="text-right text-slate-200">{inr(t.amountMinor)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}
        {!detail && notice && <div className="mt-6 text-sm text-bad">{notice}</div>}
      </aside>
    </div>
  );
}
