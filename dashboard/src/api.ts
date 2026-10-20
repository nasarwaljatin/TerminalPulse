/** Gateway base URL. Empty = same origin (dashboard served by the gateway, or the Vite dev proxy). */
export const API_URL = (import.meta.env.VITE_API_URL ?? '').replace(/\/$/, '');

export function wsUrl(): string {
  if (API_URL) return API_URL.replace(/^http/, 'ws') + '/ws';
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  return `${proto}://${location.host}/ws`;
}

export type Status = 'online' | 'offline' | 'flagged';

export type Terminal = {
  id: string;
  merchantId: string;
  model: string | null;
  firmware: string | null;
  city: string | null;
  lat: number | null;
  lng: number | null;
  lastSeen: number | null; // epoch ms
  battery: number | null;
  signalDbm: number | null;
  temperatureC: number | null;
  queueDepth: number | null;
  anomalyScore: number | null;
  flagged: boolean;
  reasons: string[];
};

export type Overview = {
  fleet: { total: number; online: number; offline: number; flagged: number; avgBattery: number | null };
  last5Min: {
    total: number;
    approved: number;
    declined: number;
    errors: number;
    approvedAmountMinor: number;
    approvalRate: number | null;
    p50Ms: number | null;
    p99Ms: number | null;
  };
  byCity: { city: string; terminals: number; flagged: number; offline: number }[];
};

export type RecentTxn = {
  key: string;
  terminalId: string;
  amountMinor: number;
  currency: string;
  method: string;
  status: 'approved' | 'declined' | 'error';
  errorCode: string | null;
  ts: number;
};

export type TerminalDetail = Terminal & {
  firstSeen: string;
  bootCount: number | null;
  status: Status;
  features: Record<string, number | null> | null;
  scoredAt: string | null;
  lastHour: { total: number; approved: number; declined: number; errors: number; approvedAmountMinor: number; avgAuthLatencyMs: number | null };
  heartbeats: { ts: string; battery: number; signalDbm: number; temperatureC: number; queueDepth: number; bootCount: number }[];
  commands: { id: string; command: string; status: string; result: string | null; requestedBy: string | null; createdAt: string; ackedAt: string | null }[];
};

export type TxnRow = {
  idempotencyKey: string;
  amountMinor: number;
  currency: string;
  method: string;
  status: string;
  errorCode: string | null;
  authLatencyMs: number;
  deviceTs: string;
  persistedAt: string;
};

export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(API_URL + path, {
    ...init,
    headers: { 'content-type': 'application/json', ...(init?.headers ?? {}) },
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new HttpError(res.status, body.error ?? `HTTP ${res.status}`);
  }
  return res.json();
}

/** Normalises a terminal row from the REST API (timestamps as ISO strings) into client shape. */
export function toTerminal(r: any): Terminal {
  return {
    id: r.id,
    merchantId: r.merchantId,
    model: r.model,
    firmware: r.firmware,
    city: r.city,
    lat: r.lat,
    lng: r.lng,
    lastSeen: r.lastSeen ? new Date(r.lastSeen).getTime() : null,
    battery: r.battery,
    signalDbm: r.signalDbm,
    temperatureC: r.temperatureC,
    queueDepth: r.queueDepth,
    anomalyScore: r.anomalyScore,
    flagged: !!r.flagged,
    reasons: r.reasons ?? [],
  };
}

export const inr = (minor: number) =>
  '₹' + (minor / 100).toLocaleString('en-IN', { maximumFractionDigits: minor >= 10_000_000 ? 0 : 2 });

export const compactInr = (minor: number) => {
  const rupees = minor / 100;
  if (rupees >= 1e7) return `₹${(rupees / 1e7).toFixed(2)} Cr`;
  if (rupees >= 1e5) return `₹${(rupees / 1e5).toFixed(2)} L`;
  return inr(minor);
};
