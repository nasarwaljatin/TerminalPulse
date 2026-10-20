import type { Redis } from 'ioredis';
import { config } from './config.js';
import { log } from './log.js';

/** Messages fanned out over Redis pub/sub to every API pod, which forwards them to WebSocket clients. */
export type TerminalDelta = {
  id: string;
  merchantId?: string;
  city?: string;
  model?: string;
  firmware?: string;
  lat?: number;
  lng?: number;
  battery?: number;
  signalDbm?: number;
  temperatureC?: number;
  queueDepth?: number;
  lastSeen?: number;
};

export type RecentTxn = {
  key: string;
  terminalId: string;
  amountMinor: number;
  currency: string;
  method: string;
  status: string;
  errorCode: string | null;
  ts: number;
};

export type BatchEvent = {
  t: 'batch';
  hb: TerminalDelta[];
  tx: {
    count: number;
    approved: number;
    declined: number;
    error: number;
    amountMinor: number;
    recent: RecentTxn[];
    latencies: number[];
  };
  dups: number;
  acks: { commandId: string; terminalId: string; ok: boolean; result: string | null }[];
};

export type AnomalyItem = { terminalId: string; score: number; flagged: boolean; reasons: string[] };
export type AnomaliesEvent = { t: 'anomalies'; items: AnomalyItem[]; scoredAt: number };
export type CommandEvent = { t: 'command'; id: string; terminalId: string; command: string; status: string };

export type FleetEvent = BatchEvent | AnomaliesEvent | CommandEvent;

export function publishEvent(redis: Redis, event: FleetEvent): void {
  redis.publish(config.eventsChannel, JSON.stringify(event)).catch((err) => {
    log.warn({ err: (err as Error).message }, 'event publish failed');
  });
}
