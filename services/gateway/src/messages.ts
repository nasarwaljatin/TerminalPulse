import { z } from 'zod';

/** Wire format published by terminals. Field names are kept short-ish but readable; `v` versions the schema. */
export const TxnMessage = z.object({
  v: z.literal(1),
  terminalId: z.string().min(1).max(64),
  merchantId: z.string().min(1).max(64),
  idempotencyKey: z.string().min(8).max(128),
  amountMinor: z.number().int().nonnegative().max(10_000_000_00),
  currency: z.string().length(3),
  method: z.enum(['card', 'upi', 'nfc', 'wallet']),
  status: z.enum(['approved', 'declined', 'error']),
  errorCode: z.string().max(32).nullable().optional(),
  authLatencyMs: z.number().int().nonnegative().max(600_000),
  ts: z.number().int().positive(),
});
export type TxnMessage = z.infer<typeof TxnMessage>;

export const HeartbeatMessage = z.object({
  v: z.literal(1),
  terminalId: z.string().min(1).max(64),
  merchantId: z.string().min(1).max(64),
  model: z.string().max(64).optional(),
  firmware: z.string().max(32).optional(),
  city: z.string().max(64).optional(),
  lat: z.number().min(-90).max(90).optional(),
  lng: z.number().min(-180).max(180).optional(),
  battery: z.number().min(0).max(100),
  signalDbm: z.number().min(-150).max(0),
  temperatureC: z.number().min(-40).max(150),
  queueDepth: z.number().int().nonnegative(),
  bootCount: z.number().int().nonnegative(),
  uptimeS: z.number().nonnegative(),
  ts: z.number().int().positive(),
});
export type HeartbeatMessage = z.infer<typeof HeartbeatMessage>;

export const AckMessage = z.object({
  v: z.literal(1),
  terminalId: z.string().min(1).max(64),
  commandId: z.uuid(),
  ok: z.boolean(),
  result: z.string().max(500).optional(),
  ts: z.number().int().positive(),
});
export type AckMessage = z.infer<typeof AckMessage>;

export type Kind = 'txn' | 'hb' | 'ack';

export const schemas = { txn: TxnMessage, hb: HeartbeatMessage, ack: AckMessage } as const;

export type ParsedMessage =
  | { kind: 'txn'; msg: TxnMessage }
  | { kind: 'hb'; msg: HeartbeatMessage }
  | { kind: 'ack'; msg: AckMessage };

/** Topic layout: <prefix>/<terminalId>/<kind>. Returns null for anything else. */
export function parseTopic(prefix: string, topic: string): { terminalId: string; kind: Kind } | null {
  if (!topic.startsWith(prefix + '/')) return null;
  const rest = topic.slice(prefix.length + 1).split('/');
  if (rest.length !== 2) return null;
  const [terminalId, kind] = rest;
  if (kind !== 'txn' && kind !== 'hb' && kind !== 'ack') return null;
  return { terminalId, kind };
}

export function parsePayload(kind: Kind, raw: string): ParsedMessage | null {
  let data: unknown;
  try {
    data = JSON.parse(raw);
  } catch {
    return null;
  }
  const res = schemas[kind].safeParse(data);
  if (!res.success) return null;
  return { kind, msg: res.data } as ParsedMessage;
}
