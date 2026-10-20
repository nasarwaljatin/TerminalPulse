import { Redis } from 'ioredis';
import { config } from './config.js';
import { log } from './log.js';

/** Each caller gets its own connection: blocking reads and pub/sub must not share one with regular commands. */
export function createRedis(name: string, opts: { blocking?: boolean } = {}): Redis {
  const client = new Redis(config.redisUrl, {
    connectionName: `tp-${name}`,
    enableAutoPipelining: !opts.blocking,
    maxRetriesPerRequest: opts.blocking ? null : 20,
    lazyConnect: false,
    // RESP2 keeps raw stream replies (XREADGROUP/XAUTOCLAIM via .call) in the classic nested-array
    // shape, and works with every Redis-compatible host (Upstash, Render Key Value, Valkey).
    protocol: 2,
    // Skip the INFO-based ready check: some Redis-compatible servers don't implement INFO.
    enableReadyCheck: false,
    // Managed Redis (Upstash, Render Key Value over TLS) needs SNI; rediss:// URLs turn TLS on automatically.
  });
  client.on('error', (err) => log.warn({ err: err.message, name }, 'redis error'));
  return client;
}
