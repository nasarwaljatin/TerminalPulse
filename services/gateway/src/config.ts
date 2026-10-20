import os from 'node:os';

function str(name: string, fallback: string): string {
  const v = process.env[name];
  return v === undefined || v === '' ? fallback : v;
}

function int(name: string, fallback: number): number {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`${name} must be a number, got "${v}"`);
  return n;
}

function bool(name: string, fallback: boolean): boolean {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  return ['1', 'true', 'yes', 'on'].includes(v.toLowerCase());
}

export type Role = 'ingest' | 'persist' | 'api' | 'jobs' | 'sim';

const roles = str('ROLES', 'ingest,persist,api,jobs')
  .split(',')
  .map((r) => r.trim())
  .filter(Boolean) as Role[];

export const config = {
  instanceId: str('INSTANCE_ID', `${os.hostname()}-${process.pid}`),
  roles: new Set<Role>(roles),
  port: int('PORT', 8080),
  logLevel: str('LOG_LEVEL', 'info'),

  databaseUrl: str('DATABASE_URL', 'postgres://terminalpulse:terminalpulse@localhost:5432/terminalpulse'),
  dbPoolSize: int('DB_POOL_SIZE', 10),
  redisUrl: str('REDIS_URL', 'redis://localhost:6379'),

  mqttUrl: str('MQTT_URL', 'mqtt://localhost:1883'),
  mqttUsername: process.env.MQTT_USERNAME || undefined,
  mqttPassword: process.env.MQTT_PASSWORD || undefined,
  /** Shared-subscription group; "none" disables $share. The embedded broker has no shared subscriptions, so it defaults to none. */
  mqttSharedGroup: ((g) => (['none', 'off', 'false'].includes(g) ? '' : g))(
    str('MQTT_SHARED_GROUP', bool('MQTT_EMBEDDED', false) ? 'none' : 'tp-gateway'),
  ),
  /** Parallel MQTT connections per ingest process. mqtt.js handles a connection's QoS 1 messages one at a time, so parallelism comes from connections. */
  mqttIngestConnections: int('MQTT_INGEST_CONNECTIONS', 4),
  topicPrefix: str('MQTT_TOPIC_PREFIX', 'tp/terminals'),

  /** Run an in-process Aedes broker (single-container deploys such as Render free tier). */
  mqttEmbedded: bool('MQTT_EMBEDDED', false),
  mqttEmbeddedPort: int('MQTT_EMBEDDED_PORT', 1883),
  /** Expose the embedded broker over WebSocket on the HTTP port, so remote simulators can connect through an HTTPS-only host. */
  mqttWsPath: str('MQTT_WS_PATH', '/mqtt'),
  mqttWsEnabled: bool('MQTT_WS_ENABLED', false),

  streamKey: str('STREAM_KEY', 'tp:ingest'),
  streamMaxLen: int('STREAM_MAXLEN', 500_000),
  consumerGroup: str('CONSUMER_GROUP', 'tp-persisters'),
  batchSize: int('BATCH_SIZE', 500),
  /** XREADGROUP BLOCK in ms. 0 polls instead of blocking (for Redis-compatible servers without blocking reads). */
  batchBlockMs: int('BATCH_BLOCK_MS', 25),
  persistConcurrency: int('PERSIST_CONCURRENCY', 2),
  /** Entries pending this long on a dead consumer are claimed and retried. */
  claimIdleMs: int('CLAIM_IDLE_MS', 15_000),
  /** Chaos knob: fraction of persisted batches whose XACK is skipped, forcing redelivery through XAUTOCLAIM. */
  chaosNoAckRate: Number(str('CHAOS_NOACK_RATE', '0')),

  eventsChannel: str('EVENTS_CHANNEL', 'tp:events'),
  wsTickMs: int('WS_TICK_MS', 250),
  offlineAfterS: int('OFFLINE_AFTER_S', 35),

  mlUrl: process.env.ML_URL || '',
  scoreIntervalS: int('SCORE_INTERVAL_S', 30),
  scoreWindowS: int('SCORE_WINDOW_S', 600),
  minHistoryS: int('MIN_HISTORY_S', 300),
  retentionHours: int('RETENTION_HOURS', 24),
  heartbeatRetentionHours: int('HEARTBEAT_RETENTION_HOURS', 2),

  adminApiKey: process.env.ADMIN_API_KEY || '',
  corsOrigin: str('CORS_ORIGIN', '*'),
  staticDir: process.env.STATIC_DIR || '',
};

export const has = (role: Role) => config.roles.has(role);
