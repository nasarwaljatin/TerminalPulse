import mqtt, { type MqttClient } from 'mqtt';
import type { Redis } from 'ioredis';
import { config } from './config.js';
import { log } from './log.js';
import { metrics } from './metrics.js';
import { parsePayload, parseTopic } from './messages.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Subscription progress, reported by /readyz: a worker is ready once every connection has its SUBACK. */
export const ingestState = {
  expected: config.mqttSharedGroup ? Math.max(1, config.mqttIngestConnections) : 1,
  subscribed: new Set<number>(),
};
export const ingestReady = () => ingestState.subscribed.size >= ingestState.expected;

/**
 * MQTT -> Redis Streams.
 *
 * Each inbound QoS 1 PUBLISH is appended to the stream *before* mqtt.js sends the PUBACK
 * (we override handleMessage, which mqtt.js awaits ahead of the ack). If Redis is down we
 * keep retrying and never ack, so the broker keeps the message and redelivers it. Duplicate
 * deliveries this creates are absorbed downstream by the idempotency-key constraint.
 *
 * mqtt.js processes one packet per connection at a time, so throughput comes from opening
 * several connections in one MQTT shared-subscription group; the broker load-balances
 * across them and across gateway pods.
 */
export function startIngest(redis: Redis): { stop: () => Promise<void> } {
  const shared = config.mqttSharedGroup;
  const connections = shared ? Math.max(1, config.mqttIngestConnections) : 1;
  const filter = `${config.topicPrefix}/+/+`;
  const subscription = shared ? `$share/${shared}/${filter}` : filter;
  const clients: MqttClient[] = [];
  ingestState.expected = connections;

  for (let i = 0; i < connections; i++) {
    const client = mqtt.connect(config.mqttUrl, {
      clientId: `tp-gw-${config.instanceId}-${i}`.slice(0, 64),
      clean: false, // persistent session: unacked QoS 1 messages survive a gateway reconnect
      username: config.mqttUsername,
      password: config.mqttPassword,
      reconnectPeriod: 1000,
      connectTimeout: 10_000,
      protocolVersion: 4,
    });

    client.handleMessage = (packet, done) => {
      const topic = parseTopic(config.topicPrefix, packet.topic);
      const raw = packet.payload?.toString() ?? '';
      const parsed = topic ? parsePayload(topic.kind, raw) : null;
      if (!topic || !parsed || parsed.msg.terminalId !== topic.terminalId) {
        // Poison messages are acked and dropped; retrying them would never succeed.
        metrics.ingestInvalid++;
        if (metrics.ingestInvalid % 100 === 1) log.warn({ topic: packet.topic }, 'dropping invalid message');
        return done();
      }
      appendWithRetry(redis, topic.kind, raw).then(() => {
        metrics.ingested++;
        metrics.ingestRate.add();
        done();
      });
    };

    client.on('connect', () => {
      client.subscribe(subscription, { qos: 1 }, (err) => {
        if (err) return log.error({ err }, 'subscribe failed');
        ingestState.subscribed.add(i);
        if (ingestReady()) log.info({ subscription, connections }, 'ingest subscribed on all connections');
      });
    });
    client.on('close', () => ingestState.subscribed.delete(i));
    client.on('error', (err) => log.warn({ err: err.message }, 'mqtt ingest error'));
    clients.push(client);
  }

  return {
    stop: async () => {
      await Promise.all(clients.map((c) => c.endAsync()));
    },
  };
}

async function appendWithRetry(redis: Redis, kind: string, raw: string): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    try {
      await redis.xadd(
        config.streamKey,
        'MAXLEN',
        '~',
        String(config.streamMaxLen),
        '*',
        'k',
        kind,
        'p',
        raw,
        'r',
        String(Date.now()),
      );
      return;
    } catch (err) {
      if (attempt % 10 === 0) log.error({ err: (err as Error).message, attempt }, 'XADD failed; holding PUBACK');
      await sleep(Math.min(2000, 50 * 2 ** Math.min(attempt, 6)));
    }
  }
}
