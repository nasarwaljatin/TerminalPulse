import mqtt, { type MqttClient } from 'mqtt';
import type { Redis } from 'ioredis';
import { config } from './config.js';
import { pool } from './db.js';
import { log } from './log.js';
import { publishEvent } from './events.js';

export const COMMANDS = ['reboot', 'ping', 'clear_queue'] as const;
export type CommandName = (typeof COMMANDS)[number];

let client: MqttClient | null = null;

function publisher(): MqttClient {
  if (!client) {
    client = mqtt.connect(config.mqttUrl, {
      clientId: `tp-cmd-${config.instanceId}`.slice(0, 64),
      username: config.mqttUsername,
      password: config.mqttPassword,
      reconnectPeriod: 1000,
      protocolVersion: 4,
    });
    client.on('error', (err) => log.warn({ err: err.message }, 'mqtt command publisher error'));
  }
  return client;
}

/** Records the command, then publishes it at QoS 1; the terminal's ack flows back through ingest. */
export async function sendCommand(events: Redis, terminalId: string, command: CommandName, requestedBy: string) {
  const exists = await pool.query('SELECT 1 FROM terminals WHERE id = $1', [terminalId]);
  if (!exists.rowCount) return null;
  const row = (
    await pool.query<{ id: string; created_at: Date }>(
      `INSERT INTO commands (terminal_id, command, requested_by) VALUES ($1, $2, $3) RETURNING id, created_at`,
      [terminalId, command, requestedBy],
    )
  ).rows[0];
  await publisher().publishAsync(
    `${config.topicPrefix}/${terminalId}/cmd`,
    JSON.stringify({ v: 1, commandId: row.id, command, ts: Date.now() }),
    { qos: 1 },
  );
  publishEvent(events, { t: 'command', id: row.id, terminalId, command, status: 'sent' });
  return { id: row.id, terminalId, command, status: 'sent', createdAt: row.created_at };
}

export async function stopCommands() {
  await client?.endAsync();
}
