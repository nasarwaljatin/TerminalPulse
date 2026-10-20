import http from 'node:http';
import { fork, type ChildProcess } from 'node:child_process';
import { createRequire } from 'node:module';
import path from 'node:path';
import { config, has } from './config.js';
import { log } from './log.js';
import { migrate, pool } from './db.js';
import { createRedis } from './redis.js';
import { createApp } from './api.js';
import { startEmbeddedBroker } from './broker.js';
import { startIngest } from './ingest.js';
import { startPersist } from './persist.js';
import { startWsHub } from './ws.js';
import { startJobs } from './jobs.js';
import { stopCommands } from './commands.js';

const stops: (() => unknown)[] = [];
log.info({ roles: [...config.roles] }, 'starting TerminalPulse gateway');

await migrate();

const redis = createRedis('main');
const events = createRedis('events-pub');

// The HTTP server always runs: it carries health checks even on ingest/persist-only pods.
const app = createApp(redis, events);
const server = http.createServer(app);

if (config.mqttEmbedded) {
  const broker = await startEmbeddedBroker(server);
  stops.push(broker.stop);
}
if (has('api')) {
  const hub = startWsHub(server, createRedis('events-sub'));
  stops.push(hub.stop);
}
await new Promise<void>((resolve) => server.listen(config.port, resolve));
log.info({ port: config.port }, 'http listening');

if (has('persist')) {
  const persist = await startPersist(events);
  stops.push(persist.stop);
}
if (has('ingest')) {
  const ingest = startIngest(redis);
  stops.push(ingest.stop);
}
if (has('jobs')) {
  const jobs = startJobs(redis, events);
  stops.push(jobs.stop);
}

// Single-container demo: run the fleet simulator as a child process next to the gateway.
let sim: ChildProcess | null = null;
if (has('sim')) {
  const require = createRequire(import.meta.url);
  let entry = process.env.SIM_ENTRY;
  if (!entry) {
    try {
      entry = path.join(path.dirname(require.resolve('@terminalpulse/simulator/package.json')), 'dist/index.js');
    } catch {
      entry = path.resolve('services/simulator/dist/index.js');
    }
  }
  const mqttUrl = config.mqttEmbedded ? `mqtt://127.0.0.1:${config.mqttEmbeddedPort}` : config.mqttUrl;
  sim = fork(entry, [], { env: { ...process.env, MQTT_URL: mqttUrl } });
  sim.on('exit', (code) => log.warn({ code }, 'simulator exited'));
  log.info({ entry }, 'embedded simulator started');
}

let shuttingDown = false;
async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  log.info({ signal }, 'shutting down');
  sim?.kill('SIGTERM');
  server.close();
  for (const stop of stops.reverse()) await Promise.resolve(stop()).catch(() => {});
  await stopCommands().catch(() => {});
  redis.disconnect();
  events.disconnect();
  await pool.end().catch(() => {});
  process.exit(0);
}
process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));
