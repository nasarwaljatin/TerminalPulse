import net from 'node:net';
import type http from 'node:http';
import { Aedes } from 'aedes';
import { WebSocketServer, createWebSocketStream } from 'ws';
import { config } from './config.js';
import { log } from './log.js';

/**
 * Optional in-process MQTT broker for single-container deployments (e.g. a free Render web
 * service, where only one HTTP port is exposed). TCP on MQTT_EMBEDDED_PORT is for local
 * clients (the gateway itself, the bundled simulator); MQTT-over-WebSocket on the HTTP port
 * lets remote simulators connect through the platform's HTTPS endpoint.
 */
export async function startEmbeddedBroker(httpServer: http.Server | null): Promise<{ stop: () => Promise<void> }> {
  const broker = await Aedes.createBroker({ concurrency: 1000 });

  if (config.mqttPassword) {
    broker.authenticate = (_client, username, password, done) => {
      const ok = username === config.mqttUsername && password?.toString() === config.mqttPassword;
      if (ok) return done(null, true);
      const err = Object.assign(new Error('bad credentials'), { returnCode: 4 as const });
      done(err as never, false);
    };
  }

  const tcp = net.createServer(broker.handle);
  await new Promise<void>((resolve) => tcp.listen(config.mqttEmbeddedPort, resolve));
  log.info({ port: config.mqttEmbeddedPort }, 'embedded MQTT broker listening (tcp)');

  let wss: WebSocketServer | null = null;
  if (httpServer && config.mqttWsEnabled) {
    wss = new WebSocketServer({ noServer: true });
    httpServer.on('upgrade', (req, socket, head) => {
      if (req.url?.split('?')[0] !== config.mqttWsPath) return;
      wss!.handleUpgrade(req, socket, head, (ws) => {
        broker.handle(createWebSocketStream(ws), req);
      });
    });
    log.info({ path: config.mqttWsPath }, 'embedded MQTT broker listening (websocket)');
  }

  return {
    stop: async () => {
      wss?.close();
      await new Promise<void>((r) => tcp.close(() => r()));
      await new Promise<void>((r) => broker.close(() => r()));
    },
  };
}
