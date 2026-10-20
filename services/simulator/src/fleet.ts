import { randomUUID } from 'node:crypto';
import fs from 'node:fs';
import mqtt, { type MqttClient } from 'mqtt';
import { loadProfiles, type ProfileName, type ProfileSpec, type ProfilesFile } from './profiles.js';
import { Rng } from './rng.js';

export type FleetOptions = {
  mqttUrl: string;
  username?: string;
  password?: string;
  topicPrefix: string;
  terminals: number;
  connections: number;
  /** Total transactions/sec across the fleet. 0 = each terminal's own profile rate. */
  rate: number;
  /** Fraction of transactions re-published with the same idempotency key (forced redelivery). */
  dupRate: number;
  /** Stop publishing transactions after this many unique ones (0 = unlimited). */
  maxTxns: number;
  heartbeats: boolean;
  seed: number;
  idPrefix: string;
  faultFraction?: number;
  /** Periodically turn healthy terminals faulty so the fleet keeps a steady fault population. */
  faultDrift: boolean;
  log?: (msg: string) => void;
};

type Params = { [K in keyof ProfileSpec]: number };

type Terminal = {
  id: string;
  merchantId: string;
  city: string;
  lat: number;
  lng: number;
  model: string;
  firmware: string;
  profile: ProfileName;
  p: Params;
  battery: number;
  bootCount: number;
  bootedAt: number;
  offlineUntil: number;
  nextTxnAt: number;
  nextHbAt: number;
  conn: number;
};

export type Ledger = {
  uniqueTxns: number;
  redeliveries: number;
  amountByStatus: Record<string, { count: number; amountMinor: number }>;
  perTerminal: Record<string, { count: number; amountMinor: number }>;
};

export class Fleet {
  readonly profiles: ProfilesFile;
  readonly terminals: Terminal[] = [];
  private clients: MqttClient[] = [];
  private rng: Rng;
  private timer: NodeJS.Timeout | null = null;
  private driftTimer: NodeJS.Timeout | null = null;
  private stopped = false;
  private byId = new Map<string, Terminal>();

  readonly stats = { txnPublished: 0, txnAcked: 0, hbPublished: 0, dupPublished: 0, inflight: 0, errors: 0, commands: 0 };
  readonly ledger: Ledger = { uniqueTxns: 0, redeliveries: 0, amountByStatus: {}, perTerminal: {} };
  private ackLatencies: number[] = [];
  private doneResolve: (() => void) | null = null;
  readonly done: Promise<void>;

  constructor(readonly opts: FleetOptions) {
    this.profiles = loadProfiles();
    this.rng = new Rng(opts.seed);
    this.done = new Promise((r) => (this.doneResolve = r));
    const faultFraction = opts.faultFraction ?? this.profiles.faultFraction;
    const now = Date.now();
    for (let i = 0; i < opts.terminals; i++) {
      const city = this.rng.pick(this.profiles.cities);
      const profile: ProfileName = this.rng.next() < faultFraction ? this.rng.weighted(this.profiles.faultWeights) : 'healthy';
      const t: Terminal = {
        id: `${opts.idPrefix}${String(i + 1).padStart(5, '0')}`,
        merchantId: `M${String((i % Math.max(1, Math.ceil(opts.terminals / 4))) + 1).padStart(4, '0')}`,
        city: city.name,
        lat: round(city.lat + this.rng.normal(0, 0.05), 5),
        lng: round(city.lng + this.rng.normal(0, 0.05), 5),
        model: this.rng.pick(this.profiles.models),
        firmware: this.rng.pick(this.profiles.firmware),
        profile,
        p: this.drawParams(profile),
        battery: 0,
        bootCount: this.rng.int(1, 40),
        bootedAt: now - this.rng.int(60, 86_400) * 1000,
        offlineUntil: 0,
        nextTxnAt: now + this.rng.uniform(0, 3000),
        nextHbAt: now + this.rng.uniform(0, this.profiles.heartbeatIntervalS * 1000),
        conn: i % Math.max(1, opts.connections),
      };
      t.battery = t.p.battery;
      this.terminals.push(t);
      this.byId.set(t.id, t);
    }
  }

  private drawParams(profile: ProfileName): Params {
    const spec = this.profiles.profiles[profile];
    const out = {} as Params;
    for (const key of Object.keys(spec) as (keyof ProfileSpec)[]) out[key] = this.rng.uniform(spec[key][0], spec[key][1]);
    return out;
  }

  faultyTerminals(): Record<string, ProfileName> {
    const out: Record<string, ProfileName> = {};
    for (const t of this.terminals) if (t.profile !== 'healthy') out[t.id] = t.profile;
    return out;
  }

  async start(): Promise<void> {
    const { opts } = this;
    const n = Math.max(1, opts.connections);
    const connected: Promise<void>[] = [];
    for (let i = 0; i < n; i++) {
      const owned = this.terminals.filter((t) => t.conn === i);
      const client = mqtt.connect(opts.mqttUrl, {
        clientId: n === opts.terminals && owned[0] ? `term-${owned[0].id}` : `tp-sim-${opts.idPrefix}-${opts.seed}-${i}`,
        username: opts.username,
        password: opts.password,
        clean: true,
        reconnectPeriod: 1000,
        connectTimeout: 15_000,
        protocolVersion: 4,
        keepalive: 30,
      });
      client.on('error', (err) => {
        this.stats.errors++;
        if (this.stats.errors % 50 === 1) opts.log?.(`mqtt error: ${err.message}`);
      });
      client.on('message', (topic, payload) => this.onCommand(topic, payload.toString()));
      connected.push(
        new Promise<void>((resolve) => {
          client.once('connect', () => {
            const topics = owned.map((t) => `${opts.topicPrefix}/${t.id}/cmd`);
            if (topics.length) client.subscribe(topics, { qos: 1 });
            resolve();
          });
        }),
      );
      this.clients.push(client);
      // Stagger connection setup so 1,000 terminals don't stampede the broker.
      if (i % 50 === 49) await sleep(100);
    }
    await Promise.all(connected);
    // Start every terminal's schedule now, spread over its first interval. Connecting 1,000 clients
    // takes seconds; schedules set at construction time would all be overdue and fire as one burst.
    const startAt = Date.now();
    for (const t of this.terminals) {
      const ratePerSec = opts.rate > 0 ? opts.rate / opts.terminals : t.p.txnPerMin / 60;
      t.nextTxnAt = startAt + this.rng.uniform(0, 1000 / ratePerSec);
      t.nextHbAt = startAt + this.rng.uniform(0, this.profiles.heartbeatIntervalS * 1000);
    }
    opts.log?.(`fleet online: ${opts.terminals} terminals over ${n} MQTT connections`);
    this.timer = setInterval(() => this.tick(), 20);
    if (opts.faultDrift) this.driftTimer = setInterval(() => this.drift(), 60_000);
  }

  private tick() {
    if (this.stopped) return;
    const now = Date.now();
    const { opts } = this;
    const perTerminalRate = opts.rate > 0 ? opts.rate / opts.terminals : 0; // per second
    for (const t of this.terminals) {
      if (now < t.offlineUntil) continue;
      if (opts.heartbeats && now >= t.nextHbAt) {
        t.nextHbAt += this.profiles.heartbeatIntervalS * 1000;
        if (t.nextHbAt < now) t.nextHbAt = now + this.profiles.heartbeatIntervalS * 1000;
        this.heartbeat(t, now);
      }
      // Catch up on every transaction due this tick (high rates fire several per 20 ms tick).
      let guard = 0;
      while (now >= t.nextTxnAt && guard++ < 50) {
        if (opts.maxTxns && this.ledger.uniqueTxns >= opts.maxTxns) break;
        const ratePerSec = perTerminalRate || t.p.txnPerMin / 60;
        t.nextTxnAt += this.rng.exponential(ratePerSec) * 1000;
        this.transaction(t);
      }
      if (now - t.nextTxnAt > 5000) t.nextTxnAt = now; // don't build an unbounded backlog after a stall
    }
    if (opts.maxTxns && this.ledger.uniqueTxns >= opts.maxTxns && this.stats.inflight === 0) {
      this.doneResolve?.();
    }
  }

  private heartbeat(t: Terminal, now: number) {
    // Rare reboot (Poisson): the terminal disappears for a few seconds and comes back with bootCount+1.
    if (this.rng.next() < (t.p.rebootsPerHour * this.profiles.heartbeatIntervalS) / 3600) {
      this.reboot(t, now);
      return;
    }
    if (this.rng.next() < t.p.hbDropP) return; // lost heartbeat
    const [blo, bhi] = this.profiles.profiles[t.profile].battery;
    t.battery = clamp(t.battery + this.rng.normal(0, 0.8), Math.max(0, blo - 2), Math.min(100, bhi + 2));
    const msg = {
      v: 1,
      terminalId: t.id,
      merchantId: t.merchantId,
      model: t.model,
      firmware: t.firmware,
      city: t.city,
      lat: t.lat,
      lng: t.lng,
      battery: round(t.battery, 1),
      signalDbm: Math.round(clamp(t.p.signalDbm + this.rng.normal(0, 2.5), -130, -30)),
      temperatureC: round(t.p.temperatureC + this.rng.normal(0, 1.2), 1),
      queueDepth: Math.max(0, Math.round(t.p.queueDepth + this.rng.normal(0, 0.8))),
      bootCount: t.bootCount,
      uptimeS: Math.round((now - t.bootedAt) / 1000),
      ts: now,
    };
    this.stats.hbPublished++;
    this.clients[t.conn].publish(`${this.opts.topicPrefix}/${t.id}/hb`, JSON.stringify(msg), { qos: 1 });
  }

  private transaction(t: Terminal) {
    const r = this.rng.next();
    const status = r < t.p.errorP ? 'error' : r < t.p.errorP + t.p.declineP ? 'declined' : 'approved';
    const method = this.rng.weighted({ upi: 0.42, card: 0.4, nfc: 0.14, wallet: 0.04 });
    const amountMinor = Math.max(100, Math.round(this.rng.lognormalMean(85_000, 0.9) / 100) * 100);
    const msg = {
      v: 1,
      terminalId: t.id,
      merchantId: t.merchantId,
      idempotencyKey: `${t.id}:${randomUUID()}`,
      amountMinor,
      currency: 'INR',
      method,
      status,
      errorCode: status === 'error' ? this.rng.pick(['E_CARD_READ', 'E_HOST_TIMEOUT', 'E_PINPAD']) : status === 'declined' ? this.rng.pick(['05', '51', '55']) : null,
      authLatencyMs: Math.round(this.rng.lognormalMean(t.p.latencyMs, 0.25)),
      ts: Date.now(),
    };
    const payload = JSON.stringify(msg);
    const topic = `${this.opts.topicPrefix}/${t.id}/txn`;
    const client = this.clients[t.conn];

    this.ledger.uniqueTxns++;
    const byStatus = (this.ledger.amountByStatus[status] ??= { count: 0, amountMinor: 0 });
    byStatus.count++;
    byStatus.amountMinor += amountMinor;
    const per = (this.ledger.perTerminal[t.id] ??= { count: 0, amountMinor: 0 });
    per.count++;
    per.amountMinor += amountMinor;

    this.stats.txnPublished++;
    this.stats.inflight++;
    const sentAt = Date.now();
    client.publish(topic, payload, { qos: 1 }, (err) => {
      this.stats.inflight--;
      if (err) {
        this.stats.errors++;
        return;
      }
      this.stats.txnAcked++;
      if (this.ackLatencies.length < 50_000) this.ackLatencies.push(Date.now() - sentAt);
      // Forced redelivery: as if the PUBACK was lost and the terminal retransmitted.
      if (this.opts.dupRate > 0 && this.rng.next() < this.opts.dupRate) {
        this.ledger.redeliveries++;
        this.stats.dupPublished++;
        this.stats.inflight++;
        client.publish(topic, payload, { qos: 1, dup: true }, () => this.stats.inflight--);
      }
    });
  }

  private reboot(t: Terminal, now: number, cure = false) {
    t.bootCount++;
    t.offlineUntil = now + this.rng.uniform(6_000, 14_000);
    t.bootedAt = t.offlineUntil;
    t.nextHbAt = t.offlineUntil + 500;
    t.nextTxnAt = Math.max(t.nextTxnAt, t.offlineUntil);
    if (cure && t.profile !== 'healthy' && t.profile !== 'card_reader_fault' && this.rng.next() < 0.75) {
      t.profile = 'healthy';
      t.p = this.drawParams('healthy');
      t.battery = t.p.battery;
      return true;
    }
    return false;
  }

  private onCommand(topic: string, raw: string) {
    const id = topic.split('/').at(-2) ?? '';
    const t = this.byId.get(id);
    if (!t) return;
    let cmd: { commandId: string; command: string };
    try {
      cmd = JSON.parse(raw);
    } catch {
      return;
    }
    this.stats.commands++;
    const ack = (ok: boolean, result: string, delay = 0) =>
      setTimeout(() => {
        this.clients[t.conn].publish(
          `${this.opts.topicPrefix}/${t.id}/ack`,
          JSON.stringify({ v: 1, terminalId: t.id, commandId: cmd.commandId, ok, result, ts: Date.now() }),
          { qos: 1 },
        );
      }, delay);
    const now = Date.now();
    if (cmd.command === 'reboot') {
      const was = t.profile;
      const cured = this.reboot(t, now, true);
      ack(true, cured ? `rebooted; ${was} cleared` : was === 'healthy' ? 'rebooted' : `rebooted; ${was} persists, dispatch technician`, t.offlineUntil - now);
    } else if (cmd.command === 'ping') {
      ack(now >= t.offlineUntil, now >= t.offlineUntil ? `pong battery=${Math.round(t.battery)}%` : 'offline');
    } else if (cmd.command === 'clear_queue') {
      const before = Math.round(t.p.queueDepth);
      t.p.queueDepth = 0;
      ack(true, `cleared ${before} queued items`);
    } else {
      ack(false, `unknown command ${cmd.command}`);
    }
  }

  private drift() {
    const target = Math.round((this.opts.faultFraction ?? this.profiles.faultFraction) * this.terminals.length);
    const faulty = this.terminals.filter((t) => t.profile !== 'healthy').length;
    if (faulty >= target) return;
    const healthy = this.terminals.filter((t) => t.profile === 'healthy');
    const t = this.rng.pick(healthy);
    t.profile = this.rng.weighted(this.profiles.faultWeights);
    t.p = this.drawParams(t.profile);
    t.battery = t.p.battery;
    this.opts.log?.(`fault injected: ${t.id} -> ${t.profile}`);
  }

  ackLatency() {
    const s = [...this.ackLatencies].sort((a, b) => a - b);
    const p = (q: number) => (s.length ? s[Math.min(s.length - 1, Math.ceil(q * s.length) - 1)] : null);
    return { n: s.length, p50: p(0.5), p99: p(0.99) };
  }

  async waitForInflight(timeoutMs = 30_000) {
    const until = Date.now() + timeoutMs;
    while (this.stats.inflight > 0 && Date.now() < until) await sleep(100);
  }

  async stop() {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    if (this.driftTimer) clearInterval(this.driftTimer);
    await this.waitForInflight(10_000);
    await Promise.all(this.clients.map((c) => c.endAsync().catch(() => {})));
  }

  writeLedger(file: string) {
    fs.writeFileSync(file, JSON.stringify(this.ledger, null, 2));
  }
}

export function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}
function round(v: number, d: number) {
  const f = 10 ** d;
  return Math.round(v * f) / f;
}
function clamp(v: number, lo: number, hi: number) {
  return Math.min(hi, Math.max(lo, v));
}

/** Reads options from `--flag value` / `--flag=value` arguments, falling back to environment variables. */
export function cliOptions(argv = process.argv.slice(2)) {
  const args: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const [k, v] = a.slice(2).split('=');
    if (v !== undefined) args[k] = v;
    else if (argv[i + 1] && !argv[i + 1].startsWith('--')) args[k] = argv[++i];
    else args[k] = 'true';
  }
  const get = (flag: string, env: string, fallback: string) => args[flag] ?? process.env[env] ?? fallback;
  const num = (flag: string, env: string, fallback: number) => Number(get(flag, env, String(fallback)));
  const terminals = num('terminals', 'SIM_TERMINALS', 200);
  return {
    args,
    get,
    num,
    fleet: {
      mqttUrl: get('mqtt', 'MQTT_URL', 'mqtt://localhost:1883'),
      username: process.env.MQTT_USERNAME || undefined,
      password: process.env.MQTT_PASSWORD || undefined,
      topicPrefix: get('topic-prefix', 'MQTT_TOPIC_PREFIX', 'tp/terminals'),
      terminals,
      connections: num('connections', 'SIM_CONNECTIONS', Math.min(terminals, 1000)),
      rate: num('rate', 'SIM_RATE', 0),
      dupRate: num('dup-rate', 'SIM_DUP_RATE', 0),
      maxTxns: num('max-txns', 'SIM_MAX_TXNS', 0),
      heartbeats: get('heartbeats', 'SIM_HEARTBEATS', 'true') !== 'false',
      seed: num('seed', 'SIM_SEED', 42),
      idPrefix: get('id-prefix', 'SIM_ID_PREFIX', 'T'),
      faultFraction: args['fault-fraction'] || process.env.SIM_FAULT_FRACTION ? num('fault-fraction', 'SIM_FAULT_FRACTION', 0.08) : undefined,
      faultDrift: get('fault-drift', 'SIM_FAULT_DRIFT', 'true') !== 'false',
      log: (m: string) => console.log(`[sim] ${m}`),
    } satisfies FleetOptions,
  };
}
