import { Fleet, cliOptions, sleep } from './fleet.js';

/**
 * Publish-to-persist load test.
 *   node dist/loadtest.js --terminals 1000 --rate 5000 --duration 60 --api http://localhost:8080
 *
 * Drives the fleet at a fixed transaction rate, waits for the pipeline to drain, then asks the
 * gateway for latency percentiles computed from the persisted rows (device ts -> row written),
 * which covers every gateway pod, not just one process.
 */
const { fleet: base, get, num } = cliOptions();
const api = get('api', 'API_URL', 'http://localhost:8080').replace(/\/$/, '');
const durationS = num('duration', 'LOADTEST_DURATION_S', 60);
const warmupS = num('warmup', 'LOADTEST_WARMUP_S', 5);
const maxP99 = num('max-p99', 'LOADTEST_MAX_P99_MS', 0);

const opts = {
  ...base,
  terminals: num('terminals', 'SIM_TERMINALS', 1000),
  rate: num('rate', 'SIM_RATE', 5000),
  idPrefix: get('id-prefix', 'SIM_ID_PREFIX', `LT${Date.now().toString(36).slice(-4)}-`),
  faultDrift: false,
};
opts.connections = num('connections', 'SIM_CONNECTIONS', Math.min(opts.terminals, 1000));

async function json(path: string) {
  const res = await fetch(api + path);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json() as Promise<any>;
}

console.log(`[load] ${opts.terminals} terminals, target ${opts.rate} txn/s for ${durationS}s (+${warmupS}s warmup) via ${opts.mqttUrl}`);
const fleet = new Fleet(opts);
await fleet.start();
await sleep(warmupS * 1000);

const startMs = Date.now();
const startPublished = fleet.stats.txnPublished;
const progress = setInterval(() => {
  const s = fleet.stats;
  const elapsed = (Date.now() - startMs) / 1000;
  console.log(`[load] t=${elapsed.toFixed(0)}s published=${s.txnPublished - startPublished} (${((s.txnPublished - startPublished) / elapsed).toFixed(0)}/s) inflight=${s.inflight}`);
}, 5000);
await sleep(durationS * 1000);
clearInterval(progress);
const published = fleet.stats.txnPublished - startPublished;
const hbPublished = fleet.stats.hbPublished;
await fleet.stop();
const endMs = Date.now();

// Drain: wait until the consumer group has nothing pending or lagging.
console.log('[load] publishing stopped; waiting for the pipeline to drain...');
for (let i = 0; i < 120; i++) {
  const m = await json('/api/metrics').catch(() => null);
  const g = m?.stream?.groups?.[0];
  if (g && Number(g.pending) === 0 && (g.lag === null || Number(g.lag) === 0)) break;
  await sleep(1000);
}

const lat = await json(`/api/metrics/latency?sinceMs=${startMs}`);
const gw = await json('/api/metrics');
const seconds = (endMs - startMs) / 1000;
const msgs = published + hbPublished;
const result = {
  terminals: opts.terminals,
  durationS: Math.round(seconds),
  txnPublished: published,
  txnPerSec: Math.round(published / seconds),
  messagesPerSecIncludingHeartbeats: Math.round(msgs / (seconds + warmupS)),
  txnPersisted: lat.count,
  publishToPersistMs: { p50: lat.p50, p95: lat.p95, p99: lat.p99, max: lat.max },
  gatewayInstanceLatencyMs: gw.latencyMs,
  duplicatesSuppressed: gw.idempotency?.duplicatesSuppressed,
};
console.log(JSON.stringify(result, null, 2));

if (maxP99 > 0 && (lat.p99 === null || lat.p99 > maxP99)) {
  console.error(`[load] FAIL: p99 ${lat.p99} ms > ${maxP99} ms`);
  process.exit(1);
}
process.exit(0);
