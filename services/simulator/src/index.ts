import fs from 'node:fs';
import { Fleet, cliOptions } from './fleet.js';

/**
 * Continuous fleet simulator.
 *   node dist/index.js --terminals 1000 --mqtt mqtt://localhost:1883
 * Every option also reads from env (SIM_TERMINALS, SIM_RATE, SIM_DUP_RATE, MQTT_URL, ...).
 */
const { fleet: opts, get, num } = cliOptions();
const durationS = num('duration', 'SIM_DURATION_S', 0);
const ledgerFile = get('ledger', 'SIM_LEDGER', '');
const truthFile = get('truth-file', 'SIM_TRUTH_FILE', '');

const fleet = new Fleet(opts);
const faulty = Object.keys(fleet.faultyTerminals()).length;
console.log(`[sim] ${opts.terminals} terminals (${faulty} faulty), rate=${opts.rate || 'profile'}, dupRate=${opts.dupRate}, broker=${opts.mqttUrl}`);
await fleet.start();

let last = { ...fleet.stats, at: Date.now() };
const statsTimer = setInterval(() => {
  const s = fleet.stats;
  const dt = (Date.now() - last.at) / 1000;
  const ack = fleet.ackLatency();
  console.log(
    `[sim] txn ${((s.txnPublished - last.txnPublished) / dt).toFixed(0)}/s hb ${((s.hbPublished - last.hbPublished) / dt).toFixed(0)}/s ` +
      `total=${s.txnPublished} dups=${s.dupPublished} inflight=${s.inflight} puback p50=${ack.p50}ms p99=${ack.p99}ms errors=${s.errors}`,
  );
  last = { ...s, at: Date.now() };
  if (truthFile) fs.writeFileSync(truthFile, JSON.stringify(fleet.faultyTerminals(), null, 2));
}, 10_000);

async function finish() {
  clearInterval(statsTimer);
  await fleet.stop();
  if (ledgerFile) fleet.writeLedger(ledgerFile);
  console.log(`[sim] stopped; unique txns=${fleet.ledger.uniqueTxns} redeliveries=${fleet.ledger.redeliveries}`);
  process.exit(0);
}

if (durationS > 0) setTimeout(finish, durationS * 1000);
if (opts.maxTxns > 0) fleet.done.then(finish);
process.on('SIGINT', finish);
process.on('SIGTERM', finish);
