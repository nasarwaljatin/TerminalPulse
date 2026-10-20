import pg from 'pg';
import { Fleet, cliOptions, sleep } from './fleet.js';

/**
 * Reconciliation test (runs in CI).
 *   node dist/reconcile.js --txns 100000 --dup-rate 0.1 --terminals 1000 --rate 5000
 *
 * Publishes N unique transactions, re-publishes a fraction of them with the same idempotency
 * key, and can be combined with the gateway's CHAOS_NOACK_RATE to force stream-level redelivery.
 * Then it compares the simulator's ledger with Postgres: row counts, per-terminal counts and
 * sums, per-status totals, and that no idempotency key appears twice. Exits 1 on any mismatch.
 */
pg.types.setTypeParser(20, (v) => Number(v));
pg.types.setTypeParser(1700, (v) => Number(v));

const { fleet: base, get, num } = cliOptions();
const txns = num('txns', 'RECONCILE_TXNS', 100_000);
const prefix = get('id-prefix', 'SIM_ID_PREFIX', `RC${Date.now().toString(36).slice(-5)}-`);
const databaseUrl = get('database', 'DATABASE_URL', 'postgres://terminalpulse:terminalpulse@localhost:5432/terminalpulse');
const drainTimeoutS = num('drain-timeout', 'RECONCILE_DRAIN_TIMEOUT_S', 180);

const opts = {
  ...base,
  terminals: num('terminals', 'SIM_TERMINALS', 1000),
  rate: num('rate', 'SIM_RATE', 5000),
  dupRate: num('dup-rate', 'SIM_DUP_RATE', 0.1),
  maxTxns: txns,
  heartbeats: get('heartbeats', 'SIM_HEARTBEATS', 'false') === 'true',
  idPrefix: prefix,
  faultDrift: false,
};
opts.connections = num('connections', 'SIM_CONNECTIONS', Math.min(opts.terminals, 200));

console.log(`[reconcile] ${txns} txns from ${opts.terminals} terminals (prefix ${prefix}), dupRate=${opts.dupRate}, rate=${opts.rate}/s`);
const fleet = new Fleet(opts);
const started = Date.now();
await fleet.start();
const progress = setInterval(() => {
  console.log(`[reconcile] published ${fleet.ledger.uniqueTxns}/${txns} (+${fleet.ledger.redeliveries} redeliveries)`);
}, 5000);
await fleet.done;
clearInterval(progress);
await fleet.stop();
console.log(`[reconcile] publishing done in ${((Date.now() - started) / 1000).toFixed(1)}s; ${fleet.ledger.redeliveries} redeliveries sent`);

const db = new pg.Client({ connectionString: databaseUrl });
await db.connect();
const like = `${prefix}%`;

// Wait until the row count stops changing (or reaches the expected total).
let count = -1;
let stableFor = 0;
const deadline = Date.now() + drainTimeoutS * 1000;
while (Date.now() < deadline) {
  const c = (await db.query('SELECT count(*)::int AS n FROM transactions WHERE terminal_id LIKE $1', [like])).rows[0].n;
  if (c === fleet.ledger.uniqueTxns) {
    count = c;
    break;
  }
  stableFor = c === count ? stableFor + 1 : 0;
  count = c;
  // Stream-level redeliveries come back after CLAIM_IDLE_MS, so allow a quiet period before giving up.
  if (stableFor >= 40) break;
  await sleep(1000);
}

const failures: string[] = [];
const dupKeys = (
  await db.query(
    `SELECT idempotency_key, count(*) FROM transactions WHERE terminal_id LIKE $1 GROUP BY 1 HAVING count(*) > 1 LIMIT 10`,
    [like],
  )
).rows;
if (dupKeys.length) failures.push(`duplicate idempotency keys in DB: ${dupKeys.map((r) => r.idempotency_key).join(', ')}`);

if (count !== fleet.ledger.uniqueTxns) failures.push(`row count ${count} != ledger ${fleet.ledger.uniqueTxns}`);

const perTerminal = (
  await db.query(
    `SELECT terminal_id, count(*)::int AS n, sum(amount_minor) AS amount FROM transactions WHERE terminal_id LIKE $1 GROUP BY 1`,
    [like],
  )
).rows;
const dbTerminals = new Map(perTerminal.map((r) => [r.terminal_id, r]));
let terminalMismatches = 0;
for (const [id, l] of Object.entries(fleet.ledger.perTerminal)) {
  const r = dbTerminals.get(id);
  if (!r || r.n !== l.count || r.amount !== l.amountMinor) {
    if (terminalMismatches++ < 5) failures.push(`terminal ${id}: ledger ${l.count}/${l.amountMinor} vs db ${r?.n ?? 0}/${r?.amount ?? 0}`);
  }
}
if (terminalMismatches > 5) failures.push(`...and ${terminalMismatches - 5} more terminal mismatches`);

const byStatus = (
  await db.query(
    `SELECT status, count(*)::int AS n, sum(amount_minor) AS amount FROM transactions WHERE terminal_id LIKE $1 GROUP BY 1`,
    [like],
  )
).rows;
for (const r of byStatus) {
  const l = fleet.ledger.amountByStatus[r.status];
  if (!l || l.count !== r.n || l.amountMinor !== r.amount) failures.push(`status ${r.status}: ledger ${JSON.stringify(l)} vs db ${r.n}/${r.amount}`);
}
await db.end();

const summary = {
  uniqueTransactionsPublished: fleet.ledger.uniqueTxns,
  redeliveriesPublished: fleet.ledger.redeliveries,
  redeliveryRate: Math.round((fleet.ledger.redeliveries / Math.max(1, fleet.ledger.uniqueTxns)) * 1000) / 1000,
  rowsInPostgres: count,
  duplicateRows: dupKeys.length,
  terminalsReconciled: Object.keys(fleet.ledger.perTerminal).length - terminalMismatches,
  result: failures.length ? 'FAIL' : 'PASS',
};
console.log(JSON.stringify(summary, null, 2));
if (failures.length) {
  for (const f of failures) console.error(`[reconcile] ${f}`);
  process.exit(1);
}
process.exit(0);
