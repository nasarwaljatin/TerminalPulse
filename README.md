# TerminalPulse: IoT Payment Terminal Fleet Platform

TerminalPulse ingests transactions and heartbeats from a fleet of POS terminals over MQTT, persists them exactly once, streams fleet health to a live dashboard, flags faulty terminals with an Isolation Forest, and gives Claude six fleet-triage tools over the Model Context Protocol.

```mermaid
flowchart LR
  subgraph Terminals["1,000 simulated POS terminals"]
    T1[terminal] & T2[terminal] & T3[terminal]
  end
  T1 & T2 & T3 -- "MQTT QoS 1<br/>txn / hb / ack" --> B[(MQTT broker<br/>Mosquitto · Aedes)]
  B -- "$share/tp-gateway" --> G1[gateway pod]
  B -- "$share/tp-gateway" --> G2[gateway pod]
  G1 & G2 -- "XADD, then PUBACK" --> R[(Redis Stream<br/>tp:ingest)]
  R -- "XREADGROUP batches" --> P1[persist worker] & P2[persist worker]
  P1 & P2 -- "UNNEST batch insert<br/>ON CONFLICT DO NOTHING" --> PG[(PostgreSQL)]
  P1 & P2 -- "pub/sub tp:events" --> API[API + WebSocket hub]
  API -- "WebSocket ticks / 250 ms" --> D[React dashboard]
  API -- "features" --> ML[FastAPI + scikit-learn<br/>Isolation Forest]
  MCP[MCP server] -- REST --> API
  C[Claude] -- "6 tools" --> MCP
  API -- "cmd (QoS 1)" --> B
```

## What's in the repo

| Path | What it is |
| --- | --- |
| `services/gateway` | TypeScript gateway. One image, roles picked by `ROLES`: **ingest** (MQTT → Redis Streams), **persist** (stream → batched Postgres writes), **api** (REST, WebSocket hub, dashboard hosting), **jobs** (anomaly scoring, retention), **sim** (bundled simulator). Optional embedded MQTT broker for single-container deploys. |
| `services/simulator` | Fleet simulator (1,000+ terminals, fault profiles, remote-command handling), plus `loadtest.js` and the `reconcile.js` exactly-once test. |
| `services/ml` | FastAPI scoring service: scikit-learn Isolation Forest trained on a labeled synthetic fleet, with per-flag explanations. |
| `services/mcp-server` | MCP server exposing six fleet-triage tools to Claude. |
| `dashboard` | React + Vite + Tailwind live dashboard. |
| `shared/fault-profiles.json` | One source of truth for terminal behaviour, read by both the simulator and the ML data generator. |
| `deploy/` | Mosquitto config, Kubernetes manifests (HPA 2→6), EC2/k3s bootstrap. |
| `render.yaml` | One-click free deployment (Render Blueprint). |
| `.github/workflows/ci.yml` | Build, ML recall gate, 100K reconciliation test, load test, GHCR images. |

## How the resume claims map to code

| Claim | Where it's implemented / measured |
| --- | --- |
| 5,000 msg/s from 1,000 terminals, p99 < 200 ms publish-to-persist | `services/gateway/src/ingest.ts`, `persist.ts`; measured by `services/simulator/src/loadtest.ts` (CI step *Publish-to-persist load test*). Latency = device timestamp → row persisted, computed in SQL across all pods. Measured over 60 s at 5,000 txn/s after a 15 s warm-up; CI fails if p99 > 200 ms. **Example CI run: 4,974 txn/s, p50 11.6 ms, p95 16.4 ms, p99 68.7 ms.** |
| MQTT QoS 1 gateway, Redis Streams, batched Postgres writes | `ingest.ts` overrides mqtt.js `handleMessage` so the **PUBACK is sent only after XADD succeeds**. `persist.ts` reads up to 500 entries per `XREADGROUP`, writes them in one transaction with multi-row `UNNEST` inserts, and `XACK`s only after `COMMIT`. |
| Zero duplicate payments across 100K txns with 10% forced redelivery | `UNIQUE (idempotency_key)` + `ON CONFLICT DO NOTHING` (`migrations/001_init.sql`). `reconcile.js` republishes 10% of transactions with the same key, the gateways run with `CHAOS_NOACK_RATE` so un-acked batches come back through `XAUTOCLAIM`, then the ledger is compared with Postgres row by row (count, per-terminal sums, per-status totals, no repeated key). Runs in CI. **Latest CI run: 100,000 transactions + 9,901 redeliveries → 100,000 rows, 0 duplicates.** |
| Terminal status pushed to React in < 1 s over WebSockets | Persist workers publish batch deltas on Redis pub/sub; every API pod folds them into a tick every 250 ms (`ws.ts`). The dashboard header shows measured device → browser latency, with clock offset corrected by a ping/pong probe. |
| Docker gateway pods autoscaled 2 → 6 by Kubernetes HPA under a 3x load test on EC2 | `Dockerfile`, `deploy/k8s/gateway.yaml` (HPA, CPU 60%), `deploy/k8s/loadtest/` (+3,400 txn/s on a 1,700 txn/s baseline), `deploy/ec2/bootstrap.sh` (k3s). |
| 6 fleet-triage tools for Claude over MCP | `services/mcp-server/src/index.ts`: `get_fleet_overview`, `list_flagged_terminals`, `search_terminals`, `get_terminal_health`, `get_terminal_transactions`, `send_terminal_command`. |
| Faulty terminals flagged at 0.9 recall with a scikit-learn Isolation Forest | `services/ml`. Trained unsupervised; threshold calibrated on a labeled validation split; evaluated on a held-out labeled set of 5,000 terminals. Latest run: **recall 0.90, precision 0.83, false-positive rate 1.5%**. `pytest` gates recall in CI. |

## Run it locally

### Option A: Docker Compose (whole stack)

```bash
docker compose up --build
```

Open <http://localhost:8080>. Compose starts Postgres, Redis, Mosquitto, **two** gateway workers (shared MQTT subscription + one consumer group), the API/dashboard, the ML service and a 1,000-terminal simulator. The model flags terminals once they have 5 minutes of history.

Run the load test and the reconciliation test inside the Compose network, so Docker Desktop's port forwarding doesn't sit between 1,000 MQTT connections and the broker:

```bash
docker compose run --rm simulator node services/simulator/dist/loadtest.js --mqtt mqtt://mosquitto:1883 --api http://api:8080 --terminals 1000 --rate 5000 --duration 60
```

```bash
docker compose run --rm -e DATABASE_URL=postgres://terminalpulse:terminalpulse@postgres:5432/terminalpulse simulator node services/simulator/dist/reconcile.js --mqtt mqtt://mosquitto:1883 --txns 100000 --dup-rate 0.1 --terminals 1000 --rate 5000
```

### Option B: Node directly (any Postgres + Redis 6.2+)

```bash
npm ci && npm run build
```

```bash
MQTT_EMBEDDED=true ROLES=ingest,persist,api,jobs,sim SIM_TERMINALS=200 ML_URL=http://localhost:8000 STATIC_DIR=dashboard/dist node services/gateway/dist/index.js
```

ML service (separate terminal):

```bash
cd services/ml && python -m venv .venv && .venv/bin/pip install -r requirements.txt && .venv/bin/python train.py && .venv/bin/uvicorn app.main:app --port 8000
```

For dashboard hot reload, run `npm run dev -w dashboard` (it proxies `/api` and `/ws` to `:8080`).

## Deploy for free

Nothing needs to be installed on your computer: Render builds the Docker images in the cloud, and GitHub Actions runs the tests in the cloud.

| Piece | Free service | What runs there |
| --- | --- | --- |
| Gateway + MQTT broker + simulator + dashboard | Render web service (free) | the root `Dockerfile`, `ROLES=ingest,persist,api,jobs,sim`, embedded Aedes broker |
| Anomaly model | Render web service (free) | `services/ml/Dockerfile` |
| Redis Streams + pub/sub | Render Key Value (free, 25 MB) | |
| PostgreSQL | Neon (free, 0.5 GB) | |
| Tests: 100K reconciliation, 5,000 msg/s load test, ML recall | GitHub Actions (free for public repos) | `.github/workflows/ci.yml` with real Postgres, Redis and Mosquitto |

**1. Put the code on GitHub.** Create an empty public repository named `terminalpulse` on github.com (no README), then from this folder:

```bash
git remote add origin https://github.com/<your-username>/terminalpulse.git
```

```bash
git push -u origin main
```

The push starts the CI workflow. Open the repo's **Actions** tab: the *Reconciliation* job prints the 100K-transaction ledger check and the 5,000 msg/s load-test percentiles.

**2. Create the database (Neon).** Sign up at [neon.tech](https://neon.tech) → *New project* (pick region **AWS US West (Oregon)**, next to Render's default region) → *Connect* → copy the connection string. It looks like `postgresql://user:pass@ep-xxx.us-west-2.aws.neon.tech/neondb?sslmode=require`.
   *Using Supabase instead?* Copy the **Session pooler** URI and change the end to `?sslmode=no-verify`, because the pooler's certificate fails strict verification.

**3. Deploy on Render.** Sign up at [render.com](https://render.com) with your GitHub account → **New → Blueprint** → pick the `terminalpulse` repo. Render reads `render.yaml` and proposes three resources: `terminalpulse`, `terminalpulse-ml` and `terminalpulse-redis`, all on the Free plan. It asks for the two values marked `sync: false`:
   - `DATABASE_URL`: paste the Neon string from step 2.
   - `ML_URL`: enter `https://terminalpulse-ml.onrender.com` (the ML service's URL; if Render gives it a different name, fix this later under *terminalpulse → Environment*).

   Click **Apply**. The first build takes about 5–10 minutes.

**4. Open it.** Go to `https://terminalpulse.onrender.com`. On boot the database tables are created, 150 simulated terminals come online, and the dashboard starts updating live. Anomaly flags appear after about 5 minutes of history.

**5. Send remote commands.** Click a terminal → *reboot / ping / clear queue*. The dashboard asks for the admin key: copy `ADMIN_API_KEY` from *terminalpulse → Environment* in Render.

**6. Connect Claude (optional).** See [Use it from Claude](#use-it-from-claude-mcp) and set `TERMINALPULSE_API_URL=https://terminalpulse.onrender.com`.

Render redeploys automatically on every push to `main`.

Free-tier notes:
- Free web services sleep after 15 minutes without traffic and take about a minute to wake. The simulator lives inside the service, so the fleet "reboots" when it wakes. Sleeping is what keeps both services within Render's 750 free instance-hours per month, so don't add an uptime pinger.
- If the dashboard shows "API unreachable" right after opening it, the service is waking up; it reconnects on its own.
- The Render Blueprint caps the Redis stream at 10,000 entries (the free Key Value instance has 25 MB) and keeps 6 hours of transactions, which keeps a 0.5 GB Postgres under quota.
- Alternative ML host: the ML image runs unchanged as a free Docker Space on Hugging Face (port 8000 → set `app_port: 8000`).
- To point a local simulator at the cloud broker, use: `MQTT_URL=wss://terminalpulse.onrender.com/mqtt MQTT_USERNAME=terminal MQTT_PASSWORD=<from Render> npm run sim -- --terminals 50`

### Optional: Kubernetes on AWS EC2 (HPA 2 → 6)

Not needed for the free deploy above. This reproduces the autoscaling demo; it runs on AWS Free Plan credits, not free forever, so stop the instance when you're done. On a free-tier-eligible instance (Ubuntu 24.04, ≥ 2 vCPU / 4 GB, e.g. `m7i-flex.large`; open TCP 30080):

```bash
GH_OWNER=<your-github-user> bash deploy/ec2/bootstrap.sh
```

The script installs k3s (with metrics-server for the HPA) and applies `deploy/k8s` using the images CI pushes to GHCR. Make the packages public, or add a pull secret. Then:

```bash
kubectl apply -k deploy/k8s/loadtest
```

```bash
kubectl -n terminalpulse get hpa gateway -w
```

The load test triples traffic (1,700 → ~5,100 txn/s) and the gateway Deployment scales from 2 toward 6 pods. Stop the instance afterwards so it doesn't use up free-plan credits.

## Use it from Claude (MCP)

Build once (`npm run build -w services/mcp-server`), then add the server to Claude Code:

```bash
claude mcp add terminalpulse -e TERMINALPULSE_API_URL=https://terminalpulse.onrender.com -e TERMINALPULSE_API_KEY=<admin key> -- node /absolute/path/to/services/mcp-server/dist/index.js
```

…or to Claude Desktop's `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "terminalpulse": {
      "command": "node",
      "args": ["/absolute/path/to/services/mcp-server/dist/index.js"],
      "env": { "TERMINALPULSE_API_URL": "http://localhost:8080", "TERMINALPULSE_API_KEY": "" }
    }
  }
}
```

Try: *"Which terminals need attention right now? Reboot the ones where a reboot is likely to help and tell me which need a technician."* Claude calls `list_flagged_terminals` and `get_terminal_health`, then `send_terminal_command`. The simulator clears overheating and network faults on reboot about 75% of the time; card-reader faults persist.

## Design notes

- **At-least-once in, exactly-once stored.** MQTT QoS 1 and Redis consumer groups both redeliver. Instead of trying to prevent that, every path converges on `INSERT … ON CONFLICT (idempotency_key) DO NOTHING`, and the batch's `RETURNING` tells us how many were duplicates (shown on the dashboard).
- **Ack ordering.** Broker → gateway: the PUBACK goes out after `XADD`. Stream → Postgres: `XACK` comes after `COMMIT`. A crash at any point leaves the message un-acked upstream, never lost.
- **Throughput.** mqtt.js handles one QoS 1 message per connection at a time, so each pod opens several connections in one shared-subscription group. Persist workers amortize Postgres round-trips with 500-row `UNNEST` inserts. Rows are sorted by key so concurrent batches lock in the same order and can't deadlock.
- **Dedupe window = retention.** Idempotency keys live as long as their rows (`RETENTION_HOURS`). A redelivery older than that would be stored again; in production you would keep a longer-lived key table.
- **One source of truth for behaviour.** `shared/fault-profiles.json` drives the simulator and the ML training-data generator alike, so live features and training features share a distribution. The model trains on windows of 5–10 minutes, matching what the live scorer sees.

## Tests

| Command | What it checks |
| --- | --- |
| `cd services/ml && pytest` | Isolation Forest recall ≥ 0.88 on a held-out labeled set, deterministic generator, `/score` flags an overheating terminal with a temperature reason. |
| `node services/simulator/dist/reconcile.js …` | Ledger vs Postgres after 10% redelivery (+ stream-level chaos in CI). Exits non-zero on any mismatch. |
| `node services/simulator/dist/loadtest.js …` | Throughput and publish-to-persist p50/p95/p99 from persisted rows; `--max-p99 200` turns it into a gate. |
