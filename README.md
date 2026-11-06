# AutoScale AI — Distributed Load-Aware API System

A lightweight demo of a load-aware API platform: a FastAPI gateway balances traffic across worker instances, a Redis queue feeds asynchronous workers, and an autoscaling simulation reacts to latency, throughput and queue depth. A live dashboard shows it all as it happens.

**Project year:** 2026 · **Stack:** Python, FastAPI, Redis, Docker

## Features

- **Gateway** with round-robin or least-connections balancing, switchable at runtime
- **Redis task queue** consumed by asynchronous workers (`BRPOP` consumer pools)
- **Rate limiting** per client (`X-Client-Id` header, falls back to IP), fixed one-minute window
- **Autoscaling simulation** that adjusts a simulated worker count from live arrival rate, queue depth and task latency, with cooldowns and a scaling event log
- **Live dashboard** showing requests/sec, latency, queue depth, active workers, per-instance stats and scaling events
- **Load generator** with a built-in spike scenario, plus a one-click "demo spike" button in the dashboard
- Health checks, retries on worker failure, and JSON error responses

### Real vs simulated

| Real (measured live) | Simulated (marked in the UI) |
|---|---|
| Request rate, API latency, task latency, throughput, queue depth, rate-limit hits, worker instances and heartbeats | Worker count and scaling events. The autoscaler doesn't start containers. It sets a target capacity, and each real worker resizes its consumer pool to match, so scaling still changes how fast the queue drains. Task work is a timed sleep, and the load generator / demo spike send synthetic traffic. |

## Architecture

```
            ┌──────────────┐        ┌───────────────────────┐
 clients ──►│   Gateway    │──HTTP─►│ Worker 1..N (/work)    │
 loadgen    │ FastAPI      │        │ + async queue consumers│
 dashboard  │ balancer     │        └──────────▲────────────┘
            │ rate limiter │                   │ BRPOP / heartbeat
            └──────┬───────┘        ┌──────────┴────────────┐
                   └──── LPUSH ────►│        Redis          │◄── Autoscaler
                                    │ queue · metrics · reg │    (simulation)
                                    └───────────────────────┘
```

- `POST /api/compute` is proxied synchronously to a worker picked by the balancer.
- `POST /api/tasks` is queued in Redis and processed in the background by worker consumers.
- Workers register in Redis with a heartbeat. The gateway discovers them from there.
- Metrics are per-second Redis counters, aggregated over a 5-second window.

## Run with Docker

```bash
cp .env.example .env
docker compose up --build             # gateway, redis, 3 workers, autoscaler
docker compose --profile loadgen up   # optional: continuous spike traffic
```

Open the dashboard at http://localhost:8000 and click **Trigger demo spike**. API docs are at http://localhost:8000/docs.

To change the number of real worker instances: `docker compose up --scale worker=5`.

## Run locally (without Docker)

Start Redis on `localhost:6379`, then in separate terminals:

```bash
pip install -r requirements.txt
WORKER_ID=w1 WORKER_URL=http://127.0.0.1:8001 uvicorn autoscale.worker:app --port 8001
WORKER_ID=w2 WORKER_URL=http://127.0.0.1:8002 uvicorn autoscale.worker:app --port 8002
python -m autoscale.autoscaler
uvicorn autoscale.gateway:app --port 8000
python -m autoscale.loadgen --url http://localhost:8000
```

## API

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Redis, worker and autoscaler status |
| POST | `/api/tasks` | Queue a task `{"complexity": 1.0}` → `202` with task id |
| GET | `/api/tasks/{id}` | Task status and result |
| POST | `/api/compute` | Synchronous request routed to a worker |
| GET | `/api/metrics` | Current metrics snapshot |
| GET | `/api/workers` | Live worker instances |
| GET | `/api/events` | Recent scaling events |
| GET/PUT | `/api/strategy` | Read or set `round_robin` / `least_connections` |
| POST | `/api/demo/spike?rate=80&seconds=20` | Generate synthetic spike traffic |

## Configuration

See `.env.example`. Key settings: `RATE_LIMIT_PER_MIN`, `BALANCER_STRATEGY`, `WORKER_REPLICAS`, `MIN_SIM_WORKERS` / `MAX_SIM_WORKERS`, and `SIM_WORKER_CAPACITY` (tasks per second per simulated worker).
