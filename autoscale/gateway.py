import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from . import config, loadgen, metrics
from .balancer import STRATEGIES, LoadBalancer

STATIC = Path(__file__).parent / "static"
SELF_URL = os.getenv("GATEWAY_SELF_URL", "http://127.0.0.1:8000")
UNMETERED = {"/api/metrics", "/api/workers", "/api/events", "/api/strategy", "/api/demo/spike"}

r = Redis.from_url(config.REDIS_URL, decode_responses=True)
balancer = LoadBalancer(r)
http: httpx.AsyncClient | None = None
spike_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=10)
    try:
        saved = await r.get(config.STRATEGY_KEY)
        if saved in STRATEGIES:
            balancer.strategy = saved
    except Exception:
        pass
    yield
    await http.aclose()
    await r.aclose()


app = FastAPI(title="AutoScale AI Gateway", lifespan=lifespan)


async def rate_limited(client: str) -> bool:
    key = f"rl:{client}:{int(time.time() // 60)}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, 60)
    count, _ = await pipe.execute()
    return count > config.RATE_LIMIT_PER_MIN


@app.middleware("http")
async def meter(request: Request, call_next):
    path = request.url.path
    if not path.startswith("/api/") or path in UNMETERED:
        return await call_next(request)

    client = request.headers.get("x-client-id") or (request.client.host if request.client else "anon")
    try:
        if await rate_limited(client):
            await metrics.record(r, "limited")
            return JSONResponse({"detail": "Rate limit exceeded"}, status_code=429,
                                headers={"Retry-After": "60"})
    except Exception:
        return JSONResponse({"detail": "Redis unavailable"}, status_code=503)

    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        response = JSONResponse({"detail": "Internal error"}, status_code=500)
    elapsed = time.perf_counter() - start
    await metrics.record(r, "req", elapsed)
    if response.status_code >= 500:
        await metrics.record(r, "err")
    response.headers["X-Response-Time"] = f"{elapsed * 1000:.1f}ms"
    return response


class TaskIn(BaseModel):
    complexity: float = Field(1.0, ge=0.1, le=10)


class StrategyIn(BaseModel):
    strategy: str


@app.get("/", include_in_schema=False)
async def dashboard():
    return FileResponse(STATIC / "index.html")


@app.get("/health")
async def health():
    try:
        await r.ping()
    except Exception:
        raise HTTPException(503, "Redis unavailable")
    workers = await balancer.workers()
    return {"status": "ok" if workers else "degraded", "redis": "ok", "workers": len(workers)}


@app.post("/api/tasks", status_code=202)
async def submit_task(task: TaskIn):
    task_id = uuid.uuid4().hex[:12]
    payload = {"id": task_id, "complexity": task.complexity, "enqueued_at": time.time()}
    pipe = r.pipeline()
    pipe.hset(f"task:{task_id}", mapping={"status": "queued", "complexity": task.complexity})
    pipe.expire(f"task:{task_id}", 600)
    pipe.lpush(config.QUEUE_KEY, json.dumps(payload))
    await pipe.execute()
    return {"id": task_id, "status": "queued"}


@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str):
    data = await r.hgetall(f"task:{task_id}")
    if not data:
        raise HTTPException(404, "Task not found")
    if "result" in data:
        data["result"] = json.loads(data["result"])
    return {"id": task_id, **data}


@app.post("/api/compute")
async def compute(task: TaskIn):
    tried: set[str] = set()
    for _ in range(2):
        worker = await balancer.pick(exclude=tried)
        if not worker:
            break
        wid = worker["id"]
        tried.add(wid)
        balancer.in_flight[wid] += 1
        try:
            resp = await http.post(f"{worker['url']}/work", json={"complexity": task.complexity})
            resp.raise_for_status()
            balancer.served[wid] += 1
            return {"strategy": balancer.strategy, **resp.json()}
        except httpx.HTTPError:
            continue
        finally:
            balancer.in_flight[wid] -= 1
    raise HTTPException(503, "No healthy workers available")


@app.get("/api/metrics")
async def get_metrics():
    rps, api_latency = await metrics.window(r, "req")
    throughput, task_latency = await metrics.window(r, "task")
    limited, _ = await metrics.window(r, "limited")
    errors, _ = await metrics.window(r, "err")
    queue = await r.llen(config.QUEUE_KEY)
    sim = int(await r.get(config.SIM_WORKERS_KEY) or config.MIN_SIM_WORKERS)
    workers = await balancer.workers()
    return {
        "timestamp": time.time(),
        "requests_per_sec": round(rps, 2),
        "api_latency_ms": round(api_latency * 1000, 1),
        "task_throughput": round(throughput, 2),
        "task_latency_ms": round(task_latency * 1000, 1),
        "queue_depth": queue,
        "rate_limited_per_sec": round(limited, 2),
        "errors_per_sec": round(errors, 2),
        "worker_instances": len(workers),
        "simulated_workers": sim,
        "strategy": balancer.strategy,
        "demo_spike_running": bool(spike_task and not spike_task.done()),
    }


@app.get("/api/workers")
async def list_workers():
    return [
        {**w, "gateway_in_flight": balancer.in_flight[w["id"]], "gateway_served": balancer.served[w["id"]]}
        for w in await balancer.workers()
    ]


@app.get("/api/events")
async def scaling_events(limit: int = 30):
    return [json.loads(e) for e in await r.lrange(config.EVENTS_KEY, 0, limit - 1)]


@app.get("/api/strategy")
async def get_strategy():
    return {"strategy": balancer.strategy, "available": STRATEGIES}


@app.put("/api/strategy")
async def set_strategy(body: StrategyIn):
    if body.strategy not in STRATEGIES:
        raise HTTPException(400, f"Strategy must be one of {STRATEGIES}")
    balancer.strategy = body.strategy
    await r.set(config.STRATEGY_KEY, body.strategy)
    return {"strategy": body.strategy}


@app.post("/api/demo/spike", status_code=202)
async def demo_spike(rate: float = 80, seconds: float = 20):
    global spike_task
    if spike_task and not spike_task.done():
        raise HTTPException(409, "A demo spike is already running")
    rate, seconds = min(rate, 200), min(seconds, 60)
    scenario = [(rate * 0.4, 4), (rate, seconds), (rate * 0.2, 6)]
    spike_task = asyncio.create_task(loadgen.run(SELF_URL, scenario, verbose=False))
    return {"status": "started", "demo": True, "scenario": scenario}
