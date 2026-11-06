import asyncio
import json
import math
import os
import random
import socket
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel
from redis.asyncio import Redis

from . import config, metrics

WORKER_ID = os.getenv("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
BASE_SERVICE_TIME = 1 / config.SIM_WORKER_CAPACITY

r = Redis.from_url(config.REDIS_URL, decode_responses=True)
state = {"active": 0, "processed": 0, "slots": 1, "started": time.time()}


def worker_url() -> str:
    if config.WORKER_URL:
        return config.WORKER_URL
    ip = socket.gethostbyname(socket.gethostname())
    return f"http://{ip}:{config.WORKER_PORT}"


async def simulate_work(complexity: float) -> dict:
    duration = BASE_SERVICE_TIME * complexity * random.uniform(0.6, 1.4)
    state["active"] += 1
    try:
        await asyncio.sleep(duration)
    finally:
        state["active"] -= 1
    state["processed"] += 1
    return {"worker": WORKER_ID, "service_time": round(duration, 4)}


async def heartbeat() -> None:
    url = worker_url()
    while True:
        info = {
            "id": WORKER_ID,
            "url": url,
            "active": state["active"],
            "processed": state["processed"],
            "slots": state["slots"],
            "seen": time.time(),
        }
        await r.hset(config.WORKERS_KEY, WORKER_ID, json.dumps(info))
        await asyncio.sleep(1)


async def desired_slots() -> int:
    sim = int(await r.get(config.SIM_WORKERS_KEY) or config.MIN_SIM_WORKERS)
    now = time.time()
    live = [json.loads(v) for v in (await r.hvals(config.WORKERS_KEY))]
    real = max(1, sum(1 for w in live if now - w["seen"] < 5))
    return max(1, math.ceil(sim / real))


async def consume(slot: int) -> None:
    while True:
        if slot >= state["slots"]:
            await asyncio.sleep(0.5)
            continue
        item = await r.brpop(config.QUEUE_KEY, timeout=1)
        if not item:
            continue
        task = json.loads(item[1])
        key = f"task:{task['id']}"
        try:
            await r.hset(key, mapping={"status": "running", "worker": WORKER_ID})
            result = await simulate_work(float(task.get("complexity", 1)))
            latency = time.time() - task["enqueued_at"]
            await r.hset(key, mapping={
                "status": "done",
                "result": json.dumps(result),
                "latency": round(latency, 4),
                "finished_at": time.time(),
            })
            await metrics.record(r, "task", latency)
        except Exception as exc:
            await r.hset(key, mapping={"status": "failed", "error": str(exc)})
            await metrics.record(r, "task_error")
        finally:
            await r.expire(key, 600)


async def scale_slots(tasks: list[asyncio.Task]) -> None:
    while True:
        try:
            state["slots"] = await desired_slots()
            while len(tasks) < state["slots"]:
                tasks.append(asyncio.create_task(consume(len(tasks))))
        except Exception:
            pass
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    consumers: list[asyncio.Task] = []
    background = [asyncio.create_task(heartbeat()), asyncio.create_task(scale_slots(consumers))]
    yield
    for t in background + consumers:
        t.cancel()
    await r.hdel(config.WORKERS_KEY, WORKER_ID)
    await r.aclose()


app = FastAPI(title="AutoScale AI Worker", lifespan=lifespan)


class WorkRequest(BaseModel):
    complexity: float = 1.0


@app.get("/health")
async def health():
    await r.ping()
    return {"status": "ok", "worker": WORKER_ID, "active": state["active"], "slots": state["slots"]}


@app.post("/work")
async def work(req: WorkRequest):
    return await simulate_work(max(0.1, min(req.complexity, 10)))
