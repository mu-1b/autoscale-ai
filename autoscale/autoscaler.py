"""Autoscaling simulation.

Watches real gateway/worker metrics in Redis and adjusts a *simulated* worker
count. Workers read that count to size their consumer pools, so scaling
decisions visibly change queue drain rate without starting new containers.
"""
import asyncio
import json
import logging
import math
import time

from redis.asyncio import Redis

from . import config, metrics

log = logging.getLogger("autoscaler")

TARGET_UTILIZATION = 0.75
DRAIN_SECONDS = 10
LATENCY_SLO = 2.0
SCALE_UP_COOLDOWN = 4
SCALE_DOWN_COOLDOWN = 12
HEARTBEAT_KEY = "autoscaler:heartbeat"


def desired_workers(current: int, arrival: float, queue: int, latency: float) -> tuple[int, str]:
    demand = arrival + queue / DRAIN_SECONDS
    target = math.ceil(demand / (config.SIM_WORKER_CAPACITY * TARGET_UTILIZATION))
    reason = f"arrival {arrival:.1f}/s, queue {queue}, latency {latency * 1000:.0f}ms"
    if latency > LATENCY_SLO and queue > 0:
        target = max(target, current + 1)
        reason += " (latency above SLO)"
    return max(config.MIN_SIM_WORKERS, min(config.MAX_SIM_WORKERS, target)), reason


async def record_event(r: Redis, before: int, after: int, reason: str, snapshot: dict) -> None:
    event = {
        "timestamp": time.time(),
        "from": before,
        "to": after,
        "direction": "up" if after > before else "down",
        "reason": reason,
        "metrics": snapshot,
        "simulated": True,
    }
    await r.lpush(config.EVENTS_KEY, json.dumps(event))
    await r.ltrim(config.EVENTS_KEY, 0, 99)
    log.info("scale %s: %d -> %d (%s)", event["direction"], before, after, reason)


async def run() -> None:
    r = Redis.from_url(config.REDIS_URL, decode_responses=True)
    current = int(await r.get(config.SIM_WORKERS_KEY) or config.MIN_SIM_WORKERS)
    await r.set(config.SIM_WORKERS_KEY, current)
    last_change = 0.0
    log.info("autoscaler started with %d simulated workers", current)

    while True:
        try:
            arrival, _ = await metrics.window(r, "enqueue")
            throughput, latency = await metrics.window(r, "task")
            queue = await r.llen(config.QUEUE_KEY)
            target, reason = desired_workers(current, arrival, queue, latency)
            since = time.time() - last_change
            snapshot = {"arrival": round(arrival, 2), "throughput": round(throughput, 2),
                        "queue": queue, "latency_ms": round(latency * 1000, 1)}

            new = current
            if target > current and since >= SCALE_UP_COOLDOWN:
                new = target
            elif target < current and since >= SCALE_DOWN_COOLDOWN and queue < 5:
                new = current - max(1, (current - target) // 2)

            if new != current:
                await record_event(r, current, new, reason, snapshot)
                current, last_change = new, time.time()
                await r.set(config.SIM_WORKERS_KEY, current)
            await r.set(HEARTBEAT_KEY, time.time(), ex=30)
        except Exception as exc:
            log.warning("autoscaler tick failed: %s", exc)
        await asyncio.sleep(config.AUTOSCALE_INTERVAL)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(run())
