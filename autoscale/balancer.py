import itertools
import json
import time
from collections import defaultdict

from redis.asyncio import Redis

from . import config

STRATEGIES = ("round_robin", "least_connections")


class LoadBalancer:
    def __init__(self, redis: Redis, strategy: str = config.BALANCER_STRATEGY):
        self.redis = redis
        self.strategy = strategy if strategy in STRATEGIES else "round_robin"
        self.in_flight: dict[str, int] = defaultdict(int)
        self.served: dict[str, int] = defaultdict(int)
        self._workers: list[dict] = []
        self._fetched = 0.0
        self._counter = itertools.count()

    async def workers(self) -> list[dict]:
        if time.time() - self._fetched > 1:
            now = time.time()
            raw = await self.redis.hvals(config.WORKERS_KEY)
            live = [w for w in map(json.loads, raw) if now - w["seen"] < 5]
            self._workers = sorted(live, key=lambda w: w["id"])
            self._fetched = now
        return self._workers

    async def pick(self, exclude: set[str] = frozenset()) -> dict | None:
        candidates = [w for w in await self.workers() if w["id"] not in exclude]
        if not candidates:
            return None
        if self.strategy == "least_connections":
            return min(candidates, key=lambda w: (self.in_flight[w["id"]], self.served[w["id"]]))
        return candidates[next(self._counter) % len(candidates)]
