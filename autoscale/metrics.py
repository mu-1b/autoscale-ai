import time

from redis.asyncio import Redis

TTL = 300


def _sec() -> int:
    return int(time.time())


async def record(r: Redis, name: str, value: float | None = None) -> None:
    sec = _sec()
    pipe = r.pipeline()
    pipe.incr(f"m:{name}:count:{sec}")
    pipe.expire(f"m:{name}:count:{sec}", TTL)
    if value is not None:
        pipe.incrbyfloat(f"m:{name}:sum:{sec}", value)
        pipe.expire(f"m:{name}:sum:{sec}", TTL)
    await pipe.execute()


async def window(r: Redis, name: str, seconds: int = 5) -> tuple[float, float]:
    """Return (rate per second, average value) over the last full `seconds`."""
    now = _sec()
    secs = range(now - seconds, now)
    keys = [f"m:{name}:count:{s}" for s in secs] + [f"m:{name}:sum:{s}" for s in secs]
    vals = await r.mget(keys)
    counts = sum(int(v) for v in vals[:seconds] if v)
    total = sum(float(v) for v in vals[seconds:] if v)
    return counts / seconds, (total / counts if counts else 0.0)
