import argparse
import asyncio
import random
import time

import httpx

DEFAULT_SCENARIO = [(5, 15), (20, 15), (90, 25), (30, 15), (5, 20)]


async def fire(client: httpx.AsyncClient, stats: dict) -> None:
    headers = {"X-Client-Id": f"loadgen-{random.randint(0, 9)}"}
    complexity = round(random.uniform(0.5, 1.5), 2)
    try:
        if random.random() < 0.8:
            resp = await client.post("/api/tasks", json={"complexity": complexity}, headers=headers)
        else:
            resp = await client.post("/api/compute", json={"complexity": complexity}, headers=headers)
        stats[resp.status_code] = stats.get(resp.status_code, 0) + 1
    except httpx.HTTPError:
        stats["error"] = stats.get("error", 0) + 1


async def phase(client: httpx.AsyncClient, rate: float, seconds: float, stats: dict) -> None:
    pending = set()
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        task = asyncio.create_task(fire(client, stats))
        pending.add(task)
        task.add_done_callback(pending.discard)
        await asyncio.sleep(random.expovariate(rate))
    if pending:
        await asyncio.wait(pending, timeout=10)


async def run(base_url: str, scenario: list[tuple[float, float]], verbose: bool = True) -> dict:
    stats: dict = {}
    limits = httpx.Limits(max_connections=200)
    async with httpx.AsyncClient(base_url=base_url, timeout=15, limits=limits) as client:
        for rate, seconds in scenario:
            if verbose:
                print(f"[loadgen] {rate:>5.0f} req/s for {seconds:.0f}s", flush=True)
            await phase(client, rate, seconds, stats)
            if verbose:
                print(f"[loadgen] responses so far: {stats}", flush=True)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="AutoScale AI traffic generator")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--rate", type=float, help="constant req/s instead of the spike scenario")
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--loop", action="store_true", help="repeat the scenario forever")
    args = parser.parse_args()

    scenario = [(args.rate, args.duration)] if args.rate else DEFAULT_SCENARIO
    while True:
        asyncio.run(run(args.url, scenario))
        if not args.loop:
            break


if __name__ == "__main__":
    main()
