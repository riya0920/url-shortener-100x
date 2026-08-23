"""Load generator: zipf-distributed resolves plus a steady create rate.

    python -m shortener.loadtest --url http://localhost:8000 --duration 30 --concurrency 64

Design decisions that make the numbers mean something:

* **Zipf traffic, not uniform.** Uniform key selection defeats the cache by
  construction and measures a workload nobody has. Real short-link traffic is
  extremely skewed, and the cache hit ratio -- the number that decides database
  load at scale -- only becomes meaningful under skew.
* **Latency measured client-side, per request**, including connection reuse but
  not connection setup, because that is what a user experiences.
* **Coordinated-omission awareness.** This is a closed-loop generator: each
  worker waits for its response before issuing the next request. Under
  saturation that UNDERSTATES tail latency, because a slow response also slows
  the offered load. The report says so rather than pretending otherwise, and the
  open-loop variant is noted in the roadmap.
* **Warmup excluded.** The first seconds pay cache-fill and JIT costs.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import time

import numpy as np

try:
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None


def zipf_keys(codes: list, n: int, a: float = 1.2, seed: int = 0) -> list:
    """Sample codes with a zipf popularity distribution."""
    rng = np.random.default_rng(seed)
    ranks = np.arange(1, len(codes) + 1)
    weights = 1.0 / np.power(ranks, a)
    weights /= weights.sum()
    idx = rng.choice(len(codes), size=n, p=weights)
    return [codes[i] for i in idx]


async def seed_links(session, base: str, count: int, api_key: str) -> list:
    """Create the corpus the resolve load will read."""
    codes = []
    for i in range(count):
        async with session.post(
            f"{base}/links",
            json={"target": f"https://example.com/page/{i}"},
            headers={"x-api-key": api_key},
        ) as r:
            if r.status == 201:
                codes.append((await r.json())["code"])
            elif r.status == 429:
                await asyncio.sleep(0.05)
    return codes


class Results:
    def __init__(self):
        self.latencies = []
        self.status_counts = {}
        self.errors = 0

    def record(self, ms: float, status: int):
        self.latencies.append(ms)
        self.status_counts[status] = self.status_counts.get(status, 0) + 1

    def summary(self, wall_s: float, warmup_s: float = 0.0) -> dict:
        lat = np.array(self.latencies) if self.latencies else np.array([0.0])
        total = int(len(self.latencies))
        return {
            "requests": total,
            "rps": total / wall_s if wall_s else 0.0,
            "p50_ms": float(np.percentile(lat, 50)),
            "p95_ms": float(np.percentile(lat, 95)),
            "p99_ms": float(np.percentile(lat, 99)),
            "max_ms": float(lat.max()),
            "status_counts": self.status_counts,
            "errors": self.errors,
            "non_2xx_3xx": sum(v for k, v in self.status_counts.items() if k >= 400),
        }


async def resolve_worker(session, base: str, keys: list, stop_at: float, results: Results,
                         api_key: str, start_after: float):
    i = 0
    n = len(keys)
    while time.perf_counter() < stop_at:
        code = keys[i % n]
        i += 1
        t0 = time.perf_counter()
        try:
            async with session.get(f"{base}/{code}", headers={"x-api-key": api_key},
                                   allow_redirects=False) as r:
                await r.read()
                ms = (time.perf_counter() - t0) * 1000.0
                if time.perf_counter() >= start_after:
                    results.record(ms, r.status)
        except Exception:
            results.errors += 1


async def create_worker(session, base: str, stop_at: float, rate_per_s: float, results: Results,
                        api_key: str, start_after: float):
    """Steady, low-rate creates running alongside the read load.

    `rate_per_s <= 0` means "no writes", and it has to return rather than pick a
    very long interval. The clamp used to be `max(rate, 0.001)`, which turned
    --create-rps 0 into one request every 1000 seconds -- and since the run ends
    with `gather`, the whole load test then blocked for those 1000 seconds. A
    read-only run looked like a hang.
    """
    if rate_per_s <= 0:
        return
    interval = 1.0 / rate_per_s
    i = 0
    while time.perf_counter() < stop_at:
        t0 = time.perf_counter()
        try:
            async with session.post(f"{base}/links", json={"target": f"https://example.com/new/{i}"},
                                    headers={"x-api-key": api_key}) as r:
                await r.read()
                ms = (time.perf_counter() - t0) * 1000.0
                if time.perf_counter() >= start_after:
                    results.record(ms, r.status)
        except Exception:
            results.errors += 1
        i += 1
        await asyncio.sleep(max(interval - (time.perf_counter() - t0), 0))


async def run(base: str, duration: float, concurrency: int, n_links: int, create_rps: float,
              warmup: float, api_key: str, zipf_a: float) -> dict:
    if aiohttp is None:
        raise SystemExit("pip install aiohttp")

    connector = aiohttp.TCPConnector(limit=concurrency + 8)
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        codes = await seed_links(session, base, n_links, api_key)
        if not codes:
            raise SystemExit("failed to seed links -- is the service up and the limiter budget large enough?")

        # Reset the server's metrics so the reported cache hit ratio covers the
        # measured window only, not the seeding phase.
        try:
            async with session.post(f"{base}/admin/reset-metrics") as r:
                await r.read()
        except Exception:
            pass

        keys = zipf_keys(codes, 20000, a=zipf_a)
        resolve_results, create_results = Results(), Results()

        t_start = time.perf_counter()
        start_after = t_start + warmup
        stop_at = t_start + warmup + duration

        tasks = [
            asyncio.create_task(resolve_worker(session, base, keys[i::concurrency] or keys, stop_at,
                                               resolve_results, api_key, start_after))
            for i in range(concurrency)
        ]
        tasks.append(asyncio.create_task(
            create_worker(session, base, stop_at, create_rps, create_results, api_key, start_after)))
        await asyncio.gather(*tasks)

        metrics = {}
        try:
            async with session.get(f"{base}/metrics") as r:
                metrics = await r.json()
        except Exception:
            pass

    return {
        "hardware": {
            "platform": platform.platform(),
            "processor": platform.processor() or platform.machine(),
            "cpu_count": os.cpu_count(),
            "python": platform.python_version(),
        },
        "config": {"duration_s": duration, "concurrency": concurrency, "links": n_links,
                   "create_rps": create_rps, "warmup_s": warmup, "zipf_a": zipf_a},
        "resolve": resolve_results.summary(duration),
        "create": create_results.summary(duration),
        "server_metrics": metrics,
        "caveat": ("closed-loop generator: each worker waits for its response before issuing the "
                   "next request, which UNDERSTATES tail latency under saturation "
                   "(coordinated omission)"),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--duration", type=float, default=20.0)
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--links", type=int, default=500)
    ap.add_argument("--create-rps", type=float, default=5.0)
    ap.add_argument("--warmup", type=float, default=3.0)
    ap.add_argument("--api-key", default="loadtest")
    ap.add_argument("--zipf-a", type=float, default=1.2)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    result = asyncio.run(run(args.url, args.duration, args.concurrency, args.links,
                             args.create_rps, args.warmup, args.api_key, args.zipf_a))
    print(json.dumps(result, indent=2))
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=2)
        print("\nwrote", args.out)


if __name__ == "__main__":
    main()
