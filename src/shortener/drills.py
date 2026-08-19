"""Scripted failure drills, run against a live server under load.

    python -m shortener.drills stampede --url http://localhost:8000

Drill 1 -- cache stampede: put the service under load on a hot key, then drop
          the entire cache mid-flight. Every concurrent request misses at the
          same instant. The service must not error, and SingleFlight must
          collapse the herd into a small number of backend calls.

Drill 2 -- limiter-store failure: point the limiter at a dead backend while load
          is running. The service must keep serving (fail-open) and the
          fail_open counter must climb, so the outage is visible rather than
          silent.

Claiming a mitigation works is free. These make it observable.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

import numpy as np

try:
    import aiohttp
except ImportError:  # pragma: no cover
    aiohttp = None


async def _hammer(session, base, code, stop_at, out, api_key):
    while time.perf_counter() < stop_at:
        t0 = time.perf_counter()
        try:
            async with session.get(f"{base}/{code}", headers={"x-api-key": api_key},
                                   allow_redirects=False) as r:
                await r.read()
                out.append(((time.perf_counter() - t0) * 1000.0, r.status, time.perf_counter()))
        except Exception:
            out.append((0.0, -1, time.perf_counter()))


async def run_stampede(base: str, concurrency: int, duration: float, api_key: str) -> dict:
    if aiohttp is None:
        raise SystemExit("pip install aiohttp")

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=concurrency + 8),
        timeout=aiohttp.ClientTimeout(total=15),
    ) as session:
        async with session.post(f"{base}/links", json={"target": "https://example.com/viral"},
                                headers={"x-api-key": api_key}) as r:
            code = (await r.json())["code"]

        async with session.post(f"{base}/admin/reset-metrics") as r:
            await r.read()

        samples = []
        t_start = time.perf_counter()
        stop_at = t_start + duration
        tasks = [asyncio.create_task(_hammer(session, base, code, stop_at, samples, api_key))
                 for _ in range(concurrency)]

        # Let the cache warm, then pull it out from under the load.
        await asyncio.sleep(duration / 2)
        t_flush = time.perf_counter()
        async with session.post(f"{base}/admin/flush-cache") as r:
            evicted = (await r.json())["evicted"]

        await asyncio.gather(*tasks)

        async with session.get(f"{base}/metrics") as r:
            metrics = await r.json()

    lat = np.array([s[0] for s in samples]) if samples else np.array([0.0])
    errors = sum(1 for s in samples if s[1] < 0 or s[1] >= 500)
    non_redirect = sum(1 for s in samples if s[1] not in (307, 302, -1) and s[1] < 500)

    def _window(lo, hi):
        # `np.array(xs) or fallback` is a ValueError on a multi-element array --
        # numpy refuses to guess the truth value. Build the fallback explicitly.
        vals = [s[0] for s in samples if lo <= s[2] < hi]
        return np.array(vals) if vals else np.array([0.0])

    # Latency in the second after the flush -- the stampede window.
    after = _window(t_flush, t_flush + 1.0)
    before = _window(t_flush - 1.0, t_flush)

    return {
        "drill": "cache_stampede",
        "concurrency": concurrency,
        "requests": len(samples),
        "cache_entries_evicted": evicted,
        "errors_5xx_or_connection": errors,
        "unexpected_status": non_redirect,
        "p99_ms_before_flush": float(np.percentile(before, 99)),
        "p99_ms_after_flush": float(np.percentile(after, 99)),
        "singleflight_collapsed": metrics.get("singleflight_collapsed"),
        "cache": metrics.get("cache"),
        "passed": errors == 0,
        "claim": ("the cache was dropped under %d concurrent clients and the service served "
                  "every request without a 5xx" % concurrency),
    }


async def run_fail_open(base: str, concurrency: int, duration: float, api_key: str) -> dict:
    """Requires the server to be started with LIMITER_BACKEND pointing at a
    backend that can be broken (redis at a dead address)."""
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=concurrency + 8),
        timeout=aiohttp.ClientTimeout(total=15),
    ) as session:
        async with session.post(f"{base}/links", json={"target": "https://example.com/x"},
                                headers={"x-api-key": api_key}) as r:
            if r.status != 201:
                raise SystemExit("could not create a link; is the limiter already failing closed?")
            code = (await r.json())["code"]

        samples = []
        stop_at = time.perf_counter() + duration
        tasks = [asyncio.create_task(_hammer(session, base, code, stop_at, samples, api_key))
                 for _ in range(concurrency)]
        await asyncio.gather(*tasks)

        async with session.get(f"{base}/metrics") as r:
            metrics = await r.json()

    errors = sum(1 for s in samples if s[1] < 0 or s[1] >= 500)
    return {
        "drill": "limiter_fail_open",
        "requests": len(samples),
        "errors_5xx_or_connection": errors,
        "limiter": metrics.get("limiter"),
        "passed": errors == 0 and metrics.get("limiter", {}).get("fail_open_count", 0) > 0,
        "claim": "limiter store unreachable: traffic still served, fail_open_count climbing",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("drill", choices=["stampede", "fail-open"])
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--concurrency", type=int, default=64)
    ap.add_argument("--duration", type=float, default=8.0)
    ap.add_argument("--api-key", default="drill")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    fn = run_stampede if args.drill == "stampede" else run_fail_open
    result = asyncio.run(fn(args.url, args.concurrency, args.duration, args.api_key))
    print(json.dumps(result, indent=2))
    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=2)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
