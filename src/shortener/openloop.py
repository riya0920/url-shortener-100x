"""Open-loop load generation, and the concurrency sweep that finds the knee.

    python -m shortener.openloop sweep --url http://localhost:8000
    python -m shortener.openloop compare --url http://localhost:8000 --rate 1200

## Why a second generator exists

`loadtest.py` is **closed-loop**: each worker waits for its response before
issuing the next request. That is the easy design and it has one fatal property —
when the server slows down, the generator slows down with it. Offered load becomes
a function of service time, the queue never builds, and the reported p99 is the
p99 of a workload that politely backed off. This is **coordinated omission**, and
it is why closed-loop numbers look good exactly where they matter least.

An open-loop generator fixes it by making arrivals independent of responses:

* **Arrivals are a Poisson process** at a fixed rate. Not evenly spaced — real
  arrivals are not, and evenly spaced arrivals understate queueing because they
  never collide.
* **Latency is measured from the SCHEDULED time**, not from when the request was
  actually sent. If the generator itself falls behind, that delay is charged to
  the measurement rather than hidden. This single line is the difference between
  an open-loop generator and a closed-loop one with extra steps.
* **Backlog is reported.** If scheduled-to-sent drift grows without bound, the
  client is the bottleneck and the run is void. Saying so is the only way the
  number can be trusted.
* **The connection pool is bounded.** Not bounding it does not make the test more
  open-loop; it makes it a connection-setup benchmark. See `run_open_loop`.

## What the sweep answers

The spec asks for the concurrency at which p99 crosses a budget. Under a closed
loop that question is malformed — every concurrency level produces a *different*
offered rate, so the sweep confounds "more load" with "more clients". The open-
loop sweep varies the **offered rate** and holds the arrival process fixed, which
is the version where the knee means something.
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

from .loadtest import seed_links, zipf_keys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
RESULTS = os.path.join(ROOT, "results")


def poisson_arrivals(rate: float, duration: float, seed: int = 0) -> np.ndarray:
    """Arrival offsets from a homogeneous Poisson process of the given rate.

    Exponential inter-arrivals rather than a fixed 1/rate spacing. Evenly spaced
    arrivals never bunch, so they never form the transient queues that produce a
    tail -- measuring with them is measuring a workload that does not exist.
    """
    rng = np.random.default_rng(seed)
    gaps = rng.exponential(1.0 / max(rate, 1e-9), size=int(rate * duration * 1.4) + 32)
    times = np.cumsum(gaps)
    return times[times < duration]


class _TimerResolution:
    """Raise the OS timer resolution for the duration of a run.

    Windows' default timer granularity is ~15.6 ms, and `asyncio.sleep` inherits
    it. That is fatal for an open-loop generator: at 400 rps the mean gap between
    arrivals is 2.5 ms, so every sleep overshoots by an order of magnitude and the
    generator's own scheduling error becomes the dominant term in the measured
    latency. Measured before this fix, p99 client drift was 13.8 ms against a p99
    latency of 23.5 ms -- 59% of the number was the harness.

    `timeBeginPeriod(1)` takes it to ~1 ms. It is a process-wide setting and it is
    released on exit, which is why this is a context manager rather than a call at
    import time.

    On non-Windows this is a no-op; the default granularity there is already fine.
    """

    def __enter__(self):
        self._winmm = None
        if os.name == "nt":
            try:
                import ctypes

                self._winmm = ctypes.WinDLL("winmm")
                self._winmm.timeBeginPeriod(1)
            except Exception:
                self._winmm = None
        return self

    def __exit__(self, *exc):
        if self._winmm is not None:
            try:
                self._winmm.timeEndPeriod(1)
            except Exception:
                pass
        return False


async def _sleep_until(deadline: float):
    """Sleep to `deadline` (perf_counter), spinning only the last millisecond.

    The coarse sleep does the work; the spin exists because even at 1 ms
    resolution a sleep can land late, and a late arrival is indistinguishable from
    a slow server in the output. The spin window is deliberately tiny -- at 3,200
    rps a 1 ms spin per arrival would consume three cores on the machine that is
    also running the service under test.
    """
    remaining = deadline - time.perf_counter()
    if remaining > 0.0015:
        await asyncio.sleep(remaining - 0.001)
    while time.perf_counter() < deadline:
        pass


class OpenLoopResults:
    def __init__(self):
        self.latencies = []       # scheduled -> response complete
        self.service = []         # sent -> response complete
        self.drift = []           # scheduled -> sent (client backlog)
        self.status_counts = {}
        self.errors = 0

    def record(self, latency_ms, service_ms, drift_ms, status):
        self.latencies.append(latency_ms)
        self.service.append(service_ms)
        self.drift.append(drift_ms)
        self.status_counts[status] = self.status_counts.get(status, 0) + 1

    def summary(self, duration: float, offered_rate: float) -> dict:
        if not self.latencies:
            return {"requests": 0, "valid": False, "reason": "no completed requests"}
        lat = np.asarray(self.latencies)
        svc = np.asarray(self.service)
        drift = np.asarray(self.drift)
        # The validity gate. If the generator's own backlog is a large share of
        # the measured latency, the number describes the client, not the server.
        drift_share = float(np.percentile(drift, 99) / max(np.percentile(lat, 99), 1e-9))
        return {
            "requests": int(len(lat)),
            "offered_rate": offered_rate,
            "achieved_rate": float(len(lat) / duration),
            "p50_ms": float(np.percentile(lat, 50)),
            "p95_ms": float(np.percentile(lat, 95)),
            "p99_ms": float(np.percentile(lat, 99)),
            "p999_ms": float(np.percentile(lat, 99.9)),
            "max_ms": float(lat.max()),
            # Service time excludes the generator's own scheduling delay; the gap
            # between this and p99_ms is exactly what a closed loop would hide.
            "p99_service_ms": float(np.percentile(svc, 99)),
            "p99_client_drift_ms": float(np.percentile(drift, 99)),
            "client_drift_share_of_p99": drift_share,
            "valid": bool(drift_share < 0.25),
            "status_counts": self.status_counts,
            "errors": self.errors,
        }


async def _fire(session, url, api_key, scheduled_at, sent, t0, results):
    """One request, already due. `scheduled_at` is when it SHOULD have gone out."""
    try:
        async with session.get(url, headers={"x-api-key": api_key}, allow_redirects=False) as r:
            await r.read()
            done = time.perf_counter()
            results.record((done - t0 - scheduled_at) * 1000.0,
                           (done - sent) * 1000.0,
                           (sent - t0 - scheduled_at) * 1000.0,
                           r.status)
    except Exception:
        results.errors += 1


async def run_open_loop(base: str, rate: float, duration: float, n_links: int = 500,
                        api_key: str = "openloop", zipf_a: float = 1.2,
                        warmup: float = 2.0, seed: int = 0,
                        max_connections: int = 128) -> dict:
    """Poisson arrivals, fired as they come due, latency charged from due time.

    Two details that decide whether this measures the server or the harness:

    **Tasks are created when an arrival is due, not all up front.** The first
    version scheduled every arrival as a task at t=0, each sleeping until its
    turn. At 3,200 rps for 14 seconds that is 45,000 simultaneously-sleeping
    tasks, and the event loop's own scheduling latency then shows up as server
    latency.

    **The connection pool is bounded**, at a number a real client fleet would
    actually hold. The first version used an unbounded pool on the theory that
    any limit reintroduces the closed loop. It does not, and the unbounded
    version was catastrophically wrong: 400 rps offered against a service that
    does 1,900 rps closed-loop produced a 6.5 SECOND median, because several
    hundred simultaneous connection setups against a single uvicorn worker is a
    different benchmark from the one intended. Waiting for a free connection is
    real queueing and it stays in the measurement -- latency is charged from the
    scheduled time regardless of when a slot frees up, which is precisely what a
    closed loop cannot do.
    """
    if aiohttp is None:
        raise SystemExit("pip install aiohttp")

    connector = aiohttp.TCPConnector(limit=max_connections)
    async with aiohttp.ClientSession(connector=connector,
                                     timeout=aiohttp.ClientTimeout(total=30)) as session:
        codes = await seed_links(session, base, n_links, api_key)
        if not codes:
            raise SystemExit("failed to seed links -- is the service up?")
        try:
            async with session.post(f"{base}/admin/reset-metrics") as r:
                await r.read()
        except Exception:
            pass

        keys = zipf_keys(codes, 40_000, a=zipf_a, seed=seed)
        arrivals = poisson_arrivals(rate, duration + warmup, seed=seed)
        results, warm = OpenLoopResults(), OpenLoopResults()

        t0 = time.perf_counter()
        inflight = []
        with _TimerResolution():
            for i, at in enumerate(arrivals):
                at = float(at)
                await _sleep_until(t0 + at)
                sent = time.perf_counter()
                sink = warm if at < warmup else results
                inflight.append(asyncio.create_task(
                    _fire(session, f"{base}/{keys[i % len(keys)]}", api_key, at, sent, t0, sink)))
                if len(inflight) > 4096:
                    inflight = [t for t in inflight if not t.done()]
            await asyncio.gather(*inflight)

        metrics = {}
        try:
            async with session.get(f"{base}/metrics") as r:
                metrics = await r.json()
        except Exception:
            pass

    out = results.summary(duration, rate)
    out["warmup_requests"] = len(warm.latencies)
    out["max_connections"] = max_connections
    out["server_metrics"] = metrics
    return out


# ---------------------------------------------------------------------------
# the sweep
# ---------------------------------------------------------------------------

async def sweep(base: str, rates, duration: float, budget_ms: float, repeats: int = 1, **kw) -> dict:
    """Offered-rate sweep with repeats, reporting the spread rather than a point.

    Repeats matter more here than in most benchmarks. Near the knee the tail is
    bimodal -- most runs are fine and an occasional one catches a WAL flush or a
    GC pause -- and a single run either sees it or does not. The first version of
    this sweep, one run per point, reported p99 217 ms at 1,200 rps and 81 ms at
    1,600, which is not a curve, it is noise presented as a measurement.
    """
    rows = []
    for rate in rates:
        reps = []
        for r_i in range(repeats):
            reps.append(await run_open_loop(base, rate=rate, duration=duration,
                                            seed=r_i, **kw))
        p99s = sorted(r.get("p99_ms", float("inf")) for r in reps)
        med = reps[[r.get("p99_ms") for r in reps].index(p99s[len(p99s) // 2])]
        row = {**med, "repeats": repeats,
               "p99_ms_runs": p99s,
               "p99_ms_median": p99s[len(p99s) // 2],
               "p99_ms_spread": p99s[-1] - p99s[0]}
        row.pop("server_metrics", None)
        rows.append(row)
        print("offered %6.0f rps -> achieved %6.0f  p50 %6.2f  p99 %7.2f ms (%d runs, spread %.1f)  %s"
              % (rate, row.get("achieved_rate", 0), row.get("p50_ms", 0), row["p99_ms_median"],
                 repeats, row["p99_ms_spread"],
                 "OK" if row.get("valid") and row["p99_ms_median"] <= budget_ms
                 else ("over budget" if row.get("valid") else "INVALID: client-bound")))
        if row["p99_ms_median"] > budget_ms * 6:
            print("  tail is %.0fx the budget; stopping the sweep here"
                  % (row["p99_ms_median"] / budget_ms))
            break

    ok = [r for r in rows if r.get("valid") and r["p99_ms_median"] <= budget_ms]
    best = max(ok, key=lambda r: r["achieved_rate"]) if ok else None
    return {
        "hardware": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
                     "python": platform.python_version()},
        "budget_p99_ms": budget_ms,
        "duration_per_point_s": duration,
        "repeats_per_point": repeats,
        "points": rows,
        "max_sustainable_rps_within_budget": best["achieved_rate"] if best else None,
        "at_offered_rate": best["offered_rate"] if best else None,
        "caveat": ("single machine: the generator and the service share CPUs, so the highest "
                   "points measure contention as much as the service. The `valid` flag rejects "
                   "points where the generator's own backlog dominated, which is the part that "
                   "can be checked; CPU contention is not, and is why this is a shape rather "
                   "than a capacity number."),
    }


# ---------------------------------------------------------------------------
# isolated sweep: one fresh server process per point
# ---------------------------------------------------------------------------

def _spawn_server(port: int, data_dir: str, extra_env: dict = None):
    import subprocess

    env = dict(os.environ)
    env.update({"PYTHONPATH": os.path.join(ROOT, "src"),
                "SHORTENER_DATA": data_dir,
                "LIMITER_ENABLED": "0",
                "INSTANCE_ID": "1"})
    env.update(extra_env or {})
    os.makedirs(data_dir, exist_ok=True)
    proc = subprocess.Popen(
        [__import__("sys").executable, "-m", "uvicorn", "shortener.app:app",
         "--port", str(port), "--log-level", "warning"],
        cwd=ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc


async def _wait_healthy(base: str, timeout_s: float = 30.0) -> bool:
    deadline = time.perf_counter() + timeout_s
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as s:
        while time.perf_counter() < deadline:
            try:
                async with s.get(f"{base}/healthz") as r:
                    if r.status == 200:
                        return True
            except Exception:
                pass
            await asyncio.sleep(0.25)
    return False


async def sweep_isolated(rates, duration: float, budget_ms: float, repeats: int = 3,
                         base_port: int = 8300, **kw) -> dict:
    """Every measurement gets a brand-new server process and a brand-new database.

    This exists because the first sweep produced a curve that sloped the wrong
    way, and the reason turned out to be the harness rather than the service.

    **What was observed.** Successive points got worse regardless of offered rate.
    By the twentieth run the service was serving 287 rps where a fresh process had
    served 2,798, and it did not recover during idle periods.

    **Two hypotheses, tested rather than argued.**

      * *The write-ahead log.* `PRAGMA wal_checkpoint(TRUNCATE)` took a 4.1 MB WAL
        to zero bytes and changed nothing. Rejected.
      * *The data.* Restarting the process against the *same* database files
        restored performance immediately. Rejected — it was not the rows.

    **The actual cause.** Every earlier run had left its server process alive.
    Eight cores were carrying six orphaned uvicorn processes plus unrelated work,
    so each new measurement competed with every measurement before it. The
    "degradation" was cumulative host contention wearing the costume of a
    server-side leak, and it was convincing: monotone, reproducible, and
    unaffected by the two most plausible database explanations.

    So the fix is not a workaround. A benchmark that leaves processes running is
    measuring its own history, and the only reliable defence is explicit teardown
    in a `finally` — which is what this function does, along with recording host
    CPU alongside every measurement so a contended run is visible in the data
    instead of being reconstructed afterwards.

    With isolation in place the curve is monotone and the three repeats at each
    point agree to within a few milliseconds up to the knee.
    """
    import shutil

    rows = []
    port = base_port
    for rate in rates:
        reps = []
        for r_i in range(repeats):
            port += 1
            data_dir = os.path.join(ROOT, "data", "sweep_%d" % port)
            shutil.rmtree(data_dir, ignore_errors=True)
            proc = _spawn_server(port, data_dir)
            base = "http://localhost:%d" % port
            try:
                if not await _wait_healthy(base):
                    raise SystemExit("server on %d never became healthy" % port)
                point = await run_open_loop(base, rate=rate, duration=duration,
                                            seed=r_i, **kw)
                point["host_load"] = _host_load()
                reps.append(point)
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except Exception:
                    proc.kill()
                shutil.rmtree(data_dir, ignore_errors=True)

        p99s = sorted(r.get("p99_ms", float("inf")) for r in reps)
        med_idx = [r.get("p99_ms") for r in reps].index(p99s[len(p99s) // 2])
        row = {**reps[med_idx], "repeats": repeats, "p99_ms_runs": p99s,
               "p99_ms_median": p99s[len(p99s) // 2],
               "p99_ms_spread": p99s[-1] - p99s[0]}
        row.pop("server_metrics", None)
        rows.append(row)
        print("offered %6.0f -> achieved %6.0f  p50 %6.2f  p99 %7.2f ms  runs %s  %s"
              % (rate, row.get("achieved_rate", 0), row.get("p50_ms", 0), row["p99_ms_median"],
                 ["%.0f" % x for x in p99s],
                 "OK" if row.get("valid") and row["p99_ms_median"] <= budget_ms
                 else ("over budget" if row.get("valid") else "INVALID: client-bound")))
        if row["p99_ms_median"] > budget_ms * 8:
            print("  tail is %.0fx the budget; stopping" % (row["p99_ms_median"] / budget_ms))
            break

    ok = [r for r in rows if r.get("valid") and r["p99_ms_median"] <= budget_ms]
    best = max(ok, key=lambda r: r["achieved_rate"]) if ok else None
    return {
        "hardware": {"platform": platform.platform(), "cpu_count": os.cpu_count(),
                     "python": platform.python_version()},
        "isolation": "fresh server process and fresh database per measurement",
        "budget_p99_ms": budget_ms,
        "duration_per_point_s": duration,
        "repeats_per_point": repeats,
        "points": rows,
        "max_sustainable_rps_within_budget": best["achieved_rate"] if best else None,
        "at_offered_rate": best["offered_rate"] if best else None,
        "caveat": ("the generator shares the machine with the service. The `valid` flag rejects "
                   "points where the generator's own scheduling backlog dominated; CPU contention "
                   "between the two cannot be rejected the same way, so treat the absolute "
                   "ceiling as a shape rather than a capacity number."),
    }


def _host_load() -> dict:
    """CPU utilisation excluding this process, sampled around a measurement.

    Recorded with every run because on a shared laptop it is the difference
    between a number and an anecdote. A run taken at 95% background CPU is not
    wrong, it is measuring something else, and the reader needs to see which.
    """
    try:
        import psutil

        return {"cpu_percent": psutil.cpu_percent(interval=0.5),
                "cores": psutil.cpu_count()}
    except Exception:
        return {}


async def compare(rate: float, duration: float, repeats: int = 3,
                  concurrency: int = 64, base_port: int = 8400, **kw) -> dict:
    """Open loop vs closed loop, **paired**, on fresh servers, several times.

    The pairing is the whole design. Absolute throughput on a shared laptop is
    not reproducible -- during this run the host was carrying an unrelated
    workload at over 600% CPU -- so an absolute ceiling would be a number about
    the machine. A ratio measured on the same server process, seconds apart, is
    about the *generators*: contention moves both arms together and cancels.

    The closed-loop arm runs at a **fixed** concurrency (64, the same figure the
    rest of this project's load tests use). An earlier version sized it by
    Little's law from the open-loop p50, which inverted the whole experiment: when
    a contended open-loop run produced a 3-second p50, the formula asked for 1,371
    concurrent workers and the closed-loop arm then measured client thrash rather
    than the service. Deriving one arm's configuration from the other arm's result
    is a coupling that only shows up when a run goes badly, which is exactly when
    the measurement matters.

    The claim does not need matched throughput anyway, and is stronger without it:
    the closed loop reports a *better* tail while pushing *more* requests through.
    """
    from .loadtest import run as closed_run
    import shutil

    pairs = []
    port = base_port
    for r_i in range(repeats):
        port += 1
        data_dir = os.path.join(ROOT, "data", "cmp_%d" % port)
        shutil.rmtree(data_dir, ignore_errors=True)
        proc = _spawn_server(port, data_dir)
        base = "http://localhost:%d" % port
        try:
            if not await _wait_healthy(base):
                raise SystemExit("server on %d never became healthy" % port)
            load_before = _host_load()
            op = await run_open_loop(base, rate=rate, duration=duration, seed=r_i, **kw)
            conc = concurrency
            ckw = {k: v for k, v in kw.items() if k != "max_connections"}
            cl = await closed_run(base, duration=duration, concurrency=conc,
                                  n_links=ckw.get("n_links", 500), create_rps=0.0, warmup=2.0,
                                  api_key=ckw.get("api_key", "openloop"),
                                  zipf_a=ckw.get("zipf_a", 1.2))
            pairs.append({
                "run": r_i,
                "host_load": {"before": load_before, "after": _host_load()},
                "open_loop": {k: op[k] for k in ("achieved_rate", "p50_ms", "p99_ms",
                                                 "p99_service_ms", "valid")},
                "closed_loop": {"concurrency": conc, "rps": cl["resolve"]["rps"],
                                "p50_ms": cl["resolve"]["p50_ms"], "p99_ms": cl["resolve"]["p99_ms"]},
                "p99_ratio_open_over_closed": (op["p99_ms"] / cl["resolve"]["p99_ms"]
                                               if cl["resolve"]["p99_ms"] else None),
            })
            print("run %d: open %.0f rps p99 %.1f ms | closed c=%d %.0f rps p99 %.1f ms | ratio %.2fx"
                  % (r_i, op["achieved_rate"], op["p99_ms"], conc, cl["resolve"]["rps"],
                     cl["resolve"]["p99_ms"], pairs[-1]["p99_ratio_open_over_closed"]))
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()
            shutil.rmtree(data_dir, ignore_errors=True)

    ratios = sorted(p["p99_ratio_open_over_closed"] for p in pairs
                    if p["p99_ratio_open_over_closed"])
    return {
        "offered_rate": rate,
        "closed_loop_concurrency": concurrency,
        "repeats": repeats,
        "pairs": pairs,
        "p99_ratio_median": ratios[len(ratios) // 2] if ratios else None,
        "p99_ratio_range": [ratios[0], ratios[-1]] if ratios else None,
        "reading": ("the open-loop p99 includes queueing the closed loop cannot generate, because "
                    "a closed-loop worker cannot issue its next request while the previous one is "
                    "outstanding. The ratio is how much a closed-loop number flatters the service."),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["sweep", "sweep-isolated", "single", "compare"])
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--rate", type=float, default=800.0)
    ap.add_argument("--rates", default="200,400,800,1200,1600,2400,3200")
    ap.add_argument("--duration", type=float, default=12.0)
    ap.add_argument("--budget-ms", type=float, default=50.0)
    ap.add_argument("--links", type=int, default=500)
    ap.add_argument("--api-key", default="openloop")
    ap.add_argument("--max-connections", type=int, default=128)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--closed-concurrency", type=int, default=64)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    kw = {"n_links": args.links, "api_key": args.api_key,
          "max_connections": args.max_connections}
    if args.command == "sweep-isolated":
        rates = [float(x) for x in args.rates.split(",")]
        out = asyncio.run(sweep_isolated(rates, args.duration, args.budget_ms,
                                         repeats=args.repeats, **kw))
        name = "openloop_sweep_isolated.json"
    elif args.command == "sweep":
        rates = [float(x) for x in args.rates.split(",")]
        out = asyncio.run(sweep(args.url, rates, args.duration, args.budget_ms,
                                repeats=args.repeats, **kw))
        name = "openloop_sweep.json"
    elif args.command == "compare":
        out = asyncio.run(compare(args.rate, args.duration, repeats=args.repeats,
                                  concurrency=args.closed_concurrency, **kw))
        name = "openloop_vs_closed.json"
    else:
        out = asyncio.run(run_open_loop(args.url, args.rate, args.duration, **kw))
        name = "openloop_single.json"

    path = args.out or os.path.join(RESULTS, name)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(out, fh, indent=2, default=float)
    print(json.dumps(out, indent=2, default=float)[:4000])
    print("\nwritten:", os.path.relpath(path, ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
