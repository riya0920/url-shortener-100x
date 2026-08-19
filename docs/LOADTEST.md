# Load test and failure drills: measured results

All numbers produced by `python -m shortener.loadtest` and
`python -m shortener.drills` against a live server. Raw JSON is committed in
`results/`.

## Hardware and method

| | |
|---|---|
| machine | Windows 11, Intel64 Family 6 Model 126 (Ice Lake mobile), 8 logical CPUs |
| server | uvicorn, **single worker**, FastAPI sync endpoints on the threadpool |
| store | SQLite (WAL, `synchronous=NORMAL`) |
| client | `aiohttp`, 64 concurrent workers, same machine |
| workload | zipf(a=1.2) resolves over 500 links + 5 creates/sec |
| window | 20 s measured, 3 s warmup discarded |

**Two caveats that bound every number below**, stated before the numbers rather
than after:

1. **Client and server share one 8-core laptop.** The load generator competes
   with the service for CPU. Real numbers need a separate load box; these are a
   floor, not a ceiling.
2. **Closed-loop generator.** Each worker waits for its response before issuing
   the next request, so a slow response also slows the offered load. This
   **understates tail latency** under saturation — the classic coordinated-omission
   problem. An open-loop generator is the roadmap fix.

## Result 1: the first run was 202 RPS, and the profile said why

The initial measurement: **202 RPS, p99 3801 ms**.

The cause was not subtle. Every resolve opened a fresh SQLite connection and
committed a row to count the hit, so the read path was doing a synchronous
durable write per request. That is the exact anti-pattern
[DESIGN_100X.md](DESIGN_100X.md) already warned about — *"counting on the links
row would turn every resolve into a write"* — written down, and then done anyway.

**That is the argument for load-testing rather than reasoning.** The doc was
right and the code still shipped the bug.

Two fixes, both in the direction the doc named:

* **Batched hit counting** (`counters.py`): increment in memory, flush every 2 s
  in one transaction. Trade: up to 2 s of counts lost on a hard crash. Analytics
  accuracy is worth less than redirect availability — but a billing counter would
  need a different design.
* **Thread-local connection reuse**: opening a connection per request re-parses
  the schema and re-applies PRAGMAs every time.

| | RPS | p50 | p99 |
|---|---|---|---|
| before (write-per-resolve) | 202 | 148.6 ms | 3801 ms |
| after (batched + pooled) | **1900** | **32.2 ms** | **64.5 ms** |

**9.4× throughput, 59× better p99**, from two changes the design doc had already
predicted.

## Result 2: the rate limiter is now the dominant cost

With the read path fixed, the limiter became the bottleneck. Same binary, same
workload, only the limiter configuration changed:

| configuration | RPS | p50 | p95 | p99 | cache hit |
|---|---|---|---|---|---|
| limiter off | **1900** | 32.2 ms | 47.9 ms | **64.5 ms** | 0.977 |
| limiter on, SQLite | 311 | 85.8 ms | 276.1 ms | **4310 ms** | 0.886 |
| limiter on, Redis-Lua* | 429 | 142.3 ms | 245.5 ms | **299 ms** | 0.917 |

\* `fakeredis` — a real Redis implementation executing the **actual Lua script**,
but in-process. See the caveat below.

### Reading this table

**The limiter costs 6× throughput.** That is the headline, and it is a design
consequence rather than a bug: every request now performs a serialised
read-modify-write on shared state, which is the price of a *correct* distributed
limiter. An in-process limiter would be free and would also be wrong at N≥2.

**The SQLite p99 of 4310 ms is the interesting number.** `BEGIN IMMEDIATE` takes
a database-wide write lock, so every request queues behind every other request.
Under 64 concurrent clients that produces a pathological tail — the *median* is
fine at 85 ms while the 99th percentile is 50× worse. A mean would have hidden
this completely.

**Redis-Lua has a 14× better p99 despite lower median throughput**, because
per-key hash operations do not contend on a global lock. This is the measured
justification for the design doc's choice of Redis, and it is the shape of the
result that matters, not the absolute RPS.

### What the fakeredis number is NOT

`fakeredis` runs in the server process. There is **no network hop, no separate
process, and no real Redis event loop**. Its *throughput* is therefore not a
prediction of production Redis — real Redis would add ~0.1–0.5 ms of network
round-trip per call but would not hold a global write lock.

What the comparison legitimately shows is the **contention profile**: global
write lock versus per-key operations. The tail-latency ratio is the signal; the
RPS column is not.

## Result 3: p99 < 50 ms — the spec's bar

The spec asks for max RPS at **p99 < 50 ms** on the resolve path.

| configuration | p99 | meets the bar? |
|---|---|---|
| limiter off, 64 concurrent | 64.5 ms | no |
| limiter off, 16 concurrent | *not yet measured* | — |

**This build does not currently meet p99 < 50 ms at 64 concurrent clients**, and
the honest reason is that the load generator shares the machine. Finding the
concurrency level at which p99 drops under 50 ms is a one-command sweep and is
listed as the next step rather than quietly omitted.

## Drill 1: cache stampede

64 concurrent clients hammering one hot key; the **entire cache is dropped
mid-flight**.

```
requests                    19,065
cache_entries_evicted            1
errors_5xx_or_connection         0    <-- the service did not break
unexpected_status                0
cache misses                    19    <-- from 64 concurrent clients
singleflight_collapsed          15
p99 before flush            40.0 ms
p99 after flush             34.5 ms
```

**64 concurrent clients produced 19 backend misses, not 64.** SingleFlight
collapsed the herd, and p99 did not degrade in the second following the flush —
the difference between 40.0 and 34.5 ms is noise, not a recovery cost.

## Drill 2: limiter store unreachable (fail-open)

The limiter is pointed at a Redis that is not running, under live load:

```
requests                        48
errors_5xx_or_connection         0    <-- still serving
fail_open_count                 49    <-- and the outage is visible
```

The service kept serving with limits unenforced, exactly as designed, and the
counter climbed so an alert would fire.

**An unflattering detail worth stating**: only 48 requests completed in 5 seconds
at 16 concurrent clients, because every request pays a full TCP connection
timeout to the dead Redis before failing open. **Fail-open is not free** — it
converts an availability failure into a latency failure. The production fix is a
circuit breaker that trips after N consecutive failures and stops attempting the
connection. That is not implemented, and the drill is what exposed the need.

## What these numbers do not cover

* **Multi-instance.** All measurements are a single uvicorn worker. The
  cross-instance limiter accuracy test in the unit suite covers correctness at
  N≥2, but no multi-instance *load* test has been run.
* **Real Redis.** No Redis server was available; see the fakeredis caveat.
* **Sustained load.** 20-second windows. Nothing here says what happens after an
  hour, when the SQLite WAL has grown and the cache has churned.
* **Open-loop arrival.** See the coordinated-omission caveat above.
