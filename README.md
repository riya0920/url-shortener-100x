# Rate-Limited URL Shortener + the 100x Design Doc

Working multi-instance code, correctness tests that spawn real processes, and a
design review that argues its own decisions — including the ones it rejected.

> **Status: ~85% built.** ID generation, the atomic distributed limiter (SQLite
> *and* Redis-Lua, cross-verified), cache-aside with stampede protection, the
> API, the **load test with measured numbers**, and two scripted failure drills
> are done, alongside **[docs/DESIGN_100X.md](docs/DESIGN_100X.md)**. Remaining:
> a real Redis server, multi-instance load, an open-loop generator — see
> [Roadmap](#roadmap).

## The two artifacts

1. **The code**, which is designed for N≥2 instances because the interesting
   bugs — shared limiter state, duplicate short codes, cache coherence — do not
   exist at N=1. A single-instance demo proves nothing about any of them.
2. **[The design doc](docs/DESIGN_100X.md)**, which is roughly half the project:
   capacity arithmetic with the working shown, alternatives seriously considered
   and rejected at every layer, failure modes with exact behaviour, and the SLIs
   worth paging on.

## Run it

```bash
pip install -r requirements.txt
make test        # 24 tests, two of which spawn real processes
make run         # single instance on :8000
make up          # two instances behind nginx on :8080
```

## Measured results

Full method and caveats in **[docs/LOADTEST.md](docs/LOADTEST.md)**. Single
uvicorn worker, SQLite, 64 concurrent clients, zipf(1.2) over 500 links, on a
Windows 11 / Ice Lake 8-core laptop **sharing the machine with the load
generator**.

**The first run was 202 RPS at a 3801 ms p99.** The cause was a synchronous
durable write on every resolve — the exact anti-pattern the design doc had
already warned about, written down and then shipped anyway. Batching the hit
counter and pooling connections fixed it:

| | RPS | p50 | p99 |
|---|---|---|---|
| before | 202 | 148.6 ms | 3801 ms |
| after | **1900** | **32.2 ms** | **64.5 ms** |

**9.4x throughput, 59x better p99.** That is the argument for load-testing rather
than reasoning: the doc was right and the code was still wrong.

With the read path fixed, the limiter became the bottleneck:

| configuration | RPS | p50 | p99 |
|---|---|---|---|
| limiter off | 1900 | 32.2 ms | 64.5 ms |
| limiter on, SQLite | 311 | 85.8 ms | **4310 ms** |
| limiter on, Redis-Lua (*) | 429 | 142.3 ms | **299 ms** |

The SQLite p99 is the interesting number: `BEGIN IMMEDIATE` takes a
database-wide write lock, so requests queue behind each other and the tail
collapses while the *median* still looks fine at 85 ms. Redis-Lua is **14x
better at p99** because per-key hash ops do not contend globally. That is the
measured justification for the design doc's Redis choice.

(*) `fakeredis` executing the real Lua script in-process — no network hop, so its
*throughput* is not a production prediction. The contention profile is the
signal, not the RPS column.

**This build does not yet meet the spec's p99 < 50 ms bar at 64 concurrent
clients** (64.5 ms with the limiter off). The load generator shares the machine;
finding the concurrency at which it clears 50 ms is the next measurement.

## Failure drills, run against a live server

**Cache stampede** — 64 concurrent clients on one hot key, entire cache dropped
mid-flight:

```
requests                 19,065
errors                        0    <-- service did not break
cache misses                 19    <-- from 64 concurrent clients
singleflight_collapsed       15
p99 before / after   40.0 / 34.5 ms
```

**Limiter store unreachable** — traffic kept flowing with `fail_open_count`
climbing. But only 48 requests completed in 5 s, because every request pays a
full TCP timeout to the dead Redis. **Fail-open is not free**: it converts an
availability failure into a latency failure, and the fix is a circuit breaker
that is not yet built. The drill is what exposed that.

## The rate limiter is atomic, and that's tested across processes

The bug this project exists to not have:

```python
tokens = store.get(key)          # another instance reads the same value here
if tokens > 0:
    store.set(key, tokens - 1)   # and both decrement from it
```

Under real concurrency that leaks roughly `(instances - 1) x budget`, and it is
**invisible in single-instance testing** — which is why almost nobody catches it.

The fix is that the whole check-and-decrement runs as one indivisible operation:
a **Lua script** on Redis (Redis executes scripts atomically), or a
`BEGIN IMMEDIATE` transaction on SQLite. Same algorithm, two backends.

`test_limiter_accuracy_across_processes` spawns **4 real processes** that each
send half the budget at a shared bucket, and asserts the total admitted is within
5% of the budget. SQLite is the backend there specifically so the test can run
without a Redis server — the point is to *prove* the atomicity claim rather than
assert it in a comment.

**Token bucket** over fixed-window (which lets a client send 2x its budget across
a window edge) and sliding-window-log (which stores a timestamp per request, so
memory grows with traffic and the heaviest client costs the most). Refill is
**lazy** — computed from elapsed time on each request — so there is no timer, no
drift, and correct behaviour after an idle period.

**What breaks token bucket:** a client that idles until its bucket is full and
then dumps the whole capacity at once. That burst is *allowed by design* —
capacity is precisely how much burst we permit. If bursts rather than sustained
rate are the threat, capacity must shrink toward 1, and a sliding window becomes
the better tool.

## Fail open, and it's a product argument

When the limiter store is unreachable, requests are **allowed** and
`fail_open_count` increments.

Failing *closed* turns a Redis outage into a total outage: the limiter, a
protective control, becomes the single point of failure for the entire service.
Failing *open* turns the same outage into a window of unenforced limits during
which the product still works. That trade is acceptable **because of what this
limiter protects** — fair use of a public redirect service, where a few unlimited
minutes cost some extra load.

**If this limiter guarded a payment endpoint or metered billing, the answer would
flip**, because there unlimited requests are the more expensive failure. So it is
a constructor flag, not a hardcoded assumption, and both paths are tested.

The doc walks through [exactly what happens in the next 500 ms](docs/DESIGN_100X.md#41-redis-dies--what-happens-in-the-next-500-ms).

## Short codes: snowflake, and what it costs

`timestamp(41) | instance(10) | sequence(12)`, rendered base62. No coordination
on the hot path and no uniqueness check — unlike random base62, which needs a
read-and-retry on every create, or an auto-increment integer, which makes a
shared sequence the write bottleneck exactly when you scale out.

The costs are named rather than hidden: instance ids must be unique (coordination
moved to startup, not eliminated), and ids leak creation time plus a rough
creation rate. Fine for public short links; **not** fine if codes were capability
tokens.

A backwards clock step could repeat a `(ms, sequence)` pair, so small drift spins
until the clock catches up and a jump over 5 s **raises** — loud beats silently
wrong. `test_large_backwards_clock_jump_raises_instead_of_duplicating` covers it.

## Cache stampede: the fix that's easy to get wrong

When a hot link's TTL lapses, every concurrent request for it misses at once — a
miss that should cost one query costs thousands. `SingleFlight` collapses those
into exactly one backend call; the rest wait and share the leader's result.

**The subtle bug, which the first version of this file had:** taking a per-key
lock and calling the loader inside it *serialises* the stampede without
*collapsing* it. The backend still sees N calls, just one at a time — strictly
worse than no mitigation, because it adds latency without removing load.
`test_singleflight_collapses_a_stampede` caught it by counting actual loader
invocations under 16 concurrent threads.

Per-key locking, not global: a global lock would serialise misses for unrelated
keys and turn a stampede on one link into a latency problem for every link.

## Other decisions worth a line

* **Hit counts live in a separate table.** Counting on the `links` row would turn
  every resolve into a write, serialise on the row, and invalidate the cache
  entry it just served.
* **Cache TTL is enforced on read as well as write.** A cached entry outliving
  its link is how an expired link keeps redirecting.
* **Non-http schemes are rejected at create time.** A shortener that accepts
  `javascript:` or `data:` URLs becomes an XSS-laundering service, since the
  short domain is what users see and trust.
* **A code collision returns 500, not a silent retry.** It should be unreachable;
  if it fires, two instances share an `INSTANCE_ID` and hiding that would be
  worse than the error.

## Roadmap (the remaining ~60%)

| Milestone | Status |
|---|---|
| Snowflake ids + base62, clock-safety | done |
| Atomic token bucket (Redis Lua + SQLite) | done |
| Cross-process limiter accuracy test | done |
| Cache-aside, LRU with TTL, hit-ratio metric | done |
| SingleFlight stampede protection | done |
| Fail-open/closed with instrumentation | done |
| Multi-instance compose + nginx | done |
| `docs/DESIGN_100X.md` | done |
| Load test: zipf resolves + steady creates | done |
| Limiter-on vs limiter-off vs Redis-Lua overhead | done |
| Cache-stampede drill under live load | done |
| Limiter-store-failure drill (fail-open verified) | done |
| Redis Lua path executed and cross-verified against SQLite | done |
| Batched hit counting + connection pooling (9.4x, found by measurement) | done |
| **Concurrency sweep to find where p99 < 50 ms** | not done |
| **Open-loop load generator (closed-loop understates the tail)** | not started |
| **Real Redis server rather than fakeredis** | not started |
| **Multi-instance load test (correctness tested, load not)** | not started |
| **Circuit breaker so fail-open stops paying the connect timeout** | not started |
| **Postgres backend (SQLite stands in today)** | not started |
| **Abuse controls: URL reputation, takedown, interstitial** | not started |

## Honesty notes

* **Every throughput and latency number came from a run on one laptop that was
  also generating the load.** They are a floor, not a ceiling, and the method
  section states that before the numbers. The design doc's 100x figures remain
  capacity *arithmetic*, explicitly labelled, not measurements.
* **The closed-loop generator understates tail latency** under saturation
  (coordinated omission). Written in the doc rather than left for a reader.
* **fakeredis is not Redis.** It runs the real Lua script but in-process, so its
  throughput column is not a production prediction.
* **Storage is SQLite, not Postgres.** The design doc argues for Postgres at
  100x; the code today uses SQLite, which is a legitimate choice at this size and
  a stand-in above it. The interfaces are narrow enough that the swap is small.
* The limiter accuracy result is from the **SQLite** backend across 4 processes.
  The Redis Lua path implements the same algorithm and is not covered by that
  test, because no Redis server runs in CI.
* Abuse prevention is entirely absent, and for a real public shortener that is a
  larger problem than everything above.
