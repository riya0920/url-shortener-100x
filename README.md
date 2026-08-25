# Rate-Limited URL Shortener + the 100x Design Doc

![tests](https://img.shields.io/badge/tests-112%20passing-1a7a56) [![demo](https://img.shields.io/badge/demo-live-1d4e7c)](https://riya0920.github.io/url-shortener-100x/) ![license](https://img.shields.io/badge/license-MIT-555555)

Working multi-instance code, correctness tests that spawn real processes, and a
design review that argues its own decisions - including the ones it rejected.

**[See the measured results](https://riya0920.github.io/url-shortener-100x/)** &nbsp;·&nbsp; [Design doc](docs/DESIGN_100X.md) &nbsp;·&nbsp; [Load test method](docs/LOADTEST.md)

[![Deploy to Render](https://img.shields.io/badge/deploy-to%20Render-46E3B7)](https://render.com/deploy?repo=https://github.com/riya0920/url-shortener-100x)

One click brings up the **live sandbox**: create a short link, watch it resolve, then fire
a burst of 80 requests and watch the real token bucket refuse the tail. Nothing is simulated.


## The two artifacts

1. **The code**, which is designed for N≥2 instances because the interesting
   bugs - shared limiter state, duplicate short codes, cache coherence - do not
   exist at N=1. A single-instance demo proves nothing about any of them.
2. **[The design doc](docs/DESIGN_100X.md)**, which is roughly half the project:
   capacity arithmetic with the working shown, alternatives seriously considered
   and rejected at every layer, failure modes with exact behaviour, and the SLIs
   worth paging on.

## Run it

```bash
pip install -r requirements.txt
make test        # 112 tests with Redis + Postgres, 78 without (the rest skip)
make sweep       # open-loop capacity sweep, fresh server per point
make compare     # open loop vs closed loop, paired
make run         # single instance on :8000
make up          # two instances behind nginx on :8080
```

## Measured results

Full method and caveats in **[docs/LOADTEST.md](docs/LOADTEST.md)**. Single
uvicorn worker, SQLite, 64 concurrent clients, zipf(1.2) over 500 links, on a
Windows 11 / Ice Lake 8-core laptop **sharing the machine with the load
generator**.

**The first run was 202 RPS at a 3801 ms p99.** The cause was a synchronous
durable write on every resolve - the exact anti-pattern the design doc had
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
| limiter on, Redis-Lua (fakeredis) | 429 | 142.3 ms | 299 ms |
| limiter on, **real Redis 8.0.5** | **563** | 105.6 ms | **228 ms** |

The SQLite p99 is the interesting number: `BEGIN IMMEDIATE` takes a
database-wide write lock, so requests queue behind each other and the tail
collapses while the *median* still looks fine at 85 ms. Redis-Lua is **14x
better at p99** because per-key hash ops do not contend globally. That is the
measured justification for the design doc's Redis choice.

(*) `fakeredis` executing the real Lua script in-process - no network hop, so its
*throughput* is not a production prediction. The contention profile is the
signal, not the RPS column.

**This build does not yet meet the spec's p99 < 50 ms bar at 64 concurrent
clients** (64.5 ms with the limiter off). The load generator shares the machine;
finding the concurrency at which it clears 50 ms is the next measurement.

## Failure drills, run against a live server

**Cache stampede** - 64 concurrent clients on one hot key, entire cache dropped
mid-flight:

```
requests                 19,065
errors                        0    <-- service did not break
cache misses                 19    <-- from 64 concurrent clients
singleflight_collapsed       15
p99 before / after   40.0 / 34.5 ms
```

**Limiter store unreachable** - traffic kept flowing with `fail_open_count`
climbing. But only **48 requests completed in 5 s**, because every request paid a
full TCP timeout to the dead Redis. **Fail-open is not free**: it converts an
availability failure into a latency failure.

### The circuit breaker, and the measurement that justifies it

After N consecutive failures the circuit opens and calls short-circuit
immediately instead of waiting for a timeout. Same 5-second drill, same 16
clients, same dead Redis:

| | requests completed | calls reaching the dead store | errors |
|---|---|---|---|
| fail-open only | 48 | 48 | 0 |
| **fail-open + breaker** | **2,459** | **20** | 0 |

**51x more throughput while the dependency is down**, and the availability
guarantee is unchanged - requests are still allowed, just allowed *immediately*.
2,440 calls were short-circuited rather than attempted.

Half-open admits **exactly one probe**, not a burst: sending a burst at a service
that just recovered is how you knock it over again - the same mistake as
replaying a DLQ at full rate. A failed probe restarts the cooldown rather than
resuming it, and a single success anywhere resets the consecutive-failure run so
an intermittent blip cannot trip the breaker. Seven tests pin those transitions.

## The rate limiter is atomic, and that's tested across processes

The bug this project exists to not have:

```python
tokens = store.get(key)          # another instance reads the same value here
if tokens > 0:
    store.set(key, tokens - 1)   # and both decrement from it
```

Under real concurrency that leaks roughly `(instances - 1) x budget`, and it is
**invisible in single-instance testing** - which is why almost nobody catches it.

The fix is that the whole check-and-decrement runs as one indivisible operation:
a **Lua script** on Redis (Redis executes scripts atomically), or a
`BEGIN IMMEDIATE` transaction on SQLite. Same algorithm, two backends.

`test_limiter_accuracy_across_processes` spawns **4 real processes** that each
send half the budget at a shared bucket, and asserts the total admitted is within
5% of the budget. SQLite is the backend there specifically so the test can run
without a Redis server - the point is to *prove* the atomicity claim rather than
assert it in a comment.

**Token bucket** over fixed-window (which lets a client send 2x its budget across
a window edge) and sliding-window-log (which stores a timestamp per request, so
memory grows with traffic and the heaviest client costs the most). Refill is
**lazy** - computed from elapsed time on each request - so there is no timer, no
drift, and correct behaviour after an idle period.

**What breaks token bucket:** a client that idles until its bucket is full and
then dumps the whole capacity at once. That burst is *allowed by design* - capacity is precisely how much burst we permit. If bursts rather than sustained
rate are the threat, capacity must shrink toward 1, and a sliding window becomes
the better tool.

## Fail open, and it's a product argument

When the limiter store is unreachable, requests are **allowed** and
`fail_open_count` increments.

Failing *closed* turns a Redis outage into a total outage: the limiter, a
protective control, becomes the single point of failure for the entire service.
Failing *open* turns the same outage into a window of unenforced limits during
which the product still works. That trade is acceptable **because of what this
limiter protects** - fair use of a public redirect service, where a few unlimited
minutes cost some extra load.

**If this limiter guarded a payment endpoint or metered billing, the answer would
flip**, because there unlimited requests are the more expensive failure. So it is
a constructor flag, not a hardcoded assumption, and both paths are tested.

The doc walks through [exactly what happens in the next 500 ms](docs/DESIGN_100X.md#41-redis-dies--what-happens-in-the-next-500-ms).

## Short codes: snowflake, and what it costs

`timestamp(41) | instance(10) | sequence(12)`, rendered base62. No coordination
on the hot path and no uniqueness check - unlike random base62, which needs a
read-and-retry on every create, or an auto-increment integer, which makes a
shared sequence the write bottleneck exactly when you scale out.

The costs are named rather than hidden: instance ids must be unique (coordination
moved to startup, not eliminated), and ids leak creation time plus a rough
creation rate. Fine for public short links; **not** fine if codes were capability
tokens.

A backwards clock step could repeat a `(ms, sequence)` pair, so small drift spins
until the clock catches up and a jump over 5 s **raises** - loud beats silently
wrong. `test_large_backwards_clock_jump_raises_instead_of_duplicating` covers it.

## Cache stampede: the fix that's easy to get wrong

When a hot link's TTL lapses, every concurrent request for it misses at once - a
miss that should cost one query costs thousands. `SingleFlight` collapses those
into exactly one backend call; the rest wait and share the leader's result.

**The subtle bug, which the first version of this file had:** taking a per-key
lock and calling the loader inside it *serialises* the stampede without
*collapsing* it. The backend still sees N calls, just one at a time - strictly
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

## Known limitations

Things this repository does not prove, and what each one would need.

| limitation | why |
|---|---|
| **Multi-instance load test (correctness tested, load not)** | not possible here: needs a second machine |

## The closed-loop number was 24x too optimistic

Every load figure above this line came from a **closed-loop** generator: each
worker waits for its response before issuing the next request. That design has one
fatal property - when the server slows down, the generator slows down with it.
Offered load becomes a function of service time, the queue never builds, and the
reported tail is the tail of a workload that politely backed off. This is
**coordinated omission**.

`src/shortener/openloop.py` is the fix. Arrivals are a **Poisson process** at a
fixed rate, fired whether or not anything is outstanding, and **latency is charged
from the scheduled time** rather than from when the request actually went out.

### The measurement

Same server process, ~2,000 rps through both arms, five paired runs, a fresh
server per run:

| generator | achieved | p99 |
|---|---|---|
| closed loop, 64 workers | 2,050-2,750 rps | **34-43 ms** |
| open loop, 2,000 rps offered | 1,991-2,030 rps | **110-1,533 ms** (median 1,038) |

**Median ratio: 24x.** The closed loop reports a 43 ms tail for a workload whose
real tail is a full second - and it does so while pushing *more* throughput, which
is what makes the number so persuasive and so wrong.

### The capacity curve, from the open loop

Fresh server process and fresh database per point, three repeats, 10 s each:

| offered | achieved | p50 | p99 (3 runs) | within 50 ms? |
|---|---|---|---|---|
| 400 | 394 | 2.3 ms | 9 ms (9, 9, 11) | yes |
| 800 | 818 | 4.9 ms | 26 ms (18, 26, 26) | yes |
| **1,200** | **1,210** | **11.4 ms** | **37 ms** (33, 37, 44) | **yes** |
| 1,600 | 1,610 | 31.3 ms | 81 ms (59, 81, 94) | no |
| 2,000 | 2,012 | 773 ms | 1,077 ms (969, 1077, 1696) | no |

**Max sustainable throughput within a 50 ms p99: ~1,210 rps.** The closed-loop
runs claimed 1,900-2,800. The knee is sharp - between 1,600 and 2,000 offered rps
the tail moves from 81 ms to 1,077 ms, which is the queueing cliff you cannot see
from inside a closed loop because the loop cannot climb it.

### Four things the generator got wrong first

Every one of these produced a plausible number before it was caught.

* **`--create-rps 0` hung the load test for 1,000 seconds.** The write-worker
  clamped its rate with `max(rate, 0.001)`, turning "no writes" into one request
  every thousand seconds - and since the run ends on `gather`, a read-only run
  looked like a hang. Fixed by returning instead of clamping.
* **An unbounded connection pool is not more open-loop, it is a different
  benchmark.** The first version left the connector unlimited on the theory that
  any limit reintroduces the closed loop. It does not. 400 rps offered against a
  service that does 1,900 closed-loop produced a **6.5 second median**, because
  several hundred simultaneous TCP setups against one uvicorn worker is not the
  experiment. Waiting for a free connection is real queueing and stays in the
  number; latency is charged from the scheduled time either way.
* **Windows' timer granularity is 15.6 ms**, and `asyncio.sleep` inherits it. At
  400 rps the mean gap between arrivals is 2.5 ms, so every sleep overshot by an
  order of magnitude: measured p99 client drift was **13.8 ms against a p99
  latency of 23.5 ms - 59% of the number was the harness**. `timeBeginPeriod(1)`
  plus a one-millisecond spin took drift to 0.93 ms, 10% of p99. Every run now
  reports `client_drift_share_of_p99` and marks itself invalid above 25%.
* **Sizing one arm from the other arm's result couples them.** The closed-loop
  arm was originally sized by Little's law from the open-loop p50; when a
  contended run produced a 3-second p50, the formula asked for 1,371 workers and
  the closed-loop arm measured client thrash. Fixed concurrency instead - and the
  claim is stronger without matching, because the closed loop reports a better
  tail while pushing more work.

### And one that was not the service's fault

The first sweep sloped downward regardless of offered rate: by the twentieth run
the service did 287 rps where a fresh process did 2,798, and it never recovered.
Two hypotheses were tested rather than argued. `PRAGMA wal_checkpoint(TRUNCATE)`
took a 4.1 MB write-ahead log to zero bytes and changed nothing. Restarting the
process against the *same* database files restored performance instantly. So not
the WAL and not the data - the cause was that **every previous run had left its
server process alive**, and eight cores were carrying six orphaned uvicorn
processes. Cumulative host contention wearing the costume of a server-side leak,
and it was convincing: monotone, reproducible, and immune to both database
explanations.

The fix is not a workaround. A benchmark that leaves processes running measures
its own history, so `sweep-isolated` tears every server down in a `finally` and
records host CPU next to each measurement, which makes a contended run visible in
the data rather than reconstructed afterwards.

```bash
make sweep      # fresh server + fresh DB per point, 3 repeats
make compare    # open vs closed loop, paired, 5 runs
```

### Real Redis turned out to be *faster* than the fake

Every Redis number in this repo used to come from **fakeredis**, marked with an
asterisk because an in-process reimplementation is not a server. The obvious
expectation was that the asterisked number flattered the Redis path - no network
hop, no serialisation, no separate process.

It was the opposite. Real Redis 8.0.5 over a loopback into WSL2: **563 rps
against fakeredis's 429**, p99 **228 ms against 299 ms**.

The reason is that fakeredis is *Python*. It runs the same Lua, in-process, under
the same GIL as the web server's own worker threads - so every limiter check
contends with the request handling it is supposed to be measuring. A real Redis is
a C process on its own cores, and the TCP round trip costs less than the GIL
contention it removes.

So the asterisk was pessimistic, not optimistic, and the honest reading of the old
table is that the Redis-Lua row understated its own case by 30%. The new number is
still a lower bound: this Redis is inside a VM, so it pays a boundary crossing a
co-located server would not.

### What a real server tests that a fake cannot

`SHORTENER_REDIS_URL=redis://... pytest tests/test_real_redis.py` - 9 tests, and
two of them could not have existed before:

**Lua atomicity across real processes.** Eight threads, each on its *own*
connection, racing for a 50-token bucket. Exactly 50 admitted. fakeredis has no
other process to race with, so it can only ever confirm that the script is
single-threaded-correct - which is not the property the script exists for.

**The pub/sub bus, previously never executed.** `RedisPubSubBus` was written,
labelled unrun, and is now exercised: the push arrives, the durable log records it
regardless, and a broken Redis costs latency rather than correctness.

And one test demonstrates *why* pub/sub alone is the wrong shape rather than
asserting it: a subscriber that was not listening when the message went out
receives nothing, forever, with no error anywhere - while the log replay catches
it up immediately.

## The Postgres link store

`LINK_BACKEND=postgres SHORTENER_PG_DSN=postgresql://...` swaps the store.
`tests/test_pgstore.py` runs **one conformance suite over both backends**, so
"interface-compatible" is checked rather than claimed - 25 tests, all passing on
both.

### Postgres is 3x slower here, and that is not the argument against it

| backend | rps | p50 | p99 |
|---|---|---|---|
| SQLite | **1,900** | **32.1 ms** | **64.5 ms** |
| Postgres 18.6 | 632 | 87.3 ms | 313.4 ms |

Every cache miss now crosses a TCP boundary that a local file does not have, and
on one machine that is a straight loss. Anyone choosing Postgres for
single-node read throughput is choosing wrong.

The reason to run it is that **SQLite cannot be shared**. Two instances against
one SQLite file over a network filesystem is a corruption story, not a scaling
one - so the SQLite number is not "the same service, faster", it is a different
deployment that happens to be one process. The moment there is a second instance,
the comparison stops being 1,900-vs-632 and starts being 632-vs-doesn't-work.

### Three things that are genuinely different, not just dialect

* **Creation is one statement.** SQLite catches `IntegrityError` to detect a code
  collision; Postgres uses `ON CONFLICT DO NOTHING RETURNING code`. Same outcome
  without using an exception for control flow across a network.
* **Expiry is a real `TIMESTAMPTZ`** with a **partial** index (`WHERE expires_at
  IS NOT NULL` - permanent links are the overwhelming majority and a full index
  would carry every one of them forever). That enables `purge_expired()`, which
  SQLite cannot do usefully: with expiry as an integer and no scheduler, an
  expired link nobody requests sits on disk forever. It returns a count, so an
  operator can tell "nothing to delete" from "the job never ran".
* **Hit counting adds inside the database.** Eight threads flushing counters
  concurrently lose nothing; a read-then-write in the client drops a batch
  whenever two flushes land together, and Postgres is the backend that actually
  has other writers to race.

One bug worth recording: passing `NULL` into `CASE WHEN %s IS NULL THEN NULL ELSE
to_timestamp(%s/1000.0) END` fails with `could not determine data type of
parameter $4` - an untyped NULL inside a CASE has no inferable type. Converting in
Python and passing a real `datetime | None` lets the driver carry the type with
the value, which is what it is for.

## Abuse controls

A shortener is an open redirector with a database. Every one of them becomes a
phishing channel, so this is not an optional feature.

**Policy runs at create, not at resolve.** Once per link instead of once per click
keeps it off a path that has to run at a thousand requests a second. The cost is
real and stated: an abuser can probe the blocklist. Takedown is what covers it.

Three outcomes, not two:

| destination | outcome | why |
|---|---|---|
| `https://bit.ly/x` (or `evil.bit.ly`) | **refuse** | chaining a shortener hides the destination from every downstream scanner - the most common evasion, and a two-line fix |
| `http://169.254.169.254/latest/meta-data/` | **refuse** | a redirector that emits redirects into private space is an SSRF pivot for anything that follows them server-side, which is most link previewers |
| `https://promo.xyz/free` | **interstitial** | most suspicious links are not provably malicious; a hard block on a maybe is a support ticket, an interstitial costs the attacker their automation |
| anything else | allow | |

The suffix match is on labels, so `evil.bit.ly` matches `bit.ly` and `notbit.ly`
does **not** - a blocklist that a single extra label defeats is not a blocklist,
and a naive `endswith` gives you both bugs at once.

The interstitial escapes the destination (rendering an attacker-supplied URL raw
is stored XSS on your own domain, handed to you by whoever made the link) and
**fetches nothing from it** - not a favicon, not a preview image. One fetch
confirms to the attacker that the link was opened.

### Takedown, and the part that is not solved

`POST /admin/takedown/{code}` marks the code, purges it from *this* instance's
cache, and is checked **before** the cache on the read path - checking after would
let a hot code keep redirecting from cache after it was withdrawn, which is the
entire failure mode a takedown exists to prevent.

### Cross-instance invalidation, and why it is a log rather than a message

Across instances this used to not work at all, and the response said so instead
of reporting success. It works now, and the shape of the fix is the point:
**cache invalidation across instances is a distributed-systems problem, not a
cache problem.**

The obvious implementation - publish a message, everyone deletes - is
*at-most-once delivery of a correctness-critical event*. An instance that is
restarting, GC-paused or briefly partitioned when the message goes out never
learns, and it keeps serving a phishing link with nothing raising anywhere.

So invalidations go to a **durable, replayable log**: a monotonic sequence, and
each instance tracks the last sequence it applied. That turns invalidation into
*state* rather than an event you had to be awake for - an instance that was down
replays what it missed, a partitioned one catches up when it heals, and a brand
new one is made correct by replaying from zero. Duplicate delivery costs nothing
because eviction is idempotent, and the cursor advances only after a batch is
applied, so a crash mid-poll replays rather than skips.

`synchronous=FULL` on this one database, against `NORMAL` everywhere else in the
service. Losing the last few invalidations to power loss means a withdrawn
phishing link quietly comes back; that is not a durability trade anyone takes to
save an fsync on an event that happens a few times a day.

**Redis pub/sub is written and deliberately left as the incomplete half.** The
production shape is pub/sub *plus* the log - push for latency, poll for
certainty - and the log is the part that cannot be skipped, which is why it is
the part that is implemented and tested.

### A modelling error the measurement caught

`measure_propagation` first asserted the mean would land under one poll interval.
It came in at **1.18x** with three instances, and the model was wrong rather than
the code: a takedown is done when the **last** instance converges, so the fleet's
wait is the maximum of N per-instance draws, not one of them.

    E[max of N uniform(0, T)] = T * N / (N + 1)

That is 0.50T at one instance, 0.75T at three, and **0.91T at ten**. Propagation
*degrades as the fleet grows*, and a window measured on a single instance
understates a real deployment - in the direction that matters. The function now
reports the theoretical expectation next to the measurement so the two can be
compared instead of the measurement standing alone.

### What it still refuses to claim

```json
{"code": "G3zKeXrXIu", "reason": "phishing", "local_cache_purged": true,
 "invalidation_seq": 1,
 "propagation": "published to the invalidation log at seq 1; every instance
  polling at 1.0s has applied it within that bound, including instances that
  were down when it was written",
 "confirmed_on_all_instances": false,
 "why_not_confirmed": "no instance knows how many instances exist; this is a
  propagation bound, not an acknowledgement"}
```

A bound is not an acknowledgement. No instance knows how many instances there
are, so confirming would need registration and acks - service discovery this repo
does not have. `confirmed_on_all_instances` is there because `propagation` now
*sounds* reassuring, and the distinction has to survive that.

Every test for this builds **at least two caches with separate cursors**, because
a single-instance test cannot fail on the bug being fixed: the old code purged
locally, returned success, and passed everything.

### The half-open probe, now tested under threads

The circuit breaker's "exactly one probe" rule was only ever checked
sequentially, which is where that class of invariant always holds. The new test
puts **64 threads on a barrier** and releases them the instant the cooldown
expires: exactly 1 is admitted, 63 are short-circuited. A service that has just
fallen over should not receive 64 simultaneous probes, and a for-loop cannot tell
you whether it will.

## Honesty notes

* **Every throughput number here shares a machine with its own load generator.**
  The open-loop runs mark themselves invalid when the generator's scheduling
  backlog exceeds 25% of the measured p99, which catches the harness being the
  bottleneck; they cannot catch CPU contention between the two processes. Treat
  the absolute ceiling as a shape, not a capacity.
* **The 24x coordinated-omission ratio is the robust part.** It is a paired
  measurement on the same server process seconds apart, so contention moves both
  arms together. The absolute rps figures are the fragile part.
* **Takedown propagation is a bound, not a confirmation.** Every instance
  converges within one poll interval, including instances that were down when the
  takedown was written - but nothing acknowledges, because no instance knows how
  many instances exist. The API says that on every call.
* **The Redis results come from a server inside WSL2**, so every command crosses
  a VM boundary. That makes the real-Redis throughput figure a lower bound, not an
  upper one - a co-located server would do better, not worse.

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
