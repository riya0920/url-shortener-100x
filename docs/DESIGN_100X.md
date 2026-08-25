# Design review: the URL shortener at 100x

**Status:** design doc for a scale we have not reached. Every number in the
"today" column is either configuration or arithmetic; nothing here is presented
as a measured production result, because there is no production.

---

## 1. What we have and what 100x means

| | today (design target) | 100x |
|---|---|---|
| resolve QPS (peak) | 1,000 | 100,000 |
| create QPS (peak) | 10 | 1,000 |
| links stored | 50 M | 5 B |
| unique links/day | 500 K | 50 M |

Read:write is **100:1** and that ratio drives every decision below. This is a
read-mostly system with a small, append-only write path, which is a much easier
problem than it first appears - and mistaking it for a write-heavy one is how
these designs get over-built.

## 2. Capacity arithmetic

### Storage

One row: code (~11 B) + target URL (~200 B avg, 2 KB cap) + timestamps (16 B) +
row overhead (~40 B) ≈ **270 B**.

```
5 B links x 270 B                  = 1.35 TB of row data
primary key index (code -> row)     ~ 5 B x 30 B  = 150 GB
                                    ------------------------
total                               ~ 1.5 TB
```

**1.5 TB fits on one machine.** A single 2 TB NVMe instance holds the entire
corpus at 100x. This is the number that decides Postgres vs Cassandra in §5, and
it is the reason the answer is Postgres.

Growth: 50 M links/day x 270 B = **13.5 GB/day, ~5 TB/year**. So single-node
storage has roughly a 2-3 year runway at 100x before sharding is forced - not
"never", but far enough out that building for it now is speculative.

### Cache

Short-link traffic is extremely zipfian: a small number of links carry most of
the traffic, and freshly created links dominate.

```
working set: top 10 M links x (11 B key + 200 B value + ~60 B overhead)
           = 10 M x 271 B ~ 2.7 GB
```

**2.7 GB is nothing.** A single 8 GB cache node holds a 10 M-link working set
with room to spare. At a 95% hit ratio:

```
100,000 resolve QPS x 5% miss = 5,000 QPS to the database
```

5,000 point-lookup QPS on an indexed primary key is comfortable for one Postgres
instance. **The cache hit ratio is therefore the single most important number in
this system** - at 90% it doubles to 10,000 QPS, and at 80% it doubles again.
This is why hit ratio is a first-class metric in `/metrics` rather than an
afterthought.

### Bandwidth

Redirect response ≈ 300 B of headers. 100,000 QPS x 300 B = **30 MB/s = 240
Mbps** egress. Not a constraint.

### Rate limiter

One bucket per client: key (~40 B) + hash of two fields (~100 B) ≈ 140 B. One
million active clients ≈ **140 MB**. Keys expire after refill-from-empty time, so
this does not grow without bound - the `PEXPIRE` in the Lua script is what makes
that true, and omitting it is a slow memory leak that only shows up in month
three.

## 3. Alternatives considered, per layer

### 3.1 Short-code generation

| option | verdict |
|---|---|
| auto-increment integer | **rejected.** Coordination on the hot path; the sequence becomes the write bottleneck exactly when you scale out. Also enumerable - `/1`, `/2` walks the corpus. |
| random base62 + uniqueness check | **rejected.** Requires a read + retry loop on every create. Works fine, but the check is unremovable and collision probability grows with the corpus. |
| UUID4 | **rejected.** 22+ characters is a bad short link, and random ids destroy index insert locality. |
| **snowflake (ts \| instance \| seq)** | **chosen.** No hot-path coordination, no uniqueness check, k-sorted for index locality. |

**What it costs.** Instance ids must be unique - that is still coordination, just
moved to startup, and a duplicated instance id silently produces duplicate codes.
Ids leak creation time and, given two links, a rough creation rate. Acceptable
for public short links; **not** acceptable if codes were capability tokens, which
is the condition under which this decision flips.

**Failure handling:** a backwards clock step could repeat a (ms, sequence) pair.
Small drift spins until the clock catches up; a jump over 5 s raises rather than
risking a duplicate. Loud beats silently wrong.

### 3.2 Rate limiter placement

| option | verdict |
|---|---|
| in-process, per instance | **rejected.** Budget multiplies by the instance count, and it resets on deploy. Fine only if the limit is advisory. |
| at the load balancer / CDN edge | **strong option, deferred.** Cheapest possible enforcement - rejected requests never reach us. But per-API-key logic and custom quotas are awkward there. **At 100x this becomes the right first line of defence**, with the application limiter as the second. |
| **shared store (Redis) + Lua** | **chosen for now.** One authoritative budget across all instances, atomic, with per-key logic we control. |
| sidecar with local buckets + async reconciliation | **rejected.** Lower latency, but eventually-consistent budgets mean bursts leak. Right answer at very high QPS if you can tolerate approximate limits - we cannot state a bound on the leak, so we did not take it. |

### 3.3 Algorithm: token bucket vs the alternatives

| option | verdict |
|---|---|
| fixed window counter | **rejected.** The boundary problem: a client can send 2x its budget across a window edge (full budget at 00:59, full budget again at 01:00). |
| sliding window log | **rejected.** Exact, but stores a timestamp per request - memory grows with traffic, and a heavy client is the one that costs you most. |
| sliding window counter | reasonable approximation; more complex than token bucket for no benefit here. |
| **token bucket** | **chosen.** O(1) memory per client, allows a controlled burst (capacity) while bounding the sustained rate (refill), and refills lazily so there is no timer and no drift. |

**What traffic pattern breaks token bucket:** a client that idles long enough to
fill its bucket and then dumps the entire capacity instantly. That burst is
*allowed by design* - capacity is exactly how much burst we permit. If bursts are
the thing being protected against rather than the sustained rate, capacity must
shrink toward 1, and at that point a sliding window is the better tool.

### 3.4 Cache strategy

| option | verdict |
|---|---|
| **cache-aside** | **chosen.** The app owns the cache logic; a cache outage degrades to slower, not broken. |
| read-through | equivalent behaviour, but couples us to a caching library's semantics. |
| write-through | **rejected.** Optimises writes we barely have (100:1 read:write) and caches links nobody may ever resolve. |
| no cache | **rejected.** 100,000 QPS of point lookups is achievable but wasteful and leaves no headroom. |

### 3.5 Database

Covered in §5, because "convince me you don't need Cassandra" deserves its own
section.

## 4. Failure modes

### 4.1 Redis dies - what happens in the next 500 ms

**Exactly this, in order:**

1. `t+0 ms` - the first `EVALSHA` fails with a connection error.
2. `t+0 ms` - `FailOpenLimiter` catches it, increments `fail_open_count`, and
   returns `allowed=True`. **The request proceeds.**
3. `t+0..500 ms` - every subsequent request does the same. At 1,000 QPS that is
   ~500 requests admitted without limit enforcement.
4. Meanwhile the resolve path is **unaffected**: the LRU cache is in-process and
   the database is a separate dependency. Resolves keep serving at full speed.
5. `fail_open_count` climbing triggers an alert within one scrape interval.

**We fail OPEN, and it is a product decision before it is a technical one.**
Failing closed turns a Redis outage into a *total* outage - the limiter, a
protective control, becomes the single point of failure for the whole service.
Failing open turns it into a window of unenforced limits during which the product
still works.

That trade is only acceptable because of what this limiter protects: fair use of
a public redirect service. The cost of a few unlimited minutes is extra load; the
cost of failing closed is total unavailability.

**Where this flips:** if the limiter guarded a payment endpoint, a metered
billing API, or an abuse-prone write path, unlimited requests would be the more
expensive failure and **closed** would be correct. The behaviour is therefore a
constructor flag, not a hardcoded assumption.

**Mitigation before it comes to that:** a local per-instance bucket as a
fallback, sized to `global_budget / instance_count`. Approximate, but far better
than unlimited. Not built - noted as the first thing to add.

### 4.2 A link goes viral: 100 K resolves/sec on one key

Layer by layer:

1. **Load balancer** - spreads across instances. No single-key affinity, so this
   is just traffic.
2. **In-process LRU** - after the first resolve on each instance, every
   subsequent hit is served from memory. A hot key is the *best* case for a
   cache: it is always resident and never evicted. At N instances this costs N
   cache misses total, forever.
3. **The dangerous moment is expiry.** When the TTL lapses, every concurrent
   request for that key misses simultaneously - the classic stampede, where a
   miss that should cost one query costs 100,000.
4. **SingleFlight** collapses those concurrent misses into exactly one backend
   call per instance; the rest wait on the leader's result. `collapsed` is a
   metric. `test_singleflight_collapses_a_stampede` measures it under 16
   concurrent threads.
5. **Database** sees N queries (one per instance), not 100,000.

**The subtle bug worth naming:** an obvious "fix" is a per-key lock around the
load. That *serialises* the stampede without *collapsing* it - the backend still
sees N calls, just one at a time - which is strictly worse than nothing, because
it adds latency without removing load. The first version of `SingleFlight` here
had exactly that bug and the test caught it.

**Remaining exposure:** hit counting. Every resolve writes to `link_hits`, so a
viral link is 100,000 writes/sec to one row. At 100x this must become
fire-and-forget batched counters (in-memory aggregation flushed every few
seconds, accepting bounded loss on crash) or a stream. Analytics accuracy is
worth strictly less than redirect availability.

### 4.3 Database failover

Resolves survive on cache for the cache TTL. Creates fail - correctly, with 503;
a shortener that accepts a link it did not store is worse than one that says no.
Recovery is bounded by the failover, and the cache is the buffer that makes it
invisible to most users. **What makes this survivable is the read:write ratio:**
99% of traffic never needs the primary.

### 4.4 Cache node loss

The LRU is in-process, so losing an instance loses only its cache. The replacement
starts cold and its miss rate spikes until the working set refills - with a
zipfian distribution that is fast, seconds not minutes. This is a strong argument
for keeping the cache in-process rather than centralising it: there is no shared
cache tier to lose.

## 5. Postgres at 100x - convince me you don't need Cassandra

**The claim: Postgres, and this is not a close call at this scale.**

1. **The data fits.** 1.5 TB on one node (§2). Cassandra's core value is
   horizontal scale past a single machine, and we are not past it.
2. **The access pattern is a single-key point lookup** on an immutable row.
   Postgres serves that from an index in well under a millisecond. There is no
   query Cassandra answers *faster* here.
3. **Writes are trivial.** 1,000 creates/sec of ~270 B rows is nothing for a
   single Postgres instance. Write throughput is Cassandra's strength and we do
   not need it.
4. **The cache absorbs the read volume**, so the database sees ~5,000 QPS, not
   100,000 (§2).
5. **Operational cost is real.** Cassandra means a cluster, repairs, compaction
   tuning, and eventual-consistency semantics in application code. For a
   read-mostly key-value workload that fits on one box, that is complexity with
   no corresponding benefit.

**What would change the answer:**

* **Multi-region active-active writes.** Cassandra's leaderless replication is
  genuinely better than Postgres for this, and if the product needs low-latency
  creates on three continents the calculus flips.
* **Corpus growth past ~2 TB with no archival.** At 5 TB/year we hit this in year
  two or three, at which point the choice is sharding Postgres by code prefix
  (straightforward - the key space is uniform and there are no cross-shard
  queries) or moving to a system that shards for us.
* **A write pattern we do not have today**, such as heavy per-link mutation or
  large analytics rows.

**Honest read of the tradeoff:** the strongest argument *for* Cassandra is
avoiding a migration later. The counter-argument is that sharding this particular
schema is unusually easy - immutable rows, uniformly distributed keys, no joins,
no transactions across links - so the migration we would be pre-paying for is one
of the cheapest possible. Pre-paying an expensive operational cost to avoid a
cheap future migration is a bad trade.

## 6. What we would measure in production

### SLIs and SLOs

| SLI | SLO | rationale |
|---|---|---|
| resolve availability | 99.99% | a redirect that fails breaks someone else's content |
| resolve latency p99 | < 50 ms server-side | the redirect is on a user's critical path |
| create availability | 99.9% | one order of magnitude looser; a failed create is retryable by a human |
| create latency p99 | < 200 ms | not user-blocking in the same way |
| cache hit ratio | > 90% | below this, database load doubles (§2) |
| limiter accuracy | within 5% of budget | verified in test; needs production confirmation |

### Alerts that would page

* `fail_open_count > 0` sustained - limits are not being enforced
* cache hit ratio below 85% - the database is about to take 3x its expected load
* resolve 5xx rate above 0.1%
* short-code collision - should be impossible; means duplicate `INSTANCE_ID`

### Deliberately *not* alerted

Create latency, hit-count lag, and cache eviction rate. These are dashboard
metrics: they inform capacity planning but nobody should be woken for them.

## 7. What this design does not solve

* **Abuse.** A public shortener is a phishing and malware laundry. Real
  deployment needs URL reputation checks at create time, a takedown path, and an
  interstitial for flagged targets. This is a bigger problem than the scaling
  work above and is entirely absent.
* **Custom aliases.** Vanity codes reintroduce the uniqueness check that
  snowflake ids let us avoid, and need their own namespace and reservation flow.
* **Analytics beyond a counter.** Referrers, geography, and time series are a
  separate pipeline; bolting them onto the resolve path would compromise it.
* **Multi-region.** Everything above assumes one region. Global creates need
  either regional instance-id ranges (easy - the id scheme already supports it)
  or a different database (§5).
