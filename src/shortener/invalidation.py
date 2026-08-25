"""Cross-instance cache invalidation - closing the gap takedown left open.

`TakedownList` could purge the local cache and nothing else. On one instance that
is a complete takedown; across N instances the withdrawn code keeps redirecting
from every other instance's LRU until the entry ages out, which for a cache with
no TTL is *forever*. The API said so on every call rather than reporting success,
and that honesty is not a fix.

This is the fix, and the thing worth understanding is that **cache invalidation
across instances is a distributed-systems problem, not a cache problem.** The
naive version - "publish a message, everyone deletes" - is at-most-once delivery
of a correctness-critical event. An instance that is restarting, GC-paused, or
briefly partitioned when the message goes out never learns, and it keeps serving
a phishing link with no error anywhere.

## The design: a durable log, not a fire-and-forget message

Every invalidation is appended to a table with a monotonic sequence number. Each
instance remembers the last sequence it applied and asks for everything after it.
That turns invalidation into **replayable state** rather than an event you had to
be awake for:

  * an instance that was down replays what it missed on the way back up
  * an instance that was partitioned catches up when the partition heals
  * a *new* instance can be brought up correct by replaying from zero
  * duplicate delivery is free, because applying an invalidation twice is a no-op

The cost is that it is polled rather than pushed, so propagation is bounded by
the poll interval rather than by network latency. `measure_propagation` measures
that bound rather than quoting the configured interval, and the two differ in a
way that is easy to get backwards: a takedown is done when the **last** instance
converges, so the fleet's wait is the max of N per-instance draws, and

    E[max of N uniform(0, T)] = T * N / (N + 1)

which is 0.50T at one instance and **0.91T at ten**. Propagation degrades as the
fleet grows, and a window measured on a single instance understates a deployment.

## Why not Redis pub/sub

`RedisPubSubBus` below is the same interface over Redis. It is written and
**never executed**, because there is no Redis server in this environment. It is
also, on its own, the *wrong* choice for this job: Redis pub/sub is fire-and-
forget, so a subscriber that is disconnected for two seconds silently misses
every invalidation in that window and no error is raised anywhere. The
production shape is pub/sub for latency **plus** a durable log for correctness - push to make it fast, poll to make it certain. The log is the part that cannot be
skipped, which is why it is the part that is implemented and tested here.

## What is still not solved

A takedown is not complete until every instance has applied it, and no instance
knows how many instances there are. So `TakedownList.add` reports the **bound**
("every instance polling at interval T has applied this within T") and sets
`confirmed_on_all_instances: false` rather than letting a reassuring-sounding
`propagation` field read as success. Turning that bound into a real confirmation
needs instance registration and acknowledgement, which is a service-discovery
problem this repo does not have and does not pretend to.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS invalidations (
    seq        INTEGER PRIMARY KEY AUTOINCREMENT,
    code       TEXT NOT NULL,
    reason     TEXT,
    created_at REAL NOT NULL
);
-- Deliberately NOT unique on code. The same code can be invalidated more than
-- once (taken down, restored, taken down again), and each is a distinct event
-- that every instance must apply in order.
CREATE INDEX IF NOT EXISTS idx_inval_seq ON invalidations(seq);
"""


class SqliteInvalidationBus:
    """Durable, replayable invalidation log shared by every instance.

    SQLite here for the same reason as everywhere else in this repo: it is what
    exists. The shape is what matters and it ports directly to a Postgres table
    or a Kafka topic - an append-only sequence that consumers track an offset
    into. Nothing about the design depends on it being SQLite.
    """

    def __init__(self, path: str, poll_interval_s: float = 1.0):
        self.path = path
        self.poll_interval_s = poll_interval_s
        self._local = threading.local()
        self._lock = threading.Lock()
        # Per-subscriber cursors, keyed by instance id. A single shared cursor
        # would mean whichever instance polled first consumed the event for
        # everybody -- a queue, when what this needs is a broadcast.
        self._cursors = {}
        self.applied = 0
        self.polls = 0
        con = self._connect()
        con.executescript(SCHEMA)
        con.commit()

    def _connect(self):
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA busy_timeout=30000")
            # FULL, not NORMAL. Everywhere else in this service NORMAL is the
            # right trade; here it is not. Losing the last few invalidations on
            # power loss means a withdrawn phishing link quietly comes back, and
            # that is not a durability trade anyone would take to save an fsync
            # on an event that happens a handful of times a day.
            con.execute("PRAGMA synchronous=FULL")
            self._local.con = con
        return con

    # -- publishing --------------------------------------------------------

    def publish(self, code: str, reason: str = "") -> int:
        """Append an invalidation. Returns its sequence number."""
        con = self._connect()
        cur = con.execute(
            "INSERT INTO invalidations (code, reason, created_at) VALUES (?,?,?)",
            (code, reason, time.time()))
        con.commit()
        return cur.lastrowid

    def head(self) -> int:
        con = self._connect()
        row = con.execute("SELECT COALESCE(MAX(seq), 0) FROM invalidations").fetchone()
        return row[0] if row else 0

    # -- subscribing -------------------------------------------------------

    def register(self, instance_id: str, from_seq: int = None) -> int:
        """Start tracking an instance's cursor.

        `from_seq=None` means "replay everything", which is the correct default
        for a *new* instance: it has an empty cache, so replaying is cheap, and
        starting at the head would leave it correct only by luck. An instance
        restoring a persisted cache should pass the sequence it had applied when
        that cache was written.
        """
        with self._lock:
            self._cursors[instance_id] = 0 if from_seq is None else from_seq
        return self._cursors[instance_id]

    def poll(self, instance_id: str, apply_fn) -> int:
        """Apply everything this instance has not seen. Returns how many.

        `apply_fn(code)` must be idempotent, which cache eviction naturally is.
        The cursor advances only after the batch is applied, so a crash mid-poll
        replays rather than skips -- at-least-once, which for an idempotent
        operation is exactly right.
        """
        with self._lock:
            cursor = self._cursors.get(instance_id, 0)
        con = self._connect()
        rows = con.execute(
            "SELECT seq, code FROM invalidations WHERE seq > ? ORDER BY seq", (cursor,)).fetchall()
        self.polls += 1
        if not rows:
            return 0
        for _seq, code in rows:
            apply_fn(code)
            self.applied += 1
        with self._lock:
            self._cursors[instance_id] = rows[-1][0]
        return len(rows)

    def cursor(self, instance_id: str) -> int:
        with self._lock:
            return self._cursors.get(instance_id, 0)

    def lag(self, instance_id: str) -> int:
        """How many invalidations this instance has not yet applied."""
        return max(self.head() - self.cursor(instance_id), 0)

    def stats(self) -> dict:
        return {"head": self.head(), "applied": self.applied, "polls": self.polls,
                "poll_interval_s": self.poll_interval_s,
                "instances": {k: v for k, v in self._cursors.items()}}


class InvalidationPoller:
    """Background thread that drains the bus into one instance's cache."""

    def __init__(self, bus: SqliteInvalidationBus, instance_id: str, cache,
                 interval_s: float = None):
        self.bus = bus
        self.instance_id = instance_id
        self.cache = cache
        self.interval_s = interval_s if interval_s is not None else bus.poll_interval_s
        self._stop = threading.Event()
        self._thread = None
        bus.register(instance_id)

    def _run(self):
        while not self._stop.is_set():
            try:
                self.bus.poll(self.instance_id, self.cache.invalidate)
            except Exception:
                # A failed poll must not kill the thread: the next one replays
                # from the same cursor, so the only cost of a transient error is
                # one interval of extra staleness.
                pass
            self._stop.wait(self.interval_s)

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def drain_now(self) -> int:
        """Apply immediately, out of band. Used by tests and by the admin path.

        The publishing instance calls this on itself right after a takedown, so
        its own cache is correct before the response is written -- there is no
        reason for the instance that handled the request to wait a poll interval
        to learn about its own action.
        """
        return self.bus.poll(self.instance_id, self.cache.invalidate)


class RedisPubSubBus:
    """Same interface over Redis pub/sub. Written; NEVER EXECUTED.

    There is no Redis server here. Beyond that, this is deliberately kept as the
    *incomplete* half of the design: pub/sub is fire-and-forget, so a subscriber
    disconnected for two seconds misses every invalidation in that window and
    nothing anywhere raises. Shipping this alone would be a takedown that works
    almost always, which for a phishing control is the same as not working.

    The production shape is this **plus** the durable log above: publish for
    latency, poll for certainty, and let the log be the source of truth. That is
    why `last_seq` is carried in the message -- a subscriber whose sequence has
    skipped knows it missed something and can fall back to replaying the log
    rather than assuming the gap was nothing.
    """

    CHANNEL = "shortener:invalidate"

    def __init__(self, redis_client, log: SqliteInvalidationBus):
        self.redis = redis_client
        self.log = log

    def publish(self, code: str, reason: str = "") -> int:
        seq = self.log.publish(code, reason)          # durable first, always
        try:
            self.redis.publish(self.CHANNEL, "%d:%s" % (seq, code))
        except Exception:
            # The publish is best-effort by construction. The log already has
            # it, so every instance still converges within one poll interval;
            # losing the push costs latency, never correctness.
            pass
        return seq


def measure_propagation(bus: SqliteInvalidationBus, pollers, n: int = 20) -> dict:
    """Measure the real propagation window rather than quoting the interval.

    A takedown is not done when *an* instance has applied it, it is done when
    **every** instance has. So what this measures is the time until the last
    instance converges, and that is not the same distribution as one instance's
    wait.

    Each instance's wait is roughly uniform over [0, T] -- an invalidation
    published just after that instance polls waits nearly a whole interval, one
    published just before waits almost nothing. The fleet's wait is the **maximum
    of N such draws**, and

        E[max of N uniform(0, T)] = T * N / (N + 1)

    which is 0.50T for one instance, **0.75T for three**, and 0.91T for ten. The
    first version of this function asserted the mean would come in under T and
    was surprised when three instances produced 1.18T -- the model was wrong, not
    the code. Propagation to a whole fleet *degrades as the fleet grows*, and an
    SLA written from a single-instance measurement is wrong in the direction that
    matters.

    The measured mean also sits above the theoretical one for two reasons worth
    naming rather than smoothing away: the polling loop checks convergence on a
    5 ms granularity, and every publish pays an fsync because this log is
    `synchronous=FULL`. Both are real costs of the design, so both stay in the
    number.
    """
    latencies = []
    now_ms = lambda: int(time.time() * 1000)
    for i in range(n):
        code = "probe-%d" % i
        for p_ in pollers:
            p_.cache.put(code, "https://example.test/%d" % i)
        t0 = time.perf_counter()
        bus.publish(code, "propagation probe")
        deadline = t0 + max(p_.interval_s for p_ in pollers) * 8
        while time.perf_counter() < deadline:
            if all(p_.cache.get(code, now_ms()) is None for p_ in pollers):
                break
            time.sleep(0.005)
        latencies.append((time.perf_counter() - t0) * 1000.0)

    latencies.sort()
    n_inst = len(pollers)
    interval_ms = max(p_.interval_s for p_ in pollers) * 1000.0
    expected_mean = interval_ms * n_inst / (n_inst + 1)
    measured_mean = sum(latencies) / len(latencies)

    return {
        "samples": len(latencies),
        "instances": n_inst,
        "poll_interval_ms": interval_ms,
        "p50_ms": latencies[len(latencies) // 2],
        "p95_ms": latencies[int(0.95 * (len(latencies) - 1))],
        "max_ms": latencies[-1],
        "mean_ms": measured_mean,
        # The theoretical fleet-wide expectation, so the measurement has
        # something to be compared against rather than merely reported.
        "expected_mean_ms": expected_mean,
        "mean_over_expected": measured_mean / expected_mean if expected_mean else None,
        # The bound an SLA can be written against. Every instance polls at least
        # once per interval, so the worst case is one interval plus whatever a
        # single poll costs -- generous here for the 5 ms check granularity.
        "sla_bound_ms": interval_ms * 1.5,
        "within_sla_bound": latencies[-1] <= interval_ms * 1.5,
        "note": ("propagation to the FLEET is the max over instances, so it grows with fleet "
                 "size: E[max of N uniform(0,T)] = T*N/(N+1), i.e. 0.50T at one instance, 0.75T "
                 "at three, 0.91T at ten. A window measured on one instance understates a "
                 "deployment, in the direction that matters."),
    }
