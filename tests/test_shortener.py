"""Correctness under concurrency. These are the tests the project exists for.

Two of them spawn real processes, because the bugs being tested for (a
non-atomic limiter, colliding id generators) are invisible in a single process.
"""
import multiprocessing as mp
import os
import threading
import time

import pytest

from shortener.ids import (
    MAX_SEQUENCE,
    SnowflakeGenerator,
    decode_base62,
    decode_id,
    encode_base62,
)
from shortener.limiter import Decision, FailOpenLimiter, RateLimiter, SqliteStore
from shortener.store import LinkStore, LruCache, SingleFlight


# --------------------------------------------------------------------------
# id generation
# --------------------------------------------------------------------------

def test_base62_roundtrip():
    for n in [0, 1, 61, 62, 63, 12345, 2 ** 40, 2 ** 63 - 1]:
        assert decode_base62(encode_base62(n)) == n


def test_base62_rejects_invalid_characters():
    with pytest.raises(ValueError):
        decode_base62("abc-def")


def test_ids_are_unique_within_one_instance():
    gen = SnowflakeGenerator(1)
    ids = [gen.next_id() for _ in range(50_000)]
    assert len(set(ids)) == len(ids)


def test_ids_are_monotonic_within_one_instance():
    gen = SnowflakeGenerator(1)
    ids = [gen.next_id() for _ in range(10_000)]
    assert ids == sorted(ids)


def test_ids_are_unique_across_instances():
    """Different instance ids must never collide, even at the same millisecond."""
    frozen = lambda: 1_800_000_000_000
    gens = [SnowflakeGenerator(i, clock=frozen) for i in range(8)]
    ids = [g.next_id() for g in gens for _ in range(200)]
    assert len(set(ids)) == len(ids)


def test_id_decodes_to_its_parts():
    gen = SnowflakeGenerator(37, clock=lambda: 1_800_000_000_000)
    parts = decode_id(gen.next_id())
    assert parts["instance_id"] == 37
    assert parts["timestamp_ms"] == 1_800_000_000_000


def test_sequence_exhaustion_rolls_to_the_next_millisecond():
    """4096 ids in one ms is the documented ceiling; it must not wrap silently."""
    clock = {"ms": 1_800_000_000_000}
    gen = SnowflakeGenerator(0, clock=lambda: clock["ms"])
    ids = []
    for i in range(MAX_SEQUENCE + 1):
        ids.append(gen.next_id())
    # the next call would exhaust the millisecond; advance the clock so the
    # generator does not spin forever
    clock["ms"] += 1
    ids.append(gen.next_id())
    assert len(set(ids)) == len(ids)


def test_large_backwards_clock_jump_raises_instead_of_duplicating():
    clock = {"ms": 1_800_000_000_000}
    gen = SnowflakeGenerator(0, clock=lambda: clock["ms"])
    gen.next_id()
    clock["ms"] -= 60_000
    with pytest.raises(RuntimeError):
        gen.next_id()


def test_concurrent_generation_in_threads_is_unique():
    gen = SnowflakeGenerator(2)
    out, lock = [], threading.Lock()

    def worker():
        local = [gen.next_id() for _ in range(2000)]
        with lock:
            out.extend(local)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(set(out)) == len(out) == 16_000


# --------------------------------------------------------------------------
# rate limiter
# --------------------------------------------------------------------------

def test_bucket_allows_exactly_capacity_then_rejects():
    import tempfile

    path = os.path.join(tempfile.mkdtemp(), "rl.db")
    limiter = RateLimiter(SqliteStore(path), capacity=10, refill_per_second=0, clock=lambda: 1_000_000)
    allowed = sum(limiter.check("c1").allowed for _ in range(20))
    assert allowed == 10


def test_bucket_refills_over_time(tmp_path):
    now = {"ms": 1_000_000}
    limiter = RateLimiter(SqliteStore(str(tmp_path / "rl.db")), capacity=10, refill_per_second=5,
                          clock=lambda: now["ms"])
    for _ in range(10):
        assert limiter.check("c1").allowed
    assert not limiter.check("c1").allowed

    now["ms"] += 1000        # one second -> 5 tokens
    allowed = sum(limiter.check("c1").allowed for _ in range(10))
    assert allowed == 5


def test_refill_is_capped_at_capacity(tmp_path):
    now = {"ms": 1_000_000}
    limiter = RateLimiter(SqliteStore(str(tmp_path / "rl.db")), capacity=10, refill_per_second=100,
                          clock=lambda: now["ms"])
    limiter.check("c1")
    now["ms"] += 3_600_000   # an hour idle
    allowed = sum(limiter.check("c1").allowed for _ in range(50))
    assert allowed == 10, "an idle bucket must not accumulate beyond capacity"


def test_buckets_are_isolated_per_client(tmp_path):
    limiter = RateLimiter(SqliteStore(str(tmp_path / "rl.db")), capacity=5, refill_per_second=0,
                          clock=lambda: 1_000_000)
    assert sum(limiter.check("a").allowed for _ in range(5)) == 5
    assert not limiter.check("a").allowed
    assert limiter.check("b").allowed, "client b must have its own bucket"


def test_retry_after_is_populated_when_rejected(tmp_path):
    limiter = RateLimiter(SqliteStore(str(tmp_path / "rl.db")), capacity=1, refill_per_second=2,
                          clock=lambda: 1_000_000)
    assert limiter.check("c").allowed
    d = limiter.check("c")
    assert not d.allowed and d.retry_after_s > 0


def _hammer(args):
    """Runs in a separate PROCESS: shared state is only the SQLite file."""
    db_path, client, attempts, capacity, rate, now_ms = args
    limiter = RateLimiter(SqliteStore(db_path), capacity=capacity, refill_per_second=rate,
                          clock=lambda: now_ms)
    return sum(limiter.check(client).allowed for _ in range(attempts))


@pytest.mark.parametrize("n_procs", [4])
def test_limiter_accuracy_across_processes(tmp_path, n_procs):
    """THE test: a client sending 2x its budget through N instances.

    A non-atomic read-modify-write leaks roughly (n_procs - 1) x capacity here.
    The spec allows +/-5%; the atomic implementation should be exact.
    """
    db_path = str(tmp_path / "rl.db")
    SqliteStore(db_path)
    capacity, now_ms = 200, 1_000_000
    attempts_each = (capacity * 2) // n_procs

    with mp.Pool(n_procs) as pool:
        results = pool.map(
            _hammer, [(db_path, "burst-client", attempts_each, capacity, 0.0, now_ms)] * n_procs
        )

    total_allowed = sum(results)
    assert abs(total_allowed - capacity) <= capacity * 0.05, (
        "limiter leaked: allowed %d against a budget of %d (per-process: %r)"
        % (total_allowed, capacity, results)
    )


def test_fail_open_allows_traffic_when_the_store_is_down():
    class Broken:
        def consume(self, *a, **k):
            raise ConnectionError("redis is gone")

    guarded = FailOpenLimiter(RateLimiter(Broken()), fail_open=True)
    assert guarded.check("c").allowed
    assert guarded.fail_open_count == 1


def test_fail_closed_is_available_and_rejects():
    class Broken:
        def consume(self, *a, **k):
            raise ConnectionError("redis is gone")

    guarded = FailOpenLimiter(RateLimiter(Broken()), fail_open=False)
    assert not guarded.check("c").allowed
    assert guarded.error_count == 1


# --------------------------------------------------------------------------
# store, cache, singleflight
# --------------------------------------------------------------------------

def test_expired_links_do_not_resolve(tmp_path):
    store = LinkStore(str(tmp_path / "l.db"))
    store.create("abc", "https://example.com", ttl_seconds=10, now_ms=1_000_000)
    link = store.get("abc")
    assert not link.is_expired(1_005_000)
    assert link.is_expired(1_011_000)


def test_duplicate_code_is_rejected(tmp_path):
    store = LinkStore(str(tmp_path / "l.db"))
    store.create("abc", "https://example.com")
    with pytest.raises(KeyError):
        store.create("abc", "https://other.com")


def test_cache_expiry_is_enforced_on_read():
    cache = LruCache(capacity=10)
    cache.put("k", "https://example.com", expires_ms=1_000)
    assert cache.get("k", 999) == "https://example.com"
    assert cache.get("k", 1_001) is None


def test_cache_evicts_least_recently_used():
    cache = LruCache(capacity=3)
    for k in "abc":
        cache.put(k, k)
    cache.get("a", 0)          # 'a' becomes most recent, so 'b' is next out
    cache.put("d", "d")
    assert cache.get("b", 0) is None
    assert cache.get("a", 0) == "a"


def test_hit_ratio_is_tracked():
    cache = LruCache()
    cache.put("k", "v")
    cache.get("k", 0)
    cache.get("missing", 0)
    assert cache.hit_ratio == 0.5


def test_singleflight_collapses_a_stampede():
    """The cache-stampede mitigation, measured rather than asserted."""
    flight = SingleFlight()
    calls = []
    barrier = threading.Barrier(16)

    def slow_load():
        calls.append(1)
        time.sleep(0.05)
        return "value"

    def worker():
        barrier.wait()
        return flight.do("hot-key", slow_load)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    [t.start() for t in threads]
    [t.join() for t in threads]

    # Without singleflight this would be 16 backend calls.
    assert len(calls) < 16
    assert flight.collapsed > 0


def test_singleflight_does_not_serialise_unrelated_keys():
    flight = SingleFlight()
    order = []

    def make(key, delay):
        def fn():
            time.sleep(delay)
            order.append(key)
            return key
        return fn

    t1 = threading.Thread(target=lambda: flight.do("slow", make("slow", 0.15)))
    t2 = threading.Thread(target=lambda: flight.do("fast", make("fast", 0.01)))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert order[0] == "fast", "a slow key must not block an unrelated key"


# --------------------------------------------------------------------------
# the Redis Lua path -- exercised against fakeredis, which runs real Lua
# --------------------------------------------------------------------------

def _redis_limiter(capacity, rate, clock):
    import fakeredis

    from shortener.limiter import RedisStore

    return RateLimiter(RedisStore(fakeredis.FakeStrictRedis()), capacity=capacity,
                       refill_per_second=rate, clock=clock)


def test_lua_script_executes_and_enforces_capacity():
    """The Lua path is real code that can be wrong; running it found two bugs.

    PEXPIRE rejects a float (math.ceil returns one under Lua 5.1), and a zero
    refill rate divided by zero. Neither is visible by reading the script.
    """
    lim = _redis_limiter(10, 0, lambda: 1_000_000)
    assert sum(lim.check("c").allowed for _ in range(20)) == 10


def test_lua_refill_matches_elapsed_time():
    now = {"ms": 1_000_000}
    lim = _redis_limiter(10, 5, lambda: now["ms"])
    for _ in range(10):
        lim.check("c")
    assert not lim.check("c").allowed
    now["ms"] += 1000
    assert sum(lim.check("c").allowed for _ in range(10)) == 5


def test_lua_refill_is_capped_at_capacity():
    now = {"ms": 1_000_000}
    lim = _redis_limiter(10, 100, lambda: now["ms"])
    lim.check("c")
    now["ms"] += 3_600_000
    assert sum(lim.check("c").allowed for _ in range(50)) == 10


def test_both_backends_agree_exactly(tmp_path):
    """Cross-backend equivalence: two implementations of one algorithm.

    This is the test that keeps the SQLite path honest as a stand-in for Redis.
    If they ever disagree, the local development experience stops predicting
    production behaviour -- which is the whole reason for having two backends.
    """
    now = {"ms": 1_000_000}
    clock = lambda: now["ms"]
    sqlite_lim = RateLimiter(SqliteStore(str(tmp_path / "rl.db")), capacity=25, refill_per_second=7,
                             clock=clock)
    redis_lim = _redis_limiter(25, 7, clock)

    seq = []
    for step in range(120):
        if step % 17 == 0:
            now["ms"] += 500
        a = sqlite_lim.check("client-a").allowed
        b = redis_lim.check("client-a").allowed
        seq.append((a, b))
    mismatches = [(i, a, b) for i, (a, b) in enumerate(seq) if a != b]
    assert not mismatches, "backends diverged at steps: %r" % mismatches[:5]


# --------------------------------------------------------------------------
# circuit breaker -- the gap the fail-open drill exposed
# --------------------------------------------------------------------------

def test_breaker_opens_after_consecutive_failures():
    from shortener.breaker import CircuitBreaker, CircuitOpenError, CircuitState

    now = {"t": 0.0}
    b = CircuitBreaker(failure_threshold=3, cooldown_s=5.0, clock=lambda: now["t"])

    def boom():
        raise ConnectionError("store is gone")

    for _ in range(3):
        with pytest.raises(ConnectionError):
            b.call(boom)
    assert b.state is CircuitState.OPEN
    # Now calls short-circuit instead of attempting and timing out.
    with pytest.raises(CircuitOpenError):
        b.call(boom)
    assert b.stats()["short_circuited"] == 1


def test_a_single_success_resets_the_failure_run():
    """The threshold counts CONSECUTIVE failures; an intermittent blip must not trip it."""
    from shortener.breaker import CircuitBreaker, CircuitState

    b = CircuitBreaker(failure_threshold=3)

    def boom():
        raise ConnectionError("blip")

    for _ in range(2):
        with pytest.raises(ConnectionError):
            b.call(boom)
    b.call(lambda: "ok")
    with pytest.raises(ConnectionError):
        b.call(boom)
    assert b.state is CircuitState.CLOSED


def test_half_open_lets_exactly_one_probe_through():
    """A burst at a service that just recovered is how you knock it over again."""
    from shortener.breaker import CircuitBreaker, CircuitState

    now = {"t": 0.0}
    b = CircuitBreaker(failure_threshold=1, cooldown_s=5.0, clock=lambda: now["t"])
    b.record_failure()
    assert b.state is CircuitState.OPEN

    now["t"] = 6.0
    assert b.allow(), "the first caller after the cooldown is the probe"
    assert b.state is CircuitState.HALF_OPEN
    assert not b.allow(), "everyone else waits while the probe is in flight"
    assert not b.allow()


def test_a_failed_probe_reopens_and_restarts_the_cooldown():
    from shortener.breaker import CircuitBreaker, CircuitState

    now = {"t": 0.0}
    b = CircuitBreaker(failure_threshold=1, cooldown_s=5.0, clock=lambda: now["t"])
    b.record_failure()
    now["t"] = 6.0
    assert b.allow()
    b.record_failure()                       # probe failed
    assert b.state is CircuitState.OPEN
    assert not b.allow(), "cooldown must restart, not resume"
    now["t"] = 12.0
    assert b.allow()


def test_a_successful_probe_closes_the_circuit():
    from shortener.breaker import CircuitBreaker, CircuitState

    now = {"t": 0.0}
    b = CircuitBreaker(failure_threshold=1, cooldown_s=5.0, clock=lambda: now["t"])
    b.record_failure()
    now["t"] = 6.0
    b.allow()
    b.record_success()
    assert b.state is CircuitState.CLOSED
    assert b.allow()


def test_guarded_limiter_still_fails_open_but_stops_calling_the_dead_store():
    """The availability guarantee is unchanged; the latency cost of it collapses."""
    from shortener.breaker import BreakerGuardedLimiter

    calls = {"n": 0}

    class DeadStore:
        def consume(self, *a, **k):
            calls["n"] += 1
            raise ConnectionError("redis is gone")

    guarded = BreakerGuardedLimiter(RateLimiter(DeadStore()), fail_open=True,
                                    failure_threshold=3, cooldown_s=60.0)
    allowed = sum(guarded.check("c").allowed for _ in range(50))

    assert allowed == 50, "fail-open behaviour must be unchanged"
    assert calls["n"] == 3, "after the breaker trips, the dead store is not called again"
    assert guarded.stats()["breaker"]["short_circuited"] > 0


def test_guarded_limiter_can_fail_closed_too():
    from shortener.breaker import BreakerGuardedLimiter

    class DeadStore:
        def consume(self, *a, **k):
            raise ConnectionError("gone")

    guarded = BreakerGuardedLimiter(RateLimiter(DeadStore()), fail_open=False,
                                    failure_threshold=2, cooldown_s=60.0)
    assert not any(guarded.check("c").allowed for _ in range(20))
