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
