"""The Redis paths, against a real Redis server.

Everything Redis in this repo has run against **fakeredis** — which executes the
same Lua, and is still an in-process Python reimplementation. Two classes of thing
it cannot tell you:

  * whether the Lua is atomic *across processes*, since fakeredis has no other
    process to race with
  * whether the pub/sub invalidation path works at all, since that half of
    `invalidation.py` was written and never executed

Both are checked here. The suite skips when `SHORTENER_REDIS_URL` is unset, so
the repo stays runnable without a server — and skipping is reported rather than
counted as a pass.
"""
import json
import os
import threading
import time

import pytest

from shortener.invalidation import SqliteInvalidationBus
from shortener.limiter import RateLimiter, RedisStore

URL = os.environ.get("SHORTENER_REDIS_URL")


def _client():
    import redis

    return redis.Redis.from_url(URL, socket_timeout=5)


pytestmark = pytest.mark.skipif(
    not URL, reason="set SHORTENER_REDIS_URL to run against a real Redis server")


@pytest.fixture
def r():
    c = _client()
    c.flushdb()
    yield c
    c.flushdb()


def test_the_server_is_a_real_redis_not_a_fake(r):
    """Guard against this file quietly passing against fakeredis and reporting a
    result it did not earn."""
    info = r.info("server")
    assert "redis_version" in info
    assert type(r).__module__.startswith("redis"), type(r).__module__


# --- the token bucket ------------------------------------------------------

def test_the_lua_bucket_enforces_its_budget_on_a_real_server(r):
    lim = RateLimiter(RedisStore(r), capacity=10, refill_per_second=0)
    allowed = sum(1 for _ in range(30) if lim.check("client").allowed)
    assert allowed == 10, "hard quota of 10 leaked %d" % allowed


def test_the_bucket_refills_over_real_wall_clock(r):
    lim = RateLimiter(RedisStore(r), capacity=2, refill_per_second=20)
    assert lim.check("c").allowed and lim.check("c").allowed
    assert not lim.check("c").allowed
    time.sleep(0.25)
    assert lim.check("c").allowed, "no refill after 250ms at 20/s"


def test_the_lua_is_atomic_across_real_concurrent_clients(r):
    """The claim fakeredis cannot test.

    Eight threads, each with its OWN connection, hammering the same key. If the
    read-modify-write inside the script were not atomic, the total admitted would
    exceed the capacity -- that is the whole reason the bucket is a Lua script
    rather than a GET, some arithmetic and a SET.
    """
    capacity = 50
    results = []
    lock = threading.Lock()
    barrier = threading.Barrier(8)

    def worker():
        lim = RateLimiter(RedisStore(_client()), capacity=capacity, refill_per_second=0)
        barrier.wait()
        got = sum(1 for _ in range(40) if lim.check("shared").allowed)
        with lock:
            results.append(got)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    total = sum(results)
    assert total == capacity, (
        "admitted %d against a capacity of %d -- the script is not atomic" % (total, capacity))


def test_separate_clients_have_separate_buckets(r):
    lim = RateLimiter(RedisStore(r), capacity=3, refill_per_second=0)
    assert sum(1 for _ in range(5) if lim.check("a").allowed) == 3
    assert sum(1 for _ in range(5) if lim.check("b").allowed) == 3


def test_a_zero_rate_bucket_reports_infinite_retry_rather_than_dividing_by_zero(r):
    lim = RateLimiter(RedisStore(r), capacity=1, refill_per_second=0)
    lim.check("c")
    d = lim.check("c")
    assert not d.allowed
    assert d.retry_after_s == float("inf")


# --- pub/sub invalidation, previously never executed -----------------------

def test_the_pubsub_bus_publishes_and_the_log_still_records(r, tmp_path):
    """`RedisPubSubBus` was written and never run. It has one job beyond the log:
    push the invalidation so subscribers do not wait for a poll."""
    from shortener.invalidation import RedisPubSubBus

    log = SqliteInvalidationBus(str(tmp_path / "log.db"), poll_interval_s=1.0)
    bus = RedisPubSubBus(r, log)

    sub = _client().pubsub(ignore_subscribe_messages=True)
    sub.subscribe(RedisPubSubBus.CHANNEL)
    time.sleep(0.2)

    seq = bus.publish("hot-code", "phishing")

    got = None
    deadline = time.time() + 3
    while time.time() < deadline and got is None:
        msg = sub.get_message(timeout=0.2)
        if msg and msg.get("type") == "message":
            got = msg["data"].decode()
    sub.close()

    assert got == "%d:hot-code" % seq, "subscriber never received the push: %r" % got
    # And the durable log has it regardless -- that is the part that carries
    # correctness, and the push is only latency.
    assert log.head() == seq


def test_the_push_is_best_effort_and_the_log_is_not(tmp_path):
    """A broken Redis must cost latency, never correctness. The log write happens
    first and unconditionally."""
    from shortener.invalidation import RedisPubSubBus

    class BrokenRedis:
        def publish(self, *_a, **_k):
            raise ConnectionError("redis is down")

    log = SqliteInvalidationBus(str(tmp_path / "log2.db"), poll_interval_s=1.0)
    seq = RedisPubSubBus(BrokenRedis(), log).publish("code", "phishing")
    assert seq == 1
    assert log.head() == 1, "the durable write did not survive a failed push"


def test_a_subscriber_that_was_disconnected_misses_the_push_but_not_the_log(r, tmp_path):
    """The reason pub/sub alone is the wrong shape, demonstrated rather than
    asserted: a subscriber that is not listening when the message goes out never
    learns of it, and nothing anywhere raises."""
    from shortener.invalidation import RedisPubSubBus

    log = SqliteInvalidationBus(str(tmp_path / "log3.db"), poll_interval_s=1.0)
    bus = RedisPubSubBus(r, log)

    bus.publish("missed-while-away", "phishing")      # nobody is subscribed

    sub = _client().pubsub(ignore_subscribe_messages=True)
    sub.subscribe(RedisPubSubBus.CHANNEL)
    time.sleep(0.2)
    assert sub.get_message(timeout=0.5) is None, "a late subscriber cannot receive a past message"
    sub.close()

    # The log, however, has it -- and a replay catches up.
    applied = []
    log.register("late-instance")
    log.poll("late-instance", applied.append)
    assert applied == ["missed-while-away"]
