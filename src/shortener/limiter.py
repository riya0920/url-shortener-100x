"""Distributed token-bucket rate limiter.

**The rule: the read-modify-write must be atomic.** The naive implementation

    tokens = store.get(key)          # <-- another instance reads the same value
    if tokens > 0:                   #     here
        store.set(key, tokens - 1)   #     and both decrement from it

lets two instances spend the same token. Under real concurrency that leaks
roughly (instances - 1) x the budget, and it is invisible in single-instance
testing -- which is why almost nobody catches it.

Two backends, one algorithm:

* `RedisStore` runs the whole bucket update inside a **Lua script**. Redis
  executes scripts atomically, so the check-and-decrement is a single indivisible
  operation with no round trip in the middle.
* `SqliteStore` does the same inside an `IMMEDIATE` transaction. It exists so the
  accuracy test can actually run **across real processes** without requiring a
  Redis server, and because it is the honest answer for a small deployment.

Both implement the same algorithm, and `test_limiter_accuracy_across_processes`
runs against SQLite specifically to prove the atomicity claim rather than assert
it.
"""
from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass

# Token bucket, evaluated lazily. Tokens are not refilled by a timer -- they are
# computed from elapsed time on each request, which means no background job, no
# drift, and correct behaviour after an idle period.
#
# KEYS[1] = bucket key
# ARGV[1] = capacity, ARGV[2] = refill per second, ARGV[3] = now (ms), ARGV[4] = cost
LUA_TOKEN_BUCKET = """
local key      = KEYS[1]
local capacity = tonumber(ARGV[1])
local rate     = tonumber(ARGV[2])
local now_ms   = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'updated_ms')
local tokens = tonumber(bucket[1])
local updated = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  updated = now_ms
end

-- Lazy refill: however long we were away, credit that much, capped at capacity.
local elapsed = math.max(0, now_ms - updated) / 1000.0
tokens = math.min(capacity, tokens + elapsed * rate)

local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'updated_ms', now_ms)

-- Expire after the time it would take to refill from empty, plus slack. Without
-- this, every client that ever appears leaks a key forever.
--
-- Two details that only running the script reveals:
--   1. PEXPIRE demands an INTEGER. math.ceil returns a float under Lua 5.1/LuaJIT,
--      which Redis rejects with "value is not an integer or out of range".
--      string.format('%d', ...) forces the integer representation.
--   2. rate == 0 is a legitimate configuration (a hard quota that never refills)
--      and would divide by zero here, producing inf. Fall back to a fixed TTL.
local ttl_ms
if rate > 0 then
  ttl_ms = math.ceil((capacity / rate) * 1000) + 10000
else
  ttl_ms = 3600000
end
redis.call('PEXPIRE', key, string.format('%d', ttl_ms))

return {allowed, tostring(tokens)}
"""


@dataclass
class Decision:
    allowed: bool
    tokens_remaining: float
    retry_after_s: float = 0.0


def _retry_after(allowed: bool, cost: float, tokens: float, rate: float) -> float:
    """Seconds until `cost` tokens are available again.

    A refill rate of zero is a legitimate configuration -- a hard quota with no
    replenishment -- and it must not divide by zero. The honest answer there is
    "never", surfaced as infinity so a caller converting it to a Retry-After
    header has to make a deliberate decision about what to send.
    """
    if allowed:
        return 0.0
    if rate <= 0:
        return float("inf")
    return max((cost - tokens) / rate, 0.0)


class SqliteStore:
    """Shared bucket state in SQLite. Atomic via BEGIN IMMEDIATE."""

    def __init__(self, path: str):
        self.path = path
        con = self._connect()
        con.execute(
            "CREATE TABLE IF NOT EXISTS buckets ("
            " key TEXT PRIMARY KEY, tokens REAL NOT NULL, updated_ms INTEGER NOT NULL)"
        )
        con.commit()
        con.close()

    def _connect(self):
        con = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        # WAL keeps readers from blocking the writer; busy_timeout makes
        # contending writers wait rather than immediately erroring, which is what
        # turns a "database is locked" flake into a queued request.
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def consume(self, key: str, capacity: float, rate: float, cost: float, now_ms: int) -> Decision:
        con = self._connect()
        try:
            # IMMEDIATE takes the write lock up front, so the read below cannot be
            # interleaved with another process's write. This is the SQLite
            # equivalent of running the Lua script.
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT tokens, updated_ms FROM buckets WHERE key = ?", (key,)).fetchone()
            if row is None:
                tokens, updated = capacity, now_ms
            else:
                tokens, updated = row

            elapsed = max(0, now_ms - updated) / 1000.0
            tokens = min(capacity, tokens + elapsed * rate)

            allowed = tokens >= cost
            if allowed:
                tokens -= cost

            con.execute(
                "INSERT INTO buckets (key, tokens, updated_ms) VALUES (?,?,?)"
                " ON CONFLICT(key) DO UPDATE SET tokens=excluded.tokens, updated_ms=excluded.updated_ms",
                (key, tokens, now_ms),
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

        return Decision(allowed, tokens, _retry_after(allowed, cost, tokens, rate))


class RedisStore:
    """Production backend. The Lua script is the atomicity guarantee."""

    def __init__(self, client):
        self.client = client
        self._script = client.register_script(LUA_TOKEN_BUCKET)

    def consume(self, key: str, capacity: float, rate: float, cost: float, now_ms: int) -> Decision:
        allowed, tokens = self._script(keys=[key], args=[capacity, rate, now_ms, cost])
        # redis-py returns bytes for the string half of the reply; fakeredis and
        # real Redis agree on this, so decode rather than assuming str.
        if isinstance(tokens, bytes):
            tokens = tokens.decode()
        tokens, allowed = float(tokens), bool(int(allowed))
        return Decision(allowed, tokens, _retry_after(allowed, cost, tokens, rate))


class RateLimiter:
    def __init__(self, store, capacity: float = 60, refill_per_second: float = 10, clock=None):
        self.store = store
        self.capacity = capacity
        self.refill_per_second = refill_per_second
        self._clock = clock or (lambda: int(time.time() * 1000))

    def check(self, client_id: str, cost: float = 1.0) -> Decision:
        return self.store.consume(
            "rl:%s" % client_id, self.capacity, self.refill_per_second, cost, self._clock()
        )


class FailOpenLimiter:
    """Wraps a limiter and decides what happens when the store is unreachable.

    **Fail open, and this is a product decision before it is a technical one.**

    Failing CLOSED turns a Redis outage into a total outage: every request is
    rejected and the limiter -- a protective control -- becomes the single point
    of failure for the whole service. Failing OPEN turns the same outage into a
    period of unenforced limits, during which the service still works.

    The trade is only acceptable because of what this limiter protects: fair use
    of a public redirect service. The cost of a few unlimited minutes is some
    extra load; the cost of failing closed is total unavailability. **If this
    limiter were protecting a payment endpoint or metered billing, the answer
    would flip** -- there, letting unlimited requests through is the more
    expensive failure and closed is correct.

    Either way it is instrumented: `fail_open_count` is a metric, so an outage is
    visible rather than silent.
    """

    def __init__(self, limiter: RateLimiter, fail_open: bool = True):
        self.limiter = limiter
        self.fail_open = fail_open
        self.fail_open_count = 0
        self.error_count = 0

    def check(self, client_id: str, cost: float = 1.0) -> Decision:
        try:
            return self.limiter.check(client_id, cost)
        except Exception:
            self.error_count += 1
            if self.fail_open:
                self.fail_open_count += 1
                return Decision(True, float("nan"), 0.0)
            return Decision(False, 0.0, 1.0)
