"""HTTP tier. Stateless: every instance shares the link store and the limiter store.

Run two of these behind a load balancer -- the interesting bugs (shared limiter
state, duplicate codes, cache coherence) only exist at N >= 2.
"""
from __future__ import annotations

import os
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .breaker import BreakerGuardedLimiter
from .counters import HitCounter
from .ids import SnowflakeGenerator
from .limiter import FailOpenLimiter, RateLimiter, RedisStore, SqliteStore
from .store import LinkStore, LruCache, SingleFlight

DATA = os.environ.get("SHORTENER_DATA", "./data")
INSTANCE_ID = int(os.environ.get("INSTANCE_ID", "0"))
CAPACITY = float(os.environ.get("RATE_CAPACITY", "60"))
REFILL = float(os.environ.get("RATE_REFILL_PER_S", "10"))
# Lets the load test measure the limiter's cost by running the identical binary
# with enforcement off. Comparing two different builds would confound the
# measurement with whatever else differed between them.
LIMITER_ENABLED = os.environ.get("LIMITER_ENABLED", "1") != "0"
# sqlite | redis | fakeredis. The load test measures all three, because the
# limiter turned out to be the dominant cost and "which backend" is therefore a
# throughput decision rather than a deployment detail.
LIMITER_BACKEND = os.environ.get("LIMITER_BACKEND", "sqlite")

os.makedirs(DATA, exist_ok=True)

links = LinkStore(os.path.join(DATA, "links.db"))
def _build_limiter_store():
    if LIMITER_BACKEND == "redis":
        import redis as _redis

        return RedisStore(_redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0")))
    if LIMITER_BACKEND == "fakeredis":
        # An in-process Redis implementation that executes the SAME Lua script.
        # It is not a substitute for a real Redis benchmark -- there is no network
        # hop and no separate process -- but it does exercise the actual script
        # and shows the cost of the algorithm without SQLite's write lock.
        import fakeredis

        return RedisStore(fakeredis.FakeStrictRedis())
    return SqliteStore(os.path.join(DATA, "limiter.db"))


# Breaker-guarded rather than bare fail-open. The fail-open drill showed that
# honouring the availability guarantee cost a full TCP timeout per request when
# the store was dead; the breaker keeps the guarantee and drops the cost.
limiter = BreakerGuardedLimiter(
    RateLimiter(_build_limiter_store(), CAPACITY, REFILL),
    fail_open=True,
    failure_threshold=int(os.environ.get("BREAKER_THRESHOLD", "5")),
    cooldown_s=float(os.environ.get("BREAKER_COOLDOWN_S", "5")),
)
ids = SnowflakeGenerator(INSTANCE_ID)
cache = LruCache(capacity=50_000)
flight = SingleFlight()
hits = HitCounter(links, flush_interval=2.0)

app = FastAPI(title="url-shortener", version="0.4.0")


class CreateRequest(BaseModel):
    target: str = Field(min_length=1, max_length=2048)
    ttl_seconds: int | None = Field(default=None, ge=1, le=60 * 60 * 24 * 365)

    @field_validator("target")
    @classmethod
    def must_be_http(cls, v: str) -> str:
        # Rejecting non-http schemes is a security control, not validation
        # pedantry: a shortener that accepts javascript: or data: URLs becomes an
        # XSS-laundering service, since the short domain is what users see and
        # trust.
        if not v.startswith(("http://", "https://")):
            raise ValueError("target must be an http(s) URL")
        return v


class CreateResponse(BaseModel):
    code: str
    target: str
    expires_ms: int | None


def client_id(request: Request) -> str:
    return request.headers.get("x-api-key") or (request.client.host if request.client else "anonymous")


def enforce_limit(request: Request):
    if not LIMITER_ENABLED:
        return
    decision = limiter.check(client_id(request))
    if not decision.allowed:
        raise HTTPException(
            status_code=429,
            detail="rate limit exceeded",
            # retry_after_s is infinite for a hard quota (zero refill); cap the
            # header at a minute so a client retries rather than treating the
            # limit as permanent.
            headers={"Retry-After": str(min(max(int(decision.retry_after_s), 1), 60)
                                        if decision.retry_after_s != float("inf") else "60")},
        )


@app.get("/healthz")
def healthz():
    return {"status": "ok", "instance": INSTANCE_ID}


@app.post("/admin/reset-metrics")
def reset_metrics():
    """Zero the counters so a load test reports its own window, not the seeding
    phase that preceded it."""
    cache.hits = 0
    cache.misses = 0
    flight.collapsed = 0
    hits.recorded = 0
    hits.flushes = 0
    limiter.fail_open_count = 0
    limiter.error_count = 0
    return {"reset": True}


@app.post("/admin/flush-cache")
def flush_cache():
    """Drop the entire cache. Used by the stampede drill: every in-flight
    request for a hot key misses at the same instant, which is exactly the
    thundering herd SingleFlight exists to collapse."""
    before = len(cache._data)
    with cache._lock:
        cache._data.clear()
    return {"evicted": before}


@app.get("/metrics")
def metrics():
    return {
        "instance": INSTANCE_ID,
        "limiter_enabled": LIMITER_ENABLED,
        "limiter_backend": LIMITER_BACKEND,
        "cache": cache.stats(),
        "singleflight_collapsed": flight.collapsed,
        "hit_counter": hits.stats(),
        "limiter": limiter.stats(),
        "links": links.count(),
    }


@app.post("/links", response_model=CreateResponse, status_code=201)
def create_link(body: CreateRequest, request: Request):
    enforce_limit(request)
    code = ids.next_code()
    try:
        link = links.create(code, body.target, body.ttl_seconds)
    except KeyError:
        # Should be unreachable: snowflake ids are unique without a check. If it
        # ever fires, two instances share an INSTANCE_ID and that is an ops bug
        # worth a 500 rather than a silent retry that would hide it.
        raise HTTPException(status_code=500, detail="short code collision -- check INSTANCE_ID uniqueness")
    return CreateResponse(code=link.code, target=link.target, expires_ms=link.expires_ms)


@app.get("/{code}")
def resolve(code: str, request: Request):
    enforce_limit(request)
    now_ms = int(time.time() * 1000)

    target = cache.get(code, now_ms)
    if target is None:
        # Cache-aside with singleflight: concurrent misses on the same key make
        # exactly one trip to the store.
        def load():
            cached = cache.get(code, now_ms)
            if cached is not None:
                return cached
            link = links.get(code)
            if link is None or link.is_expired(now_ms):
                return None
            cache.put(code, link.target, link.expires_ms)
            return link.target

        target = flight.do(code, load)

    if target is None:
        raise HTTPException(status_code=404, detail="unknown or expired short code")

    # In-memory increment; flushed in batches. One durable write per resolve
    # is what capped the first measured load test at 202 RPS.
    hits.record(code)
    return RedirectResponse(url=target, status_code=307)


@app.on_event("shutdown")
def _flush_counters():
    """Flush pending hit counts on a clean shutdown, so only a hard CRASH can
    lose them -- which is the bounded-loss trade the counter documents."""
    hits.flush()
