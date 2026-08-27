"""HTTP tier. Stateless: every instance shares the link store and the limiter store.

Run two of these behind a load balancer -- the interesting bugs (shared limiter
state, duplicate codes, cache coherence) only exist at N >= 2.
"""
from __future__ import annotations

import os
import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator

from .breaker import BreakerGuardedLimiter
from .invalidation import InvalidationPoller, SqliteInvalidationBus
from .counters import HitCounter
from .ids import CodeAllocator, build_generator
from .limiter import FailOpenLimiter, RateLimiter, RedisStore, SqliteStore
from .abuse import ALLOW, INTERSTITIAL, REFUSE, ReputationPolicy, TakedownList, render_interstitial
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
# random | snowflake. Random is the default: 7 characters instead of 10, and no
# structure for a holder of one code to walk to its neighbours. Snowflake stays
# selectable because the design doc argues the two against each other, and an
# argument whose losing side cannot be run is an assertion.
CODE_SCHEME = os.environ.get("CODE_SCHEME", "random")
CODE_LENGTH = int(os.environ.get("CODE_LENGTH", "7"))
# Bounded. Exhaustion means a duplicated INSTANCE_ID or a broken rng, not bad
# luck, so it must fail loudly instead of looping.
CODE_MAX_ATTEMPTS = int(os.environ.get("CODE_MAX_ATTEMPTS", "5"))

os.makedirs(DATA, exist_ok=True)

# The link store backend. SQLite is the default because it needs nothing;
# Postgres is what a deployment with more than one instance actually wants,
# since SQLite over a shared filesystem is a corruption story rather than a
# scaling one.
LINK_BACKEND = os.environ.get("LINK_BACKEND", "sqlite")
if LINK_BACKEND == "postgres":
    from .pgstore import PgLinkStore

    links = PgLinkStore(os.environ["SHORTENER_PG_DSN"])
else:
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
# Abuse controls. ALLOW_PRIVATE_TARGETS exists because tests and the local demo
# shorten http://localhost URLs; in production it must stay off, or the service
# is an SSRF pivot for anything that follows redirects server-side.
policy = ReputationPolicy(allow_private=os.environ.get("ALLOW_PRIVATE_TARGETS", "0") == "1")

# The invalidation log lives in the shared data directory, which is what makes it
# shared: every instance in a deployment points SHORTENER_DATA at the same place,
# exactly as they already do for the link store.
invalidation_bus = SqliteInvalidationBus(
    os.path.join(DATA, "invalidations.db"),
    poll_interval_s=float(os.environ.get("INVALIDATION_POLL_S", "1.0")))
takedowns = TakedownList(bus=invalidation_bus)
# Codes that resolve through an interstitial instead of a 307. In-process on
# purpose and wrong on purpose: like the LRU it does not survive a restart and
# does not cross instances, and it is listed with the cache-invalidation gap
# rather than pretended away.
interstitials = {}

allocator = CodeAllocator(
    build_generator(CODE_SCHEME, instance_id=INSTANCE_ID, length=CODE_LENGTH),
    max_attempts=CODE_MAX_ATTEMPTS)
cache = LruCache(capacity=50_000)
# Started after the cache exists, and registered from sequence zero: a fresh
# instance has an empty cache, so replaying the whole log is cheap and makes it
# correct by construction rather than by luck.
invalidation_poller = InvalidationPoller(
    invalidation_bus, "instance-%s" % INSTANCE_ID, cache).start()
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
    interstitial: bool = False
    reason: str = "ok"


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
    return {"status": "ok", "instance": INSTANCE_ID, "link_backend": LINK_BACKEND,
            "code_scheme": CODE_SCHEME, "code_length": CODE_LENGTH}


@app.post("/admin/reset-metrics")
def reset_metrics():
    """Zero the counters so a load test reports its own window, not the seeding
    phase that preceded it."""
    cache.hits = 0
    cache.misses = 0
    flight.collapsed = 0
    hits.recorded = 0
    hits.flushes = 0
    allocator.allocated = 0
    allocator.collisions = 0
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


@app.get("/", response_class=HTMLResponse)
def sandbox():
    """The live sandbox page.

    Declared before the /{code} resolver because FastAPI matches in definition
    order, and a bare "/" would otherwise be a candidate for the catch-all.
    Serving it from the app rather than a CDN keeps the demo on the same origin
    as the API, so the page can call it without CORS.
    """
    page = os.path.join(os.path.dirname(__file__), "..", "..", "public", "index.html")
    try:
        with open(page, encoding="utf-8") as fh:
            return HTMLResponse(fh.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>url-shortener</h1><p>API is up. See /metrics.</p>")


@app.get("/metrics")
def metrics():
    return {
        "instance": INSTANCE_ID,
        "limiter_enabled": LIMITER_ENABLED,
        "limiter_backend": LIMITER_BACKEND,
        # Surfaced so the live sandbox can show the bucket it is actually
        # running with. Without it, "why did nothing get refused" is
        # unanswerable from outside the process.
        "limiter_config": {"capacity": CAPACITY, "refill_per_s": REFILL},
        "cache": cache.stats(),
        "singleflight_collapsed": flight.collapsed,
        "hit_counter": hits.stats(),
        "codes": allocator.stats(),
        "limiter": limiter.stats(),
        "links": links.count(),
    }


@app.post("/links", response_model=CreateResponse, status_code=201)
def create_link(body: CreateRequest, request: Request):
    enforce_limit(request)

    # Policy runs at CREATE, not at resolve. Once per link rather than once per
    # click keeps it off the hot path; the cost is that an abuser can probe the
    # blocklist, which is what takedown is for.
    verdict = policy.evaluate(body.target)
    if verdict.action == REFUSE:
        raise HTTPException(status_code=422, detail={"error": "destination refused",
                                                     "reason": verdict.reason,
                                                     "host": verdict.host})

    try:
        # Retries on collision. Under snowflake a collision is an ops bug (two
        # instances sharing an INSTANCE_ID) and the retry will keep failing;
        # under random codes it is ordinary bad luck and the retry is the design.
        # Either way, exhausting the attempts is a 500 rather than a silent loop.
        link = allocator.allocate(links, body.target, body.ttl_seconds)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    if verdict.action == INTERSTITIAL:
        interstitials[link.code] = verdict.reason
    return CreateResponse(code=link.code, target=link.target, expires_ms=link.expires_ms,
                          interstitial=verdict.action == INTERSTITIAL, reason=verdict.reason)


@app.get("/{code}")
def resolve(code: str, request: Request):
    enforce_limit(request)
    now_ms = int(time.time() * 1000)

    # Takedown is checked BEFORE the cache. Checking after would mean a hot code
    # keeps redirecting from cache after it has been withdrawn, which is the
    # entire failure mode a takedown exists to prevent.
    if code in takedowns:
        raise HTTPException(status_code=410, detail={"error": "link withdrawn",
                                                     "reason": takedowns.reason(code)})

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

    if code in interstitials:
        # 200, not a redirect: the whole point is that the browser stops here and
        # the user sees the destination before anything from it loads.
        return HTMLResponse(render_interstitial(target, interstitials[code]), status_code=200)

    return RedirectResponse(url=target, status_code=307)


@app.post("/admin/takedown/{code}")
def takedown(code: str, reason: str = "policy violation"):
    """Withdraw a link.

    Returns the propagation status honestly. On a single instance the local cache
    purge closes the window; across instances it does not, and the response says
    so rather than reporting success.
    """
    result = takedowns.add(code, reason, purge=cache.invalidate)
    interstitials.pop(code, None)
    # Drain our own cursor immediately so this instance's poller does not
    # re-apply what it just published, and so `invalidation_lag` reads zero here.
    invalidation_poller.drain_now()
    return result


@app.get("/admin/abuse")
def abuse_stats():
    return {"takedowns": takedowns.stats(),
            "invalidation_lag": invalidation_bus.lag("instance-%s" % INSTANCE_ID),
            "interstitial_codes": len(interstitials),
            "policy": {"blocklisted_domains": len(policy.blocklist),
                       "shortener_domains": len(policy.shorteners),
                       "allow_private_targets": policy.allow_private}}


@app.on_event("shutdown")
def _stop_poller():
    invalidation_poller.stop()


@app.on_event("shutdown")
def _flush_counters():
    """Flush pending hit counts on a clean shutdown, so only a hard CRASH can
    lose them -- which is the bounded-loss trade the counter documents."""
    hits.flush()
