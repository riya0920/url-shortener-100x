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

from .ids import SnowflakeGenerator
from .limiter import FailOpenLimiter, RateLimiter, SqliteStore
from .store import LinkStore, LruCache, SingleFlight

DATA = os.environ.get("SHORTENER_DATA", "./data")
INSTANCE_ID = int(os.environ.get("INSTANCE_ID", "0"))
CAPACITY = float(os.environ.get("RATE_CAPACITY", "60"))
REFILL = float(os.environ.get("RATE_REFILL_PER_S", "10"))

os.makedirs(DATA, exist_ok=True)

links = LinkStore(os.path.join(DATA, "links.db"))
limiter = FailOpenLimiter(
    RateLimiter(SqliteStore(os.path.join(DATA, "limiter.db")), CAPACITY, REFILL),
    fail_open=True,
)
ids = SnowflakeGenerator(INSTANCE_ID)
cache = LruCache(capacity=50_000)
flight = SingleFlight()

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


@app.get("/metrics")
def metrics():
    return {
        "instance": INSTANCE_ID,
        "cache": cache.stats(),
        "singleflight_collapsed": flight.collapsed,
        "limiter": {"fail_open_count": limiter.fail_open_count, "errors": limiter.error_count},
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

    links.record_hit(code)
    return RedirectResponse(url=target, status_code=307)
