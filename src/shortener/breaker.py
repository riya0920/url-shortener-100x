"""Circuit breaker — the gap the fail-open drill exposed.

The drill showed the limiter failing open correctly when its store was dead, but
also showed the cost: **only 48 requests completed in 5 seconds at 16 concurrent
clients**, because every single request paid a full TCP connect timeout before
giving up. Fail-open converted an availability failure into a latency failure.
That is better than an outage and still not acceptable.

A circuit breaker fixes it by remembering. After N consecutive failures the
circuit **opens** and calls short-circuit immediately instead of waiting for a
timeout. After a cooldown it goes **half-open** and lets a single probe through:
success closes it, failure re-opens it.

    CLOSED --failures >= threshold--> OPEN --after cooldown--> HALF_OPEN
      ^                                                          |
      +---------------- probe succeeds --------------------------+
                                 |
                        probe fails -> OPEN

**Why a single probe and not a burst:** the whole point of half-open is to test
recovery cheaply. Sending a burst at a service that just came back is how you
knock it over again, which is the same mistake as replaying a DLQ at full rate.
"""
from __future__ import annotations

import threading
import time
from enum import Enum


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised instead of attempting a call the breaker believes will fail."""


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, cooldown_s: float = 5.0, clock=None):
        self.failure_threshold = failure_threshold
        self.cooldown_s = cooldown_s
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self.state = CircuitState.CLOSED
        self.consecutive_failures = 0
        self.opened_at = None
        # Counters, because a breaker nobody can observe is a breaker nobody
        # trusts. short_circuited is the number that shows the breaker paid off.
        self.short_circuited = 0
        self.trips = 0
        self.probes = 0

    def _now(self) -> float:
        return self._clock()

    def allow(self) -> bool:
        """May the caller attempt the protected operation?"""
        with self._lock:
            if self.state is CircuitState.CLOSED:
                return True
            if self.state is CircuitState.OPEN:
                if self._now() - self.opened_at >= self.cooldown_s:
                    self.state = CircuitState.HALF_OPEN
                    self.probes += 1
                    return True            # exactly one probe
                self.short_circuited += 1
                return False
            # HALF_OPEN: a probe is already in flight, everyone else waits.
            self.short_circuited += 1
            return False

    def record_success(self):
        with self._lock:
            self.consecutive_failures = 0
            self.state = CircuitState.CLOSED
            self.opened_at = None

    def record_failure(self):
        with self._lock:
            self.consecutive_failures += 1
            if self.state is CircuitState.HALF_OPEN:
                # The probe failed: back to open, restart the cooldown.
                self.state = CircuitState.OPEN
                self.opened_at = self._now()
                return
            if self.consecutive_failures >= self.failure_threshold:
                if self.state is not CircuitState.OPEN:
                    self.trips += 1
                self.state = CircuitState.OPEN
                self.opened_at = self._now()

    def call(self, fn, *args, **kwargs):
        """Run `fn` under the breaker. Raises CircuitOpenError when open."""
        if not self.allow():
            raise CircuitOpenError("circuit open")
        try:
            result = fn(*args, **kwargs)
        except Exception:
            self.record_failure()
            raise
        self.record_success()
        return result

    def stats(self) -> dict:
        return {"state": self.state, "consecutive_failures": self.consecutive_failures,
                "trips": self.trips, "short_circuited": self.short_circuited,
                "probes": self.probes}


class BreakerGuardedLimiter:
    """Wraps a limiter with a breaker, so fail-open stops paying the timeout.

    Behaviour is unchanged from the caller's point of view -- requests are still
    allowed when the store is unreachable -- but once the breaker opens they are
    allowed *immediately* rather than after a connect timeout. The availability
    guarantee is the same; the latency cost of honouring it collapses.
    """

    def __init__(self, limiter, fail_open: bool = True, failure_threshold: int = 5,
                 cooldown_s: float = 5.0, clock=None):
        self.limiter = limiter
        self.fail_open = fail_open
        self.breaker = CircuitBreaker(failure_threshold, cooldown_s, clock=clock)
        self.fail_open_count = 0
        self.error_count = 0

    def check(self, client_id: str, cost: float = 1.0):
        from .limiter import Decision

        if not self.breaker.allow():
            # Open circuit: skip the doomed call entirely.
            self.fail_open_count += 1
            return Decision(self.fail_open, float("nan"), 0.0)

        try:
            decision = self.limiter.check(client_id, cost)
        except Exception:
            self.breaker.record_failure()
            self.error_count += 1
            self.fail_open_count += 1
            return Decision(self.fail_open, float("nan"), 0.0)

        self.breaker.record_success()
        return decision

    def stats(self) -> dict:
        return {"fail_open_count": self.fail_open_count, "errors": self.error_count,
                "breaker": self.breaker.stats()}
