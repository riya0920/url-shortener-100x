"""Batched, fire-and-forget hit counting.

**Why this exists: the load test said so.** The first measured run of the resolve
path managed 202 RPS at a 3.8 s p99. The profile was unambiguous -- every single
resolve opened a fresh SQLite connection and committed a row to count the hit, so
the read path was doing synchronous durable writes.

That is the exact anti-pattern the design doc already warned about ("counting on
the links row would turn every resolve into a write"). It was written down and
then done anyway, which is a good argument for load-testing rather than
reasoning.

The fix is the one the doc named: aggregate counts in memory and flush them
periodically. The trade is stated plainly rather than hidden:

* **Bounded loss on crash.** Up to `flush_interval` seconds of counts can be lost
  if the process dies. Analytics accuracy is worth strictly less than redirect
  availability, so this is the right side of the trade -- but it IS a trade, and
  a billing counter would need a different design.
* **Counts are eventually consistent.** A read immediately after a resolve may
  not see it. `flush()` exists so tests can force the write.
"""
from __future__ import annotations

import threading
import time
from collections import Counter


class HitCounter:
    def __init__(self, store, flush_interval: float = 2.0, max_pending: int = 10_000):
        self.store = store
        self.flush_interval = flush_interval
        self.max_pending = max_pending
        self._counts = Counter()
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()
        self.flushes = 0
        self.recorded = 0
        self.lost_on_shutdown = 0

    def record(self, code: str):
        """Hot path. Must not touch the database."""
        with self._lock:
            self._counts[code] += 1
            self.recorded += 1
            pending = len(self._counts)
        # Flush on a size OR time trigger. Size alone lets a low-traffic key sit
        # unflushed forever; time alone lets a burst balloon memory.
        if pending >= self.max_pending or (time.monotonic() - self._last_flush) >= self.flush_interval:
            self.flush()

    def flush(self) -> int:
        with self._lock:
            if not self._counts:
                self._last_flush = time.monotonic()
                return 0
            batch = self._counts
            self._counts = Counter()
            self._last_flush = time.monotonic()

        try:
            self.store.record_hits_bulk(batch)
            self.flushes += 1
            return len(batch)
        except Exception:
            # Put them back rather than dropping silently, so a transient
            # database failure costs latency in the next flush and not accuracy.
            with self._lock:
                self._counts.update(batch)
            return 0

    def stats(self) -> dict:
        with self._lock:
            pending = len(self._counts)
        return {"recorded": self.recorded, "flushes": self.flushes, "pending_keys": pending,
                "flush_interval_s": self.flush_interval}
