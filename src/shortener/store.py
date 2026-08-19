"""Link storage with TTL, plus a cache-aside read path with a measured hit ratio."""
from __future__ import annotations

import sqlite3
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass


@dataclass
class Link:
    code: str
    target: str
    created_ms: int
    expires_ms: int | None
    hits: int = 0

    def is_expired(self, now_ms: int) -> bool:
        return self.expires_ms is not None and now_ms >= self.expires_ms


class LinkStore:
    def __init__(self, path: str):
        self.path = path
        self._local = threading.local()
        con = self._connect()
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS links (
                code       TEXT PRIMARY KEY,
                target     TEXT NOT NULL,
                created_ms INTEGER NOT NULL,
                expires_ms INTEGER
            );
            -- Hit counts live in their own table so the hot read path never
            -- writes to the `links` row. Counting on the links table would turn
            -- every resolve into a write, serialise on the row, and invalidate
            -- the cache entry it just served.
            CREATE TABLE IF NOT EXISTS link_hits (
                code  TEXT PRIMARY KEY,
                hits  INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_links_expiry ON links(expires_ms);
            """
        )
        con.commit()

    def _connect(self):
        """Reuse one connection per thread.

        Opening a fresh SQLite connection per request was half of the measured
        202 RPS bottleneck: each open re-parses the schema, re-applies PRAGMAs
        and re-acquires locks. Connections are not shareable across threads, so
        the pool is thread-local rather than global.
        """
        con = getattr(self._local, "con", None)
        if con is None:
            con = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA busy_timeout=30000")
            # NORMAL rather than FULL: WAL + NORMAL survives process crash and
            # only risks the last transactions on OS/power loss. For link data
            # that is an acceptable trade for not fsyncing on every commit.
            con.execute("PRAGMA synchronous=NORMAL")
            self._local.con = con
        return con

    def create(self, code: str, target: str, ttl_seconds: int | None = None, now_ms: int | None = None) -> Link:
        now_ms = now_ms or int(time.time() * 1000)
        expires = now_ms + ttl_seconds * 1000 if ttl_seconds else None
        con = self._connect()
        try:
            con.execute(
                "INSERT INTO links (code, target, created_ms, expires_ms) VALUES (?,?,?,?)",
                (code, target, now_ms, expires),
            )
            con.commit()
        except sqlite3.IntegrityError:
            raise KeyError("short code already exists: %s" % code)
        return Link(code, target, now_ms, expires)

    def get(self, code: str) -> Link | None:
        con = self._connect()
        row = con.execute(
            "SELECT code, target, created_ms, expires_ms FROM links WHERE code = ?", (code,)
        ).fetchone()
        return Link(*row) if row else None

    def record_hit(self, code: str):
        """Single-hit write. Kept for tests and admin paths; the serving path
        uses record_hits_bulk via HitCounter, because one durable write per
        resolve is what capped the first load test at 202 RPS."""
        self.record_hits_bulk({code: 1})

    def record_hits_bulk(self, counts) -> int:
        """One transaction for many codes -- the whole point of batching."""
        items = list(counts.items())
        if not items:
            return 0
        con = self._connect()
        con.executemany(
            "INSERT INTO link_hits (code, hits) VALUES (?, ?)"
            " ON CONFLICT(code) DO UPDATE SET hits = hits + excluded.hits",
            items,
        )
        con.commit()
        return len(items)

    def hits(self, code: str) -> int:
        con = self._connect()
        row = con.execute("SELECT hits FROM link_hits WHERE code = ?", (code,)).fetchone()
        return row[0] if row else 0

    def count(self) -> int:
        con = self._connect()
        return con.execute("SELECT COUNT(*) FROM links").fetchone()[0]


class LruCache:
    """Bounded LRU with per-entry TTL and hit/miss counters."""

    def __init__(self, capacity: int = 10_000):
        self.capacity = capacity
        self._data = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str, now_ms: int):
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                self.misses += 1
                return None
            value, expires_ms = entry
            if expires_ms is not None and now_ms >= expires_ms:
                # Expiry is enforced on READ as well as by the writer's TTL,
                # because a cached entry outliving its link is how a deleted or
                # expired link keeps redirecting.
                del self._data[key]
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self.hits += 1
            return value

    def put(self, key: str, value, expires_ms: int | None = None):
        with self._lock:
            self._data[key] = (value, expires_ms)
            self._data.move_to_end(key)
            while len(self._data) > self.capacity:
                self._data.popitem(last=False)

    def invalidate(self, key: str):
        with self._lock:
            self._data.pop(key, None)

    @property
    def hit_ratio(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0

    def stats(self) -> dict:
        return {"hits": self.hits, "misses": self.misses, "hit_ratio": self.hit_ratio,
                "size": len(self._data), "capacity": self.capacity}


class SingleFlight:
    """Collapses concurrent misses for the same key into one backend call.

    The cache-stampede mitigation. Without it, a viral link expiring at peak
    sends every concurrent request for that key to the database simultaneously --
    the classic thundering herd, where the cache miss that was supposed to cost
    one query costs ten thousand.

    Per-key locking rather than a global lock: a global lock would serialise
    misses for *unrelated* keys and turn a stampede on one link into a latency
    problem for every link.
    """

    class _Call:
        __slots__ = ("done", "value", "error")

        def __init__(self):
            self.done = threading.Event()
            self.value = None
            self.error = None

    def __init__(self):
        self._calls = {}
        self._guard = threading.Lock()
        self.collapsed = 0

    def do(self, key: str, fn):
        """Exactly one caller runs `fn`; the rest wait and share its result.

        The waiters must NOT run `fn` themselves after the leader finishes --
        that is the bug this class exists to prevent, and it is easy to write by
        accident: taking a per-key lock and calling `fn` inside it serialises the
        stampede without collapsing it, so the backend still sees N calls, just
        one at a time. That is strictly worse than no mitigation, because it adds
        latency without removing load.
        """
        with self._guard:
            call = self._calls.get(key)
            if call is None:
                call = self._calls[key] = self._Call()
                leader = True
            else:
                leader = False

        if not leader:
            self.collapsed += 1
            call.done.wait()
            if call.error is not None:
                raise call.error
            return call.value

        try:
            call.value = fn()
        except Exception as exc:
            call.error = exc
            raise
        finally:
            with self._guard:
                self._calls.pop(key, None)
            call.done.set()
        return call.value
