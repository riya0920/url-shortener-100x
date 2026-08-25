"""The Postgres link store - the backend SQLite has been standing in for.

    SHORTENER_PG_DSN=postgresql://... LINK_BACKEND=postgres  python -m uvicorn ...

Interface-identical to `LinkStore`, and `tests/test_pgstore.py` runs one
conformance suite against both so "identical" is checked rather than asserted.

## What actually differs, and why it is not just a dialect change

**Creation is one statement, not a try/except.** SQLite catches
`IntegrityError` to detect a code collision. Postgres uses
`ON CONFLICT (code) DO NOTHING RETURNING code`: a returned row means the insert
won, no row means someone else already had that code. Same outcome, one round
trip instead of two, and no exception used for control flow across a network.

**Hit counting is an upsert with an atomic add**, `ON CONFLICT DO UPDATE SET
hits = link_hits.hits + EXCLUDED.hits`. The SQLite path does the same thing, and
the reason it matters more here is that Postgres actually has concurrent writers
to contend with - this is the statement that decides whether two instances
flushing their counters at the same moment lose one of the batches.

**Expiry is `TIMESTAMPTZ`, not epoch milliseconds.** The SQLite store stores
`expires_ms` because SQLite has no date type worth using. Storing a real
timestamp lets the *database* evaluate expiry, which is what makes a background
sweep possible without shipping every row to the client to check it.

The API still speaks epoch milliseconds in both directions, because the cache and
the HTTP layer do, and changing that would have leaked a storage decision into
three other modules for no benefit.

## Connections

One per thread, reused - the same shape as `LinkStore` and for the same reason.
The job-queue backend in this portfolio learned that the hard way: it opened one
connection per operation, which passed a conformance suite doing a handful of
calls per test and then took down the machine's networking under a benchmark.
Starting from the fixed shape here rather than rediscovering it.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone

from .store import Link

SCHEMA = """
CREATE TABLE IF NOT EXISTS links (
    code       TEXT PRIMARY KEY,
    target     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

-- Hit counts in their own table so the hot read path never writes to the `links`
-- row. Counting on `links` would turn every resolve into a write, serialise on
-- the row, and invalidate the cache entry it just served.
CREATE TABLE IF NOT EXISTS link_hits (
    code  TEXT PRIMARY KEY,
    hits  BIGINT NOT NULL DEFAULT 0
);

-- Partial: only rows that CAN expire. A full index would carry every permanent
-- link forever, and permanent links are the overwhelming majority.
CREATE INDEX IF NOT EXISTS idx_links_expiry ON links (expires_at)
    WHERE expires_at IS NOT NULL;
"""


def _ms(dt) -> int | None:
    return None if dt is None else int(dt.timestamp() * 1000)


def _dt(ms):
    """Epoch millis -> aware datetime, or None.

    Converting in Python rather than with `to_timestamp(%s / 1000.0)` in SQL:
    a NULL parameter inside a CASE has no inferable type, and Postgres rejects
    the statement with `could not determine data type of parameter $4`. Passing a
    real `datetime | None` lets psycopg carry the type with the value, which is
    what it is for.
    """
    return None if ms is None else datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


class PgLinkStore:
    """Interface-compatible with `LinkStore`."""

    def __init__(self, dsn: str):
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("psycopg (v3) is required: pip install 'psycopg[binary]'") from exc
        self._psycopg = psycopg
        self.dsn = dsn
        self._local = threading.local()
        con = self._connect()
        with con.transaction():
            with con.cursor() as cur:
                cur.execute(SCHEMA)

    def _connect(self):
        con = getattr(self._local, "con", None)
        if con is None or con.closed:
            con = self._psycopg.connect(self.dsn, autocommit=False)
            self._local.con = con
        return con

    # -- writes ------------------------------------------------------------

    def create(self, code: str, target: str, ttl_seconds: int | None = None,
               now_ms: int | None = None) -> Link:
        now_ms = now_ms or int(time.time() * 1000)
        expires = now_ms + ttl_seconds * 1000 if ttl_seconds else None
        con = self._connect()
        with con.transaction():
            with con.cursor() as cur:
                # One statement. The SQLite path catches IntegrityError to detect
                # a collision; using an exception for control flow across a
                # network is a round trip and a stack unwind for a condition the
                # database can just report.
                cur.execute(
                    "INSERT INTO links (code, target, created_at, expires_at) "
                    "VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (code) DO NOTHING RETURNING code",
                    (code, target, _dt(now_ms), _dt(expires)))
                won = cur.fetchone()
        if won is None:
            raise KeyError("short code already exists: %s" % code)
        return Link(code, target, now_ms, expires)

    def record_hit(self, code: str):
        self.record_hits_bulk({code: 1})

    def record_hits_bulk(self, counts) -> int:
        items = list(counts.items())
        if not items:
            return 0
        con = self._connect()
        with con.transaction():
            with con.cursor() as cur:
                # The add happens inside the database. Two instances flushing
                # their counters at the same instant both apply; a read-then-
                # write in the client would lose one of the batches, and this is
                # the backend where that actually has other writers to race.
                cur.executemany(
                    "INSERT INTO link_hits (code, hits) VALUES (%s, %s) "
                    "ON CONFLICT (code) DO UPDATE SET hits = link_hits.hits + EXCLUDED.hits",
                    items)
        return len(items)

    # -- reads -------------------------------------------------------------

    def get(self, code: str) -> Link | None:
        con = self._connect()
        with con.transaction():
            with con.cursor() as cur:
                cur.execute(
                    "SELECT code, target, created_at, expires_at FROM links WHERE code = %s",
                    (code,))
                row = cur.fetchone()
        if row is None:
            return None
        return Link(row[0], row[1], _ms(row[2]), _ms(row[3]))

    def hits(self, code: str) -> int:
        con = self._connect()
        with con.transaction():
            with con.cursor() as cur:
                cur.execute("SELECT hits FROM link_hits WHERE code = %s", (code,))
                row = cur.fetchone()
        return row[0] if row else 0

    def count(self) -> int:
        con = self._connect()
        with con.transaction():
            with con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM links")
                return cur.fetchone()[0]

    # -- the thing SQLite cannot do ---------------------------------------

    def purge_expired(self, now_ms: int | None = None) -> int:
        """Delete expired rows, evaluated by the DATABASE.

        The SQLite store cannot do this usefully: with expiry stored as epoch
        milliseconds and no scheduler, expired rows are only ever noticed when
        something reads them, so a link that nobody requests stays on disk
        forever. Here expiry is a real timestamp with an index over it, so a
        sweep is one indexed delete rather than a full scan shipped to the client.

        Returns the number removed, so a caller can tell an empty sweep from a
        sweep that did not run.
        """
        con = self._connect()
        with con.transaction():
            with con.cursor() as cur:
                if now_ms is None:
                    cur.execute("DELETE FROM links WHERE expires_at IS NOT NULL "
                                "AND expires_at <= NOW() RETURNING code")
                else:
                    cur.execute("DELETE FROM links WHERE expires_at IS NOT NULL "
                                "AND expires_at <= %s RETURNING code", (_dt(now_ms),))
                gone = cur.fetchall()
        return len(gone)


def available() -> bool:
    dsn = os.environ.get("SHORTENER_PG_DSN")
    if not dsn:
        return False
    try:
        import psycopg

        with psycopg.connect(dsn, connect_timeout=3) as con:
            con.execute("SELECT 1")
        return True
    except Exception:
        return False
