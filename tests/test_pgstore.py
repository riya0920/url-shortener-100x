"""One conformance suite, both link stores.

Every test is written against the *interface* and parameterised over SQLite and
Postgres, so "interface-compatible" is checked rather than claimed. Postgres skips
without `SHORTENER_PG_DSN`, and the skip is reported rather than counted as a pass.
"""
import os
import threading
import time

import pytest

from shortener import pgstore
from shortener.store import LinkStore

DSN = os.environ.get("SHORTENER_PG_DSN")


def _sqlite(tmp_path):
    return LinkStore(str(tmp_path / "links.db"))


def _postgres(_tmp_path):
    s = pgstore.PgLinkStore(DSN)
    con = s._connect()
    with con.transaction():
        with con.cursor() as cur:
            cur.execute("TRUNCATE links, link_hits")
    return s


BACKENDS = [
    pytest.param(_sqlite, id="sqlite"),
    pytest.param(_postgres, id="postgres",
                 marks=pytest.mark.skipif(not pgstore.available(),
                                          reason="set SHORTENER_PG_DSN to run the Postgres arm")),
]


@pytest.fixture(params=BACKENDS)
def store(request, tmp_path):
    return request.param(tmp_path)


# --- the contract ----------------------------------------------------------

def test_a_created_link_reads_back(store):
    store.create("abc", "https://example.com/x")
    link = store.get("abc")
    assert link.code == "abc" and link.target == "https://example.com/x"


def test_an_unknown_code_is_none(store):
    assert store.get("nope") is None


def test_a_duplicate_code_raises_rather_than_overwriting(store):
    """Silently overwriting would let one user hijack another's link."""
    store.create("dup", "https://first.example/")
    with pytest.raises(KeyError):
        store.create("dup", "https://second.example/")
    assert store.get("dup").target == "https://first.example/"


def test_ttl_round_trips_as_epoch_millis(store):
    """Postgres stores a real TIMESTAMPTZ and SQLite stores an integer; the API
    speaks milliseconds either way, because the cache and the HTTP layer do."""
    now = int(time.time() * 1000)
    link = store.create("ttl", "https://example.com/", ttl_seconds=60, now_ms=now)
    assert link.expires_ms == now + 60_000
    assert abs(store.get("ttl").expires_ms - (now + 60_000)) <= 1


def test_a_link_with_no_ttl_never_expires(store):
    store.create("forever", "https://example.com/")
    assert store.get("forever").expires_ms is None
    assert not store.get("forever").is_expired(int(time.time() * 1000) + 10 ** 12)


def test_expiry_is_evaluated_against_the_clock(store):
    now = int(time.time() * 1000)
    store.create("soon", "https://example.com/", ttl_seconds=1, now_ms=now)
    link = store.get("soon")
    assert not link.is_expired(now)
    assert link.is_expired(now + 2000)


# --- hit counting ----------------------------------------------------------

def test_hits_start_at_zero_and_accumulate(store):
    store.create("h", "https://example.com/")
    assert store.hits("h") == 0
    store.record_hit("h")
    store.record_hit("h")
    assert store.hits("h") == 2


def test_bulk_hits_add_rather_than_replace(store):
    """The whole point of the upsert. A read-then-write in the client loses a
    batch whenever two flushes land together."""
    store.create("b", "https://example.com/")
    store.record_hits_bulk({"b": 5})
    store.record_hits_bulk({"b": 3})
    assert store.hits("b") == 8


def test_an_empty_bulk_write_is_a_no_op(store):
    assert store.record_hits_bulk({}) == 0


def test_concurrent_hit_flushes_do_not_lose_a_batch(store):
    """Eight threads flushing at once. Any lost update shows up as a total below
    the number recorded, and this is the statement that decides whether two
    instances flushing their counters simultaneously both land."""
    store.create("race", "https://example.com/")
    barrier = threading.Barrier(8)

    def flush():
        barrier.wait()
        for _ in range(20):
            store.record_hits_bulk({"race": 1})

    threads = [threading.Thread(target=flush) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert store.hits("race") == 160, "lost %d updates" % (160 - store.hits("race"))


def test_count_reflects_created_links(store):
    before = store.count()
    store.create("c1", "https://example.com/1")
    store.create("c2", "https://example.com/2")
    assert store.count() == before + 2


# --- what only Postgres can do --------------------------------------------

@pytest.mark.skipif(not pgstore.available(), reason="needs SHORTENER_PG_DSN")
def test_postgres_can_sweep_expired_rows_in_the_database():
    """SQLite cannot do this usefully: expiry is an integer with no scheduler, so
    an expired link that nobody requests sits on disk forever. Here it is an
    indexed delete rather than a full scan shipped to the client."""
    s = _postgres(None)
    now = int(time.time() * 1000)
    s.create("gone", "https://example.com/", ttl_seconds=1, now_ms=now - 10_000)
    s.create("stays", "https://example.com/", ttl_seconds=3600, now_ms=now)
    s.create("permanent", "https://example.com/")

    removed = s.purge_expired(now)
    assert removed == 1
    assert s.get("gone") is None
    assert s.get("stays") is not None
    assert s.get("permanent") is not None


@pytest.mark.skipif(not pgstore.available(), reason="needs SHORTENER_PG_DSN")
def test_an_empty_sweep_is_distinguishable_from_a_sweep_that_did_not_run():
    """`purge_expired` returns a count for exactly this reason -- an operator
    needs to tell 'nothing to delete' from 'the job never fired'."""
    s = _postgres(None)
    s.create("keep", "https://example.com/")
    assert s.purge_expired() == 0


@pytest.mark.skipif(not pgstore.available(), reason="needs SHORTENER_PG_DSN")
def test_the_expiry_index_is_partial():
    """A full index carries every permanent link forever, and permanent links are
    the overwhelming majority."""
    s = _postgres(None)
    con = s._connect()
    with con.transaction():
        with con.cursor() as cur:
            cur.execute("SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_links_expiry'")
            row = cur.fetchone()
    assert row and "WHERE" in row[0].upper(), row
