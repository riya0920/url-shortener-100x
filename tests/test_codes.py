"""Short-code generation: the random scheme, and the collision path it needs.

The claim under test is not "random codes are shorter" -- that is arithmetic. It
is that dropping snowflake costs nothing in correctness, because the primary key
already reports collisions and a bounded retry is enough to absorb them.
"""
import pytest

from shortener.ids import (
    ALPHABET,
    CodeAllocator,
    RandomCodeGenerator,
    SnowflakeGenerator,
    build_generator,
    decode_base62,
    encode_base62,
)
from shortener.store import LinkStore


@pytest.fixture()
def store(tmp_path):
    """pytest's tmp_path rather than TemporaryDirectory: SQLite keeps a
    thread-local connection open, and Windows refuses to unlink an open file."""
    return LinkStore(str(tmp_path / "links.db"))


# --------------------------------------------------------------------------
# shape
# --------------------------------------------------------------------------

def test_random_codes_have_the_requested_length():
    gen = RandomCodeGenerator(7)
    assert all(len(gen.next_code()) == 7 for _ in range(1000))


def test_random_codes_use_only_the_base62_alphabet():
    gen = RandomCodeGenerator(7)
    for _ in range(1000):
        assert set(gen.next_code()) <= set(ALPHABET)


def test_random_is_shorter_than_snowflake_today():
    """The whole point of the change, asserted rather than assumed."""
    assert len(RandomCodeGenerator(7).next_code()) < len(SnowflakeGenerator(1).next_code())


def test_zero_length_is_refused():
    with pytest.raises(ValueError):
        RandomCodeGenerator(0)


# --------------------------------------------------------------------------
# the property snowflake does not have
# --------------------------------------------------------------------------

def test_snowflake_codes_made_together_are_enumerable():
    """Documents the weakness being fixed, as the attack rather than as a shared
    prefix: decode one code, add one, re-encode, and you hold its neighbour.

    The clock is frozen because the property is about codes issued in the same
    millisecond; against the wall clock three creates may straddle a tick, which
    would make this assert about scheduling rather than about the id scheme."""
    gen = SnowflakeGenerator(1, clock=lambda: 1_800_000_000_000)
    codes = [gen.next_code() for _ in range(3)]
    numbers = [decode_base62(c) for c in codes]

    assert numbers == [numbers[0], numbers[0] + 1, numbers[0] + 2]
    assert encode_base62(numbers[0] + 1) == codes[1], "one code walks to the next"


def test_random_codes_made_together_share_no_prefix():
    gen = RandomCodeGenerator(7)
    codes = [gen.next_code() for _ in range(100)]
    assert len({c[:-1] for c in codes}) == 100


def test_random_codes_do_not_repeat_in_bulk():
    """Not a uniqueness guarantee -- 62**7 makes a repeat in 50k astronomically
    unlikely, so a failure here means the rng is broken, not unlucky."""
    gen = RandomCodeGenerator(7)
    codes = [gen.next_code() for _ in range(50_000)]
    assert len(set(codes)) == len(codes)


def test_every_position_varies():
    """A generator that fixed a position would still pass the tests above."""
    gen = RandomCodeGenerator(7)
    codes = [gen.next_code() for _ in range(500)]
    for pos in range(7):
        assert len({c[pos] for c in codes}) > 20, "position %d barely varies" % pos


# --------------------------------------------------------------------------
# collision handling -- forced, not waited for
# --------------------------------------------------------------------------

class _FixedGenerator:
    """Emits the same code `repeat` times, then fresh ones. Collisions at 62**7
    cannot be provoked by waiting, so they are injected."""

    def __init__(self, code, repeat):
        self.code, self.repeat, self.calls = code, repeat, 0

    def next_code(self):
        self.calls += 1
        return self.code if self.calls <= self.repeat else "fresh%d" % self.calls


def test_allocator_retries_past_a_collision(store):
    store.create("TAKEN01", "https://first.example")
    alloc = CodeAllocator(_FixedGenerator("TAKEN01", repeat=1), max_attempts=5)

    link = alloc.allocate(store, "https://second.example")

    assert link.code != "TAKEN01"
    assert alloc.collisions == 1
    assert alloc.allocated == 1
    assert store.get("TAKEN01").target == "https://first.example", "first link overwritten"


def test_allocator_gives_up_loudly(store):
    store.create("TAKEN01", "https://first.example")
    alloc = CodeAllocator(_FixedGenerator("TAKEN01", repeat=99), max_attempts=3)

    with pytest.raises(RuntimeError, match="3 attempts"):
        alloc.allocate(store, "https://second.example")

    assert alloc.collisions == 3
    assert alloc.allocated == 0


def test_a_clean_allocation_costs_exactly_one_attempt(store):
    gen = _FixedGenerator("unused", repeat=0)
    alloc = CodeAllocator(gen, max_attempts=5)

    alloc.allocate(store, "https://example.com")

    assert gen.calls == 1, "no speculative read before the insert"
    assert alloc.collisions == 0


def test_max_attempts_must_allow_one_try():
    with pytest.raises(ValueError):
        CodeAllocator(RandomCodeGenerator(7), max_attempts=0)


# --------------------------------------------------------------------------
# both schemes work end to end through the same allocator
# --------------------------------------------------------------------------

@pytest.mark.parametrize("scheme", ["random", "snowflake"])
def test_either_scheme_stores_and_resolves(store, scheme):
    alloc = CodeAllocator(build_generator(scheme, instance_id=1, length=7))

    link = alloc.allocate(store, "https://example.com/somewhere", ttl_seconds=60)

    assert store.get(link.code).target == "https://example.com/somewhere"
    assert alloc.collisions == 0


def test_unknown_scheme_is_refused():
    with pytest.raises(ValueError, match="unknown code scheme"):
        build_generator("base64ish")


def test_keyspace_matches_the_documented_arithmetic():
    assert RandomCodeGenerator(7).keyspace() == 62 ** 7
    assert RandomCodeGenerator(8).keyspace() == 62 ** 8
