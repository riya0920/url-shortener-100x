"""Short-code generation that survives multiple instances with no coordination.

Snowflake-style 64-bit id: timestamp | instance | sequence, rendered base62.

    | 41 bits ms since epoch | 10 bits instance | 12 bits sequence |

Why this and not the alternatives:

* **Auto-increment integer.** Requires a round trip to a shared sequence on every
  create -- coordination on the hot path, and the database becomes the write
  bottleneck at exactly the moment you need to scale out. Also enumerable: `/1`,
  `/2`, `/3` walks every link in the system.
* **Random base62 (e.g. 7 chars).** No coordination. This bullet used to reject
  it for "a uniqueness CHECK against the store on every create, which is a read
  plus a retry loop" -- and that was wrong. `code` is the primary key, so the
  INSERT the create was already doing reports the conflict itself; there is no
  preceding read to remove. The birthday bound is real but small: at 62**7 codes
  and ten million links a create collides once in 350,000. Implemented below as
  `RandomCodeGenerator`, and now the default, because it is three characters
  shorter and carries no structure to walk.
* **UUID4.** No coordination and no check, but 22+ base62 characters is a bad
  short link, and the randomness destroys index locality on insert.
* **Snowflake (still here, no longer the default).** No coordination on the hot
  path, no uniqueness check, and ids are k-sorted by time so index inserts stay
  local -- which matters in a write-heavy store and does not in this one, where
  resolves outnumber creates by orders of magnitude. Selectable via
  `CODE_SCHEME=snowflake` so the comparison can be run rather than asserted.

What it costs, stated honestly:
* **Instance ids must be unique.** That is coordination -- just moved off the hot
  path to startup. Duplicated instance ids silently produce duplicate codes, so
  the source must be authoritative (ordinal from an orchestrator, or a lease).
* **Clock dependency.** A backwards clock step can repeat a (ms, sequence) pair.
  Handled explicitly below rather than ignored.
* **Ids leak creation time and rough volume.** Anyone can decode a timestamp and,
  by creating two links, estimate the rate between them. Acceptable for public
  short links; not acceptable if the ids were capability tokens.
* **Ids are enumerable, which this file failed to say while rejecting the counter
  for exactly that.** Codes issued in the same millisecond are adjacent -- the
  demo shows `GABebxPvHs`, `...Ht`, `...Hu` -- so one code walks to its
  neighbours. Weaker than `/1, /2, /3` because a gap in time costs the attacker
  4194304 candidates per millisecond skipped, but the same class of flaw.
* **Codes are 10 characters** and grow to 11 in 2038, against 7 for a random code
  of equivalent practical safety.
"""
from __future__ import annotations

import secrets
import threading
import time

ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
BASE = len(ALPHABET)

EPOCH_MS = 1_735_689_600_000       # 2025-01-01T00:00:00Z
INSTANCE_BITS = 10                 # 1024 instances
SEQUENCE_BITS = 12                 # 4096 ids per instance per millisecond
MAX_INSTANCE = (1 << INSTANCE_BITS) - 1
MAX_SEQUENCE = (1 << SEQUENCE_BITS) - 1


def encode_base62(n: int) -> str:
    if n < 0:
        raise ValueError("negative id")
    if n == 0:
        return ALPHABET[0]
    out = []
    while n:
        n, rem = divmod(n, BASE)
        out.append(ALPHABET[rem])
    return "".join(reversed(out))


def decode_base62(s: str) -> int:
    n = 0
    for ch in s:
        idx = ALPHABET.find(ch)
        if idx < 0:
            raise ValueError("invalid base62 character: %r" % ch)
        n = n * BASE + idx
    return n


class SnowflakeGenerator:
    """Thread-safe id generator for one instance."""

    def __init__(self, instance_id: int, clock=None):
        if not 0 <= instance_id <= MAX_INSTANCE:
            raise ValueError("instance_id must be in [0, %d]" % MAX_INSTANCE)
        self.instance_id = instance_id
        self._clock = clock or (lambda: int(time.time() * 1000))
        self._lock = threading.Lock()
        self._last_ms = -1
        self._sequence = 0

    def _now(self) -> int:
        return self._clock()

    def next_id(self) -> int:
        with self._lock:
            now = self._now()

            if now < self._last_ms:
                # Clock moved backwards (NTP correction, VM migration). Reusing
                # this millisecond could repeat a (ms, sequence) pair that was
                # already issued, so we refuse to issue rather than risk a
                # duplicate short code. Waiting is correct here because the drift
                # is normally milliseconds; a large jump SHOULD be loud.
                drift = self._last_ms - now
                if drift > 5_000:
                    raise RuntimeError(
                        "clock moved backwards by %dms; refusing to generate ids" % drift
                    )
                while now < self._last_ms:
                    now = self._now()

            if now == self._last_ms:
                self._sequence += 1
                if self._sequence > MAX_SEQUENCE:
                    # Exhausted this millisecond's 4096 ids: spin to the next one.
                    while now <= self._last_ms:
                        now = self._now()
                    self._sequence = 0
            else:
                self._sequence = 0

            self._last_ms = now
            return ((now - EPOCH_MS) << (INSTANCE_BITS + SEQUENCE_BITS)) \
                | (self.instance_id << SEQUENCE_BITS) \
                | self._sequence

    def next_code(self) -> str:
        return encode_base62(self.next_id())


def decode_id(snowflake: int) -> dict:
    """Unpack an id. Used by tests and by the ops tooling, not on the hot path."""
    sequence = snowflake & MAX_SEQUENCE
    instance = (snowflake >> SEQUENCE_BITS) & MAX_INSTANCE
    ms = (snowflake >> (INSTANCE_BITS + SEQUENCE_BITS)) + EPOCH_MS
    return {"timestamp_ms": ms, "instance_id": instance, "sequence": sequence}


# ---------------------------------------------------------------------------
# Random codes -- the shorter, unguessable alternative
# ---------------------------------------------------------------------------

class RandomCodeGenerator:
    """Cryptographically random base62 codes of a fixed length.

    Shorter than snowflake (7 characters vs 10) and carrying no structure, so a
    holder of one code cannot walk to its neighbours. Snowflake codes made in the
    same millisecond differ only in the final character, which is the same
    enumeration weakness `SnowflakeGenerator` cites when rejecting a counter --
    less obvious in base62, but present.

    The cost is that uniqueness is no longer guaranteed by construction, so a
    create can collide. `CodeAllocator` handles that, and the reason it is cheap
    is that the check the module docstring warns about ("a read plus a retry
    loop") is not a read: `code` is the primary key, so the INSERT already
    reports the conflict. There is nothing to add to the hot path.

    Collision probability per create is `stored_links / 62**length`:

        length 7  ->  3.5e12 codes  ->  1 in 350,000 at ten million links
        length 8  ->  2.2e14 codes  ->  1 in 22 million at ten million links

    `secrets` rather than `random`: codes are guessable-by-design only if the
    generator is predictable, and Mersenne Twister state is recoverable from
    output. That is irrelevant for public links and fatal the day someone
    shortens something they assumed was unlisted.

    What this gives up, stated as plainly as the snowflake trade-off: random keys
    scatter B-tree inserts where k-sorted ones stay local, so writes cost more at
    high create volume. A shortener resolves far more often than it creates, so
    the trade runs the right way here; it would not in a write-heavy store.
    """

    def __init__(self, length: int = 7, rng=None):
        if length < 1:
            raise ValueError("length must be >= 1")
        self.length = length
        # Injectable so the collision tests can force one deterministically.
        self._rng = rng or secrets.choice

    def next_code(self) -> str:
        return "".join(self._rng(ALPHABET) for _ in range(self.length))

    def keyspace(self) -> int:
        return BASE ** self.length


class CodeAllocator:
    """Generates a code and writes the link, retrying if the store rejects it.

    A store signals "that code is taken" by raising `KeyError` -- both backends
    already do, from the primary key rather than from a preceding lookup.

    Retries are bounded. Exhausting them means either an impossibly unlucky run
    or a genuine bug (a duplicated INSTANCE_ID under snowflake, a broken rng), and
    both deserve a loud failure rather than a loop that hides them.
    """

    def __init__(self, generator, max_attempts: int = 5):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.generator = generator
        self.max_attempts = max_attempts
        self.allocated = 0
        self.collisions = 0

    def allocate(self, store, target: str, ttl_seconds: int | None = None):
        for _ in range(self.max_attempts):
            code = self.generator.next_code()
            try:
                link = store.create(code, target, ttl_seconds)
            except KeyError:
                self.collisions += 1
                continue
            self.allocated += 1
            return link
        raise RuntimeError(
            "could not allocate a free short code in %d attempts; "
            "check INSTANCE_ID uniqueness (snowflake) or code length (random)"
            % self.max_attempts)

    def stats(self) -> dict:
        return {"scheme": type(self.generator).__name__,
                "allocated": self.allocated,
                "collisions": self.collisions,
                "max_attempts": self.max_attempts}


def build_generator(scheme: str, instance_id: int = 0, length: int = 7):
    """Factory used by the app. Both schemes stay in the tree on purpose -- the
    design doc argues one against the other, and an argument whose losing side
    cannot be run is an assertion."""
    if scheme == "random":
        return RandomCodeGenerator(length)
    if scheme == "snowflake":
        return SnowflakeGenerator(instance_id)
    raise ValueError("unknown code scheme: %r (want 'random' or 'snowflake')" % scheme)
