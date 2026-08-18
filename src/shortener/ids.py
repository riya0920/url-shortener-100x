"""Short-code generation that survives multiple instances with no coordination.

Snowflake-style 64-bit id: timestamp | instance | sequence, rendered base62.

    | 41 bits ms since epoch | 10 bits instance | 12 bits sequence |

Why this and not the alternatives:

* **Auto-increment integer.** Requires a round trip to a shared sequence on every
  create -- coordination on the hot path, and the database becomes the write
  bottleneck at exactly the moment you need to scale out. Also enumerable: `/1`,
  `/2`, `/3` walks every link in the system.
* **Random base62 (e.g. 7 chars).** No coordination, but requires a uniqueness
  CHECK against the store on every create, which is a read plus a retry loop, and
  the birthday-bound collision probability creeps up as the corpus grows. Fine at
  small scale; the check is the thing you cannot remove.
* **UUID4.** No coordination and no check, but 22+ base62 characters is a bad
  short link, and the randomness destroys index locality on insert.
* **Snowflake (chosen).** No coordination on the hot path, no uniqueness check,
  and ids are k-sorted by time so index inserts stay local.

What it costs, stated honestly:
* **Instance ids must be unique.** That is coordination -- just moved off the hot
  path to startup. Duplicated instance ids silently produce duplicate codes, so
  the source must be authoritative (ordinal from an orchestrator, or a lease).
* **Clock dependency.** A backwards clock step can repeat a (ms, sequence) pair.
  Handled explicitly below rather than ignored.
* **Ids leak creation time and rough volume.** Anyone can decode a timestamp and,
  by creating two links, estimate the rate between them. Acceptable for public
  short links; not acceptable if the ids were capability tokens.
"""
from __future__ import annotations

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
