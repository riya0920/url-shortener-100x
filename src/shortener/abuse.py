"""Abuse controls: the part of a link shortener that is not a systems problem.

A shortener is an open redirector with a database. Every one of them becomes a
phishing and malware distribution channel, and the controls are not optional —
they are the difference between a service and a liability.

## What is here, and the reasoning for each

**Blocklist at create time, not at resolve time.** Rejecting on create is cheap
(once per link) and rejecting on resolve is expensive (once per click, on the hot
path). It is also the only one that gives the abuser useful feedback, which is a
real cost — an attacker learns the blocklist by probing. The trade is made
deliberately in favour of not putting a policy check on a path that has to run at
thousands of requests per second, and the residual risk is handled by takedown.

**Shortener chaining is refused.** Shortening another shortener defeats every
downstream reputation check, because the scanner sees a domain with a good
reputation. This is the single most common evasion and it is a two-line fix.

**Takedown invalidates caches, or it is not a takedown.** This is the part that is
easy to get wrong: marking a row in the database while a hot code sits in an
in-process LRU on every instance means the link keeps resolving. On one instance
that is a local cache purge. Across N instances it is a distributed cache
invalidation problem, and it is now solved by a **durable, replayable
invalidation log** rather than a fire-and-forget message — see
`invalidation.py`. `TakedownList` reports a measured propagation bound instead of
the "UNBOUNDED" it used to have to admit to.

**Interstitials, not blanket blocking, for the uncertain middle.** Most suspicious
links are not provably malicious. A hard block on a maybe is a support ticket; an
interstitial that names the destination and makes the user click through costs an
attacker the automation they depend on. Reputation decides which of the three
outcomes a link gets: allow, interstitial, refuse.
"""
from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, field
from urllib.parse import urlparse

# Known link shorteners. Shortening one of these hides the real destination from
# every downstream scanner, so it is refused outright rather than interstitialled.
SHORTENER_DOMAINS = {
    "bit.ly", "tinyurl.com", "t.co", "goo.gl", "ow.ly", "is.gd", "buff.ly",
    "adf.ly", "bit.do", "cutt.ly", "rebrand.ly", "shorturl.at", "rb.gy", "s.id",
}

# Domains that have earned a permanent refusal.
DEFAULT_BLOCKLIST = {"malware-example.test", "phish-example.test"}

# Free hosting and dynamic-DNS providers: not malicious, but heavily abused and
# with no reputation of their own to inherit. These get an interstitial.
LOW_TRUST_SUFFIXES = (".zip", ".mov", ".top", ".xyz", ".click", ".duckdns.org",
                      ".ngrok.io", ".trycloudflare.com")

# Anything that is not a public web destination. An open redirector that will
# emit a redirect to a private address is an SSRF pivot for anything that
# follows redirects server-side, which includes most link previewers.
PRIVATE_HOST_PATTERNS = (
    re.compile(r"^localhost$", re.I),
    re.compile(r"^127\."),
    re.compile(r"^10\."),
    re.compile(r"^192\.168\."),
    re.compile(r"^172\.(1[6-9]|2\d|3[01])\."),
    re.compile(r"^169\.254\."),          # link-local, incl. cloud metadata
    re.compile(r"^\[?::1\]?$"),
    re.compile(r"^\[?fd[0-9a-f]{2}:", re.I),
    re.compile(r"\.internal$", re.I),
    re.compile(r"\.local$", re.I),
)

ALLOW, INTERSTITIAL, REFUSE = "allow", "interstitial", "refuse"


@dataclass
class Verdict:
    action: str
    reason: str
    host: str = ""

    @property
    def allowed(self) -> bool:
        return self.action != REFUSE


def host_of(url: str) -> str:
    try:
        h = (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""
    return h


def registrable_suffix_match(host: str, domains) -> bool:
    """Match a host against a domain set, including subdomains.

    `evil.bit.ly` must match `bit.ly`. A plain set membership test misses that,
    which is a blocklist that any attacker bypasses with one label. This is not a
    public-suffix-list implementation and does not claim to be — it is a suffix
    match on labels, which is the correct semantics for "this domain and anything
    under it".
    """
    for d in domains:
        if host == d or host.endswith("." + d):
            return True
    return False


class ReputationPolicy:
    """Decide allow / interstitial / refuse for a destination URL."""

    def __init__(self, blocklist=None, shorteners=None, low_trust=None,
                 allow_private: bool = False):
        self.blocklist = set(blocklist if blocklist is not None else DEFAULT_BLOCKLIST)
        self.shorteners = set(shorteners if shorteners is not None else SHORTENER_DOMAINS)
        self.low_trust = tuple(low_trust if low_trust is not None else LOW_TRUST_SUFFIXES)
        # Tests and single-machine demos need to shorten http://localhost URLs.
        # Production must not, so the escape hatch is explicit and named.
        self.allow_private = allow_private

    def evaluate(self, url: str) -> Verdict:
        host = host_of(url)
        if not host:
            return Verdict(REFUSE, "unparseable or hostless URL")

        if not self.allow_private and any(p.search(host) for p in PRIVATE_HOST_PATTERNS):
            return Verdict(REFUSE, "destination is a private or link-local address", host)

        if registrable_suffix_match(host, self.blocklist):
            return Verdict(REFUSE, "destination domain is blocklisted", host)

        if registrable_suffix_match(host, self.shorteners):
            return Verdict(REFUSE, "chaining another shortener hides the real destination", host)

        if host.endswith(self.low_trust):
            return Verdict(INTERSTITIAL, "low-trust domain: destination shown before redirect", host)

        return Verdict(ALLOW, "ok", host)


class TakedownList:
    """Codes withdrawn after the fact, and what it takes to make that stick.

    A takedown has three parts and only two of them are easy:

      1. mark the code (durable, cheap)
      2. purge it from *this* instance's cache (local, cheap)
      3. purge it from every *other* instance's cache (distributed)

    Part 3 used to be unimplemented, and `add()` reported "UNBOUNDED" rather than
    claiming success. It is now handled by publishing to a **durable invalidation
    log** (`invalidation.SqliteInvalidationBus`) that every instance replays from
    its own cursor. The difference from a published message is that an instance
    which was restarting, paused or partitioned when the takedown happened still
    converges: it replays what it missed rather than never learning.

    What that buys is a *bound*, not a confirmation. No instance knows how many
    instances exist, so `add()` reports "every instance polling at interval T has
    applied this within T", measured rather than assumed. Turning a bound into a
    confirmation needs instance registration and acks, which is service discovery
    and is out of scope here — and is said so rather than glossed.
    """

    def __init__(self, ttl_bound_s: float = 0.0, bus=None, poll_interval_s: float = None):
        self._lock = threading.Lock()
        self._codes = {}
        # The cache TTL bounds how long a stale entry can survive elsewhere, and
        # is the fallback when there is no bus at all.
        self.ttl_bound_s = ttl_bound_s
        self.bus = bus
        self.poll_interval_s = (poll_interval_s if poll_interval_s is not None
                                else getattr(bus, "poll_interval_s", None))
        self.local_purges = 0
        self.published = 0

    def add(self, code: str, reason: str, purge=None) -> dict:
        with self._lock:
            self._codes[code] = {"reason": reason, "at": time.time()}
        if purge is not None:
            purge(code)
            self.local_purges += 1

        seq = None
        if self.bus is not None:
            # Published AFTER the local purge, so the instance handling the
            # request is never the last to know about its own action.
            seq = self.bus.publish(code, reason)
            self.published += 1

        return {
            "code": code,
            "reason": reason,
            "local_cache_purged": purge is not None,
            "invalidation_seq": seq,
            "propagation": (
                "published to the invalidation log at seq %d; every instance polling at %.1fs "
                "has applied it within that bound, including instances that were down when it "
                "was written" % (seq, self.poll_interval_s or 0.0)
                if seq is not None else
                "complete on this instance only; other instances may serve this code from "
                "their own caches until eviction"
                + (" (bounded at %.0fs by the cache TTL)" % self.ttl_bound_s
                   if self.ttl_bound_s else " (UNBOUNDED: the cache has no TTL)")),
            "confirmed_on_all_instances": False,
            "why_not_confirmed": ("no instance knows how many instances exist; this is a "
                                  "propagation bound, not an acknowledgement"),
        }

    def __contains__(self, code: str) -> bool:
        with self._lock:
            return code in self._codes

    def reason(self, code: str) -> str:
        with self._lock:
            entry = self._codes.get(code)
        return entry["reason"] if entry else ""

    def stats(self) -> dict:
        with self._lock:
            n = len(self._codes)
        return {
            "taken_down": n,
            "local_purges": self.local_purges,
            "published_to_bus": self.published,
            "cross_instance_invalidation": (
                "durable log, replayable, bound %.1fs" % (self.poll_interval_s or 0.0)
                if self.bus is not None else
                "not configured -- local purge only"),
            "bus": self.bus.stats() if self.bus is not None else None,
        }


INTERSTITIAL_HTML = """<!doctype html><meta charset="utf-8">
<title>Leaving &mdash; check this link</title>
<style>body{font:15px/1.6 -apple-system,Segoe UI,Roboto,sans-serif;max-width:640px;
margin:12vh auto;padding:0 20px;color:#222}
.dest{background:#f6f6f6;border:1px solid #e2e2e2;border-radius:6px;padding:12px;
word-break:break-all;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:13px}
.why{color:#8a6d1f;background:#fff9e6;border:1px solid #f0e0a8;border-radius:6px;padding:10px 12px}
a.go{display:inline-block;margin-top:18px;padding:9px 16px;background:#222;color:#fff;
border-radius:6px;text-decoration:none}
.small{color:#888;font-size:12.5px;margin-top:22px}</style>
<h2>You are about to leave</h2>
<p class="why">%(reason)s</p>
<p>This short link points to:</p>
<div class="dest">%(target)s</div>
<a class="go" rel="noopener noreferrer nofollow" href="%(target)s">Continue to this site</a>
<p class="small">The destination is shown in full because a short link hides it, and
hiding the destination is what makes short links useful to phishing. Nothing on this
page loads from the destination.</p>
"""


def render_interstitial(target: str, reason: str) -> str:
    """The destination is escaped, and nothing is fetched from it.

    Two things this page must not do: render the target unescaped (that is stored
    XSS on your own domain, handed to you by whoever created the link), and load
    anything from the destination for a preview — a favicon fetch alone confirms
    to the attacker that the link was opened and by whom.
    """
    import html as _html

    safe = _html.escape(target, quote=True)
    return INTERSTITIAL_HTML % {"target": safe, "reason": _html.escape(reason)}
