"""Abuse controls, and the half-open probe under real concurrency.

The breaker test at the bottom is the one that matters most: the half-open
single-probe rule was previously only checked sequentially, and a
single-probe rule is exactly the kind of invariant that holds in a for-loop and
dies under threads.
"""
import threading

import pytest

from shortener.abuse import (
    ALLOW,
    INTERSTITIAL,
    REFUSE,
    ReputationPolicy,
    TakedownList,
    host_of,
    registrable_suffix_match,
    render_interstitial,
)
from shortener.breaker import CircuitBreaker, CircuitState


# --- policy ----------------------------------------------------------------

@pytest.fixture
def policy():
    return ReputationPolicy()


def test_ordinary_url_is_allowed(policy):
    assert policy.evaluate("https://docs.example.com/guide").action == ALLOW


def test_shortener_chaining_is_refused(policy):
    """The most common evasion: the scanner sees a reputable domain."""
    assert policy.evaluate("https://bit.ly/xyz").action == REFUSE


def test_shortener_chaining_is_refused_on_subdomains(policy):
    """A blocklist that any attacker bypasses with one extra label is not one."""
    assert policy.evaluate("https://evil.bit.ly/xyz").action == REFUSE


def test_blocklist_matches_subdomains_too(policy):
    assert policy.evaluate("https://a.b.malware-example.test/p").action == REFUSE


def test_suffix_match_does_not_match_a_lookalike_domain():
    """`notbit.ly` must not match `bit.ly`. Naive `endswith` gets this wrong."""
    assert not registrable_suffix_match("notbit.ly", {"bit.ly"})
    assert registrable_suffix_match("a.bit.ly", {"bit.ly"})
    assert registrable_suffix_match("bit.ly", {"bit.ly"})


@pytest.mark.parametrize("url", [
    "http://localhost:8000/admin",
    "http://127.0.0.1/",
    "http://10.1.2.3/",
    "http://192.168.0.1/",
    "http://172.16.5.5/",
    "http://169.254.169.254/latest/meta-data/",   # cloud metadata
    "http://db.internal/",
])
def test_private_and_metadata_destinations_are_refused(policy, url):
    """An open redirector that emits redirects to private space is an SSRF pivot
    for everything that follows redirects server-side, which is most link
    previewers and every crawler."""
    assert policy.evaluate(url).action == REFUSE


def test_private_targets_can_be_allowed_explicitly():
    """The escape hatch exists for tests and the local demo, and is named."""
    assert ReputationPolicy(allow_private=True).evaluate("http://localhost:8000/x").action == ALLOW


def test_low_trust_domains_get_an_interstitial_not_a_block(policy):
    """Most suspicious links are not provably malicious. A hard block on a maybe
    is a support ticket; an interstitial costs the attacker their automation."""
    v = policy.evaluate("https://free-thing.xyz/promo")
    assert v.action == INTERSTITIAL
    assert v.allowed


def test_hostless_url_is_refused(policy):
    assert policy.evaluate("https:///nohost").action == REFUSE


def test_host_of_is_case_insensitive():
    assert host_of("https://EXAMPLE.COM/x") == "example.com"


# --- interstitial rendering ------------------------------------------------

def test_interstitial_escapes_the_destination():
    """Rendering an attacker-supplied URL unescaped is stored XSS on your own
    domain, handed to you by whoever created the link."""
    html = render_interstitial('https://x.test/"><script>alert(1)</script>', "low trust")
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;" in html


def test_interstitial_does_not_fetch_anything_from_the_destination():
    """No preview image, no favicon, no iframe. A single fetch confirms to the
    attacker that the link was opened."""
    html = render_interstitial("https://x.test/p", "low trust")
    for tag in ("<img", "<iframe", "<script", "<link rel=\"stylesheet\""):
        assert tag not in html


def test_interstitial_link_carries_noreferrer():
    html = render_interstitial("https://x.test/p", "low trust")
    assert "noopener" in html and "noreferrer" in html


# --- takedown --------------------------------------------------------------

def test_takedown_purges_the_local_cache():
    purged = []
    t = TakedownList()
    r = t.add("abc", "phishing", purge=purged.append)
    assert purged == ["abc"]
    assert r["local_cache_purged"]
    assert "abc" in t


def test_takedown_does_not_claim_cross_instance_completion():
    """The honest part. Marking a row while a hot code sits in another instance's
    LRU is not a takedown, and the response must not say it is."""
    r = TakedownList().add("abc", "phishing", purge=lambda c: None)
    assert "this instance only" in r["propagation"]
    assert "UNBOUNDED" in r["propagation"]


def test_takedown_reports_a_bounded_window_when_the_cache_has_a_ttl():
    r = TakedownList(ttl_bound_s=60).add("abc", "x", purge=lambda c: None)
    assert "bounded at 60s" in r["propagation"]


def test_takedown_stats_name_the_missing_piece():
    t = TakedownList()
    t.add("a", "x")
    assert "not implemented" in t.stats()["cross_instance_invalidation"]


# --- the breaker's half-open probe, under threads --------------------------

class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def test_exactly_one_probe_is_admitted_when_many_threads_race_the_half_open():
    """The invariant the sequential test could not check.

    `allow()` transitions OPEN -> HALF_OPEN and admits the caller in the same
    critical section. If that transition were outside the lock -- or if the
    counter were incremented before the state check -- 64 threads arriving the
    instant the cooldown expires would all be admitted, and a service that just
    fell over would take 64 simultaneous probes.
    """
    clock = _Clock()
    br = CircuitBreaker(failure_threshold=2, cooldown_s=5.0, clock=clock)
    for _ in range(2):
        br.record_failure()
    assert br.state is CircuitState.OPEN

    clock.t += 6.0                       # cooldown has expired
    barrier = threading.Barrier(64)
    admitted = []
    lock = threading.Lock()

    def worker():
        barrier.wait()                   # all 64 hit allow() together
        ok = br.allow()
        if ok:
            with lock:
                admitted.append(1)

    threads = [threading.Thread(target=worker) for _ in range(64)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(admitted) == 1, "half-open admitted %d probes, must admit exactly 1" % len(admitted)
    assert br.probes == 1
    assert br.short_circuited == 63


def test_a_failed_probe_reopens_and_restarts_the_cooldown_under_threads():
    clock = _Clock()
    br = CircuitBreaker(failure_threshold=2, cooldown_s=5.0, clock=clock)
    br.record_failure()
    br.record_failure()
    clock.t += 6.0
    assert br.allow()                    # the probe
    br.record_failure()                  # and it fails
    assert br.state is CircuitState.OPEN

    # Everyone else is refused again, and the cooldown restarted from now.
    results = []
    threads = [threading.Thread(target=lambda: results.append(br.allow())) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not any(results)


def test_a_successful_probe_closes_the_circuit_for_everyone():
    clock = _Clock()
    br = CircuitBreaker(failure_threshold=2, cooldown_s=5.0, clock=clock)
    br.record_failure()
    br.record_failure()
    clock.t += 6.0
    assert br.allow()
    br.record_success()

    results = []
    threads = [threading.Thread(target=lambda: results.append(br.allow())) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert all(results)
