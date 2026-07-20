"""Unit tests for the pure controlled-egress policy contract.

These tests enforce the strict, deterministic allowlist for the HTTPS proxy
used by networked sandboxes. All checks are static (no DNS).
"""

import pytest
from pydantic import HttpUrl, ValidationError

from vuzol.config.models import EgressDestination, NetworkPolicy
from vuzol.execution.egress import (
    AllowedConnectTarget,
    _extract_host,
    _normalize_hostname,
    allowlist_stable_hash,
    compile_proxy_allowlist,
)


def _dest(url: str, purpose: str = "test") -> EgressDestination:
    from pydantic import HttpUrl

    return EgressDestination(url=HttpUrl(url), purpose=purpose)


def test_compile_basic_https_hostname_and_implicit_port() -> None:
    pol = NetworkPolicy(
        enabled=True,
        destinations=(_dest("https://api.example.com"),),
    )
    targets = compile_proxy_allowlist(pol)
    assert len(targets) == 1
    t = targets[0]
    assert t.hostname == "api.example.com"
    assert t.port == 443
    assert t.purpose == "test"
    assert isinstance(t, AllowedConnectTarget)


def test_compile_canonicalizes_mixed_case_and_idna() -> None:
    pol = NetworkPolicy(
        enabled=True,
        destinations=(_dest("https://API.Example.COM"),),
    )
    targets = compile_proxy_allowlist(pol)
    assert targets[0].hostname == "api.example.com"


def test_compile_explicit_443_is_normalized() -> None:
    pol = NetworkPolicy(
        enabled=True,
        destinations=(_dest("https://api.example.com:443"),),
    )
    targets = compile_proxy_allowlist(pol)
    assert targets[0].port == 443


def test_compile_rejects_non_443_port() -> None:
    pol = NetworkPolicy(
        enabled=True,
        destinations=(_dest("https://api.example.com:8443"),),
    )
    with pytest.raises(ValueError, match="443"):
        compile_proxy_allowlist(pol)


def test_compile_rejects_non_https_via_policy() -> None:
    # http is rejected at NetworkPolicy / EgressDestination validation time
    with pytest.raises(ValidationError, match="https"):
        NetworkPolicy.model_validate(
            {"enabled": True, "destinations": [{"url": "http://api.example.com", "purpose": "x"}]}
        )


def test_compile_rejects_ip_literals_at_egress_validation() -> None:
    # IP rejection (including global) now happens at EgressDestination / NetworkPolicy level
    for bad in ["https://8.8.8.8", "https://[2001:db8::1]"]:
        with pytest.raises(ValidationError, match="IP literal"):
            NetworkPolicy.model_validate(
                {"enabled": True, "destinations": ({"url": bad, "purpose": "x"},)}
            )


def test_compile_rejects_forbidden_hosts_via_egress_validation() -> None:
    for bad in ["localhost", "metadata.google.internal", "example.local"]:
        with pytest.raises(ValidationError):
            NetworkPolicy.model_validate(
                {"enabled": True, "destinations": ({"url": f"https://{bad}", "purpose": "x"},)}
            )


def test_compile_rejects_bad_url_shapes_via_egress_validation() -> None:
    for url in [
        "https://user:pass@api.example.com",  # pragma: allowlist secret
        "https://api.example.com?foo=bar",
        "https://api.example.com#frag",
        "https://api.example.com:443/path",
    ]:
        with pytest.raises(ValidationError):
            NetworkPolicy.model_validate(
                {"enabled": True, "destinations": ({"url": url, "purpose": "x"},)}
            )


def test_compile_disabled_policy_must_have_no_destinations() -> None:
    with pytest.raises(ValueError, match=r"disabled.*cannot declare"):
        compile_proxy_allowlist(
            NetworkPolicy(enabled=False, destinations=(_dest("https://a.com"),))
        )


def test_compile_enabled_requires_destinations() -> None:
    with pytest.raises(ValueError, match="requires at least one"):
        compile_proxy_allowlist(NetworkPolicy(enabled=True, destinations=()))


def test_compile_deduplicates_same_purpose_and_is_deterministic() -> None:
    pol = NetworkPolicy(
        enabled=True,
        destinations=(
            _dest("https://API.example.com", "provider API"),
            _dest("https://api.example.com", "provider API"),
        ),
    )
    t1 = compile_proxy_allowlist(pol)
    t2 = compile_proxy_allowlist(pol)
    assert t1 == t2
    assert len(t1) == 1
    assert t1[0].purpose == "provider API"


def test_compile_rejects_conflicting_purpose_for_same_canonical() -> None:
    pol = NetworkPolicy(
        enabled=True,
        destinations=(
            _dest("https://api.example.com", "purpose one"),
            _dest("https://API.example.com", "purpose two"),
        ),
    )
    with pytest.raises(ValueError, match="conflicting purposes"):
        compile_proxy_allowlist(pol)


def test_compile_includes_provider_api_hosts() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://project.example.com"),))
    targets = compile_proxy_allowlist(pol, provider_api_hosts=("api.provider.com",))
    hosts = {t.hostname for t in targets}
    assert "project.example.com" in hosts
    assert "api.provider.com" in hosts


def test_compile_rejects_bad_provider_host() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    with pytest.raises(ValueError, match=r"prohibited|IP literal"):
        compile_proxy_allowlist(pol, provider_api_hosts=("*.bad.com",))


def test_compile_rejects_ip_literal_provider_host() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    for bad in ["8.8.8.8", "2001:db8::1", "127.0.0.1", "::1"]:
        with pytest.raises(ValueError, match="IP literal"):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_compile_rejects_trailing_dot_wildcard_and_invalid_hosts() -> None:
    """Trailing dot, wildcards, and leading dot are rejected (exact host only)."""
    # For dest: model accepts the URL, compile enforces
    with pytest.raises(ValueError, match="wildcard or invalid"):
        compile_proxy_allowlist(
            NetworkPolicy(enabled=True, destinations=(_dest("https://example.com."),))
        )
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    for bad in ["*.example.com", ".example.com", "example.com."]:
        with pytest.raises(ValueError, match="wildcard or invalid"):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_compile_rejects_invalid_idna_hostname() -> None:
    """Invalid for IDNA (e.g. double dot) is rejected during normalization."""
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    with pytest.raises(ValueError, match="IDNA normalization"):
        compile_proxy_allowlist(pol, provider_api_hosts=("ex..com",))


def test_normalize_and_extract_helpers_reject_invalid_hosts() -> None:
    """Hostname normalization and host extraction fail closed on invalid input."""
    # normalize
    with pytest.raises(ValueError, match="must not be empty"):
        _normalize_hostname("")
    with pytest.raises(ValueError, match="trailing dot"):
        _normalize_hostname("ex.com.")
    with pytest.raises(ValueError, match="IDNA normalization"):
        _normalize_hostname("ex..com")
    assert _normalize_hostname("API.Example.COM") == "api.example.com"

    # extract
    assert _extract_host("api.com") == "api.com"
    assert _extract_host("api.com:443") == "api.com"
    assert _extract_host("[::1]") == "::1"
    assert _extract_host("[2001:db8::1]:443") == "2001:db8::1"
    assert _extract_host("2001:db8::1") == "2001:db8::1"
    assert _extract_host("") == ""
    assert _extract_host("  spaced  ") == "spaced"


def test_compile_internal_checks_via_construct() -> None:
    """Exercise compile policy checks even if model validators bypassed (defense)."""
    # disabled with dests
    bad_disabled = NetworkPolicy.model_construct(
        enabled=False, destinations=(_dest("https://x.com"),)
    )
    with pytest.raises(ValueError, match="disabled network policy cannot declare"):
        compile_proxy_allowlist(bad_disabled)

    # enabled no dests
    bad_empty = NetworkPolicy.model_construct(enabled=True, destinations=())
    with pytest.raises(ValueError, match="requires at least one"):
        compile_proxy_allowlist(bad_empty)

    # disabled/enabled checks exercised via construct to test compiler defense


def test_target_str_and_provider_empty_skipped() -> None:
    """Cover __str__ and provider empty-skip branch."""
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://api.example.com"),))
    ts = compile_proxy_allowlist(pol, provider_api_hosts=("", "other.com"))
    assert len(ts) == 2
    s = str(ts[0])
    assert "api.example.com:443" in s
    assert "test" in s


def test_compile_canonicalizes_provider_host() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    targets = compile_proxy_allowlist(pol, provider_api_hosts=("API.Provider.COM",))
    assert any(
        t.hostname == "api.provider.com" and t.purpose == "provider API endpoint" for t in targets
    )


def test_allowed_target_is_immutable() -> None:
    t = AllowedConnectTarget(hostname="h.com", port=443, purpose="p")
    with pytest.raises(Exception, match=r"frozen|immutable|assign|set"):
        t.port = 80


def test_allowed_target_only_443() -> None:
    with pytest.raises(ValueError, match="443"):
        AllowedConnectTarget(hostname="h.com", port=80, purpose="p")


def test_compile_root_path_normalization() -> None:
    """Equivalent origins with or without root path / normalize to same target."""
    pol1 = NetworkPolicy(enabled=True, destinations=(_dest("https://api.example.com"),))
    pol2 = NetworkPolicy(enabled=True, destinations=(_dest("https://api.example.com/"),))
    t1 = compile_proxy_allowlist(pol1)
    t2 = compile_proxy_allowlist(pol2)
    assert t1 == t2
    assert t1[0].hostname == "api.example.com"
    assert t1[0].port == 443


def test_compile_deterministic_ordering() -> None:
    """Multiple distinct targets are returned in stable sorted order."""
    pol = NetworkPolicy(
        enabled=True,
        destinations=(
            _dest("https://z.example.com", "z"),
            _dest("https://a.example.com", "a"),
            _dest("https://m.example.com", "m"),
        ),
    )
    targets = compile_proxy_allowlist(pol)
    assert [t.hostname for t in targets] == ["a.example.com", "m.example.com", "z.example.com"]


def test_allowlist_stable_hash_is_deterministic() -> None:
    """Compiled allowlist has stable hash (for revisioning if used)."""
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://api.example.com"),))
    ts = compile_proxy_allowlist(pol)
    h1 = allowlist_stable_hash(ts)
    h2 = allowlist_stable_hash(ts)
    assert len(h1) == 64
    assert h1 == h2
    pol2 = NetworkPolicy(enabled=True, destinations=(_dest("https://other.example.com"),))
    ts2 = compile_proxy_allowlist(pol2)
    assert allowlist_stable_hash(ts) != allowlist_stable_hash(ts2)


# --- New focused tests for explicit provider port parsing and closed AllowedConnectTarget ---


def test_provider_host_without_port_accepted() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    targets = compile_proxy_allowlist(pol, provider_api_hosts=("api.provider.com",))
    assert any(t.hostname == "api.provider.com" and t.port == 443 for t in targets)


def test_provider_host_explicit_443_accepted() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    targets = compile_proxy_allowlist(pol, provider_api_hosts=("api.provider.com:443",))
    assert any(t.hostname == "api.provider.com" and t.port == 443 for t in targets)


def test_provider_explicit_non_443_rejected() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    for bad in ["api.example.com:8443", "api.example.com:80"]:
        with pytest.raises(ValueError, match="443"):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_provider_malformed_and_non_numeric_ports_rejected() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    for bad in ["api.example.com:", "api.example.com:notaport", "api.example.com:abc"]:
        with pytest.raises(ValueError, match="malformed port"):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_provider_url_path_query_userinfo_forms_rejected() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    bads = [
        "user@api.example.com",
        "https://api.example.com",
        "api.example.com/path",
        "api.example.com?query=x",
        "api.example.com#fragment",
    ]
    for bad in bads:
        with pytest.raises(ValueError, match="hostname or hostname:443"):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_provider_ipv4_ipv6_literals_rejected() -> None:
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    for bad in ["8.8.8.8", "2001:4860:4860::8888", "[::1]", "[2001:db8::1]:443"]:
        with pytest.raises(ValueError, match=r"IP literal|hostname or hostname:443"):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_allowed_connect_target_direct_rejects_ips() -> None:
    for bad in ["8.8.8.8", "2001:4860:4860::8888", "127.0.0.1", "::1"]:
        with pytest.raises(ValueError, match="IP literal"):
            AllowedConnectTarget(hostname=bad, port=443, purpose="p")


def test_allowed_connect_target_direct_rejects_wildcards_local_metadata_invalid() -> None:
    bads = [
        "*.example.com",
        "localhost",
        "foo.localhost",
        "bar.foo.localhost",
        "example.local",
        "sub.example.local",
        "metadata.google.internal",
        ".example.com",
        "example.com.",
    ]
    for bad in bads:
        with pytest.raises(ValueError, match=r"prohibited|wildcard|invalid hostname"):
            AllowedConnectTarget(hostname=bad, port=443, purpose="p")


def test_allowed_connect_target_direct_produces_canonical_idna() -> None:
    # mixed case becomes lowercase canonical
    t = AllowedConnectTarget(hostname="API.Example.COM", port=443, purpose="p")
    assert t.hostname == "api.example.com"
    # whitespace/control rejected (space, tab, cr, lf, nul, nbsp, zwsp etc.)
    for bad in [
        "ex ample.com",
        "ex\tample.com",
        "ex\r.com",
        "ex\n.com",
        "ex\x00.com",
        "ex\xa0.com",
        "ex\u200b.com",
    ]:
        with pytest.raises(ValueError, match="invalid hostname"):
            AllowedConnectTarget(hostname=bad, port=443, purpose="p")


def test_allowed_connect_target_port_must_be_443() -> None:
    with pytest.raises(ValueError, match="443"):
        AllowedConnectTarget(hostname="api.example.com", port=8443, purpose="p")


def test_compile_rejects_non_https_destination_independently() -> None:
    """Compiler's https check exercised via model_construct bypass.

    Must raise; no exception swallowing.
    """
    bad_dest = EgressDestination.model_construct(
        url=HttpUrl("http://api.example.com"),
        purpose="x",
    )
    bad_pol = NetworkPolicy.model_construct(enabled=True, destinations=(bad_dest,))
    with pytest.raises(ValueError, match="https"):
        compile_proxy_allowlist(bad_pol)


def test_compile_performs_no_dns_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[int] = []

    def mock_get(*a: object, **k: object) -> list[object]:
        called.append(1)
        return []

    monkeypatch.setattr("socket.getaddrinfo", mock_get)
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://example.com"),))
    compile_proxy_allowlist(pol)
    assert not called, "compile must not perform DNS"


# --- Additional focused tests for full hostname canonicalization rules ---


def test_rejects_surrounding_whitespace_in_provider_api_hosts() -> None:
    """Surrounding ws in provider_api_hosts must be rejected, not stripped+accepted."""
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    for bad in [" api.example.com", "api.example.com ", "  api.example.com  ", "\tapi.example.com"]:
        with pytest.raises(ValueError, match="surrounding whitespace"):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_rejects_single_label_hostnames() -> None:
    """Single-label (no dot) hostnames rejected for public targets."""
    bads = ["myhost", "internal", "localhost-but-no", "ex"]
    for bad in bads:
        with pytest.raises(ValueError, match="single-label hostnames are not permitted"):
            AllowedConnectTarget(hostname=bad, port=443, purpose="p")
        with pytest.raises(ValueError, match="single-label"):
            pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))


def test_rejects_invalid_dns_labels_structure() -> None:
    """Post-IDNA: no empty labels, label len, lead/trail hyphen, only alnum-."""
    pol = NetworkPolicy(enabled=True, destinations=(_dest("https://p.example.com"),))
    bads = [
        "ex..com",  # empty label
        "ex-.com",  # trailing hyphen in label
        "-ex.com",  # leading hyphen
        "ex.-com",  # empty
        "a" * 64 + ".com",  # label >63
    ]
    for bad in bads:
        with pytest.raises(
            ValueError, match=r"invalid DNS|empty label|single-label|invalid hostname"
        ):
            compile_proxy_allowlist(pol, provider_api_hosts=(bad,))
        with pytest.raises(
            ValueError, match=r"invalid DNS|empty label|single-label|invalid hostname"
        ):
            AllowedConnectTarget(hostname=bad, port=443, purpose="p")


def test_rejects_overlong_hostname() -> None:
    # total >253 after canon (use long valid labels)
    long_host = ("x" * 63 + ".") * 4 + "com"
    assert len(long_host) > 253
    with pytest.raises(ValueError, match=r"invalid hostname length|too long"):
        AllowedConnectTarget(hostname=long_host, port=443, purpose="p")


def test_valid_mixed_case_and_unicode_idna_still_canonical() -> None:
    t = AllowedConnectTarget(hostname="API.Example.COM", port=443, purpose="p")
    assert t.hostname == "api.example.com"
    t2 = AllowedConnectTarget(hostname="café.example.com", port=443, purpose="p")
    assert t2.hostname == "xn--caf-dma.example.com"
    t3 = AllowedConnectTarget(hostname="München.de", port=443, purpose="p")
    assert t3.hostname == "xn--mnchen-3ya.de"
