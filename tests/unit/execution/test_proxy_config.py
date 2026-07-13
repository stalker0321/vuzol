"""Focused tests for the pure Tinyproxy policy renderer.

These tests cover only the text-generation contract. They exercise
rendered output directly and do not rely on private attributes.
"""

import re

import pytest

from vuzol.execution.egress import AllowedConnectTarget
from vuzol.execution.proxy_config import (
    RenderedTinyproxyPolicy,
    render_tinyproxy_policy,
)


def _target(host: str, purpose: str = "test") -> AllowedConnectTarget:
    return AllowedConnectTarget(hostname=host, port=443, purpose=purpose)


def test_single_target_renders_exact_anchored_filter_rule() -> None:
    targets = (_target("api.example.com"),)
    policy = render_tinyproxy_policy(targets)
    assert policy.filter_text == "^api\\.example\\.com$\n"
    assert isinstance(policy, RenderedTinyproxyPolicy)


def test_multiple_targets_are_sorted_deterministically() -> None:
    targets = (
        _target("z.example.com"),
        _target("a.example.com"),
        _target("m.example.com"),
    )
    policy = render_tinyproxy_policy(targets)
    lines = policy.filter_text.strip().split("\n")
    assert lines == [
        "^a\\.example\\.com$",
        "^m\\.example\\.com$",
        "^z\\.example\\.com$",
    ]


def test_dots_are_escaped_correctly() -> None:
    targets = (_target("api.example.com"),)
    policy = render_tinyproxy_policy(targets)
    assert "api\\.example\\.com" in policy.filter_text
    # not a plain dot that could match any
    assert "api.example.com" not in policy.filter_text


def test_one_hostname_does_not_match_prefix_or_suffix() -> None:
    # The renderer produces exact ^host$ ; semantic test via string
    targets = (_target("example.com"),)
    policy = render_tinyproxy_policy(targets)
    rule = policy.filter_text.strip()
    assert rule == "^example\\.com$"
    # would not match sub or super without the anchors and escaping
    assert not re.match(rule, "foo.example.com")
    assert not re.match(rule, "example.com.evil")


def test_equivalent_target_order_produces_identical_output() -> None:
    t1 = (_target("a.com"), _target("b.com"))
    t2 = (_target("b.com"), _target("a.com"))
    p1 = render_tinyproxy_policy(t1)
    p2 = render_tinyproxy_policy(t2)
    assert p1.config_text == p2.config_text
    assert p1.filter_text == p2.filter_text


def test_duplicate_targets_do_not_broaden_output() -> None:
    targets = (_target("api.example.com"), _target("api.example.com"))
    policy = render_tinyproxy_policy(targets)
    assert policy.filter_text.count("^api\\.example\\.com$") == 1


def test_empty_target_list_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least one target"):
        render_tinyproxy_policy(())


def test_config_contains_default_deny_filtering() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    assert "FilterDefaultDeny Yes" in policy.config_text


def test_config_uses_domain_filtering_not_url_filtering() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    assert "FilterURLs Off" in policy.config_text
    # not the URL mode
    assert "FilterURLs On" not in policy.config_text


def test_config_contains_exactly_one_connectport_and_it_is_443() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    assert policy.config_text.count("ConnectPort 443") == 1
    assert "ConnectPort 8443" not in policy.config_text


def test_config_uses_explicit_filter_type_ere() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    assert policy.config_text.count("FilterType ere") == 1
    assert "FilterExtended" not in policy.config_text
    # ensure it is active (not commented)
    assert "FilterType ere" in policy.config_text


def test_config_has_no_active_filterextended() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    # no FilterExtended directive at all (deprecated)
    assert "FilterExtended" not in policy.config_text


def test_no_conflicting_duplicate_filter_mode_directives() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    text = policy.config_text
    # exactly the expected modes, no dups or conflicts
    assert text.count("FilterType ere") == 1
    assert text.count("FilterDefaultDeny Yes") == 1
    assert text.count("FilterURLs Off") == 1
    assert text.count("FilterCaseSensitive On") == 1
    assert "FilterExtended" not in text


def test_anchored_filter_rules_remain_valid_for_ere() -> None:
    # ^host$ with escaped dots are valid ERE (and BRE)
    policy = render_tinyproxy_policy((_target("api.example.com"), _target("sub.domain.test")))
    assert "^api\\.example\\.com$" in policy.filter_text
    assert "^sub\\.domain\\.test$" in policy.filter_text
    # still exactly anchored, no wildcards
    assert policy.filter_text.count("^") == 2
    assert policy.filter_text.count("$") == 2


def test_config_has_no_unrestricted_or_default_allow() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    # no blanket allow
    text = policy.config_text.lower()
    assert "allow 0.0.0.0" not in text
    assert "allow ::" not in text
    assert "allow all" not in text


def test_config_contains_no_upstream_basicauth_addheader_or_reverse() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    lower = policy.config_text.lower()
    assert "upstream" not in lower
    assert "basicauth" not in lower
    assert "addheader" not in lower
    assert "reverse" not in lower


def test_output_ends_with_exactly_one_newline() -> None:
    policy = render_tinyproxy_policy((_target("api.example.com"),))
    assert policy.filter_text.endswith("\n")
    assert not policy.filter_text.endswith("\n\n")
    assert policy.config_text.endswith("\n")
    assert not policy.config_text.endswith("\n\n")


def test_purpose_strings_never_appear_in_output() -> None:
    targets = (_target("api.example.com", purpose="secret purpose with /path?x=1"),)
    policy = render_tinyproxy_policy(targets)
    assert "secret purpose" not in policy.filter_text
    assert "secret purpose" not in policy.config_text
    assert "/path" not in policy.filter_text


def test_malicious_purpose_cannot_inject_directives() -> None:
    evil = 'p";\nConnectPort 0\nFilterDefaultDeny No\n'
    targets = (_target("api.example.com", purpose=evil),)
    policy = render_tinyproxy_policy(targets)
    # no injection possible
    assert '";' not in policy.config_text
    assert "ConnectPort 0" not in policy.config_text
    assert "FilterDefaultDeny No" not in policy.config_text


def test_invalid_filter_paths_are_rejected() -> None:
    targets = (_target("api.example.com"),)
    for bad in [
        "relative/filter",
        "",
        "/etc/tinyproxy/filter\nextra",
        "/etc/tinyproxy/filter\x00",
        "/etc/other/filter",
        "../etc/tinyproxy/filter",
        "/etc/tinyproxy/filter with space",
        '"/etc/tinyproxy/filter"',
    ]:
        with pytest.raises(ValueError, match="filter path"):
            render_tinyproxy_policy(targets, filter_path=bad)


def test_rendering_performs_no_dns_lookup(monkeypatch: pytest.MonkeyPatch) -> None:
    called = []

    def mock_get(*a: object, **k: object) -> list[object]:
        called.append(1)
        return []

    monkeypatch.setattr("socket.getaddrinfo", mock_get)
    targets = (_target("api.example.com"),)
    render_tinyproxy_policy(targets)
    assert not called


def test_result_is_immutable_and_byte_deterministic() -> None:
    targets = (_target("api.example.com"),)
    p1 = render_tinyproxy_policy(targets)
    p2 = render_tinyproxy_policy(targets)
    assert p1 == p2
    assert p1.config_text == p2.config_text
    assert p1.filter_text == p2.filter_text
    # frozen
    with pytest.raises(Exception, match=r"frozen|immutable"):
        p1.config_text = "mutated"


def test_stable_output_changes_when_allowed_set_changes() -> None:
    p1 = render_tinyproxy_policy((_target("a.com"),))
    p2 = render_tinyproxy_policy((_target("b.com"),))
    assert p1.filter_text != p2.filter_text
    assert p1 != p2


def test_module_documents_runtime_responsibilities() -> None:
    # The module docstring must mention pending runtime layers
    import vuzol.execution.proxy_config as m

    doc = m.__doc__ or ""
    assert "DNS" in doc
    assert "rebinding" in doc or "runtime" in doc.lower()
