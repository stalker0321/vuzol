"""Pure, deterministic controlled-egress policy contract for HTTPS proxy.

This module provides strict canonicalization of provider egress destinations
into exact allowed CONNECT targets (hostname + port) for use by the controlled
egress proxy.

Contract:
- Exact canonical hostname (lowercase + IDNA) + port 443 only.
- HTTPS origins only; no credentials, query, fragment, non-root path, wildcards, IPs.
- Deterministic, immutable tuple of AllowedConnectTarget.
- No DNS lookups, no side effects, fail-closed.
- Duplicate canonical (host,port) with differing purposes is an error.

This is the static configuration policy only.
Runtime layers remain responsible for: DNS resolution of the hostnames,
post-resolution address classification (reject private/loopback/link-local/metadata),
DNS rebinding protection, attaching sandboxes to isolated networks only,
proxy (e.g. Tinyproxy) CONNECT filtering, direct-egress prevention, and
per-task proxy/network lifecycle.
"""

from __future__ import annotations

import hashlib
import json
import unicodedata
from collections.abc import Iterable
from ipaddress import ip_address

from pydantic import BaseModel, Field, field_validator, model_validator

from vuzol.config.models import NetworkPolicy


class AllowedConnectTarget(BaseModel):
    """Immutable canonical representation of one permitted HTTPS CONNECT target.

    - hostname: exact canonical DNS hostname (lowercased, IDNA normalized)
    - port: 443 (only; enforced)
    - purpose: human readable from the policy (exact host+port only, no paths/creds)
    """

    model_config = {"frozen": True}

    hostname: str = Field(min_length=1, max_length=1024)
    # Note: final canonical hostname after IDNA must still be <=253 (enforced in _validate_dns...)
    port: int = Field(443, ge=1, le=65535)
    purpose: str = Field(min_length=1, max_length=200)

    @field_validator("hostname", mode="after")
    @classmethod
    def _canonicalize_and_validate_hostname(cls, v: str) -> str:
        """Normalize to canonical lowercase IDNA and enforce static invariants.

        Rejects IPs, wildcards, forbidden hosts, bad dots/whitespace at construction time.
        Always stores the deterministic canonical form.
        """
        return _validate_hostname(v)

    @model_validator(mode="after")
    def _only_port_443(self) -> AllowedConnectTarget:
        if self.port != 443:
            raise ValueError("AllowedConnectTarget port must be exactly 443")
        return self

    def __str__(self) -> str:
        return f"{self.hostname}:{self.port} ({self.purpose})"


def _is_ip_literal(host: str) -> bool:
    try:
        ip_address(host)
        return True
    except ValueError:
        return False


def _contains_disallowed_char(s: str) -> bool:
    """True if s contains any whitespace or control/format character.

    Covers: space, tab, CR, LF, NUL, non-breaking space (\xa0), zero-width
    space (\u200b), and all Cc/Cf/Z* categories.
    """
    for c in s:
        if c.isspace():
            return True
        cat = unicodedata.category(c)
        if cat.startswith("C"):
            return True
    return False


def _is_valid_dns_label(label: str) -> bool:
    if not (1 <= len(label) <= 63):
        return False
    if not label[0].isalnum() or not label[-1].isalnum():
        return False
    return all(c.isalnum() or c == "-" for c in label)


def _validate_dns_hostname_structure(hostname: str) -> None:
    """Post-IDNA DNS hostname rules (ASCII lowercase labels).

    - labels separated by dots, no empty labels
    - each label 1-63 chars, a-z0-9- only, no lead/trail hyphen
    - total length <= 253
    - not a single-label hostname (public targets must be FQDN-like)
    """
    if not hostname or len(hostname) > 253:
        raise ValueError(f"invalid hostname length: {hostname}")
    if hostname.endswith(".") or hostname.startswith("."):
        raise ValueError("invalid hostname characters or trailing dot")
    labels = hostname.split(".")
    if len(labels) < 2:
        raise ValueError(f"single-label hostnames are not permitted: {hostname}")
    if any(not label for label in labels):
        raise ValueError(f"invalid hostname (empty label): {hostname}")
    for label in labels:
        if not _is_valid_dns_label(label):
            raise ValueError(f"invalid DNS label in hostname: {hostname}")


def _normalize_hostname(host: str) -> str:
    """Lowercase and IDNA encode for stable canonical form. No DNS.

    After IDNA, enforces full DNS hostname structure rules (labels, lengths,
    chars, no single-label for public targets).
    """
    if not host:
        raise ValueError("hostname must not be empty")
    if _contains_disallowed_char(host) or host.endswith(".") or host.startswith("."):
        raise ValueError("invalid hostname characters or trailing dot")
    try:
        # Use lower and punycode for determinism
        canon = host.encode("idna").decode("ascii").lower()
    except Exception as e:
        raise ValueError(f"invalid hostname for IDNA normalization: {host}") from e
    _validate_dns_hostname_structure(canon)
    return canon


def _validate_hostname(host: str) -> str:
    """Validate hostname and return canonical lowercase IDNA form.

    Shared pure helper: rejects IP literals, forbidden names (localhost and
    subdomains, .local names, metadata.google.internal), wildcards, leading/
    trailing dots, any whitespace/control chars. After IDNA enforces full
    DNS label rules and rejects single-label names.
    No DNS. Used by both compiler and direct AllowedConnectTarget construction.
    """
    if _is_ip_literal(host):
        raise ValueError(f"IP literal egress destinations are prohibited: {host}")
    h = host.lower()
    if h in {"localhost", "metadata.google.internal"} or h.endswith((".localhost", ".local")):
        raise ValueError(f"prohibited hostname for egress: {host}")
    if "*" in host or host.startswith(".") or host.endswith(".") or _contains_disallowed_char(host):
        raise ValueError(f"wildcard or invalid hostname prohibited for egress: {host}")
    return _normalize_hostname(host)


def _extract_host(raw: str) -> str:
    """Extract hostname from plain host or host:port string.

    Handles IPv6 (with or without []) by not naively splitting on all : .
    Used only for provider_api_hosts which are expected to be hostnames (no port).
    """
    raw = raw.strip()
    if not raw:
        return ""
    if raw.startswith("[") and "]" in raw:
        # [ipv6] or [ipv6]:port
        end = raw.index("]")
        return raw[1:end]
    if raw.count(":") > 1:
        # bare IPv6 (multiple colons), take as-is
        return raw
    if ":" in raw:
        # host:port (IPv4 or name)
        return raw.rsplit(":", 1)[0]
    return raw


def _validate_proxy_destination(host: str, port: int | None) -> str:
    """Validate host+port for a provider destination (from policy or parsed).

    Returns the canonical hostname. Re-uses shared hostname validation.
    """
    if port not in (None, 443):
        raise ValueError(f"provider egress must use port 443 (got {port})")
    return _validate_hostname(host)


def _parse_provider_target(raw: str) -> tuple[str, int]:
    """Strict explicit parser for provider_api_hosts entries.

    Returns (canonical_hostname, 443) for accepted forms:
      - hostname
      - hostname:443

    Rejects with clear errors (no silent rewrite):
      - any other port (8443, 80, empty, non-numeric)
      - userinfo, full URLs, paths, query, fragment
      - IPv6 literals (with or without port)
      - malformed brackets/authority

    Uses _extract_host for host part + explicit port detection.
    Delegates hostname invariant to _validate_hostname.
    No DNS.
    """
    if not raw:
        raise ValueError("provider host must not be empty")
    # Reject surrounding whitespace explicitly (do not silently strip and accept)
    if raw != raw.strip():
        raise ValueError(f"provider host must not contain surrounding whitespace: {raw!r}")
    raw = raw.strip()
    if not raw:
        raise ValueError("provider host must not be empty")

    # Reject full authority/URL forms early (no silent strip)
    if (
        "://" in raw
        or "@" in raw
        or "/" in raw
        or "?" in raw
        or "#" in raw
        or raw.lower().startswith(("http:", "https:"))
    ):
        raise ValueError(f"provider_api_hosts must be hostname or hostname:443, got: {raw}")

    # Extract host portion
    host = _extract_host(raw)
    port = 443

    # Detect explicit port (handle [v6]:port vs bare v6)
    if raw.startswith("["):
        if "]" not in raw:
            raise ValueError(f"malformed IPv6 bracket in provider host: {raw}")
        after = raw[raw.index("]") + 1 :]
        if after:
            if not after.startswith(":") or not after[1:]:
                raise ValueError(f"malformed port in provider host: {raw}")
            port_str = after[1:]
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"malformed port in provider host: {raw}") from None
    elif ":" in raw and raw.count(":") == 1:
        # host:port (single colon, not bare v6)
        _, port_str = raw.rsplit(":", 1)
        if port_str:
            try:
                port = int(port_str)
            except ValueError:
                raise ValueError(f"malformed port in provider host: {raw}") from None
        else:
            raise ValueError(f"malformed port in provider host: {raw}")
    # else: no port or bare v6 (count>1) -> assume 443, IP check will catch literals

    if port != 443:
        raise ValueError(f"provider egress must use port 443 (got {port})")

    canon = _validate_hostname(host)
    return canon, 443


def compile_proxy_allowlist(
    policy: NetworkPolicy,
    provider_api_hosts: Iterable[str] = (),
) -> tuple[AllowedConnectTarget, ...]:
    """Compile NetworkPolicy destinations + provider endpoints into strict allowlist.

    Returns a deterministic, sorted, immutable tuple of exact host:443 targets.
    Raises ValueError with clear messages on any violation (fail-closed).
    Performs no DNS. See module docstring for separation of static vs runtime.
    """
    if not policy.enabled:
        if policy.destinations:
            raise ValueError("disabled network policy cannot declare destinations")
        return ()

    if not policy.destinations:
        raise ValueError("enabled network policy requires at least one destination")

    targets: list[AllowedConnectTarget] = []

    for dest in policy.destinations:
        url = dest.url
        if url.scheme != "https":
            raise ValueError("egress destinations must use https")
        host = url.host or ""
        port = url.port or 443
        canon_host = _validate_proxy_destination(host, port)
        targets.append(
            AllowedConnectTarget(
                hostname=canon_host,
                port=443,
                purpose=dest.purpose,
            )
        )

    for raw_host in provider_api_hosts:
        if not raw_host:
            continue
        canon_host, _ = _parse_provider_target(raw_host)
        targets.append(
            AllowedConnectTarget(
                hostname=canon_host,
                port=443,
                purpose="provider API endpoint",
            )
        )

    # Deduplicate by (host, port). Reject on conflicting purposes for the same canonical
    # target (prevents ambiguous policy). Same purpose deduplicates by first occurrence.
    # Result is always a sorted immutable tuple for deterministic allowlist.
    seen: dict[tuple[str, int], AllowedConnectTarget] = {}
    for t in targets:
        key = (t.hostname, t.port)
        if key in seen:
            existing = seen[key]
            if existing.purpose != t.purpose:
                raise ValueError(
                    f"duplicate canonical target {t.hostname}:{t.port} "
                    f"with conflicting purposes: {existing.purpose!r} vs {t.purpose!r}"
                )
            continue
        seen[key] = t

    # Stable order
    return tuple(sorted(seen.values(), key=lambda t: (t.hostname, t.port)))


def allowlist_stable_hash(targets: tuple[AllowedConnectTarget, ...]) -> str:
    """Deterministic hash of the compiled allowlist for revisioning."""
    payload = json.dumps(
        [t.model_dump() for t in targets], sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(payload).hexdigest()
