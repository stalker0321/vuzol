"""Pure deterministic Tinyproxy policy and filter renderer.

This module converts a validated tuple of AllowedConnectTarget into
the exact text files consumed by a hardened Tinyproxy instance.

It performs no DNS, no I/O, and produces only deterministic UTF-8 text.

See STEP_08_PROXY_EGRESS_DESIGN.md for the selected Tinyproxy
configuration style and filter semantics.

Client access control (Allow rules) is deliberately omitted: the design
uses Docker network isolation (sandbox attached only to internal net)
rather than Tinyproxy client ACLs. Runtime layers remain responsible for
network attachment, post-DNS IP classification, rebinding resistance,
and proxy process lifecycle.
"""

from __future__ import annotations

import re

from pydantic import BaseModel

from vuzol.execution.egress import AllowedConnectTarget


class RenderedTinyproxyPolicy(BaseModel):
    """Immutable result of rendering a fail-closed Tinyproxy policy.

    config_text: security-critical fragment with ConnectPort, Filter,
                 FilterDefaultDeny, FilterExtended, FilterURLs, etc.
    filter_text: domain filter file with one exact ^host$ rule per target.
    """

    model_config = {"frozen": True}

    config_text: str
    filter_text: str


def _validate_filter_path(path: str) -> str:
    """Validate filter path for the proxy container.

    Must be absolute, under the approved /etc/tinyproxy/ directory,
    contain no control/whitespace characters, no traversal, no quotes,
    no NUL or newlines.
    """
    if not path or not isinstance(path, str):
        raise ValueError("filter path must be a non-empty string")
    if not path.startswith("/"):
        raise ValueError("filter path must be absolute")
    if "\n" in path or "\0" in path or '"' in path or "'" in path:
        raise ValueError("filter path contains invalid characters")
    if any(c.isspace() for c in path):
        raise ValueError("filter path must not contain whitespace")
    if ".." in path.split("/"):
        raise ValueError("filter path must not contain parent traversal")
    if not path.startswith("/etc/tinyproxy/"):
        raise ValueError("filter path must be under /etc/tinyproxy/")
    # normalize to the exact expected for container
    if path != "/etc/tinyproxy/filter":
        # allow only the documented one for this slice; strict
        raise ValueError("filter path must be /etc/tinyproxy/filter")
    return path


def render_tinyproxy_policy(
    targets: tuple[AllowedConnectTarget, ...],
    filter_path: str = "/etc/tinyproxy/filter",
) -> RenderedTinyproxyPolicy:
    """Render deterministic Tinyproxy filter file and security fragment.

    The input targets must already be validated and canonical
    (see compile_proxy_allowlist). This renderer does not re-validate
    hostnames or perform DNS.

    filter_path must be the container-side absolute path.
    """
    if not targets:
        raise ValueError("proxy policy requires at least one target")

    # Deduplicate and sort deterministically by hostname
    unique_hosts = sorted({t.hostname for t in targets})

    # Exact anchored domain rules for FilterExtended
    filter_lines = [f"^{re.escape(h)}$" for h in unique_hosts]
    filter_text = "\n".join(filter_lines) + "\n"

    validated_path = _validate_filter_path(filter_path)

    # Security-critical fragment (fail-closed, domain filter, 443 only)
    # Follows directives from STEP_08_PROXY_EGRESS_DESIGN.md and
    # the vimagick/tinyproxy image.
    config_lines = [
        "ConnectPort 443",
        f'Filter "{validated_path}"',
        "FilterDefaultDeny Yes",
        "FilterExtended On",
        "FilterURLs Off",
        "FilterCaseSensitive On",
    ]
    config_text = "\n".join(config_lines) + "\n"

    return RenderedTinyproxyPolicy(
        config_text=config_text,
        filter_text=filter_text,
    )
