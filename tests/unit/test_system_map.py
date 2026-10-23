"""The offline map must expose omissions and cannot turn data into script."""

import copy
import json

import pytest
from devtools.system_map import MAP, build, inventory, unique_object, validate


def test_catalog_links_and_independent_entry_inventory() -> None:
    data = json.loads((MAP / "scenarios.json").read_text())
    report = validate(data)
    assert not report["errors"]
    assert not report["uncovered"]
    assert not report["stale_bindings"]
    assert not report["missing_consumers"]
    assert len(data["scenarios"]) >= 16
    assert len(report["inventory"]) > 100
    assert "http:api_route:/{request_path:path}" in {e["id"] for e in report["inventory"]}


def test_removed_binding_and_unknown_symbol_cannot_look_complete() -> None:
    data = json.loads((MAP / "scenarios.json").read_text())
    broken = copy.deepcopy(data)
    del broken["coverage"]["mcp:submit_request"]
    broken["scenarios"][0]["sources"].append("src/vuzol/cli/accounts.py::does_not_exist")
    report = validate(broken)
    assert "mcp:submit_request" in report["uncovered"]
    assert any("does_not_exist" in error for error in report["errors"])


def test_embedded_data_cannot_terminate_script_and_template_has_no_fetch() -> None:
    template = (MAP / "template.html").read_text()
    result = build({"title": "</script><script>evil()</script>"}, {}, template)
    assert "<script>evil()" not in result
    assert "\\u003c/script>" in result
    assert "fetch(" not in template and "XMLHttpRequest" not in template
    assert "innerHTML" not in template
    assert "connect-src 'none'" in template


def test_outbox_producers_and_consumers_have_real_source_references() -> None:
    entries = {e["id"]: e for e in inventory()}
    limits = entries["outbox:subscription_limits"]
    assert any("telegram/ingress.py" in s for s in limits["producers"])
    assert any("cli/executor.py" in s for s in limits["consumers"])
    assert "callback:ControlUpdate:secret_cancel" in entries
    assert "telegram-handler:filters.ALL" in entries


def test_unproven_status_and_missing_outbox_policy_are_errors() -> None:
    data = json.loads((MAP / "scenarios.json").read_text())
    data["scenarios"][0]["live"] = "passed-unit-so-live-ready"
    del data["outbox_contracts"]["subscription_limits"]
    errors = validate(data)["errors"]
    assert any("invalid live" in error for error in errors)
    assert "missing outbox contract: subscription_limits" in errors
    with pytest.raises(ValueError, match="duplicate JSON key"):
        json.loads('{"coverage":{},"coverage":{}}', object_pairs_hook=unique_object)
