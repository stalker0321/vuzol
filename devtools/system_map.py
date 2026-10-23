"""Build an offline scenario map and independently inventory code entrypoints.

No application imports, credentials, network, database or service discovery.
Generated HTML/coverage are derived from scenarios.json and the source tree.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAP = ROOT / "docs/system-map"


def inventory(root: Path = ROOT) -> list[dict]:
    trees = {
        str(p.relative_to(root)): ast.parse(p.read_text())
        for p in sorted((root / "src/vuzol").rglob("*.py"))
    }
    constants: dict[tuple[str, str], ast.AST | tuple[str, str]] = {}
    for path, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        constants[path, target.id] = node.value
                    elif (
                        isinstance(target, ast.Attribute)
                        and isinstance(target.value, ast.Name)
                        and target.value.id == "self"
                        and target.attr == "destination"
                    ):
                        constants[path, "self.destination"] = node.value
            if (
                isinstance(node, ast.ImportFrom)
                and node.module
                and node.module.startswith("vuzol.")
            ):
                source = "src/" + node.module.replace(".", "/") + ".py"
                for alias in node.names:
                    constants[path, alias.asname or alias.name] = (source, alias.name)

    def values(path: str, node: ast.AST, seen: frozenset = frozenset()) -> list[str]:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return [node.value]
        if isinstance(node, ast.JoinedStr):
            return [
                "".join(
                    v.value if isinstance(v, ast.Constant) else "{dynamic}" for v in node.values
                )
            ]
        if isinstance(node, (ast.Set, ast.List, ast.Tuple)):
            return [v for element in node.elts for v in values(path, element, seen)]
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"frozenset", "set", "tuple"}
            and node.args
        ):
            return values(path, node.args[0], seen)
        key = (path, ast.unparse(node))
        if key in seen or key not in constants:
            return []
        target = constants[key]
        if isinstance(target, tuple):
            return values(target[0], ast.Name(id=target[1]), seen | {key})
        return values(path, target, seen | {key})

    entries: dict[str, dict] = {}

    def add(identifier: str, path: str, line: int, role: str = "sources") -> None:
        entry = entries.setdefault(
            identifier, {"id": identifier, "sources": [], "producers": [], "consumers": []}
        )
        ref = f"{path}:{line}"
        if ref not in entry[role]:
            entry[role].append(ref)

    scripts = tomllib.loads((root / "pyproject.toml").read_text())["project"]["scripts"]
    for name, target in scripts.items():
        add("cli:" + name, "src/" + target.split(":")[0].replace(".", "/") + ".py", 1)
    for path, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if path.startswith("src/vuzol/cli/") and node.name == "main":
                    add(
                        "module:" + path.removeprefix("src/").removesuffix(".py").replace("/", "."),
                        path,
                        node.lineno,
                    )
                if node.name in {
                    "_recovery_loop",
                    "_refresh_subscription_limits_tick",
                    "_run_loop",
                    "run_provisioning_loop",
                }:
                    add("job:" + path + ":" + node.name, path, node.lineno)
                for dec in node.decorator_list:
                    if not isinstance(dec, ast.Call) or not isinstance(dec.func, ast.Attribute):
                        continue
                    if dec.func.attr == "tool" and path == "src/vuzol/mcp/server.py":
                        add("mcp:" + node.name, path, node.lineno)
                    if (
                        dec.func.attr in {"get", "post", "put", "delete", "websocket", "api_route"}
                        and dec.args
                        and isinstance(dec.args[0], ast.Constant)
                    ):
                        add(f"http:{dec.func.attr}:{dec.args[0].value}", path, node.lineno)
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and re.fullmatch(r"/[a-z_]+", node.value)
                and (
                    path.startswith("src/vuzol/telegram/")
                    or path == "src/vuzol/discussion/conversation.py"
                )
            ):
                add("telegram:" + node.value, path, node.lineno)
            if isinstance(node, ast.ClassDef) and node.name.endswith("CallbackKind"):
                for member in node.body:
                    if isinstance(member, ast.Assign) and isinstance(member.targets[0], ast.Name):
                        add(f"callback:{node.name}:{member.targets[0].id}", path, member.lineno)
            if isinstance(node, ast.ClassDef) and node.name == "ControlUpdate":
                for member in node.body:
                    if (
                        isinstance(member, ast.AnnAssign)
                        and isinstance(member.target, ast.Name)
                        and member.target.id == "action_kind"
                        and isinstance(member.value, ast.Call)
                    ):
                        for kw in member.value.keywords:
                            if kw.arg == "pattern" and isinstance(kw.value, ast.Constant):
                                pattern = kw.value.value
                                if re.fullmatch(r"\^\([a-z_|]+\)\$", pattern):
                                    for action in pattern[2:-2].split("|"):
                                        add("callback:ControlUpdate:" + action, path, member.lineno)
            if isinstance(node, ast.Call):
                func = ast.unparse(node.func)
                if func == "MessageHandler" and node.args:
                    add("telegram-handler:" + ast.unparse(node.args[0]), path, node.lineno)
                for kw in node.keywords:
                    role = "consumers" if kw.arg == "allowed_destinations" else "producers"
                    if kw.arg == "allowed_destinations" or (
                        kw.arg == "destination"
                        and (func.endswith(".enqueue") or func == "TransactionalOutbox")
                    ):
                        resolved = values(path, kw.value)
                        if not resolved:
                            add(
                                f"dynamic-outbox:{path}:{ast.unparse(kw.value)}",
                                path,
                                node.lineno,
                                role,
                            )
                        for value in resolved:
                            add("outbox:" + value, path, node.lineno, role)
    for kind in ("text", "voice", "audio", "document", "callback:v1", "callback:v2"):
        add("telegram-handler:" + kind, "src/vuzol/telegram/adapter.py", 1)
    add("http:mcp:/mcp", "src/vuzol/mcp/server.py", 1)
    add("http:mcp:metadata", "src/vuzol/mcp/server.py", 1)
    return [entries[key] for key in sorted(entries)]


def validate(data: dict, root: Path = ROOT) -> dict:
    errors = []
    scenarios = data["scenarios"]
    ids = [s["id"] for s in scenarios]
    if len(ids) != len(set(ids)):
        errors.append("duplicate scenario IDs")
    required = {
        "id",
        "title",
        "example",
        "implementation",
        "live",
        "route",
        "evidence",
        "principal",
        "guards",
        "steps",
        "state",
        "effects",
        "handoff",
        "success",
        "exits",
        "sources",
        "tests",
        "legacy",
    }
    for scenario in scenarios:
        if not required <= scenario.keys():
            errors.append(f"{scenario['id']}: missing {sorted(required - scenario.keys())}")
        for field, choices in {
            "implementation": {"implemented", "partial", "not_implemented"},
            "live": {"enabled", "disabled", "unverified", "blocked"},
            "route": {"canonical", "legacy", "operator", "experiment"},
        }.items():
            if scenario.get(field) not in choices:
                errors.append(f"{scenario['id']}: invalid {field}")
        for field in required:
            if not scenario.get(field):
                errors.append(f"{scenario['id']}: empty {field}")
        for reference in (*scenario.get("sources", []), *scenario.get("tests", [])):
            path, _, symbol = reference.partition("::")
            file = root / path
            if Path(path).is_absolute() or ".." in Path(path).parts:
                errors.append("non-repository reference: " + reference)
            elif not file.is_file():
                errors.append("missing file: " + reference)
            elif symbol:
                names = {
                    n.name
                    for n in ast.walk(ast.parse(file.read_text()))
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
                }
                if symbol not in names:
                    errors.append("missing symbol: " + reference)
    for edge in data.get("edges", []):
        if len(edge) != 2 or any(node not in ids for node in edge):
            errors.append("unknown edge: " + repr(edge))
    discovered = inventory(root)
    bindings = data.get("coverage", {})
    uncovered = []
    for entry in discovered:
        mapping = bindings.get(entry["id"])
        if mapping not in ids:
            uncovered.append(entry["id"])
        entry["scenario"] = mapping
    stale = sorted(set(bindings) - {e["id"] for e in discovered})
    missing_consumers = [
        e["id"]
        for e in discovered
        if e["id"].startswith("outbox:") and e["producers"] and not e["consumers"]
    ]
    contracts = data.get("outbox_contracts", {})
    queues = {e["id"].removeprefix("outbox:") for e in discovered if e["id"].startswith("outbox:")}
    for queue in sorted(queues - contracts.keys()):
        errors.append("missing outbox contract: " + queue)
    for queue, contract in contracts.items():
        if queue not in queues:
            errors.append("stale outbox contract: " + queue)
        for field in ("guards", "retry", "dead_letter", "next"):
            if not contract.get(field):
                errors.append(f"{queue}: missing {field}")
    return {
        "errors": errors,
        "uncovered": uncovered,
        "stale_bindings": stale,
        "missing_consumers": missing_consumers,
        "inventory": discovered,
        "scope_limit": (
            "Static inventory is an audit aid, not proof of runtime enablement. "
            "Scheduled systemd/cron and dynamically generated routes need operator evidence."
        ),
    }


def build(data: dict, report: dict, template: str) -> str:
    payload = (
        json.dumps({**data, "coverage_report": report}, ensure_ascii=False)
        .replace("<", "\\u003c")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )
    return template.replace("__SCENARIO_DATA__", payload)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--inventory", action="store_true")
    args = parser.parse_args()
    if args.inventory:
        print(json.dumps(inventory(), ensure_ascii=False, indent=2))
        return
    data = json.loads((MAP / "scenarios.json").read_text(), object_pairs_hook=unique_object)
    report = validate(data)
    if not args.check:
        (MAP / "index.html").write_text(build(data, report, (MAP / "template.html").read_text()))
        (MAP / "coverage.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    else:
        expected = build(data, report, (MAP / "template.html").read_text())
        if not (MAP / "index.html").exists() or (MAP / "index.html").read_text() != expected:
            report["errors"].append("generated HTML is stale; rebuild")
    print(
        json.dumps(
            {k: v for k, v in report.items() if k != "inventory"}, ensure_ascii=False, indent=2
        )
    )
    raise SystemExit(
        1
        if report["errors"]
        or (
            args.check
            and (report["uncovered"] or report["stale_bindings"] or report["missing_consumers"])
        )
        else 0
    )


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key: " + key)
        result[key] = value
    return result


if __name__ == "__main__":
    main()
