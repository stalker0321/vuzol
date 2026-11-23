"""Add an OpenCode Go account in one step: key, state directory and registry profiles.

The new account copies an existing account's profiles (base and model variants),
roles and pool. Run as root on the host, then restart vuzol-executor:

    python -m vuzol.cli.add_opencode_account --name d --key-file /path/to/key

The key is read from the file and written only to the new account's auth.json; it is
never printed. Whether the subscription is active shows on the first real call.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from vuzol.config.loader import ConfigurationLoadError, load_document
from vuzol.config.registries import ProfileRegistry

REGISTRY = Path("/var/lib/vuzol/registry/account-profiles.toml")
STATE_ROOT = Path("/var/lib/vuzol-provider-state")
_NAME = re.compile(r"^[a-z][a-z0-9]{0,15}$")
# Provider files copied from the template account; auth.json is written fresh.
_COPIED = ("models.json", "models-store.json")


class AddAccountError(Exception):
    pass


def profile_blocks(text: str) -> list[tuple[int, int]]:
    """Line spans of each [[profiles]] entry with its [profiles.*] subtables."""

    lines = text.splitlines(keepends=True)
    spans, start = [], None
    for index, line in enumerate(lines):
        header = line.strip()
        if not header.startswith("["):
            continue
        if header == "[[profiles]]" or not header.startswith(("[profiles.", "[[profiles.")):
            if start is not None:
                spans.append((start, index))
                start = None
            if header == "[[profiles]]":
                start = index
    if start is not None:
        spans.append((start, len(lines)))
    return spans


def _field(block: str, name: str) -> str | None:
    found = re.search(rf'^{name}\s*=\s*"([^"]*)"', block, re.MULTILINE)
    return found.group(1) if found else None


def clone_profiles(text: str, *, template: str, name: str, state_root: Path = STATE_ROOT) -> str:
    """The template account's profile blocks, renamed for the new account."""

    lines = text.splitlines(keepends=True)
    base = f"pi-opencode-go-{template}"
    blocks = []
    for start, end in profile_blocks(text):
        block = "".join(lines[start:end]).rstrip("\n") + "\n"
        if _field(block, "id") == base or (
            # Model variants of the account; a profile that inherits but brings its
            # own account is another account.
            _field(block, "base_profile_id") == base
            and not any(_field(block, f) for f in ("account_id", "state_directory"))
        ):
            blocks.append(block)
    base_block = next((b for b in blocks if _field(b, "id") == base), None)
    if base_block is None:
        raise AddAccountError("template_not_found")
    expected = {
        "account_id": f"opencode-go-{template}",
        "runtime_identity": f"vuzol-pi-{template}",
        "state_directory": str(state_root / base),
    }
    for field, value in expected.items():
        if _field(base_block, field) != value:
            raise AddAccountError(f"template_{field}_unexpected")
    renamed = []
    for block in blocks:
        block = re.sub(rf"\bpi-opencode-go-{template}(?=[-\"/])", f"pi-opencode-go-{name}", block)
        block = block.replace(f'"opencode-go-{template}"', f'"opencode-go-{name}"')
        block = block.replace(f'"vuzol-pi-{template}"', f'"vuzol-pi-{name}"')
        renamed.append(block)
    return "\n" + "\n".join(renamed)


def read_key(path: Path) -> str:
    try:
        key = path.read_text().strip()
    except (OSError, UnicodeError) as error:
        raise AddAccountError("key_unreadable") from error
    if not key or len(key) > 8192 or any(c.isspace() for c in key) or key.startswith("!"):
        raise AddAccountError("key_invalid")
    return key


def add_account(
    *,
    name: str,
    key_file: Path,
    template: str = "a",
    registry: Path = REGISTRY,
    state_root: Path = STATE_ROOT,
    dry_run: bool = False,
) -> dict[str, object]:
    if not _NAME.match(name) or name == template:
        raise AddAccountError("name_invalid")
    text = registry.read_text()
    if f"pi-opencode-go-{name}" in text or f'"opencode-go-{name}"' in text:
        raise AddAccountError("account_exists")
    source = state_root / f"pi-opencode-go-{template}"
    target = state_root / f"pi-opencode-go-{name}"
    if target.exists():
        raise AddAccountError("state_directory_exists")
    key = read_key(key_file)
    addition = clone_profiles(text, template=template, name=name, state_root=state_root)
    candidate = text.rstrip("\n") + "\n" + addition
    with tempfile.TemporaryDirectory() as scratch:
        check = Path(scratch) / registry.name
        check.write_text(candidate)
        try:
            document = load_document(check)
            created = [
                profile
                for profile in ProfileRegistry(document.profiles).items()
                if profile.account_id == f"opencode-go-{name}"
            ]
        except (ConfigurationLoadError, ValueError) as error:
            raise AddAccountError("registry_invalid") from error
    result: dict[str, object] = {
        "account_id": f"opencode-go-{name}",
        "profiles": sorted(profile.id for profile in created),
        "account_pool": created[0].account_pool if created else None,
        "state_directory": str(target),
        "dry_run": dry_run,
    }
    if dry_run:
        return result
    owner = source.stat()
    staging = Path(tempfile.mkdtemp(prefix=f".{target.name}.", dir=state_root))
    try:
        for name_ in _COPIED:
            shutil.copyfile(source / name_, staging / name_)
        auth = staging / "auth.json"
        descriptor = os.open(auth, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump({"opencode-go": {"type": "api_key", "key": key}}, stream)
        for path in (staging, *staging.iterdir()):
            os.chown(path, owner.st_uid, owner.st_gid)
            path.chmod(0o700 if path.is_dir() else 0o600)
        staging.rename(target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    try:
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        backup = registry.with_name(f"{registry.name}.bak-pre-add-{name}-{stamp}")
        shutil.copy2(registry, backup)
        meta = registry.stat()
        fd, temporary = tempfile.mkstemp(prefix=f".{registry.name}.", dir=registry.parent)
        with os.fdopen(fd, "w") as stream:
            stream.write(candidate)
        os.chown(temporary, meta.st_uid, meta.st_gid)
        os.chmod(temporary, meta.st_mode & 0o777)
        os.replace(temporary, registry)
    except BaseException:
        shutil.rmtree(target, ignore_errors=True)
        raise
    return {**result, "registry_backup": str(backup)}


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--name", required=True, help="new account letter, e.g. d")
    parser.add_argument("--key-file", type=Path, required=True)
    parser.add_argument("--template", default="a", help="existing account to copy")
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--state-root", type=Path, default=STATE_ROOT)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = add_account(
            name=args.name,
            key_file=args.key_file,
            template=args.template,
            registry=args.registry,
            state_root=args.state_root,
            dry_run=args.dry_run,
        )
    except (AddAccountError, OSError) as error:
        # Never echo the key or file contents; codes and errno names only.
        code = str(error) if isinstance(error, AddAccountError) else type(error).__name__
        print(json.dumps({"error": code}))
        return 1
    if not args.dry_run:
        result["next"] = "sudo systemctl restart vuzol-executor"
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
