"""Operator-only decryption of incremental runtime traces; redact by default."""

import argparse
import base64
import json
import re
from pathlib import Path

from vuzol.execution.artifacts import BUILTIN_SECRET_PATTERNS
from vuzol.execution.journal import ExecutionJournal


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--key-file", required=True, type=Path)
    parser.add_argument(
        "--raw-private",
        action="store_true",
        help="DANGER: include private prompts, credentials and tool output",
    )
    args = parser.parse_args()
    trace = args.trace.absolute()
    store = ExecutionJournal(trace.parent, args.key_file)
    # Join stream chunks before redaction: secrets and JSONL events may cross
    # arbitrary pipe read boundaries. Do not use this output as public telemetry.
    records = list(store.read(trace))
    for channel in ("start", "stdin", "stdout", "stderr", "cleanup"):
        selected = [record for record in records if record["channel"] == channel]
        if not selected:
            continue
        content = b"".join(
            base64.b64decode(record["base64"]) for record in selected if "base64" in record
        ).decode("utf-8", "replace")
        if not content:
            content = json.dumps([record.get("data") for record in selected], ensure_ascii=False)
        if not args.raw_private:
            for pattern in BUILTIN_SECRET_PATTERNS:
                content = re.sub(pattern, "[REDACTED]", content)
        print(json.dumps({"channel": channel, "content": content}, ensure_ascii=False))


if __name__ == "__main__":
    main()
