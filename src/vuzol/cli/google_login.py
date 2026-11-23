"""Connect the owner's Google Calendar and Gmail to the assistant, once.

    python -m vuzol.cli.google_login --client /path/client.json
        prints a link; sign in, allow, and copy the address the browser lands on
        (http://127.0.0.1:8765/?code=... does not load, that is expected)
    python -m vuzol.cli.google_login --client /path/client.json --code '<that address>'
        writes the token file the assistant reads

The client JSON is the "Desktop app" OAuth client from Google Cloud. Neither the client
secret nor the token is printed.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import httpx

from vuzol.assistant.google import SCOPES, TOKEN_URL

TOKEN = Path("/var/lib/vuzol/google/token.json")
REDIRECT = "http://127.0.0.1:8765"
# The interpreter container's user.
OWNER_UID, OWNER_GID = 1000, 10001


class LoginError(Exception):
    pass


def read_client(path: Path) -> tuple[str, str]:
    try:
        data = json.loads(path.read_text())
        client = data.get("installed") or data.get("web") or {}
        return client["client_id"], client["client_secret"]
    except (OSError, ValueError, KeyError) as error:
        raise LoginError("client_json_invalid") from error


def consent_url(client_id: str) -> str:
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(
        {
            "client_id": client_id,
            "redirect_uri": REDIRECT,
            "response_type": "code",
            "scope": " ".join(SCOPES),
            "access_type": "offline",
            "prompt": "consent",
        }
    )


def code_from(value: str) -> str:
    value = value.strip()
    if value.startswith("http"):
        codes = parse_qs(urlparse(value).query).get("code")
        if not codes:
            raise LoginError("no_code_in_address")
        return codes[0]
    return value


def exchange(client_id: str, secret: str, code: str, *, client: httpx.Client | None = None) -> str:
    http = client or httpx.Client(timeout=30)
    response = http.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": secret,
            "code": code,
            "redirect_uri": REDIRECT,
            "grant_type": "authorization_code",
        },
    )
    if response.status_code != 200:
        raise LoginError(f"exchange_{response.status_code}")
    refresh = response.json().get("refresh_token")
    if not refresh:
        raise LoginError("no_refresh_token")
    return str(refresh)


def write_token(path: Path, client_id: str, secret: str, refresh: str, *, chown: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix=".token.", dir=path.parent)
    with os.fdopen(fd, "w") as stream:
        json.dump(
            {"client_id": client_id, "client_secret": secret, "refresh_token": refresh}, stream
        )
    os.chmod(temporary, 0o600)
    if chown:
        os.chown(path.parent, OWNER_UID, OWNER_GID)
        os.chown(temporary, OWNER_UID, OWNER_GID)
    os.replace(temporary, path)


def run(argv: list[str] | None = None, *, client: httpx.Client | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", type=Path, required=True)
    parser.add_argument("--code", help="the address the browser landed on, or the code")
    parser.add_argument("--out", type=Path, default=TOKEN)
    args = parser.parse_args(argv)
    try:
        client_id, secret = read_client(args.client)
        if args.code is None:
            print(consent_url(client_id))
            return 0
        refresh = exchange(client_id, secret, code_from(args.code), client=client)
        write_token(args.out, client_id, secret, refresh, chown=os.geteuid() == 0)
    except (LoginError, OSError, httpx.HTTPError) as error:
        name = str(error) if isinstance(error, LoginError) else type(error).__name__
        print(json.dumps({"error": name}))
        return 1
    print(json.dumps({"token": str(args.out), "next": "restart the interpreter container"}))
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
