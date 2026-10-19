#!/usr/bin/env python3
"""Mint a read-only GitHub App installation token for also-ran.

also-ran used a personal access token, so it drew on the same 5,000/hour
allowance as every other client of that account. An installation token has an
allowance of its own. This runs on a timer and replaces one token file
atomically; the also-ran units load that file when they start, as they loaded
the personal token before.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from typing import Any


VERSION = "0.1.0"
SERVICE = "also-ran-github-token"
USER_AGENT = f"{SERVICE}/{VERSION}"
# Everything also-ran reads: workflow runs and jobs, and the repository and
# organization runner lists. Requested explicitly so the token stays read-only
# even if the App is later granted more.
PERMISSIONS = {
    "actions": "read",
    "administration": "read",
    "organization_self_hosted_runners": "read",
}
# GitHub rejects a JWT that expires more than ten minutes out, and one whose
# iat is in the future; backdating iat covers clock drift.
JWT_BACKDATE_SECONDS = 60
JWT_LIFETIME_SECONDS = 540


class TokenError(RuntimeError):
    pass


def base64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def app_jwt(app_id: str, private_key_file: str, now: float | None = None) -> str:
    issued = int(time.time() if now is None else now) - JWT_BACKDATE_SECONDS
    header = base64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
    claims = base64url(
        json.dumps(
            {"iat": issued, "exp": issued + JWT_LIFETIME_SECONDS, "iss": app_id}
        ).encode()
    )
    signing_input = f"{header}.{claims}".encode("ascii")
    # openssl is on every host; the cryptography package is not guaranteed.
    result = subprocess.run(
        ["openssl", "dgst", "-sha256", "-sign", private_key_file],
        input=signing_input,
        capture_output=True,
    )
    if result.returncode or not result.stdout:
        message = result.stderr.decode("utf-8", errors="replace").strip()
        raise TokenError(f"could not sign the App JWT: {message or 'no signature'}")
    return f"{header}.{claims}.{base64url(result.stdout)}"


def installation_token(
    api_url: str, installation_id: str, jwt: str
) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}/app/installations/{installation_id}/access_tokens",
        data=json.dumps({"permissions": PERMISSIONS}).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {jwt}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        body = error.read(2048).decode("utf-8", errors="replace")
        raise TokenError(
            f"installation token request returned HTTP {error.code}: {body}"
        ) from error
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        raise TokenError(f"installation token request failed: {error}") from error
    token = payload.get("token") if isinstance(payload, dict) else None
    if not isinstance(token, str) or not token:
        raise TokenError("installation token response had no token")
    granted = payload.get("permissions") or {}
    if any(granted.get(name) != level for name, level in PERMISSIONS.items()):
        raise TokenError(f"installation token was granted {granted}, wanted {PERMISSIONS}")
    return payload


def write_token(path: str, token: str) -> None:
    """Replace the token file in one step, so a reader never sees a partial one."""
    target = pathlib.Path(path)
    descriptor, temporary = tempfile.mkstemp(dir=target.parent, prefix=".token-")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(token + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o640)
        os.replace(temporary, target)
    except BaseException:
        pathlib.Path(temporary).unlink(missing_ok=True)
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--app-id", required=True)
    parser.add_argument("--installation-id", required=True)
    parser.add_argument("--private-key-file", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--api-url", default="https://api.github.com")
    args = parser.parse_args(argv)

    try:
        jwt = app_jwt(args.app_id, args.private_key_file)
        payload = installation_token(args.api_url, args.installation_id, jwt)
        write_token(args.output, payload["token"])
    except (TokenError, OSError) as error:
        print(
            json.dumps({"service": SERVICE, "version": VERSION, "result": "error",
                        "error": str(error)}),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {
                "service": SERVICE,
                "version": VERSION,
                "result": "ok",
                "minted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "expires_at": payload.get("expires_at"),
                "repository_selection": payload.get("repository_selection"),
                "permissions": payload.get("permissions"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
