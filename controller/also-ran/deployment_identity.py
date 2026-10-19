"""Trusted deployment identity, separate from the observed runtime policy.

Install this configuration root-owned alongside the controller. Changing a
project in the policy alone must never redirect the lifecycle helper.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import sys


def valid_legacy_images(config: dict) -> bool:
    commits = config.get("legacy_image_source_commits")
    return (
        isinstance(commits, list)
        and len(commits) <= 64
        and all(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) for value in commits)
        and len(set(commits)) == len(commits)
    )


def load_identity(path: str | Path) -> dict[str, str]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    patterns = {
        "project_id": r"[a-z][a-z0-9-]{4,28}[a-z0-9]",
        "project_number": r"[1-9][0-9]{5,19}",
        "resource_prefix": r"[a-z][a-z0-9-]{0,19}[a-z0-9]",
        "fleet": r"[a-z][a-z0-9-]{0,61}[a-z0-9]",
    }
    if not isinstance(value, dict) or set(value) != set(patterns):
        raise ValueError("deployment identity must contain exactly: " + ", ".join(patterns))
    for key, pattern in patterns.items():
        if not isinstance(value[key], str) or not re.fullmatch(pattern, value[key]):
            raise ValueError(f"invalid deployment identity: {key}")
    return value


def configured_identity() -> dict[str, str]:
    """Only installed services have an implicit trusted configuration location.

    A checkout's deployment.json is a regression fixture, never an operational
    default. Commands run from source must deliberately select their identity.
    """
    configured = os.environ.get("ALSO_RAN_DEPLOYMENT")
    if not configured and Path(__file__).resolve().parent == Path("/opt/also-ran"):
        configured = "/opt/also-ran/deployment.json"
    if not configured:
        raise ValueError(
            "set ALSO_RAN_DEPLOYMENT to your generated deployment.json; "
            "source commands never select the example identity automatically"
        )
    return load_identity(configured)


# Every importing executable uses argparse: help exits before operational code.
# Allow documentation/discovery without requiring any deployment credentials.
if "--help" in sys.argv[1:] or "-h" in sys.argv[1:]:
    IDENTITY = {key: "" for key in ("project_id", "project_number", "resource_prefix", "fleet")}
else:
    try:
        IDENTITY = configured_identity()
    except (OSError, ValueError) as error:
        print(f"also-ran configuration error: {error}", file=sys.stderr)
        raise SystemExit(1) from None
PROJECT_ID = IDENTITY["project_id"]
PROJECT_NUMBER = IDENTITY["project_number"]
RESOURCE_PREFIX = IDENTITY["resource_prefix"]
FLEET = IDENTITY["fleet"]
