#!/usr/bin/env python3
"""Validate local GCP configuration without credentials or network access."""

import argparse
import importlib.util
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment", type=Path, required=True)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--bootstrap", action="store_true", help="validate structure before regional canaries; never authorizes a production tick")
    args = parser.parse_args()
    os.environ["ALSO_RAN_DEPLOYMENT"] = str(args.deployment.resolve())
    sys.path.insert(0, str(ROOT / "controller/also-ran"))
    import gcp_broker
    import gcp_market_observer
    policy = json.loads(args.policy.read_text())
    for pool in ("heavy", "general"):
        gcp_broker.validate_policy(policy, pool, require_production_regions=not args.bootstrap)
    gcp_market_observer.validate_market_policy(policy)
    spec = importlib.util.spec_from_file_location("gcp_lifecycle", ROOT / "scripts/gcp-runner-lifecycle.py")
    lifecycle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(lifecycle)
    lifecycle.validate_policy(policy, require_production_regions=not args.bootstrap)
    print("Valid deployment identity, lifecycle policy, both pools, and market policy." + (" Bootstrap only: production regions still require acceptance evidence." if args.bootstrap else ""))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, KeyError, TypeError) as error:
        print(f"also-ran configuration error: {error}", file=sys.stderr)
        raise SystemExit(1) from None
