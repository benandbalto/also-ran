#!/usr/bin/env python3
"""Observe the first Actions listener session on an immutable GCP boot.

Only a timestamped listener marker is retained; container logs, job output and
JIT material never leave this process. This is startup evidence, not a health
probe or an admission/scheduling decision. Missing markers remain unknown.
"""

import argparse
import datetime as dt
import json
import pathlib
import re
import subprocess
import time

MARKER = re.compile(
    r"^(\S+) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}Z): Listening for Jobs\s*$"
)


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("missing timezone")
    return parsed


def listener_online_at(logs, boot_started_at, observed_at):
    times = []
    for line in logs.splitlines():
        match = MARKER.fullmatch(line)
        if not match:
            continue
        try:
            logged, listener = map(timestamp, match.groups())
            # Docker supplies the precise host timestamp. The listener's own
            # UTC time corroborates the marker to within its one-second precision.
            if (
                boot_started_at <= logged <= observed_at
                and abs((logged - listener).total_seconds()) < 2
            ):
                times.append(logged)
        except ValueError:
            continue
    return min(times).isoformat() if times else None


def docker(*args):
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=5, check=True
    )
    # Docker can put stderr stream records in the CLI's stderr. Never forward
    # either stream, even when a command fails.
    return result.stdout + result.stderr


def observe(identity, current_boot_id, observed_at, command=docker):
    if (
        identity.get("boot_id") != current_boot_id
        or not re.fullmatch(r"[0-9]+", str(identity.get("instance_id", "")))
        or not identity.get("runner_scale_set")
    ):
        return None
    boot = timestamp(identity["boot_started_at"])
    candidates = []
    for line in command(
        "ps",
        "--all",
        "--no-trunc",
        "--filter",
        "label=outrunner=true",
        "--format",
        "{{.ID}} {{.Names}}",
    ).splitlines():
        parts = line.split()
        if (
            len(parts) != 2
            or not re.fullmatch(r"[a-f0-9]{64}", parts[0])
            or not parts[1].startswith(identity["runner_scale_set"] + "-")
        ):
            continue
        try:
            logs = command("logs", "--timestamps", "--tail", "200", parts[0])
            online = listener_online_at(logs, boot, observed_at)
            if online:
                candidates.append((online, parts[0], parts[1]))
        except (subprocess.SubprocessError, OSError):
            continue  # Removal races are missing evidence, never synthetic readiness.
    if not candidates:
        return None
    online, container, name = min(candidates)
    return {
        "event": "runner_readiness_metric",
        "event_version": 1,
        "event_id": f"ready:{identity['instance_id']}:{current_boot_id}",
        "host_instance_id": identity["instance_id"],
        "boot_id": current_boot_id,
        "pool": identity.get("pool"),
        "zone": identity.get("zone"),
        "image_source_commit": identity.get("image_source_commit"),
        "runner_scale_set": identity["runner_scale_set"],
        "runner_name": name,
        "container_id": container,
        "runner_online_at": online,
        "observed_at": observed_at.isoformat(),
        "boot_started_at": boot.isoformat(),
        "boot_to_runner_online_seconds": (timestamp(online) - boot).total_seconds(),
        "readiness_status": "observed",
        "readiness_source": "actions_listener_session_log",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", default="/var/lib/gcp-runner-bootstrap")
    args = parser.parse_args()
    state = pathlib.Path(args.state_dir)
    current_boot = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    receipt = state / "readiness-metric.json"
    if not (state / "identity.json").exists():
        return  # Non-GCP hosts do not have the immutable bootstrap identity.
    while True:
        try:
            # A durable receipt and sampler replay close the collector startup race.
            if (
                receipt.exists()
                and json.loads(receipt.read_text()).get("boot_id") == current_boot
            ):
                return
            identity = json.loads((state / "identity.json").read_text())
            if identity.get("boot_id") != current_boot or not all(
                identity.get(key)
                for key in ("instance_id", "runner_scale_set", "boot_started_at")
            ):
                return
            event = observe(identity, current_boot, dt.datetime.now(dt.timezone.utc))
            if event:
                temporary = receipt.with_suffix(".tmp")
                temporary.write_text(json.dumps(event, sort_keys=True) + "\n")
                temporary.chmod(0o600)
                temporary.replace(receipt)
                print(json.dumps(event, sort_keys=True), flush=True)
                return
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            pass
        time.sleep(1)


if __name__ == "__main__":
    main()
