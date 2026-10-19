#!/usr/bin/env python3
"""Bounded Hetzner lifecycle operations for the also-ran golden image pilot."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


API = "https://api.hetzner.cloud/v1"
USER_AGENT = "also-ran-image-lifecycle/0.1"


class LifecycleError(RuntimeError):
    pass


class Client:
    def __init__(self, token: str) -> None:
        self.token = token

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            API + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as error:
            detail = error.read(2048).decode(errors="replace")
            raise LifecycleError(
                f"{method} {path} returned HTTP {error.code}: {detail}"
            ) from error

    def get(self, path: str) -> dict[str, Any]:
        return self.request("GET", path)

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self.request("POST", path, payload)

    def delete(self, path: str) -> dict[str, Any]:
        return self.request("DELETE", path)


def load_json(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise LifecycleError(f"expected an object in {path}")
    return value


def source_commit() -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def validate_policy(policy: dict[str, Any]) -> None:
    if policy.get("policy_version") != 1:
        raise LifecycleError("unsupported image policy version")
    if policy.get("builder", {}).get("name") != "also-ran-runner-image-builder":
        raise LifecycleError("builder name is not the protected stable name")
    if policy.get("burst", {}).get("name") != "also-ran-runner-burst-01":
        raise LifecycleError("burst name is not the protected stable slot")
    expected = {
        "managed-by": "runner-autoscaler",
        "fleet": "also-ran-ci",
        "lifecycle": "burst",
        "autoscale-slot": "burst-01",
        "hold": "true",
    }
    if policy["burst"].get("labels") != expected:
        raise LifecycleError("burst labels do not match the held RFC slot")
    if not policy.get("protected_server_ids"):
        raise LifecycleError("protected_server_ids must not be empty")


def list_servers(client: Client) -> list[dict[str, Any]]:
    return client.get("/servers?per_page=50").get("servers", [])


def server_summary(server: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": server["id"],
        "name": server["name"],
        "status": server["status"],
        "ipv4": server["public_net"]["ipv4"]["ip"],
        "labels": server["labels"],
    }


def refuse_duplicate(servers: list[dict[str, Any]], name: str) -> None:
    matches = [server for server in servers if server["name"] == name]
    if matches:
        raise LifecycleError(
            f"server already exists: {json.dumps(server_summary(matches[0]))}"
        )


def create_server(
    client: Client,
    config: dict[str, Any],
    policy: dict[str, Any],
    image: str | int,
    source: str,
) -> dict[str, Any]:
    servers = list_servers(client)
    refuse_duplicate(servers, config["name"])
    labels = {**config["labels"], "source-commit": source[:12]}
    response = client.post(
        "/servers",
        {
            "name": config["name"],
            "server_type": config["server_type"],
            "image": image,
            "location": config["location"],
            "ssh_keys": policy["ssh_key_ids"],
            "firewalls": [{"firewall": policy["firewall_id"]}],
            "public_net": {"enable_ipv4": True, "enable_ipv6": False},
            "labels": labels,
        },
    )
    return server_summary(response["server"])


def get_owned_server(
    client: Client, server_id: int, config: dict[str, Any], protected_ids: set[int]
) -> dict[str, Any]:
    if server_id in protected_ids:
        raise LifecycleError("refusing to operate on a protected server ID")
    server = client.get(f"/servers/{server_id}")["server"]
    if server["name"] != config["name"]:
        raise LifecycleError("server name does not match policy")
    if any(
        server["labels"].get(key) != value for key, value in config["labels"].items()
    ):
        raise LifecycleError("server labels do not match policy")
    return server


def wait_for_server(
    client: Client, server_id: int, status: str, timeout: int
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        server = client.get(f"/servers/{server_id}")["server"]
        if server["status"] == status:
            return server_summary(server)
        time.sleep(5)
    raise LifecycleError(f"server {server_id} did not reach {status} within {timeout}s")


def wait_for_action(
    client: Client, action_id: int, timeout: int = 900
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        action = client.get(f"/actions/{action_id}")["action"]
        if action["status"] == "success":
            return action
        if action["status"] == "error":
            raise LifecycleError(f"Hetzner action failed: {action.get('error')}")
        time.sleep(5)
    raise LifecycleError(f"action {action_id} did not complete within {timeout}s")


def require_apply(args: argparse.Namespace) -> None:
    if not args.apply:
        raise LifecycleError("mutation requires --apply")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--token-file", required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    for name in ("create-builder", "create-burst"):
        child = subparsers.add_parser(name)
        child.add_argument("--apply", action="store_true")
        if name == "create-burst":
            child.add_argument("--snapshot-id", required=True, type=int)
    wait = subparsers.add_parser("wait")
    wait.add_argument("--server-id", required=True, type=int)
    wait.add_argument("--status", required=True, choices=("running", "off"))
    wait.add_argument("--timeout", type=int, default=600)
    snapshot = subparsers.add_parser("snapshot-builder")
    snapshot.add_argument("--server-id", required=True, type=int)
    snapshot.add_argument("--apply", action="store_true")
    delete = subparsers.add_parser("delete-builder")
    delete.add_argument("--server-id", required=True, type=int)
    delete.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)

    policy = load_json(args.policy)
    validate_policy(policy)
    token = pathlib.Path(args.token_file).read_text(encoding="utf-8").strip()
    if not token:
        raise LifecycleError("token file is empty")
    client = Client(token)
    commit = source_commit()
    protected = {int(value) for value in policy["protected_server_ids"]}

    if args.command == "status":
        names = {policy["builder"]["name"], policy["burst"]["name"]}
        result = [
            server_summary(server)
            for server in list_servers(client)
            if server["name"] in names
        ]
    elif args.command == "create-builder":
        require_apply(args)
        result = create_server(
            client, policy["builder"], policy, policy["builder"]["base_image"], commit
        )
    elif args.command == "create-burst":
        require_apply(args)
        image = client.get(f"/images/{args.snapshot_id}")["image"]
        if (
            image["type"] != "snapshot"
            or image["labels"].get("credential-free") != "true"
        ):
            raise LifecycleError("snapshot is not labelled credential-free")
        if image["labels"].get("source-commit") != commit[:12]:
            raise LifecycleError(
                "snapshot source commit does not match the checked-out commit"
            )
        result = create_server(
            client, policy["burst"], policy, args.snapshot_id, commit
        )
    elif args.command == "wait":
        result = wait_for_server(client, args.server_id, args.status, args.timeout)
    elif args.command == "snapshot-builder":
        require_apply(args)
        server = get_owned_server(client, args.server_id, policy["builder"], protected)
        if server["status"] != "off":
            raise LifecycleError("builder must be powered off before snapshot")
        description = (
            f"also-ran-runner-{dt.datetime.now(dt.timezone.utc).date()}-{commit[:12]}"
        )
        response = client.post(
            f"/servers/{args.server_id}/actions/create_image",
            {
                "type": "snapshot",
                "description": description,
                "labels": {
                    "managed-by": "runner-autoscaler",
                    "image-purpose": "also-ran-runner",
                    "credential-free": "true",
                    "source-commit": commit[:12],
                    "policy-version": str(policy["policy_version"]),
                },
            },
        )
        action = wait_for_action(client, response["action"]["id"])
        query = urllib.parse.urlencode({"type": "snapshot", "per_page": 50})
        matches = [
            image
            for image in client.get(f"/images?{query}").get("images", [])
            if image.get("description") == description
            and image.get("labels", {}).get("source-commit") == commit[:12]
        ]
        if len(matches) != 1:
            raise LifecycleError(
                "snapshot action succeeded but exact image reconciliation failed"
            )
        result = {
            "image_id": matches[0]["id"],
            "description": description,
            "action_id": action["id"],
            "status": matches[0]["status"],
        }
    elif args.command == "delete-builder":
        require_apply(args)
        server = get_owned_server(client, args.server_id, policy["builder"], protected)
        result = {"deleted": server_summary(server)}
        client.delete(f"/servers/{args.server_id}")
    else:
        raise LifecycleError(f"unsupported command: {args.command}")

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"error": str(error)}), file=sys.stderr)
        raise SystemExit(1)
