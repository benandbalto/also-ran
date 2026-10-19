#!/usr/bin/env python3
"""Emit durable GitHub wall-clock and modeled GCP runner-cost events."""

from __future__ import annotations

from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from also_ran import (
    JsonClient,
    ObservationError,
    atomic_write_json,
    configured_repositories,
    configured_retired_repositories,
    exclusive_lock,
    load_json,
)
from gcp_broker import gcloud
from fleet_measurements import host_fields, step_fields


VERSION = "0.4.0"
SCHEMA_VERSION = 1
GCP_LABELS = {
    "heavy": "outrunner-gcp-linux-x64",
    "general": "outrunner-gcp-general-linux-x64",
}
COST_MODEL = "gcp_catalog_spot_compute_local_ssd_v1"
COST_EXCLUDES = ["boot_disk", "external_ip", "network", "credits", "tax"]
STATE_RETENTION_DAYS = 35
MINIMUM_GITHUB_RATE_REMAINING = 750
RUN_PAGES_PER_TICK = 10
JOB_PAGES_PER_TICK = 25
# Extra job pages a tick may spend while a backlog exists and the shared
# GitHub budget is comfortably above the broker's own floor. Without it the
# fixed 25 pages fell behind daytime run arrival and the backlog only grew:
# 863 -> 1,869 pending runs and ~3h -> ~13h emission lag on 2026-10-02.
JOB_CATCHUP_PAGES_PER_TICK = 75
CATCHUP_MINIMUM_RATE_REMAINING = 2000
# A workflow whose last observed runs never touched a GCP pool is fetched
# only after every other pending run. About 85% of runs are such workflows
# (coderabbit-gate, auto-approve, guards); they still get observed, just last.
IRRELEVANT_WORKFLOW_MINIMUM_RUNS = 10


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def isoformat(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def parse_time(value: Any, field: str) -> dt.datetime:
    if not isinstance(value, str):
        raise ObservationError(f"{field} is not a timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ObservationError(f"{field} is not a timestamp") from error
    if parsed.tzinfo is None:
        raise ObservationError(f"{field} is not timezone-aware")
    return parsed.astimezone(dt.timezone.utc)


def seconds_between(start: Any, end: Any) -> float | None:
    if not start or not end:
        return None
    seconds = (parse_time(end, "end") - parse_time(start, "start")).total_seconds()
    return max(0.0, seconds)


def read_secret(path: str) -> str:
    value = pathlib.Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise ObservationError(f"credential file is empty: {path}")
    return value


class GitHubClient:
    def __init__(self, token: str) -> None:
        self.token = token
        self.rate_remaining: int | None = None

    def get(self, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        client = JsonClient(self.token, "https://api.github.com")
        payload = client.get(path, query)
        remaining = client.last_headers.get("x-ratelimit-remaining")
        self.rate_remaining = int(remaining) if remaining else None
        if not isinstance(payload, dict):
            raise ObservationError(f"GitHub response was not an object for {path}")
        return payload


def github_budget_available(client: GitHubClient) -> bool:
    remaining = getattr(client, "rate_remaining", None)
    return not isinstance(remaining, int) or remaining > MINIMUM_GITHUB_RATE_REMAINING


def catchup_budget_available(client: GitHubClient) -> bool:
    remaining = getattr(client, "rate_remaining", None)
    return isinstance(remaining, int) and remaining >= CATCHUP_MINIMUM_RATE_REMAINING


def workflow_relevance(
    state: dict[str, Any], repository: str
) -> dict[str, bool]:
    """Per workflow name: True once any observed run had a GCP job, False
    after enough observed runs that never did. Unknown workflows are absent."""
    seen: dict[str, int] = {}
    relevant: set[str] = set()
    for run in state["runs"].values():
        if (
            run.get("repository") != repository
            or not run.get("jobs_observed")
            or run.get("jobs_unavailable")
        ):
            continue
        name = str(run.get("workflow_name"))
        seen[name] = seen.get(name, 0) + 1
        if run.get("relevant_jobs"):
            relevant.add(name)
    result = {name: True for name in relevant}
    for name, count in seen.items():
        if name not in relevant and count >= IRRELEVANT_WORKFLOW_MINIMUM_RUNS:
            result[name] = False
    return result


def empty_state() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "observed_at": None,
        "hosts": {},
        "runs": {},
        "jobs": {},
        "event_fingerprints": {},
        "run_scans": {},
    }


def validate_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
        raise ObservationError("run telemetry state has an unexpected schema")
    for field in ("hosts", "runs", "jobs", "event_fingerprints"):
        if not isinstance(value.get(field), dict):
            raise ObservationError(f"run telemetry state omitted {field}")
    if not isinstance(value.get("run_scans", {}), dict):
        raise ObservationError("run telemetry state has malformed run scans")
    value.setdefault("run_scans", {})
    return value


def load_state(path: str) -> dict[str, Any]:
    state_path = pathlib.Path(path)
    if not state_path.exists():
        return empty_state()
    return validate_state(load_json(path))


def validate_policy(policy: dict[str, Any]) -> None:
    if (
        policy.get("project_id") != PROJECT_ID
        or policy.get("production", {}).get("workflow_label") != GCP_LABELS["heavy"]
        or policy.get("general_production", {}).get("workflow_label")
        != GCP_LABELS["general"]
        or policy.get("production", {}).get("machine_type") != "c4-standard-8-lssd"
        or policy.get("general_production", {}).get("machine_type")
        != "c4-standard-4-lssd"
    ):
        raise ObservationError("unexpected GCP run telemetry policy")


def price_models(
    market_state: dict[str, Any], now: dt.datetime, maximum_age_hours: int
) -> dict[str, dict[str, float]]:
    observed_at = parse_time(market_state.get("price_observed_at"), "price_observed_at")
    age = now - observed_at
    if age < dt.timedelta(0) or age > dt.timedelta(hours=maximum_age_hours):
        return {}
    result: dict[str, dict[str, float]] = {}
    regions = market_state.get("regions")
    if not isinstance(regions, list):
        raise ObservationError("market state omitted regions")
    for item in regions:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            raise ObservationError("market state contained a malformed region")
        price = item.get("price")
        components = (
            price.get("components_hourly_usd") if isinstance(price, dict) else None
        )
        if not isinstance(components, dict) or set(components) != {
            "core",
            "ram",
            "local_ssd",
        }:
            raise ObservationError("market state omitted price components")
        try:
            heavy = sum(float(components[key]) for key in components)
            general = (
                float(components["core"]) / 2
                + float(components["ram"]) / 2
                + float(components["local_ssd"])
            )
        except (TypeError, ValueError) as error:
            raise ObservationError(
                "market state contained a malformed price"
            ) from error
        result[item["name"]] = {"heavy": heavy, "general": general}
    return result


def instance_pool(raw: dict[str, Any]) -> str | None:
    labels = raw.get("labels") or {}
    lifecycle = labels.get("lifecycle")
    if lifecycle == "burst":
        return "heavy"
    if lifecycle == "general":
        return "general"
    return None


def observe_instances(policy: dict[str, Any]) -> list[dict[str, Any]]:
    payload = gcloud(
        [
            "compute",
            "instances",
            "list",
            f"--project={policy['project_id']}",
            f"--filter=labels.managed-by=runner-autoscaler AND labels.fleet={FLEET} AND labels.provider=gcp",
        ]
    )
    if not isinstance(payload, list):
        raise ObservationError("GCP instance list was not an array")
    result = []
    for raw in payload:
        if not isinstance(raw, dict):
            raise ObservationError("GCP instance list contained a malformed entry")
        pool = instance_pool(raw)
        if pool is None:
            continue
        instance_id = str(raw.get("id", ""))
        name = str(raw.get("name", ""))
        zone = str(raw.get("zone", "")).rsplit("/", 1)[-1]
        created_at = raw.get("creationTimestamp")
        prefix = policy["production" if pool == "heavy" else "general_production"][
            "scale_set_prefix"
        ]
        name_prefix = policy["production" if pool == "heavy" else "general_production"][
            "name_prefix"
        ]
        if not instance_id or not name.startswith(name_prefix + "-"):
            raise ObservationError("owned GCP instance identity is malformed")
        slot = name.removeprefix(name_prefix + "-")
        if not slot.isdigit() or not zone:
            raise ObservationError("owned GCP instance slot is malformed")
        parse_time(created_at, "creationTimestamp")
        result.append(
            {
                "instance_id": instance_id,
                "instance_name": name,
                "pool": pool,
                "scale_set": f"{prefix}-{int(slot):02d}",
                "zone": zone,
                "region": zone.rsplit("-", 1)[0],
                "created_at": created_at,
                "status": raw.get("status"),
                "image_source_commit": (raw.get("labels") or {}).get("source-commit"),
            }
        )
    return result


def update_hosts(
    state: dict[str, Any],
    instances: list[dict[str, Any]],
    prices: dict[str, dict[str, float]],
    observed_at: dt.datetime,
) -> list[str]:
    now_text = isoformat(observed_at)
    current_ids = {item["instance_id"] for item in instances}
    closed: list[str] = []
    for item in instances:
        host = state["hosts"].get(item["instance_id"])
        if host is None:
            hourly = prices.get(item["region"], {}).get(item["pool"])
            host = {
                **item,
                "first_seen_at": now_text,
                "last_seen_at": now_text,
                "deleted_observed_at": None,
                "modeled_hourly_usd": hourly,
                "price_observed_at": now_text if hourly is not None else None,
            }
            state["hosts"][item["instance_id"]] = host
        else:
            if host.get("deleted_observed_at") is not None:
                raise ObservationError("a closed GCP instance identity reappeared")
            host["last_seen_at"] = now_text
            host["status"] = item.get("status")
            if host.get("modeled_hourly_usd") is None:
                hourly = prices.get(item["region"], {}).get(item["pool"])
                host["modeled_hourly_usd"] = hourly
                host["price_observed_at"] = now_text if hourly is not None else None
    for instance_id, host in state["hosts"].items():
        if host.get("deleted_observed_at") is None and instance_id not in current_ids:
            host["deleted_observed_at"] = now_text
            closed.append(instance_id)
    return closed


def discover_runs(
    client: GitHubClient,
    repository: str,
    scan: dict[str, Any],
) -> list[dict[str, Any]]:
    """Read bounded fixed ranges, splitting searches at GitHub's 1,000-run cap."""
    runs: list[dict[str, Any]] = []
    for _ in range(RUN_PAGES_PER_TICK):
        if not scan["ranges"] or not github_budget_available(client):
            break
        current = scan["ranges"][-1]
        since = parse_time(current.get("since"), "run scan since")
        until = parse_time(current.get("until"), "run scan until")
        page = current.get("page")
        if not isinstance(page, int) or isinstance(page, bool) or page < 1:
            raise ObservationError("run scan page is malformed")
        payload = client.get(
            f"/repos/{repository}/actions/runs",
            {
                "created": f"{isoformat(since)}..{isoformat(until)}",
                "per_page": 100,
                "page": page,
            },
        )
        items = payload.get("workflow_runs")
        total = payload.get("total_count")
        if not isinstance(items, list) or any(
            not isinstance(item, dict) for item in items
        ):
            raise ObservationError(
                f"GitHub returned malformed workflow_runs for {repository}"
            )
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise ObservationError(
                f"GitHub returned malformed run count for {repository}"
            )
        if total >= 1000:
            span = int((until - since).total_seconds())
            if span < 1:
                raise ObservationError(
                    f"GitHub returned at least 1,000 runs in one second for {repository}"
                )
            middle = since + dt.timedelta(seconds=span // 2)
            scan["ranges"].pop()
            scan["ranges"].extend([
                {"since": isoformat(since), "until": isoformat(middle), "page": 1},
                {
                    "since": isoformat(middle + dt.timedelta(seconds=1)),
                    "until": isoformat(until),
                    "page": 1,
                },
            ])
            continue
        runs.extend(items)
        if page * 100 >= total or len(items) < 100:
            scan["ranges"].pop()
        else:
            current["page"] = page + 1
    return runs


def run_record(run: dict[str, Any], repository: str) -> dict[str, Any]:
    run_id = run.get("id")
    if not isinstance(run_id, int) or isinstance(run_id, bool):
        raise ObservationError("GitHub workflow run omitted its numeric ID")
    created_at = run.get("created_at")
    updated_at = run.get("updated_at")
    parse_time(created_at, "workflow created_at")
    parse_time(updated_at, "workflow updated_at")
    return {
        "repository": repository,
        "workflow_run_id": run_id,
        "workflow_run_attempt": run.get("run_attempt"),
        "workflow_name": run.get("name"),
        "event": run.get("event"),
        "head_sha": run.get("head_sha"),
        "head_branch": run.get("head_branch"),
        "status": run.get("status"),
        "conclusion": run.get("conclusion"),
        "created_at": created_at,
        "run_started_at": run.get("run_started_at"),
        "updated_at": updated_at,
        "html_url": run.get("html_url"),
        "jobs_observed": False,
    }


def job_pool(job: dict[str, Any]) -> str | None:
    labels = set(job.get("labels") or [])
    matches = [pool for pool, label in GCP_LABELS.items() if label in labels]
    if len(matches) > 1:
        raise ObservationError("GitHub job matched multiple GCP pools")
    return matches[0] if matches else None


def job_record(job: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    job_id = job.get("id")
    if not isinstance(job_id, int) or isinstance(job_id, bool):
        raise ObservationError("GitHub job omitted its numeric ID")
    created_at = job.get("created_at")
    if created_at:
        parse_time(created_at, "job created_at")
    for field in ("started_at", "completed_at"):
        if job.get(field):
            parse_time(job[field], f"job {field}")
    return {
        "github_job_id": job_id,
        "repository": run["repository"],
        "workflow_run_id": run["workflow_run_id"],
        "workflow_run_attempt": job.get("run_attempt", run.get("workflow_run_attempt")),
        "job_name": job.get("name"),
        "steps": [
            {key: step.get(key) for key in (
                "name", "number", "status", "conclusion", "started_at", "completed_at"
            )}
            for step in job.get("steps", [])
        ] if "steps" in job else None,
        "pool": job_pool(job),
        "status": job.get("status"),
        "conclusion": job.get("conclusion"),
        "created_at": created_at,
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
        "runner_name": job.get("runner_name") or None,
        "html_url": job.get("html_url"),
        "host_instance_id": None,
    }


def matching_host(job: dict[str, Any], hosts: dict[str, Any]) -> str | None:
    runner = job.get("runner_name")
    if not isinstance(runner, str) or not runner:
        return None
    started = job.get("started_at") or job.get("created_at")
    if not started:
        return None
    started_at = parse_time(started, "job start")
    matches: list[tuple[dt.datetime, str]] = []
    for instance_id, host in hosts.items():
        if not runner.startswith(str(host.get("scale_set", "")) + "-"):
            continue
        created_at = parse_time(host.get("created_at"), "host created_at")
        deleted = host.get("deleted_observed_at")
        deleted_at = parse_time(deleted, "host deleted_at") if deleted else None
        if created_at <= started_at and (
            deleted_at is None or started_at <= deleted_at
        ):
            matches.append((created_at, instance_id))
    if not matches:
        return None
    # Deletion is learned by a two-minute poll, so a predecessor's observed
    # lifetime can overlap its replacement even though GCP cannot create the
    # same instance name until deletion is complete. The newest generation
    # created before the job started is therefore authoritative. Preserve an
    # ambiguity failure only for impossible duplicate creation timestamps.
    matches.sort(reverse=True)
    if len(matches) > 1 and matches[0][0] == matches[1][0]:
        raise ObservationError("GitHub job matched multiple GCP host generations")
    return matches[0][1]


def observe_github(
    state: dict[str, Any],
    client: GitHubClient,
    repositories: str | tuple[str, ...],
    since: dt.datetime,
    now: dt.datetime,
) -> None:
    served = (repositories,) if isinstance(repositories, str) else repositories
    for repository in served:
        observe_repository(state, client, repository, since, now)


def observe_repository(
    state: dict[str, Any],
    client: GitHubClient,
    repository: str,
    since: dt.datetime,
    now: dt.datetime,
) -> None:
    previous = state["run_scans"].get(repository, {})
    if previous.get("ranges"):
        scan = previous
    else:
        completed_until = previous.get("until") or state.get("observed_at")
        scan_since = min(
            since,
            parse_time(completed_until, "previous scan until")
            if completed_until else since,
        )
        scan_since = max(scan_since, now - dt.timedelta(days=STATE_RETENTION_DAYS))
        scan_since = scan_since.replace(microsecond=0)
        scan_until = now.replace(microsecond=0)
        scan = {
            "since": isoformat(scan_since),
            "until": isoformat(scan_until),
            "ranges": [
                {
                    "since": isoformat(scan_since),
                    "until": isoformat(scan_until),
                    "page": 1,
                }
            ],
        }
    runs = discover_runs(client, repository, scan)
    for raw_run in runs:
        record = run_record(raw_run, repository)
        key = str(record["workflow_run_id"])
        existing = state["runs"].get(key)
        unchanged_terminal = (
            existing is not None
            and existing.get("status") == "completed"
            and existing.get("updated_at") == record["updated_at"]
            and existing.get("jobs_observed") is True
        )
        if unchanged_terminal:
            continue
        if (
            existing
            and existing.get("updated_at") == record["updated_at"]
            and (
                not existing.get("jobs_observed")
                or existing.get("status") == "completed"
                or existing.get("jobs_unavailable")
            )
        ):
            # Keep an unfinished job cursor when the run list is read again.
            continue
        record["job_page"] = 1
        record["relevant_jobs"] = 0
        state["runs"][key] = record
    state["run_scans"][repository] = scan

    remaining_pages = JOB_PAGES_PER_TICK
    catchup_pages = JOB_CATCHUP_PAGES_PER_TICK
    relevance = workflow_relevance(state, repository)

    def tier(run: dict[str, Any]) -> int:
        known = relevance.get(str(run.get("workflow_name")))
        return 0 if known is True else 2 if known is False else 1

    pending = sorted(
        (
            run
            for run in state["runs"].values()
            if run.get("repository") == repository and not run.get("jobs_observed")
        ),
        key=lambda run: (
            tier(run),
            run.get("status") != "completed",
            run["created_at"],
            run["workflow_run_id"],
        ),
    )

    def take_page() -> bool:
        nonlocal remaining_pages, catchup_pages
        if not github_budget_available(client):
            return False
        if remaining_pages:
            remaining_pages -= 1
            return True
        if catchup_pages and catchup_budget_available(client):
            catchup_pages -= 1
            return True
        return False

    exhausted = False
    for record in pending:
        while True:
            if not take_page():
                exhausted = True
                break
            page = record.get("job_page", 1)
            if not isinstance(page, int) or isinstance(page, bool) or page < 1:
                raise ObservationError("run job page is malformed")
            try:
                payload = client.get(
                    f"/repos/{repository}/actions/runs/{record['workflow_run_id']}/jobs",
                    {"filter": "all", "per_page": 100, "page": page},
                )
            except ObservationError as error:
                cause = error.__cause__
                if not isinstance(cause, urllib.error.HTTPError) or cause.code != 404:
                    raise
                # A deleted run must not strand the cursor on every later tick.
                record["jobs_unavailable"] = True
                record["jobs_observed"] = True
                record["relevant_jobs"] = 0
                record.pop("job_page", None)
                break
            jobs = payload.get("jobs")
            if not isinstance(jobs, list) or any(
                not isinstance(job, dict) for job in jobs
            ):
                raise ObservationError(
                    "GitHub returned malformed jobs for a workflow run"
                )
            relevant = record.get("relevant_jobs", 0)
            if not isinstance(relevant, int) or relevant < 0:
                raise ObservationError("run relevant job count is malformed")
            for raw_job in jobs:
                pool = job_pool(raw_job)
                if pool is None:
                    continue
                relevant += 1
                job = job_record(raw_job, record)
                job["host_instance_id"] = matching_host(job, state["hosts"])
                state["jobs"][str(job["github_job_id"])] = job
            record["relevant_jobs"] = relevant
            if len(jobs) < 100:
                record["jobs_observed"] = True
                record.pop("job_page", None)
                break
            record["job_page"] = page + 1
        if exhausted:
            break


def event_fingerprint(event: dict[str, Any]) -> str:
    payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def maybe_emit(state: dict[str, Any], event: dict[str, Any]) -> dict[str, Any] | None:
    identity = str(event["event_id"])
    comparable = {key: value for key, value in event.items() if key != "observed_at"}
    fingerprint = event_fingerprint(comparable)
    if state["event_fingerprints"].get(identity) == fingerprint:
        return None
    state["event_fingerprints"][identity] = fingerprint
    return event


class JobIndex:
    """Jobs grouped by run and by host, built once per collection.

    Every event builder used to scan all tracked jobs (~27k) for each run and
    each job on a deleted host, which made a tick quadratic: minutes of CPU
    per tick and, with the two-minute timer, a tick only every ~7 minutes.
    """

    def __init__(self, state: dict[str, Any]) -> None:
        self.by_run: dict[Any, list[dict[str, Any]]] = {}
        self.completed_by_host: dict[str, list[dict[str, Any]]] = {}
        self.allocations: dict[str, tuple[dict[str, float], float | None]] = {}
        for job in state["jobs"].values():
            self.by_run.setdefault(job["workflow_run_id"], []).append(job)
            host_id = job.get("host_instance_id")
            if host_id and job.get("status") == "completed":
                self.completed_by_host.setdefault(str(host_id), []).append(job)


def host_allocations(
    state: dict[str, Any],
    host: dict[str, Any],
    index: JobIndex | None = None,
) -> tuple[dict[str, float], float | None]:
    index = index or JobIndex(state)
    key = str(host["instance_id"])
    if key not in index.allocations:
        index.allocations[key] = _host_allocations(host, index)
    return index.allocations[key]


def _host_allocations(
    host: dict[str, Any], index: JobIndex
) -> tuple[dict[str, float], float | None]:
    deleted = host.get("deleted_observed_at")
    hourly = host.get("modeled_hourly_usd")
    if not deleted or not isinstance(hourly, (int, float)):
        return {}, None
    lifetime = seconds_between(host.get("created_at"), deleted)
    if lifetime is None:
        return {}, None
    total = lifetime / 3600 * float(hourly)
    jobs = index.completed_by_host.get(str(host["instance_id"]), [])
    if not jobs:
        return {}, total
    durations = {
        str(job["github_job_id"]): seconds_between(
            job.get("started_at"), job.get("completed_at")
        )
        or 0.0
        for job in jobs
    }
    denominator = sum(durations.values())
    if denominator <= 0:
        share = total / len(jobs)
        return {key: share for key in durations}, total
    return {
        key: total * duration / denominator for key, duration in durations.items()
    }, total


def workflow_fields(state: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
    run = state["runs"].get(str(job["workflow_run_id"]), {})
    completed = run.get("updated_at") if run.get("status") == "completed" else None
    return {
        "workflow_name": run.get("workflow_name"),
        "workflow_event": run.get("event"),
        "head_sha": run.get("head_sha"),
        "workflow_conclusion": run.get("conclusion"),
        "workflow_created_at": run.get("created_at"),
        "workflow_started_at": run.get("run_started_at"),
        "workflow_completed_at": completed,
        "workflow_wall_seconds": seconds_between(run.get("created_at"), completed),
        "workflow_url": run.get("html_url"),
    }


def build_job_event(
    state: dict[str, Any],
    job: dict[str, Any],
    repository: str | None,
    observed_at: dt.datetime,
    index: JobIndex | None = None,
) -> dict[str, Any] | None:
    if job.get("status") != "completed":
        return None
    index = index or JobIndex(state)
    host = state["hosts"].get(str(job.get("host_instance_id")))
    hourly = host.get("modeled_hourly_usd") if host else None
    duration = seconds_between(job.get("started_at"), job.get("completed_at"))
    marginal = (
        duration / 3600 * float(hourly)
        if duration is not None and isinstance(hourly, (int, float))
        else None
    )
    allocated = None
    if host and host.get("deleted_observed_at"):
        allocations, _ = host_allocations(state, host, index)
        allocated = allocations.get(str(job["github_job_id"]))
        cost_status = "final" if allocated is not None else "unattributed"
    elif host and hourly is not None:
        cost_status = "provisional"
    elif host:
        cost_status = "price_unavailable"
    else:
        cost_status = "host_unmatched"
    host_jobs = index.completed_by_host.get(str(host["instance_id"]), []) if host else []
    first_job = min(
        (item for item in host_jobs if item.get("started_at")),
        key=lambda item: (parse_time(item["started_at"], "job started_at"), item["github_job_id"]),
        default=None,
    )
    cohort = "host_unmatched"
    if host and first_job:
        cohort = "first_job_on_observed_host" if first_job["github_job_id"] == job["github_job_id"] else "reused_host"
    event = {
        "event": "runner_job_metric",
        "event_version": SCHEMA_VERSION,
        "event_id": f"github-job:{job['github_job_id']}",
        "collector_version": VERSION,
        "observed_at": isoformat(observed_at),
        "repository": job.get("repository") or repository,
        "workflow_run_id": job["workflow_run_id"],
        "workflow_run_attempt": job.get("workflow_run_attempt"),
        "github_job_id": job["github_job_id"],
        "job_name": job.get("job_name"),
        "pool": job.get("pool"),
        "status": job.get("status"),
        "conclusion": job.get("conclusion"),
        "queued_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
        "queue_seconds": seconds_between(job.get("created_at"), job.get("started_at")),
        "job_duration_seconds": duration,
        "runner_name": job.get("runner_name"),
        "job_url": job.get("html_url"),
        "host_instance_id": host.get("instance_id") if host else None,
        "host_instance_name": host.get("instance_name") if host else None,
        "scale_set": host.get("scale_set") if host else None,
        "region": host.get("region") if host else None,
        "zone": host.get("zone") if host else None,
        "host_created_at": host.get("created_at") if host else None,
        "image_source_commit": host.get("image_source_commit") if host else None,
        "cold_start_cohort": cohort,
        "cold_start_source": "first_job_proxy_not_runner_readiness",
        "host_deleted_observed_at": host.get("deleted_observed_at") if host else None,
        "cost_status": cost_status,
        "cost_model": COST_MODEL,
        "cost_excludes": COST_EXCLUDES,
        "modeled_hourly_usd": hourly,
        "modeled_marginal_compute_usd": marginal,
        "modeled_allocated_host_usd": allocated,
        **step_fields(job),
        **workflow_fields(state, job),
    }
    return maybe_emit(state, event)


def host_repositories(
    state: dict[str, Any],
    host: dict[str, Any],
    repository: str | None,
    index: JobIndex | None = None,
) -> list[str]:
    """Repositories whose completed jobs this host served."""
    index = index or JobIndex(state)
    return sorted(
        {
            str(job.get("repository") or repository)
            for job in index.completed_by_host.get(str(host["instance_id"]), [])
            if job.get("repository") or repository
        }
    )


def build_host_event(
    state: dict[str, Any],
    host: dict[str, Any],
    repository: str | None,
    observed_at: dt.datetime,
    index: JobIndex | None = None,
) -> dict[str, Any] | None:
    """A host's lifetime cost. A host can serve jobs from several repositories
    in one lifetime; `repository` names it only when exactly one repository
    (or, with no jobs, the collector's single configured one) accounts for it,
    and `repositories` always lists them, so spend never lands on the wrong
    repository by default."""
    if not host.get("deleted_observed_at"):
        return None
    index = index or JobIndex(state)
    allocations, total = host_allocations(state, host, index)
    served = host_repositories(state, host, repository, index)
    if len(served) == 1:
        host_repository: str | None = served[0]
    elif not served:
        host_repository = repository
    else:
        host_repository = None
    lifetime = seconds_between(host.get("created_at"), host.get("deleted_observed_at"))
    event = {
        "event": "runner_host_cost",
        "event_version": SCHEMA_VERSION,
        "event_id": f"gcp-instance:{host['instance_id']}",
        "collector_version": VERSION,
        "observed_at": isoformat(observed_at),
        "repository": host_repository,
        "repositories": served,
        "host_instance_id": host["instance_id"],
        "host_instance_name": host.get("instance_name"),
        "pool": host.get("pool"),
        "scale_set": host.get("scale_set"),
        "region": host.get("region"),
        "zone": host.get("zone"),
        "created_at": host.get("created_at"),
        "deleted_observed_at": host.get("deleted_observed_at"),
        "host_lifetime_seconds": lifetime,
        "completed_jobs": len(index.completed_by_host.get(str(host["instance_id"]), [])),
        **host_fields(host, index.completed_by_host.get(str(host["instance_id"]), [])),
        "cost_status": "final" if total is not None else "price_unavailable",
        "cost_model": COST_MODEL,
        "cost_excludes": COST_EXCLUDES,
        "modeled_hourly_usd": host.get("modeled_hourly_usd"),
        "modeled_total_host_usd": total,
        "modeled_unallocated_host_usd": total if not allocations else 0.0,
    }
    return maybe_emit(state, event)


def build_workflow_event(
    state: dict[str, Any],
    run: dict[str, Any],
    repository: str | None,
    observed_at: dt.datetime,
    index: JobIndex | None = None,
) -> dict[str, Any] | None:
    if (
        run.get("status") != "completed"
        or not run.get("jobs_observed")
        or not run.get("relevant_jobs")
    ):
        return None
    run_id = run["workflow_run_id"]
    index = index or JobIndex(state)
    jobs = index.by_run.get(run_id, [])
    completed = [job for job in jobs if job.get("status") == "completed"]
    marginal_costs: list[float] = []
    allocated_costs: list[float] = []
    cost_statuses: list[str] = []
    for job in completed:
        host = state["hosts"].get(str(job.get("host_instance_id")))
        hourly = host.get("modeled_hourly_usd") if host else None
        duration = seconds_between(job.get("started_at"), job.get("completed_at"))
        if duration is not None and isinstance(hourly, (int, float)):
            marginal_costs.append(duration / 3600 * float(hourly))
        if host and host.get("deleted_observed_at"):
            allocations, _ = host_allocations(state, host, index)
            allocation = allocations.get(str(job["github_job_id"]))
            if allocation is not None:
                allocated_costs.append(allocation)
                cost_statuses.append("final")
            else:
                cost_statuses.append("unattributed")
        elif host and hourly is not None:
            cost_statuses.append("provisional")
        elif host:
            cost_statuses.append("price_unavailable")
        else:
            cost_statuses.append("host_unmatched")
    if cost_statuses and all(status == "final" for status in cost_statuses):
        cost_status = "final"
    elif cost_statuses and all(
        status in {"final", "provisional"} for status in cost_statuses
    ):
        cost_status = "provisional"
    else:
        cost_status = "incomplete"
    event = {
        "event": "runner_workflow_metric",
        "event_version": SCHEMA_VERSION,
        "event_id": f"github-run:{run_id}",
        "collector_version": VERSION,
        "observed_at": isoformat(observed_at),
        "repository": run.get("repository") or repository,
        "workflow_run_id": run_id,
        "workflow_run_attempt": run.get("workflow_run_attempt"),
        "workflow_name": run.get("workflow_name"),
        "workflow_event": run.get("event"),
        "head_sha": run.get("head_sha"),
        "conclusion": run.get("conclusion"),
        "created_at": run.get("created_at"),
        "started_at": run.get("run_started_at"),
        "completed_at": run.get("updated_at"),
        "workflow_wall_seconds": seconds_between(
            run.get("created_at"), run.get("updated_at")
        ),
        "gcp_job_makespan_seconds": seconds_between(
            min(
                (job.get("created_at") for job in jobs if job.get("created_at")),
                default=None,
            ),
            max(
                (
                    job.get("completed_at")
                    for job in completed
                    if job.get("completed_at")
                ),
                default=None,
            ),
        ),
        "gcp_jobs": len(jobs),
        "completed_gcp_jobs": len(completed),
        "cost_status": cost_status,
        "cost_model": COST_MODEL,
        "cost_excludes": COST_EXCLUDES,
        "modeled_marginal_compute_usd": (
            sum(marginal_costs)
            if completed and len(marginal_costs) == len(completed)
            else None
        ),
        "modeled_allocated_host_usd": (
            sum(allocated_costs)
            if completed and len(allocated_costs) == len(completed)
            else None
        ),
        "html_url": run.get("html_url"),
    }
    return maybe_emit(state, event)


def prune_state(state: dict[str, Any], now: dt.datetime) -> None:
    cutoff = now - dt.timedelta(days=STATE_RETENTION_DAYS)
    old_runs = {
        key
        for key, run in state["runs"].items()
        if run.get("status") == "completed"
        and parse_time(run.get("updated_at"), "run updated_at") < cutoff
    }
    old_jobs = {
        key
        for key, job in state["jobs"].items()
        if str(job.get("workflow_run_id")) in old_runs
    }
    referenced_hosts = {
        str(job.get("host_instance_id"))
        for key, job in state["jobs"].items()
        if key not in old_jobs and job.get("host_instance_id")
    }
    old_hosts = {
        key
        for key, host in state["hosts"].items()
        if host.get("deleted_observed_at")
        and parse_time(host["deleted_observed_at"], "host deleted_at") < cutoff
        and key not in referenced_hosts
    }
    for key in old_runs:
        state["runs"].pop(key, None)
        state["event_fingerprints"].pop(f"github-run:{key}", None)
    for key in old_jobs:
        state["jobs"].pop(key, None)
        state["event_fingerprints"].pop(f"github-job:{key}", None)
    for key in old_hosts:
        state["hosts"].pop(key, None)
        state["event_fingerprints"].pop(f"gcp-instance:{key}", None)


def backfill_repository(state: dict[str, Any], repository: str) -> None:
    """State written before multi-repository telemetry held one repository's
    runs and jobs without naming it. Name them once, as the first configured
    repository, so a later second repository cannot inherit their spend."""
    for record in (*state["runs"].values(), *state["jobs"].values()):
        record.setdefault("repository", repository)


def collect(
    policy: dict[str, Any],
    market_state: dict[str, Any],
    state: dict[str, Any],
    client: GitHubClient,
    repositories: str | tuple[str, ...],
    now: dt.datetime,
    lookback_hours: int,
    *,
    instances: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    validate_policy(policy)
    served = (repositories,) if isinstance(repositories, str) else repositories
    backfill_repository(state, served[0])
    # A single configured repository keeps its name on every event, exactly as
    # before; with several, an event names only the repository it belongs to.
    repository = served[0] if len(served) == 1 else None
    prices = price_models(
        market_state, now, int(policy["market"]["maximum_price_age_hours"])
    )
    update_hosts(
        state,
        instances if instances is not None else observe_instances(policy),
        prices,
        now,
    )
    observe_github(
        state,
        client,
        served,
        now - dt.timedelta(hours=lookback_hours),
        now,
    )
    events: list[dict[str, Any]] = []
    index = JobIndex(state)
    for run in state["runs"].values():
        event = build_workflow_event(state, run, repository, now, index)
        if event:
            events.append(event)
    for job in state["jobs"].values():
        event = build_job_event(state, job, repository, now, index)
        if event:
            events.append(event)
    for host in state["hosts"].values():
        event = build_host_event(state, host, repository, now, index)
        if event:
            events.append(event)
    events.append(
        {
            "event": "runner_run_telemetry_tick",
            "event_version": SCHEMA_VERSION,
            "collector_version": VERSION,
            "observed_at": isoformat(now),
            "repository": repository,
            "repositories": list(served),
            "tracked_hosts": len(state["hosts"]),
            "tracked_runs": len(state["runs"]),
            "tracked_jobs": len(state["jobs"]),
            "pending_run_ranges": sum(
                len(scan.get("ranges", []))
                for scan in state["run_scans"].values()
            ),
            "pending_job_runs": sum(
                not run.get("jobs_observed") for run in state["runs"].values()
            ),
            "oldest_pending_job_run_created_at": min(
                (
                    run["created_at"]
                    for run in state["runs"].values()
                    if not run.get("jobs_observed")
                ),
                default=None,
            ),
            "github_rate_remaining": client.rate_remaining,
        }
    )
    state["observed_at"] = isoformat(now)
    state["github_rate_remaining"] = client.rate_remaining
    prune_state(state, now)
    return events, state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--market-state", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--lock", required=True)
    parser.add_argument("--github-token-file", required=True)
    parser.add_argument(
        "--github-repository",
        help="one repository; the policy's github_repositories is the list form",
    )
    parser.add_argument("--lookback-hours", type=int, default=6)
    args = parser.parse_args()
    if args.lookback_hours < 1 or args.lookback_hours > 168:
        parser.error("--lookback-hours must be between 1 and 168")
    try:
        with exclusive_lock(args.lock):
            state = load_state(args.state)
            policy = load_json(args.policy)
            served = configured_repositories(policy, args.github_repository)
            # A retired repository's jobs can still be finishing on a host, so
            # its runs are still observed to completion and costed.
            repositories = served + configured_retired_repositories(policy, served)
            events, next_state = collect(
                policy,
                load_json(args.market_state),
                state,
                GitHubClient(read_secret(args.github_token_file)),
                repositories,
                utc_now(),
                args.lookback_hours,
            )
            # The state holds each event's fingerprint. Printing before a
            # failed write (ENOSPC, 2026-10-02 07:12-14:53) re-emitted the same
            # events every tick; committing the state before printing would
            # lose them for good on a kill mid-print. So print between the
            # durable write and the swap: a full disk prints nothing, and a
            # failed print leaves the old state to re-emit next tick.
            def emit_events() -> None:
                for event in events:
                    print(json.dumps(event, sort_keys=True), flush=True)

            atomic_write_json(args.state, next_state, before_replace=emit_events)
        return 0
    except (ObservationError, OSError, ValueError, KeyError, RuntimeError) as error:
        print(
            json.dumps(
                {
                    "event": "runner_run_telemetry_failure",
                    "collector_version": VERSION,
                    "observed_at": isoformat(utc_now()),
                    "error": str(error),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
