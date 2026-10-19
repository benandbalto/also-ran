#!/usr/bin/env python3
"""GCP capacity reconciler for also-ran."""

from __future__ import annotations

from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET, valid_legacy_images

import argparse
import base64
import binascii
import contextlib
import contextvars
import dataclasses
import fcntl
import pathlib
import datetime as dt
import json
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Iterator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from also_ran import (
    JsonClient,
    ObservationError,
    atomic_write_json,
    configured_repositories,
    configured_retired_organizations,
    configured_retired_repositories,
    configured_runner_scope,
    exclusive_lock,
    load_json,
    read_secret,
)


VERSION = "0.22.1"
DEFAULT_POLICY = "/etc/also-ran/gcp-policy.json"
DEFAULT_STATE = "/var/lib/also-ran/gcp-state.json"
DEFAULT_MARKET_STATE = "/var/lib/also-ran/gcp-market-state.json"
DEFAULT_LOCK = "/run/also-ran/gcp-lock"
ALLOWED_STATUSES = {"PROVISIONING", "STAGING", "RUNNING"}
# In flight between states. The host cannot take work, but its name still
# exists in GCP, so it must keep holding its slot or the next create collides
# with it. These resolve on their own; the broker just waits.
TRANSITIONAL_STATUSES = {"STOPPING", "SUSPENDING", "REPAIRING"}
# Powered off and staying that way -- most often a reclaimed Spot host. It
# runs nothing, so deleting it cannot interrupt a job, and deleting is the
# only way its slot ever comes back.
RECLAIMABLE_STATUSES = {"TERMINATED", "SUSPENDED"}
MINIMUM_GITHUB_RATE_REMAINING = 750
# Receipt scans and identity-guarded deletes routinely take several minutes.
# A healthy maintenance loop must be able to accumulate its idle grace.
MAX_STALL_OBSERVATION_GAP = dt.timedelta(minutes=10)
# Hard ceiling on a drain request that never produces a receipt or an
# admission-closed acknowledgement. A guest whose Outrunner ignores SIGUSR1
# stays registered, idle and billing forever otherwise: general-08 sat
# drain-requested and idle for ~39 hours on 2026-10-01/02. The clock counts
# only continuously observed, GitHub-idle time, so a host still finishing its
# last job never ages toward it.
DRAIN_REQUEST_CEILING_MINUTES = 60
# Evidence kept from a host deleted at that ceiling. The serial console is the
# only guest record that survives the delete; keep a short, printable tail so
# the next occurrence is diagnosable without bloating the state file or journal.
EVIDENCE_TAIL_LINES = 60
EVIDENCE_LINE_CHARS = 200
EVIDENCE_TAIL_CHARS = 6000
# Per-host serial and guest-attribute reads are independent gcloud calls of
# ~1s and ~100 MB each. Read a few at once; GitHub verification stays serial
# because the shared budget lock serializes it across processes anyway.
RECEIPT_OBSERVATION_WORKERS = 4
# Drain-request metadata writes made at once (each a ~100 MB gcloud process,
# like the receipt reads). Each waits for its operation, 5-10s.
DRAIN_REQUEST_WORKERS = 4
# Per-run job listings read concurrently in one demand fetch.
DEMAND_JOB_READ_WORKERS = 4
# Every pool and loop reads the same GitHub runs and jobs and only filters them
# by label, so one fetch serves all of them. Through the shared ~1 req/s budget
# a fetch took 60-90s when four processes each made their own. A tick's opening
# read may reuse a snapshot this old; the read just before a scale-down
# decision uses the tighter bound. The fallback-delete gate always reads live.
DEMAND_SNAPSHOT_MAX_AGE = dt.timedelta(seconds=30)
DEMAND_REFRESH_MAX_AGE = dt.timedelta(seconds=15)
DEMAND_SNAPSHOT_VERSION = 1
# Under load one fetch outlasts both bounds above, so a process that waited for
# another's fetch would find it "stale" on arrival and fetch again, turning the
# shared lock into a queue of private fetches. A waiter therefore also accepts
# a snapshot whose fetch completed after it asked, provided that fetch started
# no longer than this before the request.
DEMAND_SNAPSHOT_MAX_FETCH_LEAD = dt.timedelta(minutes=5)
RECEIPT_PREFIX = "ALSO_RAN_DRAIN_RECEIPT_V1 "
ADMISSION_CLOSED_PREFIX = "ALSO_RAN_ADMISSION_CLOSED_V1 "
OUTRUNNER_RESULT_TO_GITHUB = {
    "succeeded": "success",
    "failed": "failure",
    "canceled": "cancelled",
}
POOL_CONFIG_KEYS = {"heavy": "production", "general": "general_production"}


class CountingJsonClient(JsonClient):
    """JsonClient that counts requests and the wall-clock spent in them.

    Time inside `get` includes waiting on the shared GitHub budget lock, so a
    phase whose GitHub time dominates is starved by the budget, not by GitHub.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.requests = 0
        self.request_ms = 0
        self._count_lock = threading.Lock()

    def get(self, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        started = time.monotonic()
        try:
            return super().get(path, query)
        finally:
            elapsed = int((time.monotonic() - started) * 1000)
            with self._count_lock:
                self.requests += 1
                self.request_ms += elapsed


class PhaseTimer:
    """Per-phase wall-clock and GitHub request counts for one broker tick."""

    def __init__(self, client: Any = None) -> None:
        self.client = client
        self.phases: dict[str, dict[str, int]] = {}

    @contextlib.contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        requests = getattr(self.client, "requests", 0)
        request_ms = getattr(self.client, "request_ms", 0)
        try:
            yield
        finally:
            entry = self.phases.setdefault(
                name, {"ms": 0, "github_requests": 0, "github_ms": 0}
            )
            entry["ms"] += int((time.monotonic() - started) * 1000)
            entry["github_requests"] += getattr(self.client, "requests", 0) - requests
            entry["github_ms"] += getattr(self.client, "request_ms", 0) - request_ms


class BrokerError(RuntimeError):
    """Raised when GCP reconciliation cannot safely continue."""


class MalformedSerialEvidence(ObservationError):
    """The serial record was interrupted or could not be decoded."""


@dataclasses.dataclass(frozen=True)
class Demand:
    queued: int
    active_owned: int
    rate_remaining: int | None
    # Slots GitHub shows running an owned job right now, and the count of
    # owned jobs whose runner name did not parse into a slot. The second is
    # what keeps the first honest: "no job on this slot" only means the host
    # is free if every owned job was attributable in the first place.
    active_slots: frozenset[int] = frozenset()
    unattributed_active: int = 0
    # The same counts split by repository, for the decision record only;
    # scheduling reads the pool-wide totals above.
    by_repository: dict[str, dict[str, int]] = dataclasses.field(
        default_factory=dict
    )
    # When the underlying GitHub fetch started, how old a reused shared
    # snapshot was when read (None for a fetch made by this call), and how
    # long this call waited for the snapshot lock.
    observed_at: dt.datetime | None = None
    snapshot_age_ms: int | None = None
    snapshot_wait_ms: int | None = None


@dataclasses.dataclass(frozen=True)
class Instance:
    slot: int
    instance_id: str
    name: str
    zone: str
    status: str
    labels: dict[str, str]
    metadata: dict[str, str] = dataclasses.field(default_factory=dict)


SHARED_REGIONAL_CAPACITY = {
    "regions": ["northamerica-northeast1", "europe-west9"],
    "reserved_vcpus": 8,
    "maximum_heavy_hosts": 12,
    "maximum_general_hosts": 12,
}


def shared_regional_capacity(policy: dict[str, Any]) -> dict[str, Any] | None:
    market = policy.get("market", {})
    if "shared_regional_capacity" not in market:
        return None
    sharing = market["shared_regional_capacity"]
    if sharing != SHARED_REGIONAL_CAPACITY:
        raise BrokerError("unexpected shared regional capacity boundary")
    return sharing


def regional_host_cap(policy: dict[str, Any], region: str, pool: str) -> int:
    sharing = shared_regional_capacity(policy)
    if sharing and region in sharing["regions"]:
        return int(sharing[f"maximum_{pool}_hosts"])
    config = next(item for item in policy["market"]["regions"] if item["name"] == region)
    cap = int(config["regional_maximum_hosts"])
    return cap if pool == "heavy" else min(
        cap, int(policy["market"]["general_secondary_region_maximum_hosts"])
    )


WARM_POOL_KEYS = {"timezone", "days", "start", "end", "minimum_hosts", "idle_minutes"}
WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
# The maintenance loop holds the warm floor this much longer than the fast
# loop, at the window's end and after the idle limit. The fast loop sees
# demand first and so stops holding the floor no earlier; without the margin a
# maintenance drain could land just before a fast-loop tick that still holds
# the floor and replaces the drained host.
WARM_POOL_RELEASE_MARGIN = dt.timedelta(minutes=2)


def clock_minutes(value: Any, *, allow_midnight_end: bool = False) -> int | None:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{2}:[0-9]{2}", value):
        return None
    hours, minutes = int(value[:2]), int(value[3:])
    if allow_midnight_end and (hours, minutes) == (24, 0):
        return 24 * 60
    if hours > 23 or minutes > 59:
        return None
    return hours * 60 + minutes


def valid_retention(config: dict[str, Any]) -> bool:
    """demand_only, or a peak-hours warm pool described exactly.

    `warm_pool` keeps `minimum_hosts` idle-or-busy hosts during a local-time
    window on the listed days, and only while the pool has seen work within
    `idle_minutes` (the window's start counts as work, so the pool warms up
    when the window opens). Outside it, the pool is demand_only and drains to
    zero.
    """
    mode = config.get("retention_mode")
    if mode == "demand_only":
        return "warm_pool" not in config
    if mode != "peak_warm_pool":
        return False
    warm = config.get("warm_pool")
    if not isinstance(warm, dict) or set(warm) != WARM_POOL_KEYS:
        return False
    try:
        ZoneInfo(str(warm["timezone"]))
    except (ZoneInfoNotFoundError, ValueError):
        return False
    days = warm["days"]
    start = clock_minutes(warm["start"])
    end = clock_minutes(warm["end"], allow_midnight_end=True)
    minimum = warm["minimum_hosts"]
    idle = warm["idle_minutes"]
    return (
        isinstance(days, list)
        and bool(days)
        and len(days) == len(set(days))
        and all(day in WEEKDAYS for day in days)
        and start is not None
        and end is not None
        and start < end
        and type(minimum) is int
        and 1 <= minimum <= int(config.get("maximum_hosts", 0))
        and type(idle) is int
        and 5 <= idle <= 240
    )


def valid_scale_down_hold(config: dict[str, Any]) -> bool:
    """`scale_down_hold_minutes` is a whole number of minutes, 0 to 30."""
    hold = config.get("scale_down_hold_minutes")
    return type(hold) is int and 0 <= hold <= 30


def demand_history(
    hold_minutes: int,
    now: dt.datetime,
    samples: list[tuple[dt.datetime | None, int]],
    *states: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Owned demand (queued + running) observed within the hold window.

    Merges this tick's samples with the history recorded in each loop's
    state. Samples older than the window, or later than `now`, are dropped:
    a future time would hold hosts indefinitely.
    """
    if hold_minutes <= 0:
        return []
    since = now - dt.timedelta(minutes=hold_minutes)
    merged: dict[str, int] = {}

    def add(at: dt.datetime | None, count: int) -> None:
        if at is None or not since <= at <= now or count < 0:
            return
        key = at.isoformat().replace("+00:00", "Z")
        merged[key] = max(merged.get(key, 0), count)

    for at, count in samples:
        add(at, count)
    for state in states:
        for entry in (state or {}).get("demand_history") or []:
            try:
                at = parse_utc_timestamp(entry.get("at"), "demand_history.at")
                count = int(entry.get("demand"))
            except (AttributeError, ObservationError, TypeError, ValueError):
                continue
            add(at, count)
    return [{"at": at, "demand": merged[at]} for at in sorted(merged)]


def state_demand_sample(
    state: dict[str, Any] | None,
) -> tuple[dt.datetime | None, int]:
    """A loop's last recorded observation as a demand sample, if readable."""
    try:
        at = parse_utc_timestamp((state or {}).get("observed_at"), "observed_at")
        count = int(state["queued_jobs"]) + int(state["active_owned_jobs"])
    except (KeyError, ObservationError, TypeError, ValueError):
        return None, 0
    return at, count


def scale_down_hold(history: list[dict[str, Any]]) -> int:
    """Hosts maintenance keeps for recent demand: the window's peak."""
    return max((int(entry["demand"]) for entry in history), default=0)


def warm_floor(
    production: dict[str, Any],
    now: dt.datetime,
    last_demand_at: dt.datetime | None,
) -> int:
    """The warm pool's host floor at `now`, or 0 when it is not held."""
    if production.get("retention_mode") != "peak_warm_pool":
        return 0
    warm = production["warm_pool"]
    local = now.astimezone(ZoneInfo(warm["timezone"]))
    if WEEKDAYS[local.weekday()] not in warm["days"]:
        return 0
    minute = local.hour * 60 + local.minute
    start = clock_minutes(warm["start"])
    end = clock_minutes(warm["end"], allow_midnight_end=True)
    if not start <= minute < end:
        return 0
    opened = local.replace(
        hour=start // 60, minute=start % 60, second=0, microsecond=0
    )
    active_since = max(opened, last_demand_at) if last_demand_at else opened
    if now - active_since >= dt.timedelta(minutes=warm["idle_minutes"]):
        return 0
    return min(int(warm["minimum_hosts"]), int(production["maximum_hosts"]))


def maintenance_warm_floor(
    production: dict[str, Any],
    now: dt.datetime,
    last_demand_at: dt.datetime | None,
) -> int:
    """The floor maintenance keeps: held WARM_POOL_RELEASE_MARGIN past release."""
    return max(
        warm_floor(production, now, last_demand_at),
        warm_floor(production, now - WARM_POOL_RELEASE_MARGIN, last_demand_at),
    )


def last_demand_seen(
    demand: Demand,
    observed_at: dt.datetime,
    now: dt.datetime,
    *states: dict[str, Any] | None,
) -> dt.datetime | None:
    """The newest time this observation or any loop's state saw owned work.

    A recorded time later than `now` is not evidence of anything and would
    hold the floor indefinitely, so it is ignored.
    """
    seen: list[dt.datetime] = []
    if demand.queued + demand.active_owned > 0:
        seen.append(observed_at)
    latest_credible = max(now, observed_at)
    for state in states:
        value = ((state or {}).get("warm_pool") or {}).get("last_demand_at")
        try:
            parsed = parse_utc_timestamp(value, "last_demand_at") if value else None
        except ObservationError:
            parsed = None
        if parsed is not None and parsed <= latest_credible:
            seen.append(parsed)
    return max(seen) if seen else None


def warm_pool_record(
    floor: int, last_demand_at: dt.datetime | None, decided_at: dt.datetime
) -> dict[str, Any]:
    return {
        "floor": floor,
        # When the floor was computed. Both loops often share one demand
        # snapshot, so `observed_at` cannot order two floors; this can.
        "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
        "last_demand_at": (
            last_demand_at.isoformat().replace("+00:00", "Z")
            if last_demand_at
            else None
        ),
    }


def validate_policy(policy: dict[str, Any], pool: str = "heavy", *, require_production_regions: bool = True) -> dict[str, Any]:
    if (
        policy.get("policy_version") != 1
        or policy.get("project_id") != PROJECT_ID
        or policy.get("project_number") != PROJECT_NUMBER
    ):
        raise BrokerError("unexpected GCP project boundary")
    shared_regional_capacity(policy)
    heavy = policy.get("production", {})
    general = policy.get("general_production", {})
    if (
        policy.get("machine_type") != "n2-standard-8"
        or heavy.get("name_prefix") != f"{RESOURCE_PREFIX}-runner-gcp-prod"
        or heavy.get("scale_set_prefix") != f"{RESOURCE_PREFIX}-gcp-prod"
        or heavy.get("workflow_label") != "outrunner-gcp-linux-x64"
        or heavy.get("maximum_hosts") != 28
        or heavy.get("minimum_hosts") != 0
        or not valid_retention(heavy)
        or not valid_scale_down_hold(heavy)
        or heavy.get("max_jobs_per_host") != 5
        or heavy.get("vcpus_per_host") != 8
        or heavy.get("autoscale_slot_prefix") != "gcp-prod"
        or heavy.get("machine_type") != "c4-standard-8-lssd"
        or heavy.get("boot_disk_gb") != 32
        or heavy.get("boot_disk_type") != "hyperdisk-balanced"
        or heavy.get("boot_disk_provisioned_iops") != 3000
        or heavy.get("boot_disk_provisioned_throughput") != 140
        or heavy.get("use_local_ssd") is not True
        or not valid_legacy_images(heavy)
        or not isinstance(heavy.get("quarantine_grace_minutes"), int)
        or not 5 <= heavy.get("quarantine_grace_minutes") <= 240
        or heavy.get("regional_placement_enabled") is not True
        or heavy.get("create_enabled") is not True
        or not isinstance(heavy.get("automatic_delete"), bool)
        or heavy.get("include_shared_label") is not False
    ):
        raise BrokerError("unexpected GCP heavy production policy")
    heavy_zone_counts = {
        "northamerica-northeast1-b": 10,
        "europe-west9-a": 10,
        "us-south1-a": 4,
        "us-south1-b": 4,
    }
    zone_cycle = heavy.get("zone_cycle", [])
    if (
        len(zone_cycle) != heavy["maximum_hosts"]
        or {zone: zone_cycle.count(zone) for zone in set(zone_cycle)}
        != heavy_zone_counts
    ):
        raise BrokerError("unexpected GCP heavy production regional placement")
    expected_general_labels = {
        "managed-by": "runner-autoscaler",
        "fleet": FLEET,
        "lifecycle": "general",
        "provider": "gcp",
        "hold": "true",
    }
    if (
        general.get("name_prefix") != f"{RESOURCE_PREFIX}-runner-gcp-general"
        or general.get("scale_set_prefix") != f"{RESOURCE_PREFIX}-gcp-general"
        or general.get("workflow_label") != "outrunner-gcp-general-linux-x64"
        or general.get("maximum_hosts") != 12
        or general.get("minimum_hosts") != 0
        or not valid_retention(general)
        or not valid_scale_down_hold(general)
        or general.get("max_jobs_per_host") != 10
        or general.get("vcpus_per_host") != 4
        or general.get("autoscale_slot_prefix") != "gcp-general"
        or general.get("machine_type") != "c4-standard-4-lssd"
        or general.get("boot_disk_gb") != 32
        or general.get("boot_disk_type") != "hyperdisk-balanced"
        or general.get("boot_disk_provisioned_iops") != 3000
        or general.get("boot_disk_provisioned_throughput") != 140
        or general.get("use_local_ssd") is not True
        or not valid_legacy_images(general)
        or not isinstance(general.get("quarantine_grace_minutes"), int)
        or not 5 <= general.get("quarantine_grace_minutes") <= 240
        or general.get("regional_placement_enabled") is not True
        or general.get("create_enabled") is not True
        or not isinstance(general.get("automatic_delete"), bool)
        or general.get("include_shared_label") is not False
        or general.get("labels") != expected_general_labels
    ):
        raise BrokerError("unexpected GCP general production policy")
    general_zone_counts = {
        "northamerica-northeast1-b": 4,
        "europe-west9-a": 4,
        "us-south1-a": 2,
        "us-south1-b": 2,
    }
    zone_cycle = general.get("zone_cycle", [])
    if (
        len(zone_cycle) != general["maximum_hosts"]
        or {zone: zone_cycle.count(zone) for zone in set(zone_cycle)}
        != general_zone_counts
    ):
        raise BrokerError("unexpected GCP general production regional placement")
    market_regions = {
        item.get("name"): item
        for item in policy.get("market", {}).get("regions", [])
        if isinstance(item, dict)
    }
    for region in ("northamerica-northeast1", "europe-west9", "us-south1"):
        config = market_regions.get(region, {})
        if (
            (require_production_regions and config.get("canary_valid") is not True)
            or not isinstance(config.get("zones"), list)
            or not isinstance(config.get("foundation", {}).get("subnet"), str)
        ):
            raise BrokerError("production region is not validated")
    try:
        return policy[POOL_CONFIG_KEYS[pool]]
    except KeyError as error:
        raise BrokerError(f"unexpected GCP pool: {pool!r}") from error


def region_from_zone(zone: str) -> str:
    if not re.fullmatch(r"[a-z0-9-]+-[a-z]", zone):
        raise BrokerError(f"invalid GCP zone: {zone!r}")
    return zone.rsplit("-", 1)[0]


def slot_from_runner_name(name: str, runner_prefix: str) -> int | None:
    """The slot a GitHub runner name belongs to, or None when it does not parse.

    Outrunner names a runner `<scale_set_prefix>-<slot>-<suffix>`. A name that
    does not match is not an error: it is an owned job we cannot place, and the
    caller has to read that as "a host is busy and we do not know which".
    """
    match = re.fullmatch(re.escape(runner_prefix) + r"(\d{2})-[0-9a-z]+", name)
    return int(match.group(1)) if match else None


def lowest_rate_remaining(responses: list[dict[str, str]]) -> int | None:
    """The allowance left after the last of several concurrent reads.

    Completion order is unknown, so take the lowest remaining count within the
    latest reset window; an older window's count is stale, not lower.
    """
    windows: list[tuple[int, int]] = []
    for headers in responses:
        remaining = headers.get("x-ratelimit-remaining")
        if remaining is None or remaining == "":
            continue
        reset = headers.get("x-ratelimit-reset")
        windows.append((int(reset) if reset else 0, int(remaining)))
    if not windows:
        return None
    latest = max(reset for reset, _ in windows)
    return min(remaining for reset, remaining in windows if reset == latest)


class GitHubDemandObserver:
    """Reads pool demand from every served repository's Actions queue.

    One pool (one workflow label) can serve several repositories. Demand is
    the sum over them; a job only counts where it carries the pool's label.
    Where the runners are registered is a separate fact, `runner_scope`:
    repository-scoped runners are listed per repository, organization-scoped
    ones only under the organization.

    `retired_repositories` were served once and no longer are. Their queues
    are not demand, but their running jobs, runners and receipts still are
    evidence that a host is busy, so a repository leaving the list can never
    open a delete on a host still running one of its jobs.
    `retired_organizations` likewise keep an organization's runner list read
    after the scope moves away from it.
    """

    def __init__(
        self,
        client: JsonClient,
        repositories: str | tuple[str, ...],
        *,
        runner_scope: str = "repository",
        retired_repositories: tuple[str, ...] = (),
        retired_organizations: tuple[str, ...] = (),
        max_pages: int = 10,
        max_active_runs: int = 100,
    ) -> None:
        self.client = client
        self.repositories = (
            (repositories,) if isinstance(repositories, str) else tuple(repositories)
        )
        if not self.repositories:
            raise BrokerError("GitHub demand observer needs a repository")
        if runner_scope not in ("repository", "organization"):
            raise BrokerError(f"unexpected GitHub runner scope: {runner_scope!r}")
        owners = {
            repository.split("/", 1)[0].lower() for repository in self.repositories
        }
        if runner_scope == "organization" and len(owners) != 1:
            raise BrokerError("organization runner scope needs a single owner")
        self.runner_scope = runner_scope
        self.retired_repositories = tuple(
            repository
            for repository in dict.fromkeys(retired_repositories)
            if repository not in self.repositories
        )
        self.retired_organizations = tuple(retired_organizations)
        self.max_pages = max_pages
        self.max_active_runs = max_active_runs

    @property
    def known_repositories(self) -> tuple[str, ...]:
        return self.repositories + self.retired_repositories

    @property
    def runner_organizations(self) -> tuple[str, ...]:
        """Organizations whose runner list is read, current scope first."""
        current = (
            (self.repositories[0].split("/", 1)[0],)
            if self.runner_scope == "organization"
            else ()
        )
        return tuple(
            dict.fromkeys(
                (*current, *self.retired_organizations),
            )
        )

    def runner_list_paths(self) -> tuple[str, ...]:
        """Every list a runner we own could be registered in.

        Repository lists are always read. Under organization scope the org
        list is read as well, not instead: hosts booted from an image that
        predates the move still hold repository-scoped runners, and reading
        only the org list would report them absent and open the fallback
        delete on a host that may be running a job.
        """
        paths = tuple(
            f"/repos/{repository}/actions/runners"
            for repository in self.known_repositories
        )
        organizations = tuple(
            f"/orgs/{owner}/actions/runners" for owner in self.runner_organizations
        )
        return organizations + paths

    def registered_runners(self) -> Iterator[dict[str, Any]]:
        for path in self.runner_list_paths():
            yield from self._pages(path, "runners", {})

    def created_filter(self, now: dt.datetime) -> str:
        return ">=" + (now - dt.timedelta(days=1)).date().isoformat()

    def active_jobs(self, now: dt.datetime) -> dict[str, Any]:
        """Fetch every active job of every known repository, unfiltered by pool.

        Scale-up waits on this read, so it uses the GitHub budget's priority
        slot, and each run's job listing is read concurrently.
        """
        with self._prioritized():
            return self._active_jobs(now)

    def _prioritized(self) -> contextlib.AbstractContextManager[None]:
        if isinstance(self.client, JsonClient):
            return self.client.prioritized()
        return contextlib.nullcontext()

    def _run_jobs(
        self, repository: str, run_id: int
    ) -> tuple[list[dict[str, Any]], dict[str, str]]:
        """One run's jobs, plus this worker thread's last response headers."""
        with self._prioritized():
            jobs = list(
                self._pages(
                    f"/repos/{repository}/actions/runs/{run_id}/jobs",
                    "jobs",
                    {"filter": "latest"},
                )
            )
            return jobs, dict(self.client.last_headers)

    def _active_jobs(self, now: dt.datetime) -> dict[str, Any]:
        created_filter = self.created_filter(now)
        repositories: dict[str, list[dict[str, Any]]] = {}
        # Job reads run on worker threads, and response headers are per thread.
        rate_headers: list[dict[str, str]] = []
        for repository in self.known_repositories:
            runs: dict[int, dict[str, Any]] = {}
            for status in ("queued", "in_progress"):
                for run in self._pages(
                    f"/repos/{repository}/actions/runs",
                    "workflow_runs",
                    {"status": status, "created": created_filter},
                ):
                    runs[int(run["id"])] = run
            # Per repository, so serving a second repository never pushes the
            # first over the cap it already met on its own.
            if len(runs) > self.max_active_runs:
                raise ObservationError(
                    f"GitHub returned {len(runs)} active runs for {repository}; "
                    f"safe cap is {self.max_active_runs}"
                )
            jobs: list[dict[str, Any]] = []
            with ThreadPoolExecutor(max_workers=DEMAND_JOB_READ_WORKERS) as executor:
                listings = list(
                    executor.map(self._run_jobs, [repository] * len(runs), runs)
                )
            for listing, headers in listings:
                rate_headers.append(headers)
                for job in listing:
                    jobs.append(
                        {
                            "labels": [str(item) for item in job.get("labels") or []],
                            "status": str(job.get("status") or ""),
                            "runner_name": str(job.get("runner_name") or ""),
                        }
                    )
            repositories[repository] = jobs
            rate_headers.append(dict(self.client.last_headers))
        remaining = lowest_rate_remaining(rate_headers)
        return {
            "created_filter": created_filter,
            "repositories": repositories,
            "rate_remaining": remaining,
        }

    def demand_from_jobs(
        self, active: dict[str, Any], label: str, runner_prefix: str
    ) -> Demand:
        queued = 0
        active_owned = 0
        active_slots: set[int] = set()
        unattributed_active = 0
        by_repository: dict[str, dict[str, int]] = {}
        for repository in self.known_repositories:
            served = repository in self.repositories
            repository_queued = 0
            repository_active = 0
            for job in active["repositories"][repository]:
                if label not in set(job["labels"]):
                    continue
                runner_name = job["runner_name"]
                if job["status"] == "queued":
                    # A retired repository's queue is not ours to serve.
                    repository_queued += 1 if served else 0
                elif job["status"] == "in_progress" and runner_name.startswith(
                    runner_prefix
                ):
                    repository_active += 1
                    slot = slot_from_runner_name(runner_name, runner_prefix)
                    if slot is None:
                        unattributed_active += 1
                    else:
                        active_slots.add(slot)
            queued += repository_queued
            active_owned += repository_active
            by_repository[repository] = {
                "queued": repository_queued,
                "active_owned": repository_active,
            }
        return Demand(
            queued=queued,
            active_owned=active_owned,
            rate_remaining=active["rate_remaining"],
            active_slots=frozenset(active_slots),
            unattributed_active=unattributed_active,
            by_repository=by_repository,
        )

    def observe(
        self,
        label: str,
        runner_prefix: str,
        now: dt.datetime,
        snapshot: "DemandSnapshotStore | None" = None,
        max_age: dt.timedelta = DEMAND_SNAPSHOT_MAX_AGE,
        newer_than: dt.datetime | None = None,
    ) -> Demand:
        if snapshot is None:
            observed_at = dt.datetime.now(dt.timezone.utc)
            active = self.active_jobs(now)
            age_ms = wait_ms = None
        else:
            active, observed_at, age_ms, wait_ms = snapshot.read_or_fetch(
                self, now, max_age, newer_than
            )
        return dataclasses.replace(
            self.demand_from_jobs(active, label, runner_prefix),
            observed_at=observed_at,
            snapshot_age_ms=age_ms,
            snapshot_wait_ms=wait_ms,
        )

    def _pages(
        self, path: str, key: str, query: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        for page in range(1, self.max_pages + 1):
            payload = self.client.get(path, {**query, "per_page": 100, "page": page})
            items = payload.get(key)
            if not isinstance(items, list):
                raise ObservationError(f"GitHub response for {path} omitted {key}")
            yield from items
            if len(items) < 100:
                return
        raise ObservationError(f"GitHub pagination exceeded {self.max_pages} pages")

    def runner_slots(
        self, runner_prefix: str
    ) -> tuple[frozenset[int], frozenset[int], int]:
        """Read every runner list once: registered slots, busy slots, unplaceable.

        One pass, so the registered and busy sets describe the same moment.
        A runner without a `busy` flag counts as busy.

        An offline, idle registration is not counted as registered: GitHub
        cannot give it a job. Since every host keeps a pre-registered spare
        (#457), a host that exits without a receipt leaves one behind until
        GitHub purges it, and it would otherwise block the drain-stall delete
        of the next host in that slot (#456). Missing status or busy fields
        still count, so this fails closed.
        """
        registered: set[int] = set()
        busy: set[int] = set()
        unattributed = 0
        for runner in self.registered_runners():
            name = str(runner.get("name") or "")
            if not name.startswith(runner_prefix):
                continue
            slot = slot_from_runner_name(name, runner_prefix)
            if slot is None:
                unattributed += 1
                continue
            if runner.get("busy") is not False:
                busy.add(slot)
            if runner.get("status") != "offline" or slot in busy:
                registered.add(slot)
        return frozenset(registered), frozenset(busy), unattributed

    def registered_slots(self, runner_prefix: str) -> tuple[frozenset[int], int]:
        """Return registered owned runner slots and unplaceable registrations."""
        registered, _busy, unattributed = self.runner_slots(runner_prefix)
        return registered, unattributed


class DemandSnapshotStore:
    """One GitHub active-jobs fetch shared by every pool and loop on the host.

    The lock is held through a fetch, so a process that finds the snapshot
    stale while another is refreshing it waits and then reuses the result
    instead of spending the shared GitHub budget on the same reads. Anything
    unreadable, malformed, from another repository set or too old is ignored
    and refetched; the snapshot never fails a tick on its own.
    """

    def __init__(self, path: str) -> None:
        self.path = pathlib.Path(path)
        self.lock_path = pathlib.Path(path + ".lock")

    def read_or_fetch(
        self,
        observer: GitHubDemandObserver,
        now: dt.datetime,
        max_age: dt.timedelta,
        newer_than: dt.datetime | None = None,
    ) -> tuple[dict[str, Any], dt.datetime, int | None, int | None]:
        """Return (active jobs, fetch start, reuse age ms, lock wait ms).

        `newer_than` names the data a refresh replaces. That data still
        satisfies the refresh while it is within `max_age`: the refresh exists
        to bound how old the data behind a scale-down is, and a short
        maintenance tick meets the bound without spending GitHub budget. Data
        older than `max_age` is reused only when another process fetched it
        while this one waited, and then only when it is strictly newer.
        """
        key = {
            "version": DEMAND_SNAPSHOT_VERSION,
            "repositories": list(observer.repositories),
            "retired_repositories": list(observer.retired_repositories),
            "created_filter": observer.created_filter(now),
        }
        requested_at = dt.datetime.now(dt.timezone.utc)
        handle = None
        try:
            handle = self.lock_path.open("a", encoding="utf-8")
            fcntl.flock(handle, fcntl.LOCK_EX)
        except OSError as error:
            if handle is not None:
                handle.close()
            # The snapshot only saves GitHub budget. Without its lock, read
            # live rather than stop every pool and loop on a cache problem.
            self._warn("lock", error)
            return (*self._fetch(observer, now), None)
        with handle:
            wait_ms = int(
                (dt.datetime.now(dt.timezone.utc) - requested_at).total_seconds() * 1000
            )
            cached = self._fresh(key, observer, max_age, requested_at, newer_than)
            if cached is not None:
                return (*cached, wait_ms)
            active, started, age_ms = self._fetch(observer, now)
            try:
                atomic_write_json(
                    str(self.path),
                    {
                        **key,
                        "observed_at": started.isoformat().replace("+00:00", "Z"),
                        "completed_at": dt.datetime.now(dt.timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "repositories_jobs": active["repositories"],
                        "rate_remaining": active["rate_remaining"],
                    },
                )
            except OSError as error:
                # The fetch succeeded; only sharing it failed.
                self._warn("write", error)
            return active, started, age_ms, wait_ms

    @staticmethod
    def _fetch(
        observer: GitHubDemandObserver, now: dt.datetime
    ) -> tuple[dict[str, Any], dt.datetime, None]:
        started = dt.datetime.now(dt.timezone.utc)
        return observer.active_jobs(now), started, None

    def _warn(self, operation: str, error: OSError) -> None:
        print(
            json.dumps(
                {
                    "controller_version": VERSION,
                    "warning": f"demand snapshot {operation} failed",
                    "path": str(self.path),
                    "error": str(error),
                }
            ),
            file=sys.stderr,
        )

    def _fresh(
        self,
        key: dict[str, Any],
        observer: GitHubDemandObserver,
        max_age: dt.timedelta,
        requested_at: dt.datetime,
        newer_than: dt.datetime | None = None,
    ) -> tuple[dict[str, Any], dt.datetime, int] | None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if any(payload.get(name) != value for name, value in key.items()):
                return None
            observed_at = parse_utc_timestamp(payload.get("observed_at"), "observed_at")
            completed_at = (
                parse_utc_timestamp(payload["completed_at"], "completed_at")
                if payload.get("completed_at") is not None
                else None
            )
            jobs = payload["repositories_jobs"]
            rate_remaining = payload["rate_remaining"]
        except (OSError, ValueError, KeyError, AttributeError, TypeError, ObservationError):
            return None
        current = dt.datetime.now(dt.timezone.utc)
        age = current - observed_at
        if age < dt.timedelta(0):
            return None
        if completed_at is not None and not observed_at <= completed_at <= current:
            return None
        # Fresh by age, or fetched by another process while this one waited.
        # Only the second can pass data older than max_age, so only it must
        # be newer than the data a refresh replaces.
        completed_while_waiting = (
            completed_at is not None
            and completed_at >= requested_at
            and requested_at - observed_at <= DEMAND_SNAPSHOT_MAX_FETCH_LEAD
            and (newer_than is None or observed_at > newer_than)
        )
        if age > max_age and not completed_while_waiting:
            return None
        if (
            not isinstance(jobs, dict)
            or set(jobs) != set(observer.known_repositories)
            or (rate_remaining is not None and type(rate_remaining) is not int)
            or any(
                not isinstance(items, list)
                or any(
                    not isinstance(job, dict)
                    or set(job) != {"labels", "status", "runner_name"}
                    or not isinstance(job["labels"], list)
                    or any(not isinstance(item, str) for item in job["labels"])
                    or not isinstance(job["status"], str)
                    or not isinstance(job["runner_name"], str)
                    for job in items
                )
                for items in jobs.values()
            )
        ):
            return None
        active = {
            "created_filter": key["created_filter"],
            "repositories": jobs,
            "rate_remaining": rate_remaining,
        }
        return active, observed_at, int(age.total_seconds() * 1000)


# journald splits any line over LineMax (48 KiB by default) into fragments that
# are no longer JSON. Every value echoed from a provider or subprocess into the
# per-tick decision line is bounded so the line stays whole.
MAX_ECHOED_TEXT = 2000


def bounded_text(text: str, limit: int = MAX_ECHOED_TEXT) -> str:
    """Trim provider/subprocess text echoed into the journal to `limit` chars."""
    text = str(text).strip()
    if len(text) <= limit:
        return text
    return text[:limit] + f"... [{len(text) - limit} more chars truncated]"


def operation_summary(payload: Any) -> Any:
    """The identifying fields of a gcloud mutation response.

    `instances add-metadata` returns the whole instance resource, including
    the Shielded VM UEFI certificate databases (~40 KB per host). Echoing it
    put ~500 KB drain ticks in the journal, split into non-JSON fragments.
    """
    items = payload if isinstance(payload, list) else [payload]
    summary = [
        {
            key: item[key]
            for key in ("name", "id", "status", "zone", "operationType")
            if key in item and isinstance(item[key], (str, int))
        }
        for item in items
        if isinstance(item, dict)
    ]
    return summary if isinstance(payload, list) else (summary[0] if summary else {})


def gcloud(arguments: list[str]) -> Any:
    command = ["gcloud", *arguments, "--quiet", "--format=json"]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired as error:
        raise ObservationError("GCP observation timed out") from error
    if result.returncode:
        raise ObservationError(
            f"gcloud failed ({' '.join(command[:5])}): {bounded_text(result.stderr)}"
        )
    return json.loads(result.stdout) if result.stdout.strip() else {}


def parse_utc_timestamp(value: Any, field: str) -> dt.datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ObservationError(f"drain receipt {field} must be a UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ObservationError(
            f"drain receipt {field} must be a UTC timestamp"
        ) from error
    if parsed.tzinfo != dt.timezone.utc:
        raise ObservationError(f"drain receipt {field} must be UTC")
    return parsed


def price_ordered_market_regions(
    policy: dict[str, Any], market_state: dict[str, Any], pool: str, now: dt.datetime
) -> list[dict[str, Any]]:
    """Return only fresh, approved markets, sorted by Catalog price."""
    production = validate_policy(policy, pool)
    market = policy.get("market", {})
    maximum_age = market.get("maximum_price_age_hours")
    if not isinstance(maximum_age, int) or maximum_age < 1:
        raise BrokerError("unexpected GCP market price age policy")
    if (
        not isinstance(market_state, dict)
        or market_state.get("mode") != "dry-run"
        or market_state.get("mutation_enabled") is not False
        or market_state.get("price_source") != "Cloud Billing Catalog"
    ):
        raise ObservationError("GCP market observation is unavailable")
    observed_at = parse_utc_timestamp(
        market_state.get("price_observed_at"), "market price_observed_at"
    )
    age = now - observed_at
    if age < dt.timedelta(0) or age > dt.timedelta(hours=maximum_age):
        raise ObservationError("GCP market price observation is stale")
    policy_regions = {
        item.get("name"): item
        for item in market.get("regions", [])
        if isinstance(item, dict)
    }
    observed = market_state.get("regions")
    if not isinstance(observed, list):
        raise ObservationError("GCP market observation omitted regions")
    observed_regions = {
        item.get("name"): item for item in observed if isinstance(item, dict)
    }
    if len(observed_regions) != len(observed):
        raise ObservationError("GCP market observation has duplicate regions")
    candidates = []
    for name in production["price_ordered_regions"]:
        config, state = policy_regions.get(name), observed_regions.get(name)
        if not isinstance(config, dict) or not isinstance(state, dict):
            raise ObservationError(f"GCP market observation omitted {name}")
        zones = state.get("supported_zones")
        price = state.get("price", {}).get("hourly_usd")
        # Quota changes between hourly observations and differs by pool shape.
        # Recheck transient exhaustion under the shared admission lock; keep
        # every foundation, canary, shape and price rejection authoritative.
        reasons = state.get("rejection_reasons")
        live_quota_recheck = (
            state.get("eligible") is False and reasons == ["quota_exhausted"]
        )
        if (
            config.get("canary_valid") is not True
            or (state.get("eligible") is not True and not live_quota_recheck)
            or not isinstance(zones, list)
            or not zones
            or any(zone not in config.get("zones", []) for zone in zones)
            or not isinstance(price, (int, float))
            or isinstance(price, bool)
            or price <= 0
            or price > config.get("maximum_hourly_usd", 0)
        ):
            continue
        candidates.append({"name": name, "zones": zones, "hourly_usd": float(price)})
    return sorted(candidates, key=lambda item: (item["hourly_usd"], item["name"]))


def validate_drain_receipt(
    receipt: Any,
    instance: Instance,
    policy: dict[str, Any],
    repositories: str | tuple[str, ...],
    production: dict[str, Any] | None = None,
) -> dict[str, Any]:
    production = production or validate_policy(policy)
    served = (
        (repositories,) if isinstance(repositories, str) else tuple(repositories)
    )
    if not isinstance(receipt, dict):
        raise ObservationError("drain receipt must be a JSON object")
    required_v1 = {
        "version",
        "status",
        "scale_set",
        "completed_jobs",
        "max_jobs",
        "drained_at",
        "identity",
        "jobs",
    }
    version = receipt.get("version")
    allowed_receipt_schemas = {
        1: {frozenset(required_v1)},
        2: {frozenset(required_v1 | {"reason"})},
        3: {frozenset(required_v1), frozenset(required_v1 | {"reason"})},
        4: {frozenset(required_v1 | {"reason"})},
    }
    if frozenset(receipt) not in allowed_receipt_schemas.get(version, set()):
        raise ObservationError("drain receipt has an unexpected schema")
    expected_scale_set = f"{production['scale_set_prefix']}-{instance.slot:02d}"
    jobs = receipt.get("jobs")
    completed_jobs = receipt.get("completed_jobs")
    max_jobs = receipt.get("max_jobs")
    if (
        not isinstance(receipt.get("version"), int)
        or isinstance(receipt.get("version"), bool)
        or version not in {1, 2, 3, 4}
        or receipt.get("status") != "drained"
        or receipt.get("scale_set") != expected_scale_set
        or not isinstance(completed_jobs, int)
        or isinstance(completed_jobs, bool)
        or completed_jobs < (1 if version in {1, 3} else 0)
        or not isinstance(max_jobs, int)
        or isinstance(max_jobs, bool)
        or max_jobs < 1
        or (
            version in {1, 3} and "reason" not in receipt and max_jobs != completed_jobs
        )
        or not isinstance(jobs, list)
        or len(jobs) != completed_jobs
    ):
        raise ObservationError("drain receipt completion fields are invalid")
    if version == 4:
        reason = receipt.get("reason")
        if reason not in {"external", "idle", "max_jobs"}:
            raise ObservationError("drain receipt reason is invalid")
        if reason == "max_jobs" and completed_jobs < max_jobs:
            raise ObservationError("maximum-job drain has not reached its threshold")
    elif "reason" in receipt and receipt.get("reason") != "external":
        raise ObservationError("drain receipt reason is invalid")
    drained_at = parse_utc_timestamp(receipt.get("drained_at"), "drained_at")

    identity = receipt.get("identity")
    identity_fields = {
        "autoscale_slot",
        "boot_id",
        "image_source_commit",
        "instance_id",
        "instance_name",
        "project_id",
        "provider",
        "zone",
    }
    if not isinstance(identity, dict) or set(identity) != identity_fields:
        raise ObservationError("drain receipt identity has an unexpected schema")
    expected_identity = {
        "autoscale_slot": (
            f"{production['autoscale_slot_prefix']}-{instance.slot:02d}"
        ),
        "image_source_commit": instance.labels.get("source-commit"),
        "instance_id": instance.instance_id,
        "instance_name": instance.name,
        "project_id": policy["project_id"],
        "provider": "gcp",
        "zone": instance.zone,
    }
    if any(identity.get(key) != value for key, value in expected_identity.items()):
        raise ObservationError(
            f"drain receipt identity does not match GCP slot {instance.slot}"
        )
    try:
        boot_id = uuid.UUID(str(identity.get("boot_id", "")))
    except ValueError as error:
        raise ObservationError("drain receipt boot_id is not a UUID") from error
    if str(boot_id) != identity["boot_id"]:
        raise ObservationError("drain receipt boot_id is not canonical")

    legacy_job_fields = {
        "runner_name",
        "runner_id",
        "runner_request_id",
        "job_id",
        "workflow_run_id",
        "repository",
        "result",
        "finished_at",
    }
    v3_job_fields = legacy_job_fields | {
        "job_workflow_ref",
        "job_display_name",
        "request_labels",
        "queue_time",
        "scale_set_assign_time",
        "runner_assign_time",
    }
    job_fields = v3_job_fields if version in {3, 4} else legacy_job_fields
    runner_names: set[str] = set()
    job_ids: set[str] = set()
    for job in jobs:
        if not isinstance(job, dict) or set(job) != job_fields:
            raise ObservationError("drain receipt job has an unexpected schema")
        runner_name = job.get("runner_name")
        job_id = job.get("job_id")
        runner_id = job.get("runner_id")
        request_id = job.get("runner_request_id")
        workflow_run_id = job.get("workflow_run_id")
        if (
            not isinstance(runner_name, str)
            or not runner_name.startswith(expected_scale_set + "-")
            or runner_name in runner_names
            or not isinstance(runner_id, int)
            or isinstance(runner_id, bool)
            or runner_id < 1
            or not isinstance(request_id, int)
            or isinstance(request_id, bool)
            or request_id < 0
            or not isinstance(job_id, str)
            or job_id in job_ids
            or not isinstance(workflow_run_id, int)
            or isinstance(workflow_run_id, bool)
            or workflow_run_id < 1
            or job.get("repository") not in served
            or job.get("result") not in OUTRUNNER_RESULT_TO_GITHUB
        ):
            raise ObservationError("drain receipt job identity is invalid")
        try:
            parsed_job_id = uuid.UUID(job_id)
        except ValueError as error:
            raise ObservationError("drain receipt job_id is not a UUID") from error
        if str(parsed_job_id) != job_id:
            raise ObservationError("drain receipt job_id is not canonical")
        finished_at = parse_utc_timestamp(job.get("finished_at"), "finished_at")
        if finished_at > drained_at:
            raise ObservationError("drain receipt job finished after drain")
        if version in {3, 4}:
            workflow_ref = job.get("job_workflow_ref")
            display_name = job.get("job_display_name")
            request_labels = job.get("request_labels")
            queue_time = parse_utc_timestamp(job.get("queue_time"), "queue_time")
            scale_set_assign_time = parse_utc_timestamp(
                job.get("scale_set_assign_time"), "scale_set_assign_time"
            )
            runner_assign_time = parse_utc_timestamp(
                job.get("runner_assign_time"), "runner_assign_time"
            )
            if (
                not isinstance(workflow_ref, str)
                or not workflow_ref.startswith(
                    job["repository"] + "/.github/workflows/"
                )
                or "@" not in workflow_ref
                or not isinstance(display_name, str)
                or not display_name.strip()
                or not isinstance(request_labels, list)
                or not request_labels
                or any(
                    not isinstance(label, str) or not label for label in request_labels
                )
                or len(request_labels) != len(set(request_labels))
                or production["workflow_label"] not in request_labels
                or not (
                    queue_time
                    <= scale_set_assign_time
                    <= runner_assign_time
                    <= drained_at
                )
                or (
                    job.get("result") != "canceled"
                    and not (runner_assign_time <= finished_at <= drained_at)
                )
            ):
                raise ObservationError("drain receipt v3 job identity is invalid")
        runner_names.add(runner_name)
        job_ids.add(job_id)
    return receipt


def serial_console_contents(instance: Instance, policy: dict[str, Any]) -> str:
    # The same resource `gcloud compute instances get-serial-port-output` reads,
    # without its ~1s process start; a full fleet's receipt scan was ~25s of
    # gcloud starts per maintenance tick.
    payload = compute_api(
        f"projects/{policy['project_id']}/zones/{instance.zone}/instances/"
        f"{instance.name}/serialPort",
        {"port": "1", "start": "0"},
    )
    if not isinstance(payload, dict) or not isinstance(payload.get("contents"), str):
        raise ObservationError("GCP serial output response is incomplete")
    return payload["contents"]


def evidence_not_ready(error: ObservationError) -> bool:
    """GCP has no serial console for this VM yet: it is still being created.

    A host in that state has never run Outrunner, so it can hold neither a
    receipt nor an admission-closed record. Treating the refused read as
    "no evidence yet" keeps it serving; quarantining it drained ~11% of the
    hosts that never ran a job (46 of 423, 2026-10-03/05) before they booted.
    """
    text = str(error)
    return "resourceNotReady" in text or "is not ready" in text


def serial_marker_json(
    contents: str, prefix: str, slot: int, evidence: str
) -> Any | None:
    encoded = set()
    for line in contents.replace("\r", "").splitlines():
        marker = line.find(prefix)
        if marker >= 0:
            encoded.add(line[marker + len(prefix) :].strip())
    if not encoded:
        return None
    if len(encoded) != 1:
        raise ObservationError(f"conflicting {evidence} for GCP slot {slot}")
    try:
        decoded = base64.b64decode(encoded.pop(), validate=True).decode("utf-8")
        return json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MalformedSerialEvidence(f"GCP serial {evidence} is malformed") from error


def guest_attributes(
    instance: Instance, policy: dict[str, Any], cache: dict[str, Any] | None = None
) -> list[dict[str, Any]] | None:
    """Share one optional transport read between both evidence parsers per VM."""
    if cache is not None and "payload" in cache:
        return cache["payload"]
    try:
        response = compute_api(
            f"projects/{policy['project_id']}/zones/{instance.zone}/instances/"
            f"{instance.name}/getGuestAttributes",
            {"queryPath": "also-ran/"},
        )
    except ObservationError:
        payload = None
    else:
        items = (
            (response.get("queryValue") or {}).get("items", [])
            if isinstance(response, dict)
            else None
        )
        if not isinstance(items, list):
            raise ObservationError("GCP guest attributes response is incomplete")
        payload = items
    if cache is not None:
        cache["payload"] = payload
    return payload


def guest_drain_receipt(
    instance: Instance,
    policy: dict[str, Any],
    repositories: str | tuple[str, ...],
    production: dict[str, Any],
    guest_cache: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Read the structured copy when the serial transport is interrupted.

    Guest attributes carry no additional authority: the same instance identity,
    schema and GitHub completion checks apply to both transports.
    """
    payload = guest_attributes(instance, policy, guest_cache)
    if payload is None:
        return None
    values = [item.get("value") for item in payload
              if isinstance(item, dict) and item.get("namespace") == "also-ran"
              and item.get("key") == "receipt"]
    if not values:
        return None
    if len(values) != 1 or not isinstance(values[0], str):
        raise ObservationError("GCP guest drain receipt is ambiguous")
    try:
        receipt = json.loads(base64.b64decode(values[0], validate=True).decode("utf-8"))
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ObservationError("GCP guest drain receipt is malformed") from error
    return validate_drain_receipt(receipt, instance, policy, repositories, production)


def serial_drain_receipt(
    instance: Instance,
    policy: dict[str, Any],
    repositories: str | tuple[str, ...],
    production: dict[str, Any] | None = None,
    contents: str | None = None,
    guest_cache: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    production = production or validate_policy(policy)
    try:
        receipt = serial_marker_json(
            contents if contents is not None else serial_console_contents(instance, policy),
            RECEIPT_PREFIX,
            instance.slot,
            "drain receipt",
        )
    except MalformedSerialEvidence as error:
        # Conflicting records and identity/schema failures never enter this
        # transport-only fallback.
        try:
            fallback = guest_drain_receipt(instance, policy, repositories, production, guest_cache)
        except ObservationError:
            raise error
        if fallback is None:
            raise error
        return fallback
    if receipt is None:
        return guest_drain_receipt(instance, policy, repositories, production, guest_cache)
    return validate_drain_receipt(
        receipt, instance, policy, repositories, production
    )


def validate_admission_closed(
    acknowledgement: Any,
    instance: Instance,
    policy: dict[str, Any],
    production: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(acknowledgement, dict) or set(acknowledgement) != {
        "version",
        "status",
        "scale_set",
        "closed_at",
        "identity",
    }:
        raise ObservationError("admission-closed acknowledgement has unexpected schema")
    if (
        acknowledgement.get("version") != 1
        or acknowledgement.get("status") != "admission_closed"
        or acknowledgement.get("scale_set")
        != f"{production['scale_set_prefix']}-{instance.slot:02d}"
    ):
        raise ObservationError("admission-closed acknowledgement is invalid")
    parse_utc_timestamp(acknowledgement.get("closed_at"), "closed_at")
    identity = acknowledgement.get("identity")
    expected_identity = {
        "autoscale_slot": f"{production['autoscale_slot_prefix']}-{instance.slot:02d}",
        "image_source_commit": instance.labels.get("source-commit"),
        "instance_id": instance.instance_id,
        "instance_name": instance.name,
        "project_id": policy["project_id"],
        "provider": "gcp",
        "zone": instance.zone,
    }
    if not isinstance(identity, dict) or set(identity) != set(expected_identity) | {
        "boot_id"
    }:
        raise ObservationError("admission-closed identity has unexpected schema")
    if any(identity.get(key) != value for key, value in expected_identity.items()):
        raise ObservationError(
            f"admission-closed identity does not match GCP slot {instance.slot}"
        )
    try:
        boot_id = uuid.UUID(str(identity.get("boot_id", "")))
    except ValueError as error:
        raise ObservationError("admission-closed boot_id is not a UUID") from error
    if str(boot_id) != identity["boot_id"]:
        raise ObservationError("admission-closed boot_id is not canonical")
    return acknowledgement


def guest_admission_closed(
    instance: Instance, policy: dict[str, Any], production: dict[str, Any],
    guest_cache: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    payload = guest_attributes(instance, policy, guest_cache)
    if payload is None:
        return None
    values = [item.get("value") for item in payload
              if isinstance(item, dict) and item.get("namespace") == "also-ran"
              and item.get("key") == "admission-closed"]
    if not values:
        return None
    if len(values) != 1 or not isinstance(values[0], str):
        raise ObservationError("GCP guest admission acknowledgement is ambiguous")
    try:
        acknowledgement = json.loads(
            base64.b64decode(values[0], validate=True).decode("utf-8")
        )
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ObservationError("GCP guest admission acknowledgement is malformed") from error
    return validate_admission_closed(acknowledgement, instance, policy, production)


def serial_admission_closed(
    contents: str,
    instance: Instance,
    policy: dict[str, Any],
    production: dict[str, Any],
    guest_cache: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    try:
        acknowledgement = serial_marker_json(
            contents,
            ADMISSION_CLOSED_PREFIX,
            instance.slot,
            "admission-closed acknowledgement",
        )
    except MalformedSerialEvidence as error:
        fallback = guest_admission_closed(instance, policy, production, guest_cache)
        if fallback is None:
            raise error
        return fallback
    if acknowledgement is None:
        return guest_admission_closed(instance, policy, production, guest_cache)
    return validate_admission_closed(acknowledgement, instance, policy, production)


def receipt_job_repository(
    recorded: dict[str, Any], observer: GitHubDemandObserver
) -> str:
    """The served repository a receipt job ran in, or a fail-closed error."""
    repository = recorded.get("repository")
    if repository not in observer.known_repositories:
        raise ObservationError("drain receipt job repository is not served")
    return str(repository)


def verify_receipt_jobs(
    receipt: dict[str, Any],
    observer: GitHubDemandObserver,
    required_label: str,
) -> None:
    drained_at = parse_utc_timestamp(receipt["drained_at"], "drained_at")
    # Reruns must not erase proof for an older, uniquely named ephemeral runner.
    # GitHub filter=all includes old executions; admission still uses latest.
    run_jobs: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for recorded in receipt["jobs"]:
        run_key = (receipt_job_repository(recorded, observer), recorded["workflow_run_id"])
        if run_key not in run_jobs:
            run_jobs[run_key] = list(
                observer._pages(
                    f"/repos/{receipt_job_repository(recorded, observer)}"
                    f"/actions/runs/{recorded['workflow_run_id']}/jobs",
                    "jobs",
                    {"filter": "all"},
                )
            )
        jobs = run_jobs[run_key]
        matches = [
            job for job in jobs if job.get("runner_name") == recorded["runner_name"]
        ]
        expected_conclusion = OUTRUNNER_RESULT_TO_GITHUB[recorded["result"]]
        if not matches:
            verify_requeued_canceled_job(
                receipt,
                recorded,
                jobs,
                observer,
                required_label,
                drained_at,
            )
            continue
        if len(matches) > 1:
            # A failed-jobs rerun copies successful executions into its attempt
            # with new job IDs. Accept only distinct attempts whose physical
            # execution facts agree; contradictory matches remain quarantined.
            attempts = [job.get("run_attempt") for job in matches]
            ids = [job.get("id") for job in matches]
            fields = ("runner_id", "run_id", "name", "status", "conclusion",
                      "started_at", "completed_at", "labels")
            if (any(type(value) is not int or value < 1 for value in attempts + ids)
                    or len(set(attempts)) != len(matches)
                    or len(set(ids)) != len(matches)
                    or not isinstance(matches[0].get("name"), str)
                    or not matches[0]["name"]
                    or any(any(job.get(field) != matches[0].get(field) for field in fields)
                           for job in matches[1:])):
                raise ObservationError(
                    "drain receipt runner has multiple matches in GitHub"
                )
            if parse_utc_timestamp(matches[0].get("started_at"), "started_at") > parse_utc_timestamp(matches[0].get("completed_at"), "completed_at"):
                raise ObservationError("replayed execution timestamps are invalid")
        job = matches[0]
        completed_at = parse_utc_timestamp(job.get("completed_at"), "completed_at")
        if (
            job.get("status") != "completed"
            or job.get("conclusion") != expected_conclusion
            or job.get("runner_id") != recorded["runner_id"]
            or job.get("run_id") != recorded["workflow_run_id"]
            or required_label not in set(job.get("labels") or [])
            or completed_at > drained_at
        ):
            raise ObservationError("drain receipt does not match completed GitHub job")


def verify_requeued_canceled_job(
    receipt: dict[str, Any],
    recorded: dict[str, Any],
    jobs: list[dict[str, Any]],
    observer: GitHubDemandObserver,
    required_label: str,
    drained_at: dt.datetime,
) -> None:
    if receipt.get("version") not in {3, 4} or recorded.get("result") != "canceled":
        raise ObservationError("drain receipt runner was not found in GitHub")

    registered = [
        runner
        for runner in observer.registered_runners()
        if runner.get("id") == recorded["runner_id"]
        or runner.get("name") == recorded["runner_name"]
    ]
    if registered:
        raise ObservationError(
            "canceled drain receipt runner remains registered in GitHub"
        )

    replacements = [
        job
        for job in jobs
        if job.get("name") == recorded["job_display_name"]
        and required_label in set(job.get("labels") or [])
    ]
    if len(replacements) != 1:
        raise ObservationError(
            "canceled drain receipt replacement job is not unique in GitHub"
        )
    replacement = replacements[0]
    started_at = parse_utc_timestamp(replacement.get("started_at"), "started_at")
    completed_at = parse_utc_timestamp(replacement.get("completed_at"), "completed_at")
    terminal_conclusions = {"success", "failure", "cancelled", "timed_out"}
    if (
        replacement.get("status") != "completed"
        or replacement.get("conclusion") not in terminal_conclusions
        or replacement.get("run_id") != recorded["workflow_run_id"]
        or not isinstance(replacement.get("runner_id"), int)
        or isinstance(replacement.get("runner_id"), bool)
        or replacement["runner_id"] < 1
        or not isinstance(replacement.get("runner_name"), str)
        or not replacement["runner_name"]
        or replacement["runner_id"] == recorded["runner_id"]
        or replacement["runner_name"] == recorded["runner_name"]
        or started_at < drained_at
        or completed_at < started_at
    ):
        raise ObservationError(
            "canceled drain receipt does not match terminal replacement job"
        )

    run = observer.client.get(
        f"/repos/{receipt_job_repository(recorded, observer)}/actions/runs/"
        f"{recorded['workflow_run_id']}",
        {},
    )
    run_created_at = parse_utc_timestamp(run.get("created_at"), "created_at")
    run_updated_at = parse_utc_timestamp(run.get("updated_at"), "updated_at")
    queue_time = parse_utc_timestamp(recorded.get("queue_time"), "queue_time")
    if (
        run.get("id") != recorded["workflow_run_id"]
        or run.get("status") != "completed"
        or run.get("conclusion") not in terminal_conclusions
        or run_created_at > queue_time
        or run_updated_at < completed_at
    ):
        raise ObservationError(
            "canceled drain receipt workflow run is not terminal in GitHub"
        )


@dataclasses.dataclass
class HostEvidence:
    """What the provider side says about one host, before any GitHub check."""

    slot: int
    receipt: dict[str, Any] | None = None
    quarantine_reason: str | None = None
    admission_closed: bool = False


def observe_host_evidence(
    item: Instance,
    policy: dict[str, Any],
    production: dict[str, Any],
    repositories: tuple[str, ...],
) -> HostEvidence:
    """Read and locally validate one host's serial/guest drain evidence.

    Only gcloud calls and local validation happen here, so hosts can be read
    concurrently. A returned receipt still needs GitHub verification.
    """
    guest_cache: dict[str, Any] = {}
    serial_error = None
    serial_not_ready = False
    quarantine_reason = None
    try:
        contents = serial_console_contents(item, policy)
    except ObservationError as error:
        serial_error = str(error)
        serial_not_ready = evidence_not_ready(error)
        contents = ""
    try:
        if serial_error:
            receipt = guest_drain_receipt(
                item, policy, repositories, production, guest_cache
            )
            if receipt is None and not serial_not_ready:
                quarantine_reason = serial_error
        else:
            receipt = serial_drain_receipt(
                item, policy, repositories, production,
                contents=contents, guest_cache=guest_cache,
            )
    except ObservationError as error:
        # Fail closed for this exact slot without freezing unrelated fleet
        # creates. A quarantined VM is neither deleted nor counted as
        # serving capacity until its receipt can be verified later.
        try:
            acknowledgement = serial_admission_closed(
                contents, item, policy, production, guest_cache
            )
        except ObservationError:
            acknowledgement = None
        return HostEvidence(
            item.slot,
            quarantine_reason=str(error),
            admission_closed=acknowledgement is not None,
        )
    if receipt is None:
        try:
            acknowledgement = serial_admission_closed(
                contents, item, policy, production, guest_cache
            )
        except ObservationError as error:
            return HostEvidence(item.slot, quarantine_reason=str(error))
        if acknowledgement is None:
            return HostEvidence(item.slot, quarantine_reason=quarantine_reason)
        return HostEvidence(
            item.slot,
            quarantine_reason="admission closed without a usable drain receipt",
            admission_closed=True,
        )
    return HostEvidence(item.slot, receipt=receipt)


def observe_drained_instances(
    instances: tuple[Instance, ...],
    policy: dict[str, Any],
    production: dict[str, Any],
    observer: GitHubDemandObserver,
    timer: PhaseTimer | None = None,
) -> tuple[dict[int, dict[str, Any]], dict[int, str], set[int], set[int]]:
    drained: dict[int, dict[str, Any]] = {}
    quarantined: dict[int, str] = {}
    # Slots whose receipt parsed and validated locally, so the host has
    # provably stopped serving even though GitHub could not confirm it.
    # Only these are eligible for quarantine expiry; a slot we could not
    # read at all may still be running a job and must stay untouched.
    idle_confirmed: set[int] = set()
    admission_closed: set[int] = set()
    timer = timer or PhaseTimer()
    repositories = observer.known_repositories
    with timer.phase("receipts_gcp"):
        if instances:
            with ThreadPoolExecutor(
                max_workers=min(RECEIPT_OBSERVATION_WORKERS, len(instances))
            ) as executor:
                evidence = list(
                    executor.map(
                        lambda item: observe_host_evidence(
                            item, policy, production, repositories
                        ),
                        instances,
                    )
                )
        else:
            evidence = []
    with timer.phase("receipts_github"):
        for host in evidence:
            if host.quarantine_reason is not None:
                quarantined[host.slot] = host.quarantine_reason
            if host.admission_closed:
                admission_closed.add(host.slot)
            if host.receipt is None:
                continue
            try:
                verify_receipt_jobs(host.receipt, observer, production["workflow_label"])
                drained[host.slot] = host.receipt
            except ObservationError as error:
                quarantined[host.slot] = str(error)
                idle_confirmed.add(host.slot)
    return drained, quarantined, idle_confirmed, admission_closed


def observe_instances(
    policy: dict[str, Any], pool: str = "heavy"
) -> tuple[Instance, ...]:
    production = validate_policy(policy, pool)
    payload = compute_aggregated_list(
        policy["project_id"],
        "instances",
        f'labels.managed-by = "runner-autoscaler" AND labels.fleet = "{FLEET}" '
        f'AND labels.lifecycle = "{production["labels"]["lifecycle"]}" '
        'AND labels.provider = "gcp"',
    )
    if not isinstance(payload, list):
        raise ObservationError("GCP instance list was not an array")

    result: list[Instance] = []
    prefix = production["name_prefix"] + "-"
    expected_common = production["labels"]
    for raw in payload:
        name = str(raw.get("name", ""))
        match = re.fullmatch(re.escape(prefix) + r"([0-9]{2})", name)
        if not match:
            raise ObservationError(f"unknown owned GCP instance name: {name}")
        slot = int(match.group(1))
        if slot < 1 or slot > production["maximum_hosts"]:
            raise ObservationError(f"GCP instance slot is outside policy: {slot}")
        labels = {str(k): str(v) for k, v in (raw.get("labels") or {}).items()}
        metadata = {
            str(item.get("key")): str(item.get("value"))
            for item in (raw.get("metadata") or {}).get("items", [])
            if isinstance(item, dict) and item.get("key") is not None
        }
        expected = {
            **expected_common,
            "autoscale-slot": (f"{production['autoscale_slot_prefix']}-{slot:02d}"),
        }
        if any(labels.get(key) != value for key, value in expected.items()):
            raise ObservationError(f"GCP instance labels do not match slot {slot}")
        allowed_source_commits = {
            policy["image_source_commit"][:12],
            *(commit[:12] for commit in production["legacy_image_source_commits"]),
        }
        if labels.get("source-commit") not in allowed_source_commits:
            raise ObservationError(f"GCP instance labels do not match slot {slot}")
        status = str(raw.get("status", ""))
        if status not in (
            ALLOWED_STATUSES | TRANSITIONAL_STATUSES | RECLAIMABLE_STATUSES
        ):
            raise ObservationError(f"GCP slot {slot} has unsafe status {status!r}")
        instance_id = str(raw.get("id", ""))
        if not instance_id.isdigit():
            raise ObservationError(f"GCP slot {slot} has no numeric instance ID")
        result.append(
            Instance(
                slot=slot,
                instance_id=instance_id,
                name=name,
                zone=str(raw.get("zone", "")).rsplit("/", 1)[-1],
                status=status,
                labels=labels,
                metadata=metadata,
            )
        )
    slots = [item.slot for item in result]
    if len(slots) != len(set(slots)):
        raise ObservationError("duplicate GCP production slot")
    return tuple(sorted(result, key=lambda item: item.slot))


def egress_mode(policy: dict[str, Any]) -> str:
    mode = policy.get("network", {}).get("egress", {}).get("mode")
    if mode not in {"nat", "direct"}:
        raise BrokerError("unexpected GCP egress boundary")
    return mode


class _ReadCache:
    """Each keyed read runs once; concurrent callers wait for that one read."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, ...], Future] = {}

    def get(self, key: tuple[str, ...], fetch: Callable[[], Any]) -> Any:
        with self._lock:
            entry = self._entries.get(key)
            owner = entry is None
            if owner:
                entry = self._entries[key] = Future()
        if owner:
            try:
                entry.set_result(fetch())
            except BaseException as error:
                entry.set_exception(error)
        return entry.result()


_ADMISSION_READS: contextvars.ContextVar[_ReadCache | None] = contextvars.ContextVar(
    "admission_reads", default=None
)


@contextlib.contextmanager
def shared_admission_reads() -> Iterator[None]:
    """Read project-wide quota inputs once per admission, not once per region.

    Scoped to one admission under the admission lock, so a cached read is never
    older than the decision it informs.
    """
    token = _ADMISSION_READS.set(_ReadCache())
    try:
        yield
    finally:
        _ADMISSION_READS.reset(token)


def admission_read(key: tuple[str, ...], fetch: Callable[[], Any]) -> Any:
    cache = _ADMISSION_READS.get()
    return fetch() if cache is None else cache.get(key, fetch)


def quota_capacity(
    quotas: list[dict[str, Any]], metric: str, units_per_host: int
) -> int | None:
    quota = next((item for item in quotas if item.get("metric") == metric), None)
    if quota is None:
        return None
    try:
        available = float(quota["limit"]) - float(quota.get("usage", 0))
    except (KeyError, TypeError, ValueError) as error:
        raise ObservationError(f"GCP quota {metric} is malformed") from error
    return max(0, int(available // units_per_host))


# `gcloud auth print-access-token` starts a ~0.6s Python process. Its token
# lives about an hour and gcloud refreshes it well before expiry, so reuse it
# for a few minutes; a 401 drops it early.
GCP_TOKEN_REUSE_SECONDS = 180.0
GCP_API_TIMEOUT_SECONDS = 30
COMPUTE_API = "https://compute.googleapis.com/compute/v1"
_GCP_TOKEN_LOCK = threading.Lock()
_gcp_token: tuple[str, float] | None = None


def gcp_access_token(*, renew: bool = False) -> str:
    """The controller's own gcloud credential, as a bearer token."""
    global _gcp_token
    with _GCP_TOKEN_LOCK:
        if renew or _gcp_token is None or time.monotonic() >= _gcp_token[1]:
            try:
                result = subprocess.run(
                    ["gcloud", "auth", "print-access-token"],
                    capture_output=True,
                    text=True,
                    timeout=60,
                )
            except subprocess.TimeoutExpired as error:
                raise ObservationError("could not obtain a GCP access token") from error
            if result.returncode or not result.stdout.strip():
                raise ObservationError("could not obtain a GCP access token")
            _gcp_token = (
                result.stdout.strip(),
                time.monotonic() + GCP_TOKEN_REUSE_SECONDS,
            )
        return _gcp_token[0]


def gcp_api(url: str, query: dict[str, str] | None = None) -> Any:
    """GET a Google Cloud REST resource with the controller's credential.

    The broker's reads used to be `gcloud ... describe/list` subprocesses at
    1-2.7s each, most of it process start. The same reads over REST take
    0.1-1s and return the same resource JSON.
    """
    if query:
        url += "?" + urllib.parse.urlencode(query)
    for attempt in range(2):
        request = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {gcp_access_token(renew=attempt > 0)}"},
        )
        try:
            with urllib.request.urlopen(
                request, timeout=GCP_API_TIMEOUT_SECONDS
            ) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 401 and attempt == 0:
                continue
            detail = bounded_text(error.read().decode("utf-8", "replace"))
            raise ObservationError(
                f"GCP API {urllib.parse.urlsplit(url).path} failed: "
                f"HTTP {error.code} {detail}"
            ) from error
        except (OSError, ValueError) as error:
            raise ObservationError(
                f"GCP API {urllib.parse.urlsplit(url).path} failed: "
                f"{bounded_text(str(error) or type(error).__name__)}"
            ) from error
    raise AssertionError("unreachable")


def compute_api(path: str, query: dict[str, str] | None = None) -> Any:
    return gcp_api(f"{COMPUTE_API}/{path}", query)


def compute_aggregated_list(
    project: str, resource: str, filter_expression: str | None = None
) -> list[dict[str, Any]]:
    """Every zone's `resource` in one list, as `gcloud compute <resource> list`.

    Like gcloud, an unreachable zone fails the read instead of silently
    dropping that zone's hosts from the inventory.
    """
    items: list[dict[str, Any]] = []
    query: dict[str, str] = {}
    if filter_expression:
        query["filter"] = filter_expression
    while True:
        page = compute_api(f"projects/{project}/aggregated/{resource}", query)
        if not isinstance(page, dict):
            raise ObservationError(f"GCP {resource} list was not an object")
        if page.get("unreachables"):
            raise ObservationError(f"GCP {resource} list has unreachable scopes")
        for scope, scoped in (page.get("items") or {}).items():
            if (scoped.get("warning") or {}).get("code") == "UNREACHABLE":
                raise ObservationError(f"GCP {resource} list could not reach {scope}")
            items.extend(scoped.get(resource) or [])
        token = page.get("nextPageToken")
        if not token:
            return items
        query["pageToken"] = token


# A Cloud Quotas value is a limit: it moves only when someone edits the quota,
# unlike usage, which every create and delete moves and which is always read
# live. The long-running reconciler reuses a limit for this long, so scale-up
# does not re-read the same two limits per region on every create. A failed
# create forgets them, so the next admission re-reads them.
QUOTA_LIMIT_REUSE_SECONDS = 600.0
_QUOTA_LIMITS_LOCK = threading.Lock()
_quota_limits: dict[tuple[Any, ...], tuple[int, float]] = {}


def forget_quota_limits() -> None:
    with _QUOTA_LIMITS_LOCK:
        _quota_limits.clear()


def cloud_quota_value(
    policy: dict[str, Any], quota_id: str, dimensions: dict[str, str]
) -> int:
    key = (str(policy["project_number"]), quota_id, tuple(sorted(dimensions.items())))
    with _QUOTA_LIMITS_LOCK:
        cached = _quota_limits.get(key)
    if cached is not None and time.monotonic() < cached[1]:
        return cached[0]
    value = read_cloud_quota_value(policy, quota_id, dimensions)
    with _QUOTA_LIMITS_LOCK:
        _quota_limits[key] = (value, time.monotonic() + QUOTA_LIMIT_REUSE_SECONDS)
    return value


def read_cloud_quota_value(
    policy: dict[str, Any], quota_id: str, dimensions: dict[str, str]
) -> int:
    project_number = policy["project_number"]
    endpoint = (
        "https://cloudquotas.googleapis.com/v1/projects/"
        f"{project_number}/locations/global/services/compute.googleapis.com/"
        f"quotaInfos/{quota_id}"
    )
    try:
        quota_info = gcp_api(endpoint)
    except ObservationError as error:
        raise ObservationError(f"could not read GCP quota {quota_id}") from error
    infos = quota_info.get("dimensionsInfos", [])
    exact = next((item for item in infos if item.get("dimensions") == dimensions), None)
    if exact is None:
        # A limit nobody has raised has no per-region entry: it is a default
        # keyed by the other dimensions and applied to a list of regions. C4
        # local SSD is one (6000 GB, keyed only by vm_family).
        region = dimensions.get("region")
        rest = {key: value for key, value in dimensions.items() if key != "region"}
        exact = next(
            (
                item
                for item in infos
                if (item.get("dimensions") or {}) == rest
                and region in (item.get("applicableLocations") or [])
                and "value" in (item.get("details") or {})
            ),
            None,
        )
    if exact is not None:
        try:
            return int(exact["details"]["value"])
        except (KeyError, TypeError, ValueError) as error:
            raise ObservationError(f"GCP quota {quota_id} is malformed") from error
    raise ObservationError(f"GCP quota {quota_id} is missing")


def c4_family_quota_vcpus(policy: dict[str, Any], region: str) -> int:
    return cloud_quota_value(
        policy,
        "CPUS-PER-VM-FAMILY-per-project-region",
        {"region": region, "vm_family": "C4"},
    )


def project_info(project: str) -> Any:
    return admission_read(
        ("project-info", project), lambda: compute_api(f"projects/{project}")
    )


def project_instances(project: str) -> Any:
    return admission_read(
        ("instances", project),
        lambda: compute_aggregated_list(project, "instances"),
    )


def project_disks(project: str) -> Any:
    # The type is checked per disk by the caller, so list them all.
    return admission_read(
        ("disks", project), lambda: compute_aggregated_list(project, "disks")
    )


def c4_family_usage_vcpus(policy: dict[str, Any], region: str) -> int:
    payload = project_instances(policy["project_id"])
    if not isinstance(payload, list):
        raise ObservationError("GCP instance list was not an array")
    usage = 0
    for instance in payload:
        status = str(instance.get("status", ""))
        if status == "TERMINATED":
            continue
        zone = str(instance.get("zone", "")).rsplit("/", 1)[-1]
        if not zone or region_from_zone(zone) != region:
            continue
        machine_type = str(instance.get("machineType", "")).rsplit("/", 1)[-1]
        if not machine_type.startswith("c4-"):
            continue
        match = re.fullmatch(r"c4-[a-z0-9]+-([0-9]+)(?:-lssd)?", machine_type)
        if match is None:
            raise ObservationError(
                f"unrecognized C4 machine type in {region}: {machine_type!r}"
            )
        usage += int(match.group(1))
    return usage


# Local SSD bundled with each validated `-lssd` shape, in GB. GCP charges it
# against LOCAL-SSD-TOTAL-GB-PER-VM-FAMILY, a limit independent of vCPUs:
# at the 6000 GB default a C4 region holds 16 hosts of either pool. It must
# change with the machine types `validate_policy` pins for the two pools.
LOCAL_SSD_GB_PER_HOST = {"c4-standard-4-lssd": 375, "c4-standard-8-lssd": 375}


def local_ssd_gb_per_host(machine_type: str) -> int:
    try:
        return LOCAL_SSD_GB_PER_HOST[machine_type]
    except KeyError as error:
        raise ObservationError(
            f"no known local SSD size for {machine_type!r}"
        ) from error


def local_ssd_quota_gb(policy: dict[str, Any], region: str) -> int:
    return cloud_quota_value(
        policy,
        "LOCAL-SSD-TOTAL-GB-PER-VM-FAMILY-per-project-region",
        {"region": region, "vm_family": "C4"},
    )


def local_ssd_usage_gb(policy: dict[str, Any], region: str) -> int:
    payload = project_instances(policy["project_id"])
    if not isinstance(payload, list):
        raise ObservationError("GCP instance list was not an array")
    usage = 0
    for instance in payload:
        if str(instance.get("status", "")) == "TERMINATED":
            continue
        zone = str(instance.get("zone", "")).rsplit("/", 1)[-1]
        if not zone or region_from_zone(zone) != region:
            continue
        machine_type = str(instance.get("machineType", "")).rsplit("/", 1)[-1]
        if not machine_type.startswith("c4-"):
            continue
        for disk in instance.get("disks") or []:
            if disk.get("type") != "SCRATCH":
                continue
            try:
                usage += int(disk["diskSizeGb"])
            except (KeyError, TypeError, ValueError) as error:
                raise ObservationError("GCP local SSD size is malformed") from error
    return usage


def local_ssd_capacity(
    policy: dict[str, Any], region: str, machine_type: str
) -> tuple[int | None, int, int]:
    """Hosts the C4 local SSD quota still admits, with its limit and usage.

    A negative limit is GCP's "unlimited", which admits any count (None).
    """
    limit = local_ssd_quota_gb(policy, region)
    usage = local_ssd_usage_gb(policy, region)
    if limit < 0:
        return None, limit, usage
    return max(0, (limit - usage) // local_ssd_gb_per_host(machine_type)), limit, usage


def hyperdisk_quota_gb(policy: dict[str, Any], region: str) -> int:
    return cloud_quota_value(
        policy,
        "HDB-TOTAL-GB-per-project-region",
        {"region": region},
    )


def hyperdisk_usage_gb(policy: dict[str, Any], region: str) -> int:
    payload = project_disks(policy["project_id"])
    if not isinstance(payload, list):
        raise ObservationError("GCP disk list was not an array")
    usage = 0
    for disk in payload:
        zone = str(disk.get("zone", "")).rsplit("/", 1)[-1]
        disk_region = zone.rsplit("-", 1)[0] if zone else ""
        disk_type = str(disk.get("type", "")).rsplit("/", 1)[-1]
        if disk_region != region or disk_type != "hyperdisk-balanced":
            continue
        try:
            size_gb = int(disk["sizeGb"])
        except (KeyError, TypeError, ValueError) as error:
            raise ObservationError("GCP Hyperdisk size is malformed") from error
        if size_gb < 1:
            raise ObservationError("GCP Hyperdisk size is malformed")
        usage += size_gb
    return usage


def observe_create_capacity(
    policy: dict[str, Any],
    *,
    pool: str = "heavy",
    region: str | None = None,
    existing_hosts: int = 0,
) -> dict[str, Any]:
    production = validate_policy(policy, pool)
    if region is None:
        regions = {region_from_zone(zone) for zone in production["zone_cycle"]}
        if len(regions) != 1:
            raise ObservationError("region is required for multi-region production")
        region = regions.pop()
    project = policy["project_id"]
    region_info = compute_api(f"projects/{project}/regions/{region}")
    project_quotas = project_info(project)
    global_quotas = project_quotas.get("quotas")
    regional_quotas = region_info.get("quotas")
    if not isinstance(global_quotas, list) or not isinstance(regional_quotas, list):
        raise ObservationError("GCP quota response is incomplete")

    vcpus = int(production["vcpus_per_host"])
    global_cpu = quota_capacity(global_quotas, "CPUS_ALL_REGIONS", vcpus)
    preemptible = quota_capacity(regional_quotas, "PREEMPTIBLE_CPUS", vcpus)
    family_vcpus = c4_family_quota_vcpus(policy, region)
    family_usage_vcpus = c4_family_usage_vcpus(policy, region)
    sharing = shared_regional_capacity(policy)
    reserve = sharing["reserved_vcpus"] if sharing and region in sharing["regions"] else 0
    family_cpu = max(0, (family_vcpus - family_usage_vcpus - reserve) // vcpus)
    regional_cpu = (
        preemptible if preemptible is not None and preemptible > 0 else family_cpu
    )
    instances = quota_capacity(regional_quotas, "INSTANCES", 1)
    if production["boot_disk_type"] == "hyperdisk-balanced":
        disk_gb = hyperdisk_quota_gb(policy, region)
        disk_usage_gb = hyperdisk_usage_gb(policy, region)
        disks = max(
            0,
            (disk_gb - disk_usage_gb) // int(production["boot_disk_gb"]),
        )
    else:
        disk_gb = None
        disk_usage_gb = None
        disks = quota_capacity(
            regional_quotas, "SSD_TOTAL_GB", int(production["boot_disk_gb"])
        )
    capacities = {
        "global_cpu_hosts": global_cpu,
        "regional_cpu_hosts": regional_cpu,
        "family_cpu_hosts": family_cpu,
        "instance_hosts": instances,
        "disk_hosts": disks,
    }
    local_ssd_quota = None
    local_ssd_usage = None
    if production.get("use_local_ssd"):
        # GCP refuses a create past this limit even with vCPUs and disk to
        # spare; without it peak scale-up failed creates in a full region
        # instead of spilling to the next market.
        local_ssd_hosts, local_ssd_quota, local_ssd_usage = local_ssd_capacity(
            policy, region, production["machine_type"]
        )
        if local_ssd_hosts is not None:
            capacities["local_ssd_hosts"] = local_ssd_hosts
    if egress_mode(policy) == "direct":
        # Each direct-egress host holds one ephemeral external address, so the
        # regional address quota is as hard a ceiling as vCPUs or disk. Without
        # this the fleet would fill quota and then fail creates one at a time.
        capacities["address_hosts"] = quota_capacity(
            regional_quotas, "IN_USE_ADDRESSES", 1
        )
    if any(value is None for value in capacities.values()):
        missing = sorted(key for key, value in capacities.items() if value is None)
        raise ObservationError(f"required GCP quota is missing: {', '.join(missing)}")
    return {
        "region": region,
        "available_hosts": min(int(value) for value in capacities.values()),
        "hyperdisk_quota_gb": disk_gb,
        "hyperdisk_usage_gb": disk_usage_gb,
        "local_ssd_quota_gb": local_ssd_quota,
        "local_ssd_usage_gb": local_ssd_usage,
        "family_cpu_usage_vcpus": family_usage_vcpus,
        "family_cpu_reserved_vcpus": reserve,
        **capacities,
    }


def dropped_without_retirement(
    prior: dict[str, Any],
    observer: GitHubDemandObserver,
    instances: tuple[Instance, ...],
) -> list[str]:
    """What the last tick watched that this policy silently stopped watching.

    A repository or organization leaves by being retired in the policy, never
    by deletion, while the pool still has a host. This compares against the
    last recorded tick; with no readable record there is nothing to compare,
    which is why retirement is configuration and this is only a tripwire.
    """
    if not instances:
        return []
    watched_repositories = [
        *(prior.get("github_repositories") or []),
        *(prior.get("github_retired_repositories") or []),
    ]
    watched_organizations = prior.get("github_runner_organizations") or []
    return [
        *(
            repository
            for repository in dict.fromkeys(watched_repositories)
            if repository not in observer.known_repositories
        ),
        *(
            f"organization {owner}"
            for owner in dict.fromkeys(watched_organizations)
            if owner not in observer.runner_organizations
        ),
    ]


def record_repositories(
    decision: dict[str, Any], observer: GitHubDemandObserver
) -> None:
    decision["github_repositories"] = list(observer.repositories)
    decision["github_runner_scope"] = observer.runner_scope
    # Retention ends only once the pool is empty: then no host can still be
    # running a job from a repository that has left the list.
    decision["github_retired_repositories"] = list(observer.retired_repositories)
    decision["github_runner_organizations"] = list(observer.runner_organizations)


def read_prior_state(path: str) -> dict[str, Any]:
    """Last tick's decision, or an empty ledger when it is unreadable.

    A missing or corrupt state file only restarts quarantine clocks, so it
    must never take the broker down.
    """
    try:
        prior = load_json(path)
    except (OSError, json.JSONDecodeError):
        return {}
    return prior if isinstance(prior, dict) else {}


def track_quarantine(
    prior: dict[str, Any] | None,
    instances: tuple[Instance, ...],
    quarantined: dict[int, str],
    idle_confirmed: set[int],
    now: dt.datetime,
) -> dict[str, dict[str, Any]]:
    """Carry each quarantined slot's first-seen time across ticks.

    The ledger is keyed by instance_id as well as slot so that recycling a
    slot onto a fresh VM restarts its clock; otherwise a new host could
    inherit a predecessor's age and be deleted while running a job.
    """
    previous = (prior or {}).get("quarantine_since") or {}
    by_slot = {item.slot: item for item in instances}
    ledger: dict[str, dict[str, Any]] = {}
    for slot, reason in quarantined.items():
        instance = by_slot.get(slot)
        if instance is None:
            continue
        carried = previous.get(str(slot))
        if (
            isinstance(carried, dict)
            and carried.get("instance_id") == instance.instance_id
            and isinstance(carried.get("since"), str)
        ):
            since = carried["since"]
        else:
            since = now.isoformat().replace("+00:00", "Z")
        ledger[str(slot)] = {
            "instance_id": instance.instance_id,
            "since": since,
            "reason": reason,
            "idle_confirmed": slot in idle_confirmed,
        }
    return ledger


def drain_requested_slots(instances: tuple[Instance, ...]) -> set[int]:
    """Slots carrying our own drain request, bound to the live instance id."""
    return {
        item.slot
        for item in instances
        if item.metadata.get("runner-drain-request") == item.instance_id
    }


def stalled_drain_slots(
    instances: tuple[Instance, ...],
    demand: Demand,
    drained: dict[int, dict[str, Any]],
    quarantined: dict[int, str],
    receipts_observed: bool = True,
    idle_confirmed: set[int] | None = None,
    admission_closed: set[int] | None = None,
) -> set[int]:
    """Admission-closed hosts without a usable receipt that are provably idle.

    A verified receipt is the normal deletion proof. This fallback applies only
    when the guest separately published an identity-bound acknowledgement that
    admission is durably closed. This also covers bootstrap failures before a
    controller drain request. GitHub polling alone never makes a host reapable.

    The idleness is GitHub's, not the host's own claim: the slot carries no
    in-progress job on a runner we own. When any owned job cannot be attributed
    to a slot, this reaps nothing at all this tick rather than guess which host
    is free.

    A tick that did not read the serial consoles knows nothing: every host
    looks like it published nothing. Such a tick reports no stalls, which
    empties the ledger and restarts every clock, so unwatched wall-clock time
    can never count toward expiry.
    """
    if not receipts_observed or demand.unattributed_active:
        return set()
    # A parsed receipt whose GitHub cross-check failed has already proved the
    # guest stopped serving, so quarantine expiry owns it. An unreadable or
    # malformed receipt proves no such thing. Once that host has an
    # identity-bound drain request, age it through this GitHub-idle path just
    # like a host that published no receipt at all. Otherwise malformed output
    # can quarantine a stopped host forever.
    locally_drained_quarantines = set(quarantined) & (idle_confirmed or set())
    accounted = set(drained) | locally_drained_quarantines
    acknowledged = admission_closed or set()
    return {
        item.slot
        for item in instances
        if item.slot in acknowledged
        and item.slot not in accounted
        and item.slot not in demand.active_slots
        and item.status not in TRANSITIONAL_STATUSES
        and item.status not in RECLAIMABLE_STATUSES
    }


def unanswered_drain_request_slots(
    instances: tuple[Instance, ...],
    demand: Demand,
    drained: dict[int, dict[str, Any]],
    receipts_observed: bool = True,
    quarantined: dict[int, str] | None = None,
) -> set[int]:
    """Drain-requested hosts with no receipt that are idle on GitHub.

    Unlike stalled_drain_slots this needs no acknowledgement from the guest:
    it exists for the guest that never answers at all. It only feeds a clock;
    deletion waits for DRAIN_REQUEST_CEILING_MINUTES of continuous idleness
    and then still passes fallback_delete_block_reason.
    """
    if not receipts_observed or demand.unattributed_active:
        return set()
    return {
        slot
        for slot in drain_requested_slots(instances)
        if slot not in drained
        and slot not in (quarantined or {})
        and slot not in demand.active_slots
    } & {
        item.slot
        for item in instances
        if item.status not in TRANSITIONAL_STATUSES
        and item.status not in RECLAIMABLE_STATUSES
    }


def track_drain_stalls(
    prior: dict[str, Any] | None,
    instances: tuple[Instance, ...],
    stalled: set[int],
    now: dt.datetime,
    key: str = "drain_stall_since",
) -> dict[str, dict[str, Any]]:
    """Carry each stalled slot's first-seen time across ticks.

    Keyed by instance_id as well as slot, for the same reason the quarantine
    ledger is: recycling a slot onto a fresh VM must restart its clock, or a
    new host inherits its predecessor's age and is deleted while running a job.
    A slot that stops being stalled -- its receipt finally lands, or it picks
    up a job -- drops out of the ledger here and starts over if it recurs.
    """
    previous = (prior or {}).get(key) or {}
    try:
        prior_observed_at = parse_utc_timestamp(
            (prior or {}).get("observed_at"), "observed_at"
        )
    except ObservationError:
        prior_observed_at = None
    observation_contiguous = (
        prior_observed_at is not None
        and dt.timedelta(0) <= now - prior_observed_at <= MAX_STALL_OBSERVATION_GAP
    )
    by_slot = {item.slot: item for item in instances}
    ledger: dict[str, dict[str, Any]] = {}
    for slot in stalled:
        instance = by_slot.get(slot)
        if instance is None:
            continue
        carried = previous.get(str(slot))
        if (
            observation_contiguous
            and isinstance(carried, dict)
            and carried.get("instance_id") == instance.instance_id
            and isinstance(carried.get("since"), str)
        ):
            since = carried["since"]
        else:
            since = now.isoformat().replace("+00:00", "Z")
        ledger[str(slot)] = {
            "instance_id": instance.instance_id,
            "since": since,
        }
    return ledger


def expired_drain_stall_slots(
    ledger: dict[str, dict[str, Any]],
    grace_minutes: int,
    now: dt.datetime,
) -> list[int]:
    """Stalled slots past the grace window.

    Every entry in the ledger was idle on GitHub when it was written and has
    stayed idle every tick since, because a slot that takes work leaves the
    stalled set and loses its entry. The window is what separates a publisher
    that is merely slow from one that is never going to publish.
    """
    expired: list[int] = []
    for slot, entry in ledger.items():
        try:
            since = parse_utc_timestamp(entry.get("since"), "since")
        except ObservationError:
            continue
        if (now - since) >= dt.timedelta(minutes=grace_minutes):
            expired.append(int(slot))
    return sorted(expired)


def expired_quarantine_slots(
    ledger: dict[str, dict[str, Any]],
    grace_minutes: int,
    now: dt.datetime,
) -> list[int]:
    """Slots quarantined past the grace window that are provably idle.

    Quarantine fails closed so an unverifiable host is never recycled on a
    guess, but without a bound it also never goes away: the VM bills, holds
    its slot against maximum_hosts, and keeps the fleet from reaching zero.
    Expiry applies only to slots whose drain receipt parsed, so the host has
    already finished its job -- expiring forfeits the GitHub cross-check,
    never a running job.
    """
    expired: list[int] = []
    for slot, entry in ledger.items():
        if not entry.get("idle_confirmed"):
            continue
        try:
            since = parse_utc_timestamp(entry.get("since"), "since")
        except ObservationError:
            continue
        if (now - since) >= dt.timedelta(minutes=grace_minutes):
            expired.append(int(slot))
    return sorted(expired)


def decide(
    production: dict[str, Any],
    demand: Demand,
    instances: tuple[Instance, ...],
    drained: dict[int, dict[str, Any]] | None = None,
    quarantined: dict[int, str] | None = None,
    quarantine_ledger: dict[str, dict[str, Any]] | None = None,
    now: dt.datetime | None = None,
    drain_stall_ledger: dict[str, dict[str, Any]] | None = None,
    drain_request_ledger: dict[str, dict[str, Any]] | None = None,
    floor: int = 0,
    hold: int = 0,
) -> dict[str, Any]:
    drained = drained or {}
    drain_request_ledger = drain_request_ledger or {}
    quarantined = quarantined or {}
    quarantine_ledger = quarantine_ledger or {}
    drain_stall_ledger = drain_stall_ledger or {}
    now = now or dt.datetime.now(dt.timezone.utc)
    maximum = int(production["maximum_hosts"])
    minimum = max(int(production.get("minimum_hosts", 0)), floor)
    observed_demand = demand.queued + demand.active_owned
    desired = min(maximum, max(minimum, observed_demand))
    existing = {item.slot for item in instances}
    expired = (
        expired_quarantine_slots(
            quarantine_ledger,
            int(production["quarantine_grace_minutes"]),
            now,
        )
        if production["automatic_delete"]
        else []
    )
    # Same grace window as quarantine: both are a bounded wait before
    # reclaiming a host whose own account of itself never arrived.
    expired_stalls = (
        expired_drain_stall_slots(
            drain_stall_ledger,
            int(production["quarantine_grace_minutes"]),
            now,
        )
        if production["automatic_delete"]
        else []
    )
    expired_requests = (
        expired_drain_stall_slots(
            drain_request_ledger, DRAIN_REQUEST_CEILING_MINUTES, now
        )
        if production["automatic_delete"]
        else []
    )
    transitional = {
        item.slot for item in instances if item.status in TRANSITIONAL_STATUSES
    }
    reclaimable = {
        item.slot for item in instances if item.status in RECLAIMABLE_STATUSES
    }
    delete_slots = (
        sorted(
            set(drained)
            | set(expired)
            | set(expired_stalls)
            | set(expired_requests)
            | reclaimable
        )
        if production["automatic_delete"]
        else []
    )
    retained = existing - set(delete_slots)
    drain_requested = drain_requested_slots(instances)
    serving = retained - set(quarantined) - drain_requested - transitional
    # A live host can pick up a job just before its drain metadata reaches the
    # guest. That job is still real capacity even though the host cannot accept
    # another one. Count it as already hosted so we neither keep a separate
    # idle VM nor create a replacement solely for work already running.
    live_retained = retained - transitional
    active_nonserving = (set(demand.active_slots) & live_retained) - serving
    desired_serving = max(0, desired - len(active_nonserving))
    # CI arrives in bursts minutes apart. Keep enough hosts for the busiest
    # demand seen within scale_down_hold_minutes, so a host drained between
    # two bursts is not recreated for the next one: a create costs ~75s
    # before a runner is ready. Hold only defers drains; it never creates.
    held_serving = max(0, min(maximum, hold) - len(active_nonserving))
    excess = (
        max(0, len(serving) - max(desired_serving, held_serving))
        if production["automatic_delete"]
        else 0
    )
    # Reuse a VM only when current demand needs it. Drain known-idle surplus
    # immediately; active slots stay until their job completes. If GitHub shows
    # an active owned job whose runner name cannot be attributed to a slot,
    # fail safe for this tick rather than risk draining the running job.
    idle_serving = serving - set(demand.active_slots)
    scale_down_suppressed_reason = None
    quarantine_drain_candidates = (
        (
            set(quarantined)
            - drain_requested
            - set(delete_slots)
            - transitional
            - reclaimable
            - set(demand.active_slots)
        )
        if production["automatic_delete"]
        else set()
    )
    if (excess or quarantine_drain_candidates) and demand.unattributed_active:
        drain_request_slots = []
        scale_down_suppressed_reason = "active_runner_attribution_incomplete"
    else:
        # Quarantine removes a host from serving capacity, but malformed
        # serial output used to leave an undrained guest able to accept work
        # forever. Close admission on every known-idle quarantined host first;
        # subsequent ticks can then prove continuous GitHub idleness through
        # the drain-stall ledger before deletion.
        drain_request_slots = sorted(
            quarantine_drain_candidates
            | set(sorted(idle_serving, reverse=True)[:excess]),
            reverse=True,
        )
    missing_count = max(0, desired_serving - len(serving))
    vacant = [slot for slot in range(1, maximum + 1) if slot not in existing]
    # Prefer names that are already absent. A slot in delete_slots becomes
    # reusable only after a synchronous provider delete, while a vacant slot
    # can be created before that maintenance work and serve the queue sooner.
    absent = vacant + [slot for slot in delete_slots if slot in existing]
    create_slots = absent[:missing_count]
    suppressed_reason = None
    if (
        (create_slots or delete_slots or drain_request_slots)
        and demand.rate_remaining is not None
        and demand.rate_remaining < MINIMUM_GITHUB_RATE_REMAINING
    ):
        create_slots = []
        delete_slots = []
        drain_request_slots = []
        suppressed_reason = "github_rate_limit_low"
    if delete_slots and create_slots:
        action = "replace"
    elif delete_slots:
        action = "delete"
    elif create_slots:
        action = "create"
    elif drain_request_slots:
        action = "drain"
    else:
        action = "none"
    return {
        "controller_version": VERSION,
        "action": action,
        "queued_jobs": demand.queued,
        "active_owned_jobs": demand.active_owned,
        "active_slots": sorted(demand.active_slots),
        "unattributed_active_jobs": demand.unattributed_active,
        "desired_hosts": desired,
        "existing_hosts": len(instances),
        "existing_slots": sorted(existing),
        "serving_hosts": len(serving),
        "desired_serving_hosts": desired_serving,
        "active_nonserving_slots": sorted(active_nonserving),
        "drain_requested_slots": sorted(drain_requested),
        "drain_request_slots": sorted(drain_request_slots),
        "drained_slots": sorted(drained),
        "quarantined_slots": quarantined,
        "expired_quarantine_slots": expired,
        "stalled_drain_slots": sorted(int(slot) for slot in drain_stall_ledger),
        "expired_drain_stall_slots": expired_stalls,
        "unanswered_drain_request_slots": sorted(
            int(slot) for slot in drain_request_ledger
        ),
        "expired_drain_request_slots": expired_requests,
        "transitional_slots": sorted(transitional),
        "reclaimable_slots": sorted(reclaimable),
        "quarantine_since": quarantine_ledger,
        "drain_stall_since": drain_stall_ledger,
        "drain_request_since": drain_request_ledger,
        "delete_slots": delete_slots,
        "create_slots": create_slots,
        "maximum_hosts": maximum,
        "minimum_hosts": minimum,
        "scale_down_hold_hosts": hold,
        "retention_mode": production["retention_mode"],
        "max_jobs_per_host": int(production["max_jobs_per_host"]),
        "scale_down_suppressed_reason": scale_down_suppressed_reason,
        "drained_host_jobs": {
            str(slot): {
                "host_instance_id": receipt.get("identity", {}).get("instance_id"),
                "drained_at": receipt.get("drained_at"),
                "completed_jobs": receipt["completed_jobs"],
                "max_jobs": receipt["max_jobs"],
                "reason": receipt.get("reason", "maximum_jobs"),
            }
            for slot, receipt in sorted(drained.items())
        },
        "github_rate_remaining": demand.rate_remaining,
        "suppressed_reason": suppressed_reason,
        "automatic_delete": production["automatic_delete"],
    }


def fast_scale_up_decision(
    production: dict[str, Any],
    demand: Demand,
    instances: tuple[Instance, ...],
    floor: int = 0,
) -> dict[str, Any]:
    """Plan only immediately safe creates, before slow receipt observation.

    Existing drain-requested and transitional hosts cannot accept another job,
    but deleting them requires the full receipt path. This fast path fills
    genuinely absent slots without waiting for that maintenance work. It never
    deletes, drains, or reuses an occupied slot.
    """
    maximum = int(production["maximum_hosts"])
    existing = {item.slot for item in instances}
    unavailable = drain_requested_slots(instances) | {
        item.slot
        for item in instances
        if item.status in TRANSITIONAL_STATUSES | RECLAIMABLE_STATUSES
    }
    serving = existing - unavailable
    desired = min(
        maximum,
        max(
            int(production.get("minimum_hosts", 0)),
            floor,
            demand.queued + demand.active_owned,
        ),
    )
    live = {
        item.slot
        for item in instances
        if item.status not in TRANSITIONAL_STATUSES | RECLAIMABLE_STATUSES
    }
    active_nonserving = (set(demand.active_slots) & live) - serving
    desired_serving = max(0, desired - len(active_nonserving))
    missing = max(0, desired_serving - len(serving))
    absent = [slot for slot in range(1, maximum + 1) if slot not in existing]
    create_slots = absent[:missing]
    suppressed_reason = None
    if (
        create_slots
        and demand.rate_remaining is not None
        and demand.rate_remaining < MINIMUM_GITHUB_RATE_REMAINING
    ):
        create_slots = []
        suppressed_reason = "github_rate_limit_low"
    return {
        "action": "create" if create_slots else "none",
        "queued_jobs": demand.queued,
        "active_owned_jobs": demand.active_owned,
        "active_slots": sorted(demand.active_slots),
        "unattributed_active_jobs": demand.unattributed_active,
        "desired_hosts": desired,
        "existing_hosts": len(instances),
        "serving_hosts": len(serving),
        "desired_serving_hosts": desired_serving,
        "active_nonserving_slots": sorted(active_nonserving),
        "delete_slots": [],
        "drain_request_slots": [],
        "create_slots": create_slots,
        "minimum_hosts": max(int(production.get("minimum_hosts", 0)), floor),
        "retention_mode": production["retention_mode"],
        "suppressed_reason": suppressed_reason,
    }


def warm_floor_of(state: dict[str, Any]) -> int:
    try:
        return int(((state or {}).get("warm_pool") or {}).get("floor") or 0)
    except (TypeError, ValueError):
        return 0


def newer_peer_raised_floor(decision: dict[str, Any], peer_state: dict[str, Any]) -> bool:
    """The fast loop computed a higher warm floor after this decision did.

    Ordered by when each floor was computed, not by demand data time: both
    loops often reuse one snapshot, so a window that opened between them, or
    a lease renewed by work that already finished, shows only here.
    """
    try:
        decided = parse_utc_timestamp(
            ((decision or {}).get("warm_pool") or {}).get("decided_at"), "decided_at"
        )
        peer_decided = parse_utc_timestamp(
            ((peer_state or {}).get("warm_pool") or {}).get("decided_at"), "decided_at"
        )
    except ObservationError:
        return False
    return peer_decided > decided and warm_floor_of(peer_state) > warm_floor_of(decision)


def newer_peer_has_demand(decision: dict[str, Any], peer_state: dict[str, Any]) -> bool:
    """Return whether the fast loop has a newer observation that needs hosts.

    Nonzero demand on newer data counts, and so does a higher warm floor
    computed after this decision (newer_peer_raised_floor).
    """
    if newer_peer_raised_floor(decision, peer_state):
        return True
    try:
        decision_at = parse_utc_timestamp(decision.get("observed_at"), "observed_at")
        peer_at = parse_utc_timestamp(peer_state.get("observed_at"), "observed_at")
    except ObservationError:
        return False
    if decision_at is None or peer_at is None or peer_at <= decision_at:
        return False
    peer_demand = int(peer_state.get("queued_jobs", 0)) + int(
        peer_state.get("active_owned_jobs", 0)
    )
    return peer_demand > 0


def newer_peer_blocks_fallback_delete(
    decision: dict[str, Any], peer_state: dict[str, Any], slot: int
) -> bool:
    """Return whether newer fast-loop evidence makes this slot unsafe to reap."""
    try:
        decision_at = parse_utc_timestamp(decision.get("observed_at"), "observed_at")
        peer_at = parse_utc_timestamp(peer_state.get("observed_at"), "observed_at")
    except ObservationError:
        return False
    if decision_at is None or peer_at is None or peer_at <= decision_at:
        return False
    # Queued jobs cannot run on a host with durable admission closure. Keeping
    # closed hosts for the queue deadlocks a full fleet behind its failed VMs.
    # Older fast-loop state did not persist attribution. During a rolling
    # controller update, retain its pool-wide behavior until the next 30-second
    # observation replaces it rather than guessing that active work is safe.
    if "active_slots" not in peer_state or "unattributed_active_jobs" not in peer_state:
        return int(peer_state.get("active_owned_jobs", 0)) > 0
    return int(peer_state.get("unattributed_active_jobs", 0)) > 0 or slot in {
        int(item) for item in peer_state.get("active_slots", [])
    }


def suppress_stale_scale_down(
    decision: dict[str, Any], peer_state: dict[str, Any]
) -> bool:
    """Refuse a drain when the fast loop has a newer nonzero observation.

    This is a concurrency guard, not a retention lease. The next maintenance
    tick observes GitHub again and may drain immediately if demand is then zero.
    """
    if not decision.get("drain_request_slots") or not newer_peer_has_demand(
        decision, peer_state
    ):
        return False

    decision["drain_request_slots"] = []
    decision["scale_down_suppressed_reason"] = "newer_scale_up_demand"
    if decision.get("delete_slots") and decision.get("create_slots"):
        decision["action"] = "replace"
    elif decision.get("delete_slots"):
        decision["action"] = "delete"
    elif decision.get("create_slots"):
        decision["action"] = "create"
    else:
        decision["action"] = "none"
    return True


_UNPRINTABLE = re.compile(r"[^\x20-\x7e\t]")


def serial_evidence_tail(contents: str) -> str:
    """The bounded, printable tail of a serial console, newest lines kept."""
    lines = [
        _UNPRINTABLE.sub("", line)[:EVIDENCE_LINE_CHARS]
        for line in contents.replace("\r", "").splitlines()
    ]
    tail: list[str] = []
    size = 0
    for line in reversed([line for line in lines if line.strip()]):
        if len(tail) >= EVIDENCE_TAIL_LINES or size + len(line) + 1 > EVIDENCE_TAIL_CHARS:
            break
        tail.append(line)
        size += len(line) + 1
    return "\n".join(reversed(tail))


def capture_unanswered_drain_evidence(
    instance: Instance, policy: dict[str, Any]
) -> dict[str, Any]:
    """Record what a never-answering guest last said, before it is deleted.

    Best effort by design: a failed read is recorded, never raised, so the
    capture can never block the delete it precedes.
    """
    evidence: dict[str, Any] = {
        "instance_id": instance.instance_id,
        "instance_name": instance.name,
        "captured_at": dt.datetime.now(dt.timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
    }
    try:
        evidence["serial_tail"] = serial_evidence_tail(
            serial_console_contents(instance, policy)
        )
    except Exception as error:  # noqa: BLE001 - evidence must not block deletion
        evidence["error"] = str(error)[:EVIDENCE_LINE_CHARS]
    return evidence


def fallback_delete_block_reason(
    observer: GitHubDemandObserver,
    production: dict[str, Any],
    slot: int,
    decision: dict[str, Any],
    peer_state: dict[str, Any] | None = None,
    unanswered_drain: bool = False,
) -> str | None:
    """Final fail-closed gate for deletion without a usable drain receipt.

    A guest that never answered its drain request is, by definition, still
    registered with GitHub, so for that ceiling the registration itself cannot
    block: the runner must instead be reported idle (not busy).
    """
    now = dt.datetime.now(dt.timezone.utc)
    demand = observer.observe(
        production["workflow_label"],
        production["scale_set_prefix"] + "-",
        now,
    )
    if demand.unattributed_active or slot in demand.active_slots:
        return "current_github_demand"
    registered, busy, unattributed = observer.runner_slots(
        production["scale_set_prefix"] + "-"
    )
    remaining = observer.client.last_headers.get("x-ratelimit-remaining")
    if remaining is not None and int(remaining) < MINIMUM_GITHUB_RATE_REMAINING:
        return "github_rate_limit_low"
    if unattributed:
        return "registered_runner_attribution_incomplete"
    if unanswered_drain:
        if slot in busy:
            return "slot_runner_busy"
    elif slot in registered:
        return "slot_runner_still_registered"
    if peer_state and newer_peer_blocks_fallback_delete(decision, peer_state, slot):
        return "newer_scale_up_demand"
    return None


def defer_fallback_delete(decision: dict[str, Any], slot: int, reason: str) -> None:
    """Record a blocked fallback and revoke its accumulated idle proof."""
    decision.setdefault("deferred_delete_slots", {})[str(slot)] = reason
    for key in ("drain_stall_since", "drain_request_since"):
        ledger = decision.get(key)
        if isinstance(ledger, dict):
            ledger.pop(str(slot), None)


def request_drain_slots(
    instances: tuple[Instance, ...],
    slots: list[int],
    policy: dict[str, Any],
    continue_guard: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Set drain requests in concurrent waves, each behind a fresh peer check.

    Each `add-metadata` waits for its operation, 5-10s. One after another, a
    busy tick spent ~30s here and the last host's teardown waited for all of
    them. A wave takes about as long as one sequential request did, so the
    guard is still re-read that often.
    """
    by_slot = {item.slot: item for item in instances}
    if any(slot not in by_slot for slot in slots):
        raise BrokerError("drain-request slot disappeared before metadata guard")

    def request(slot: int) -> dict[str, Any]:
        instance = by_slot[slot]
        payload = gcloud(
            [
                "compute",
                "instances",
                "add-metadata",
                instance.name,
                f"--zone={instance.zone}",
                f"--project={policy['project_id']}",
                f"--metadata=runner-drain-request={instance.instance_id}",
            ]
        )
        return {
            "slot": slot,
            "instance_id": instance.instance_id,
            "zone": instance.zone,
            "result": operation_summary(payload),
        }

    requested: list[dict[str, Any]] = []
    deferred: list[int] = []
    for index in range(0, len(slots), DRAIN_REQUEST_WORKERS):
        if continue_guard is not None and not continue_guard():
            deferred = slots[index:]
            break
        wave = slots[index : index + DRAIN_REQUEST_WORKERS]
        with ThreadPoolExecutor(max_workers=len(wave)) as executor:
            futures = [executor.submit(request, slot) for slot in wave]
        # Every request in the wave has finished; report the first failure,
        # as the sequential loop did.
        requested.extend(future.result() for future in futures)
    return {
        "drain_requested": requested,
        "drain_deferred_slots": deferred,
    }


def constrain_creates_to_capacity(
    decision: dict[str, Any], available_hosts: int
) -> None:
    """Admit the safe prefix of planned creates and defer the rest.

    Quota is a concurrency ceiling, not a reason to deadlock the fleet. When
    demand exceeds current provider capacity, fill every safely available slot
    and let later reconciles handle the remainder. A true zero-capacity result
    remains fail-closed.
    """
    requested = list(decision["create_slots"])
    admitted_count = max(0, min(len(requested), available_hosts))
    if admitted_count == len(requested):
        return

    decision["create_slots"] = requested[:admitted_count]
    decision["deferred_create_slots"] = requested[admitted_count:]
    decision["capacity_limited"] = True
    if admitted_count == 0:
        decision["suppressed_reason"] = "insufficient_gcp_quota"
        if decision["delete_slots"]:
            decision["action"] = "delete"
        elif decision["drain_request_slots"]:
            decision["action"] = "drain"
        else:
            decision["action"] = "none"


def constrain_creates_to_regional_capacity(
    decision: dict[str, Any],
    policy: dict[str, Any],
    instances: tuple[Instance, ...],
    market_state: dict[str, Any],
    pool: str = "heavy",
    now: dt.datetime | None = None,
) -> None:
    production = validate_policy(policy, pool)
    now = now or dt.datetime.now(dt.timezone.utc)
    requested = list(decision["create_slots"])
    markets = price_ordered_market_regions(policy, market_state, pool, now)
    if not markets:
        decision.update(
            create_slots=[],
            deferred_create_slots=requested,
            capacity_limited=True,
            suppressed_reason="no_eligible_gcp_market",
            action="none",
        )
        return
    regions = {item["name"] for item in markets}
    existing_by_region = {
        region: sum(region_from_zone(item.zone) == region for item in instances)
        for region in regions
    }
    # Regions are read concurrently and share the project-wide reads. Each
    # task gets its own context copy, which carries the shared read cache.
    # The project-wide reads start first and alongside each other, so no
    # region waits for them one after another.
    project = policy["project_id"]
    shared_reads: list[Callable[[str], Any]] = [project_info, project_instances]
    if production["boot_disk_type"] == "hyperdisk-balanced":
        shared_reads.append(project_disks)
    with shared_admission_reads(), ThreadPoolExecutor(
        max_workers=len(regions) + len(shared_reads)
    ) as executor:
        for read in shared_reads:
            # A failed read reaches every region that needs it through the
            # shared cache, so these futures need no result of their own.
            executor.submit(contextvars.copy_context().run, read, project)
        futures = {
            region: executor.submit(
                contextvars.copy_context().run,
                observe_create_capacity,
                policy,
                pool=pool,
                region=region,
                existing_hosts=existing_by_region[region],
            )
            for region in sorted(regions)
        }
        capacities = {region: future.result() for region, future in futures.items()}
    global_remaining = min(
        (item["global_cpu_hosts"] for item in capacities.values()), default=0
    )
    regional_remaining = {
        region: item["available_hosts"] for region, item in capacities.items()
    }
    admitted: list[int] = []
    placements: list[dict[str, Any]] = []
    deferred: list[int] = []
    for slot in requested:
        candidate = next(
            (
                item
                for item in markets
                if regional_remaining[item["name"]] > 0
                and existing_by_region.get(item["name"], 0) < regional_host_cap(policy, item["name"], pool)
            ),
            None,
        )
        if global_remaining < 1 or candidate is None:
            deferred.append(slot)
            continue
        region = candidate["name"]
        zone = candidate["zones"][
            existing_by_region.get(region, 0) % len(candidate["zones"])
        ]
        admitted.append(slot)
        placements.append(
            {
                "slot": slot,
                "zone": zone,
                "region": region,
                "hourly_usd": candidate["hourly_usd"],
            }
        )
        global_remaining -= 1
        regional_remaining[region] -= 1
        existing_by_region[region] = existing_by_region.get(region, 0) + 1
    decision["create_slots"] = admitted
    decision["create_placements"] = placements
    decision["gcp_create_capacity"] = capacities
    if deferred:
        decision["deferred_create_slots"] = deferred
        decision["capacity_limited"] = True
    if not admitted:
        decision["suppressed_reason"] = "insufficient_gcp_quota"
        if decision["delete_slots"]:
            decision["action"] = "delete"
        elif decision["drain_request_slots"]:
            decision["action"] = "drain"
        else:
            decision["action"] = "none"


def emit_mutations(payload: list[dict[str, Any]], action: str, pool: str) -> None:
    """One bounded event per successful generation, even in a partial batch.

    Contains only identity/timing, never provider payloads or credentials.
    The full decision remains available for diagnostics.
    """
    for item in payload:
        result = item.get("result") or {}
        instance_id = (
            result.get("id") if action == "create" else result.get("instance_id")
        )
        if not item.get("ok") or not instance_id:
            continue
        event = {
            "event": "runner_lifecycle_metric",
            "event_version": 1,
            "event_id": f"gcp-{action}:{instance_id}",
            "pool": pool,
            "host_instance_id": str(instance_id),
            "slot": item.get("slot"),
            "action": action,
            "controller_version": VERSION,
        }
        for key in (
            "name",
            "deleted",
            "requested_at",
            "created_at",
            "api_completed_at",
            "delete_requested_at",
            "delete_completed_at",
        ):
            if result.get(key) is not None:
                event[key] = result[key]
        print(json.dumps(event, sort_keys=True), flush=True)


def emit_decision_summary(decision: dict[str, Any]) -> None:
    """Small retention evidence survives collectors with line-size limits."""
    keys = (
        "pool",
        "observed_at",
        "loop",
        "queued_jobs",
        "active_owned_jobs",
        "existing_hosts",
        "serving_hosts",
        "desired_hosts",
        "minimum_hosts",
        "scale_down_hold_hosts",
        "retention_mode",
        "max_jobs_per_host",
        "scale_down_suppressed_reason",
        "suppressed_reason",
        "warm_pool",
    )
    event = {
        "event": "runner_retention_metric",
        "event_version": 1,
        "controller_version": VERSION,
        **{key: decision.get(key) for key in keys},
    }
    event["event_id"] = (
        f"retention:{decision.get('pool')}:{decision.get('loop')}:{decision.get('observed_at')}"
    )
    print(json.dumps(event, sort_keys=True), flush=True)
    for slot, receipt in decision.get("drained_host_jobs", {}).items():
        print(
            json.dumps(
                {
                    "event": "runner_drain_reason",
                    "event_id": f"drain:{receipt.get('host_instance_id')}:{receipt.get('drained_at')}",
                    "pool": decision.get("pool"),
                    "observed_at": decision.get("observed_at"),
                    "slot": slot,
                    **receipt,
                },
                sort_keys=True,
            ),
            flush=True,
        )


def create_slots(
    placements: list[dict[str, Any]],
    policy_path: str,
    lifecycle_script: str,
    pool: str = "heavy",
) -> dict[str, Any]:
    slots = [int(item["slot"]) for item in placements]
    slot_zones = ",".join(f"{int(item['slot'])}={item['zone']}" for item in placements)
    command = [
        sys.executable,
        lifecycle_script,
        "--policy",
        policy_path,
        "create-production",
        "--pool",
        pool,
        "--slots",
        ",".join(str(slot) for slot in slots),
        "--slot-zones",
        slot_zones,
        "--apply",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise BrokerError(
            f"production create failed: {bounded_text(result.stderr)}"
        )
    payload = json.loads(result.stdout)
    if not isinstance(payload, list):
        raise BrokerError("one or more production creates failed")
    emit_mutations(payload, "create", pool)
    failed = [item for item in payload if not item.get("ok")]
    if failed:
        # Name each refused slot and GCP's own reason (QUOTA_EXCEEDED and the
        # quota, a stockout, ...): the bare count left only the operations log.
        created = [str(item.get("slot")) for item in payload if item.get("ok")]
        # Each reason is bounded on its own so one long refusal cannot push
        # the other slots' reasons out of the journal line.
        reasons = "; ".join(
            f"slot {item.get('slot')}: "
            + bounded_text(create_failure_reason(item.get("error")), 400)
            for item in failed
        )
        raise BrokerError(
            f"{len(failed)} of {len(payload)} production creates failed"
            + (f" (created slots {','.join(created)})" if created else "")
            + f": {bounded_text(reasons)}"
        )
    return {"created": payload}


def create_failure_reason(error: Any) -> str:
    """GCP's refusal from a failed create, without gcloud's command prefix."""
    text = " ".join(str(error or "unknown error").split())
    _, marker, rest = text.partition("ERROR: ")
    return rest if marker else text


def delete_slots(
    instances: tuple[Instance, ...],
    slots: list[int],
    policy_path: str,
    lifecycle_script: str,
    pool: str = "heavy",
) -> dict[str, Any]:
    by_slot = {item.slot: item for item in instances}
    if any(slot not in by_slot for slot in slots):
        raise BrokerError("delete slot disappeared before lifecycle guard")
    expected = ",".join(f"{slot}={by_slot[slot].instance_id}" for slot in slots)
    slot_zones = ",".join(f"{slot}={by_slot[slot].zone}" for slot in slots)
    command = [
        sys.executable,
        lifecycle_script,
        "--policy",
        policy_path,
        "delete-production",
        "--pool",
        pool,
        "--slots",
        ",".join(str(slot) for slot in slots),
        "--expected-instance-ids",
        expected,
        "--slot-zones",
        slot_zones,
        "--apply",
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode:
        raise BrokerError(f"production delete failed: {bounded_text(result.stderr)}")
    payload = json.loads(result.stdout)
    if isinstance(payload, list):
        emit_mutations(payload, "delete", pool)
    if not isinstance(payload, list) or any(not item.get("ok") for item in payload):
        raise BrokerError("one or more production deletes failed")
    deleted_ids = {
        int(item["slot"]): str(item.get("result", {}).get("instance_id", ""))
        for item in payload
    }
    if deleted_ids != {slot: by_slot[slot].instance_id for slot in slots}:
        raise BrokerError("production delete result did not match drain receipts")
    return {"deleted": payload}


def admit_and_create(
    decision: dict[str, Any], policy: dict[str, Any], args: argparse.Namespace
) -> dict[str, Any]:
    # Both pools and both loops share one admission lock. Demand and receipt
    # scans remain parallel; only live quota readback through completed creates
    # is serialized so both pools cannot spend the same spare vCPUs.
    lock_path = pathlib.Path(args.lock).parent / "gcp-admission-lock"
    with lock_path.open("a", encoding="utf-8") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        instances = observe_instances(policy, args.pool)
        occupied = {item.slot for item in instances}
        decision["create_slots"] = [slot for slot in decision["create_slots"] if slot not in occupied]
        if not decision["create_slots"]:
            return {"created": []}
        constrain_creates_to_regional_capacity(
            decision, policy, instances, load_json(args.market_state), args.pool,
            dt.datetime.now(dt.timezone.utc),
        )
        if args.apply and decision["create_slots"]:
            try:
                return create_slots(
                    decision["create_placements"],
                    args.policy,
                    args.lifecycle_script,
                    args.pool,
                )
            except BrokerError:
                # The provider's refusal outranks any reused limit, quota or
                # not: the next admission reads every limit live again.
                forget_quota_limits()
                raise
        return {"created": []}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", default=DEFAULT_POLICY)
    parser.add_argument("--pool", choices=sorted(POOL_CONFIG_KEYS), default="heavy")
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--market-state", default=DEFAULT_MARKET_STATE)
    parser.add_argument(
        "--scale-up-state",
        help="fast-loop state used only to reject stale concurrent scale-down",
    )
    parser.add_argument("--lock", default=DEFAULT_LOCK)
    parser.add_argument(
        "--demand-snapshot",
        help="shared GitHub active-jobs snapshot reused across pools and loops",
    )
    parser.add_argument("--github-token-file", required=True)
    parser.add_argument(
        "--github-repository",
        help=(
            "serve this one repository; the policy's github_repositories list "
            "is the multi-repository form and the two are mutually exclusive"
        ),
    )
    parser.add_argument(
        "--lifecycle-script", default="/opt/also-ran/gcp-runner-lifecycle.py"
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--scale-up-only", action="store_true")
    parser.add_argument("--maintenance-only", action="store_true")
    args = parser.parse_args(argv)
    if not args.once or args.dry_run == args.apply:
        parser.error("use --once with exactly one of --dry-run or --apply")
    if args.scale_up_only and args.maintenance_only:
        parser.error("scale-up-only and maintenance-only are mutually exclusive")

    started = time.monotonic()
    with exclusive_lock(args.lock):
        policy = load_json(args.policy)
        production = validate_policy(policy, args.pool)
        repositories = configured_repositories(policy, args.github_repository)
        now = dt.datetime.now(dt.timezone.utc)
        client = CountingJsonClient(
            read_secret(args.github_token_file), "https://api.github.com"
        )
        timer = PhaseTimer(client)
        observer = GitHubDemandObserver(
            client,
            repositories,
            runner_scope=configured_runner_scope(policy, repositories),
            retired_repositories=configured_retired_repositories(
                policy, repositories
            ),
            retired_organizations=configured_retired_organizations(policy),
        )
        snapshot = (
            DemandSnapshotStore(args.demand_snapshot) if args.demand_snapshot else None
        )
        with timer.phase("github_demand"):
            demand = observer.observe(
                production["workflow_label"],
                production["scale_set_prefix"] + "-",
                now,
                snapshot,
                DEMAND_SNAPSHOT_MAX_AGE,
            )
        # Stamp observations with when their GitHub data was fetched, so the
        # newer-peer vetoes compare evidence, not when a process read it.
        opening_at = demand.observed_at or now
        with timer.phase("gcp_instances"):
            instances = observe_instances(policy, args.pool)
        prior = read_prior_state(args.state)
        # The fast loop only creates, so it keeps serving; the maintenance
        # loop, which drains and deletes, stops until the policy is fixed.
        dropped = dropped_without_retirement(prior, observer, instances)
        if dropped and not args.scale_up_only:
            raise BrokerError(
                "the policy stopped watching "
                + ", ".join(dropped)
                + " while the pool has hosts; retire it instead "
                "(github_retired_repositories / github_retired_runner_organizations)"
            )

        # Scale up before the serial drain-receipt and deletion path. A host
        # already committed to drain is no longer capacity, but another empty
        # slot can start immediately without weakening the identity-bound
        # deletion guard for the old host.
        # The fast loop holds the floor on its own observations; maintenance
        # (below) also takes the fast loop's, and holds it a margin longer.
        fast_last_demand = last_demand_seen(demand, opening_at, now, prior)
        fast_floor = warm_floor(production, now, fast_last_demand)
        fast_scale_up = fast_scale_up_decision(
            production, demand, instances, fast_floor
        )
        fast_scale_up["warm_pool"] = warm_pool_record(
            fast_floor, fast_last_demand, now
        )
        fast_scale_up["demand_snapshot_age_ms"] = demand.snapshot_age_ms
        fast_scale_up["demand_snapshot_wait_ms"] = demand.snapshot_wait_ms
        fast_scale_up["result"] = {"created": []}
        fast_scale_up["demand_by_repository"] = demand.by_repository
        record_repositories(fast_scale_up, observer)
        if args.scale_up_only:
            # Publish the create plan before provider creation, which can take
            # longer than a maintenance tick. This keeps the slow operation
            # observable without granting the state file scheduling authority.
            fast_scale_up.update(
                {
                    "controller_version": VERSION,
                    "pool": args.pool,
                    "observed_at": opening_at.isoformat().replace("+00:00", "Z"),
                    "mode": "apply" if args.apply else "dry-run",
                    "loop": "scale_up",
                    "duration_ms": int((time.monotonic() - started) * 1000),
                    "phases": timer.phases,
                }
            )
            atomic_write_json(args.state, fast_scale_up)
        if fast_scale_up["create_slots"] and not args.maintenance_only:
            with timer.phase("fast_create"):
                fast_scale_up["result"].update(
                    admit_and_create(fast_scale_up, policy, args)
                )
                if args.apply:
                    instances = observe_instances(policy, args.pool)
        if args.maintenance_only and fast_scale_up["create_slots"]:
            fast_scale_up["deferred_create_slots"] = fast_scale_up["create_slots"]
            fast_scale_up["create_slots"] = []
            fast_scale_up["action"] = "none"
            fast_scale_up["suppressed_reason"] = "maintenance_only"
        if args.scale_up_only:
            fast_scale_up["duration_ms"] = int((time.monotonic() - started) * 1000)
            fast_scale_up["phases"] = timer.phases
            atomic_write_json(args.state, fast_scale_up)
            emit_decision_summary(fast_scale_up)
            print(json.dumps(fast_scale_up, sort_keys=True))
            return 0

        drained: dict[int, dict[str, Any]] = {}
        quarantined: dict[int, str] = {}
        idle_confirmed: set[int] = set()
        admission_closed: set[int] = set()
        receipts_observed = production["automatic_delete"] and (
            demand.rate_remaining is None
            or demand.rate_remaining >= MINIMUM_GITHUB_RATE_REMAINING
        )
        if receipts_observed:
            (
                drained,
                quarantined,
                idle_confirmed,
                admission_closed,
            ) = observe_drained_instances(
                instances, policy, production, observer, timer
            )
        # Receipt observation and provider operations can take longer than a
        # whole timer interval. Refresh GitHub immediately before any drain or
        # delete decision so scale-down never acts on data older than
        # DEMAND_REFRESH_MAX_AGE; a quick tick may still use its opening read.
        with timer.phase("github_demand_refresh"):
            demand = observer.observe(
                production["workflow_label"],
                production["scale_set_prefix"] + "-",
                dt.datetime.now(dt.timezone.utc),
                snapshot,
                DEMAND_REFRESH_MAX_AGE,
                # The opening read's data passes only while still fresh by age.
                newer_than=opening_at,
            )
        demand_refreshed_at = demand.observed_at or dt.datetime.now(dt.timezone.utc)
        ledger = track_quarantine(
            prior, instances, quarantined, idle_confirmed, demand_refreshed_at
        )
        # Symmetric with track_quarantine above, which is handed an empty
        # `quarantined` on a skipped tick and so returns an empty ledger: a
        # gap in observation restarts both clocks rather than crediting time
        # nobody watched.
        stall_ledger = track_drain_stalls(
            prior,
            instances,
            stalled_drain_slots(
                instances,
                demand,
                drained,
                quarantined,
                receipts_observed,
                idle_confirmed,
                admission_closed,
            ),
            demand_refreshed_at,
        )
        request_ledger = track_drain_stalls(
            prior,
            instances,
            unanswered_drain_request_slots(
                instances, demand, drained, receipts_observed, quarantined
            ),
            demand_refreshed_at,
            key="drain_request_since",
        )
        decision_at = dt.datetime.now(dt.timezone.utc)
        scale_up_state = (
            read_prior_state(args.scale_up_state) if args.scale_up_state else None
        )
        maintenance_last_demand = last_demand_seen(
            demand,
            demand_refreshed_at,
            decision_at,
            prior,
            fast_scale_up,
            scale_up_state,
        )
        maintenance_floor = maintenance_warm_floor(
            production, decision_at, maintenance_last_demand
        )
        history = demand_history(
            int(production["scale_down_hold_minutes"]),
            decision_at,
            [
                (demand_refreshed_at, demand.queued + demand.active_owned),
                (
                    opening_at,
                    fast_scale_up["queued_jobs"] + fast_scale_up["active_owned_jobs"],
                ),
                state_demand_sample(scale_up_state),
            ],
            prior,
            scale_up_state,
        )
        decision = decide(
            production,
            demand,
            instances,
            drained,
            quarantined,
            ledger,
            demand_refreshed_at,
            drain_stall_ledger=stall_ledger,
            drain_request_ledger=request_ledger,
            floor=maintenance_floor,
            hold=scale_down_hold(history),
        )
        decision["demand_history"] = history
        decision["warm_pool"] = warm_pool_record(
            maintenance_floor, maintenance_last_demand, decision_at
        )
        decision["pool"] = args.pool
        record_repositories(decision, observer)
        decision["demand_by_repository"] = demand.by_repository
        decision["observed_at"] = demand_refreshed_at.isoformat().replace(
            "+00:00", "Z"
        )
        decision["demand_snapshot_age_ms"] = demand.snapshot_age_ms
        decision["demand_snapshot_wait_ms"] = demand.snapshot_wait_ms
        decision["initial_demand"] = {
            "observed_at": opening_at.isoformat().replace("+00:00", "Z"),
            "snapshot_age_ms": fast_scale_up["demand_snapshot_age_ms"],
            "queued_jobs": fast_scale_up["queued_jobs"],
            "active_owned_jobs": fast_scale_up["active_owned_jobs"],
        }
        decision["admission_closed_slots"] = sorted(admission_closed)
        decision["fast_scale_up"] = fast_scale_up
        decision["loop"] = "maintenance" if args.maintenance_only else "combined"
        decision["mode"] = "apply" if args.apply else "dry-run"
        decision["result"] = {
            "deleted": [],
            "created": fast_scale_up["result"]["created"],
        }
        if args.maintenance_only and decision["create_slots"]:
            decision["scale_up_deferred_to_fast_loop"] = decision["create_slots"]
            decision["create_slots"] = []
            if decision["delete_slots"]:
                decision["action"] = "delete"
            elif decision["drain_request_slots"]:
                decision["action"] = "drain"
            else:
                decision["action"] = "none"
        create_before_delete = bool(decision["create_slots"]) and not (
            set(decision["create_slots"]) & set(decision["delete_slots"])
        )
        if create_before_delete:
            with timer.phase("create"):
                created = admit_and_create(decision, policy, args)
            decision["result"]["created"].extend(created["created"])
        # Request drains before deleting: a delete takes ~26s and only frees a
        # host that already stopped serving, while every second a drain waits
        # is a second an idle host bills. The fast and maintenance loops
        # intentionally use separate locks, so re-read the fast loop's atomic
        # state immediately before the request. A newer nonzero observation
        # vetoes only this stale scale-down decision.
        if args.scale_up_state:
            suppress_stale_scale_down(decision, read_prior_state(args.scale_up_state))
        if args.apply and decision["drain_request_slots"]:
            with timer.phase("drain_request"):
                drain_result = request_drain_slots(
                    instances,
                    decision["drain_request_slots"],
                    policy,
                    (
                        (
                            lambda: not newer_peer_has_demand(
                                decision, read_prior_state(args.scale_up_state)
                            )
                        )
                        if args.scale_up_state
                        else None
                    ),
                )
            decision["result"].update(drain_result)
            if drain_result["drain_deferred_slots"]:
                decision["scale_down_suppressed_reason"] = "newer_scale_up_demand"
                decision["drain_request_slots"] = [
                    item["slot"] for item in drain_result["drain_requested"]
                ]
                if decision["delete_slots"] and decision["create_slots"]:
                    decision["action"] = "replace"
                elif decision["delete_slots"]:
                    decision["action"] = "delete"
                elif decision["create_slots"]:
                    decision["action"] = "create"
                elif decision["drain_request_slots"]:
                    decision["action"] = "drain"
                else:
                    decision["action"] = "none"
        if args.apply and decision["delete_slots"]:
            unanswered_slots = set(decision["expired_drain_request_slots"]) - set(
                decision["expired_drain_stall_slots"]
            )
            fallback_slots = set(decision["expired_drain_stall_slots"]) | unanswered_slots
            receipt_proven_slots = [
                slot for slot in decision["delete_slots"] if slot not in fallback_slots
            ]
            if receipt_proven_slots:
                with timer.phase("delete"):
                    deleted = delete_slots(
                        instances,
                        receipt_proven_slots,
                        args.policy,
                        args.lifecycle_script,
                        args.pool,
                    )
                decision["result"]["deleted"].extend(deleted["deleted"])

            fallback_deleted: list[int] = []
            for slot in sorted(fallback_slots & set(decision["delete_slots"])):
                # Capture before the guard, never between it and the delete:
                # this guest still accepts jobs, so the guard must be the last
                # read before the VM goes. A capture the guard then wastes is
                # cheap; a slow serial read after it widens the race.
                target = next(
                    (item for item in instances if item.slot == slot), None
                )
                if slot in unanswered_slots and target is not None:
                    with timer.phase("unanswered_drain_evidence"):
                        decision.setdefault("unanswered_drain_evidence", {})[
                            str(slot)
                        ] = capture_unanswered_drain_evidence(target, policy)
                with timer.phase("fallback_delete_guard"):
                    reason = fallback_delete_block_reason(
                        observer,
                        production,
                        slot,
                        decision,
                        (
                            read_prior_state(args.scale_up_state)
                            if args.scale_up_state
                            else None
                        ),
                        unanswered_drain=slot in unanswered_slots,
                    )
                if reason is not None:
                    defer_fallback_delete(decision, slot, reason)
                    continue
                with timer.phase("fallback_delete"):
                    deleted = delete_slots(
                        instances,
                        [slot],
                        args.policy,
                        args.lifecycle_script,
                        args.pool,
                    )
                decision["result"]["deleted"].extend(deleted["deleted"])
                fallback_deleted.append(slot)
            decision["delete_slots"] = receipt_proven_slots + fallback_deleted
            if not decision["delete_slots"]:
                if decision["create_slots"]:
                    decision["action"] = "create"
                elif decision["drain_request_slots"]:
                    decision["action"] = "drain"
                else:
                    decision["action"] = "none"
        if decision["create_slots"] and not create_before_delete:
            with timer.phase("create"):
                created = admit_and_create(decision, policy, args)
            decision["result"]["created"].extend(created["created"])
        decision["duration_ms"] = int((time.monotonic() - started) * 1000)
        decision["phases"] = timer.phases
        atomic_write_json(args.state, decision)
        emit_decision_summary(decision)
        print(json.dumps(decision, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(
            json.dumps(
                {
                    "controller_version": VERSION,
                    "result": "error",
                    "error": bounded_text(str(error)),
                }
            ),
            file=sys.stderr,
        )
        raise SystemExit(1)
