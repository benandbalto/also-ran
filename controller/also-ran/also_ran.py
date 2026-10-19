#!/usr/bin/env python3
"""also-ran: a fail-closed observer for also-ran runner host capacity."""

from __future__ import annotations

from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET

import argparse
import contextlib
import dataclasses
import datetime as dt
import fcntl
import hashlib
import hmac
import http.client
import json
import math
import os
import pathlib
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Iterator


VERSION = "0.1.0"
USER_AGENT = f"also-ran/{VERSION}"
DEFAULT_CONFIG = "/etc/also-ran/config.json"
DEFAULT_STATE = "/var/lib/also-ran/state.json"
DEFAULT_LOCK = "/run/also-ran/lock"


class ObservationError(RuntimeError):
    """Raised when a provider observation is incomplete."""


REPOSITORY_PATTERN = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9-]{0,38}/[A-Za-z0-9._-]{1,100}"
)
RUNNER_SCOPES = ("repository", "organization")
# GitHub reads in flight at once across every also-ran process on the host.
# Slot 0 is kept for priority reads (the demand fetch), so receipt checks,
# telemetry and observers can never take every slot from scale-up.
GITHUB_CONCURRENCY_DEFAULT = 4
GITHUB_CONCURRENCY_MAXIMUM = 8
# Space requests one second apart only once the hourly allowance runs low.
GITHUB_PACING_BELOW_REMAINING = 2000
GITHUB_FLOOR_REMAINING = 750


def configured_repositories(
    config: dict[str, Any], override: str | None = None
) -> tuple[str, ...]:
    """The repositories whose Actions jobs this controller serves.

    `github_repositories` is the list form. The single `github_repository`
    key (and the `--github-repository` flag passed in as `override`) keep
    working so an existing deployment reads the same one repository it always
    did. Naming the set in two places is refused rather than merged: a second
    source that silently wins is how a pool starts serving a repository nobody
    reviewed. There is no built-in default repository; it is configuration.
    """
    sources = [
        key for key in ("github_repositories", "github_repository") if key in config
    ]
    if override is not None:
        sources.append("--github-repository")
    if len(sources) > 1:
        raise RuntimeError(
            "configure repositories in one place, not " + " and ".join(sources)
        )
    if not sources:
        raise RuntimeError("no GitHub repository is configured")
    if override is not None:
        values: Any = [override]
    elif "github_repositories" in config:
        values = config["github_repositories"]
    else:
        values = [config["github_repository"]]
    if (
        not isinstance(values, list)
        or not values
        or any(not isinstance(value, str) for value in values)
    ):
        raise RuntimeError("github_repositories must be a non-empty list of names")
    for value in values:
        if not REPOSITORY_PATTERN.fullmatch(value):
            raise RuntimeError(f"invalid GitHub repository name: {value!r}")
    if len({value.lower() for value in values}) != len(values):
        raise RuntimeError("github_repositories lists a repository twice")
    return tuple(values)


OWNER_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9-]{0,38}")


def configured_retired_repositories(
    config: dict[str, Any], repositories: tuple[str, ...]
) -> tuple[str, ...]:
    """Repositories no longer served whose hosts may still be running a job.

    Retiring is how a repository leaves: its queue stops counting as demand,
    but its running jobs, runner registrations and drain receipts still count
    as proof that a host is busy. Drop it from this list only once the pool
    holds no host that could still be running one of its jobs.
    """
    values = config.get("github_retired_repositories", [])
    if not isinstance(values, list) or any(
        not isinstance(value, str) or not REPOSITORY_PATTERN.fullmatch(value)
        for value in values
    ):
        raise RuntimeError("github_retired_repositories must list repository names")
    served = {repository.lower() for repository in repositories}
    if len({value.lower() for value in values}) != len(values) or served & {
        value.lower() for value in values
    }:
        raise RuntimeError(
            "github_retired_repositories must be distinct and not also served"
        )
    return tuple(values)


def configured_retired_organizations(config: dict[str, Any]) -> tuple[str, ...]:
    """Organizations whose runner list is still read after leaving org scope,
    until no host registered there remains."""
    values = config.get("github_retired_runner_organizations", [])
    if (
        not isinstance(values, list)
        or any(
            not isinstance(value, str) or not OWNER_PATTERN.fullmatch(value)
            for value in values
        )
        or len({value.lower() for value in values}) != len(values)
    ):
        raise RuntimeError(
            "github_retired_runner_organizations must list distinct owners"
        )
    return tuple(values)


def configured_runner_scope(
    config: dict[str, Any], repositories: tuple[str, ...]
) -> str:
    """Where the served runners are registered: per repository or the org.

    An organization-scoped runner does not appear in any repository's runner
    list, so reading the wrong list would report a live runner as absent.
    The scope is therefore configured explicitly, and organization scope
    requires every served repository to belong to that one organization.
    """
    scope = config.get("github_runner_scope", "repository")
    if scope not in RUNNER_SCOPES:
        raise RuntimeError(f"github_runner_scope must be one of {RUNNER_SCOPES}")
    if scope == "organization" and len(
        {repository.split("/", 1)[0].lower() for repository in repositories}
    ) != 1:
        raise RuntimeError(
            "organization runner scope requires one owner for every repository"
        )
    return scope


@dataclasses.dataclass(frozen=True)
class Policy:
    workflow_label: str
    burst_runner_prefix: str
    additional_burst_runner_prefixes: tuple[str, ...] = ()
    oldest_queue_seconds: int = 45
    consecutive_observations: int = 2
    queued_job_count_immediate: int = 2
    continuous_idle_minutes: int = 30
    maximum_extra_hosts: int = 1


@dataclasses.dataclass(frozen=True)
class Server:
    server_id: int | str
    name: str
    status: str
    labels: dict[str, str]
    created: str | None = None


@dataclasses.dataclass(frozen=True)
class Observation:
    observed_at: str
    queued_count: int
    oldest_queue_seconds: int
    active_burst_jobs: int
    servers: tuple[Server, ...]
    github_complete: bool = True
    hetzner_complete: bool = True
    azure_complete: bool = True
    github_rate_remaining: int | None = None
    github_error: str | None = None
    hetzner_error: str | None = None
    azure_error: str | None = None

    @property
    def complete(self) -> bool:
        return self.github_complete and self.hetzner_complete and self.azure_complete


def initial_state() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "state": "ABSENT",
        "pressure_observations": 0,
        "idle_since": None,
        "server_id": None,
        "last_observation": None,
        "last_decision": None,
    }


def parse_time(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def decide(
    policy: Policy,
    previous: dict[str, Any],
    observation: Observation,
    required_labels: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the next durable state and one deterministic decision event."""
    state = {**initial_state(), **previous}
    prior_name = state["state"]
    now = parse_time(observation.observed_at)
    decision: dict[str, Any] = {
        "controller_version": VERSION,
        "previous_state": prior_name,
        "next_state": prior_name,
        "action": "none",
        "suppression_reason": None,
        "queued_count": observation.queued_count,
        "oldest_queue_seconds": observation.oldest_queue_seconds,
        "active_burst_jobs": observation.active_burst_jobs,
        "server_id": None,
        "observed_at": observation.observed_at,
        "observation_complete": observation.complete,
        "github_rate_remaining": observation.github_rate_remaining,
    }

    if not observation.complete:
        decision["suppression_reason"] = "observation_incomplete"
        decision["github_error"] = observation.github_error
        decision["hetzner_error"] = observation.hetzner_error
        return _finish(state, decision, observation)

    servers = list(observation.servers)
    if len(servers) > 1:
        state["state"] = "ERROR"
        decision.update(
            next_state="ERROR", suppression_reason="duplicate_burst_servers"
        )
        return _finish(state, decision, observation)

    if servers:
        server = servers[0]
        decision["server_id"] = server.server_id
        if any(
            server.labels.get(key) != value for key, value in required_labels.items()
        ):
            state["state"] = "ERROR"
            decision.update(
                next_state="ERROR", suppression_reason="server_identity_mismatch"
            )
            return _finish(state, decision, observation)
        recorded_id = state.get("server_id")
        if recorded_id not in (None, server.server_id):
            state["state"] = "ERROR"
            decision.update(
                next_state="ERROR", suppression_reason="unrecognized_server_id"
            )
            return _finish(state, decision, observation)

        state["server_id"] = server.server_id
        state["pressure_observations"] = 0
        if server.status == "off":
            state["state"] = "ERROR"
            decision.update(next_state="ERROR", suppression_reason="burst_server_off")
        elif server.status not in {"running", "starting", "initializing"}:
            state["state"] = "ERROR"
            decision.update(
                next_state="ERROR", suppression_reason="unknown_server_status"
            )
        elif server.status in {"starting", "initializing"}:
            state["state"] = "CREATING"
            decision["next_state"] = "CREATING"
        elif observation.active_burst_jobs > 0:
            state["state"] = "BUSY"
            state["idle_since"] = None
            decision["next_state"] = "BUSY"
        elif observation.queued_count > 0:
            state["state"] = "AVAILABLE"
            state["idle_since"] = None
            decision["next_state"] = "AVAILABLE"
        else:
            if not state.get("idle_since"):
                state["idle_since"] = observation.observed_at
            state["state"] = "IDLE"
            decision["next_state"] = "IDLE"
            idle_seconds = max(
                0, int((now - parse_time(state["idle_since"])).total_seconds())
            )
            decision["idle_seconds"] = idle_seconds
            if idle_seconds >= policy.continuous_idle_minutes * 60:
                decision["suppression_reason"] = (
                    "automatic_deletion_not_implemented_without_drain"
                )
        return _finish(state, decision, observation)

    state["server_id"] = None
    state["idle_since"] = None
    pressure_sample = observation.oldest_queue_seconds >= policy.oldest_queue_seconds
    if pressure_sample:
        state["pressure_observations"] = int(state.get("pressure_observations", 0)) + 1
    else:
        state["pressure_observations"] = 0

    immediate = observation.queued_count >= policy.queued_job_count_immediate
    sustained = state["pressure_observations"] >= policy.consecutive_observations
    state["state"] = "ABSENT"
    decision["next_state"] = "ABSENT"
    if policy.maximum_extra_hosts == 0:
        decision["suppression_reason"] = "maximum_extra_hosts_zero"
    elif immediate or sustained:
        decision["action"] = "create"
        decision["suppression_reason"] = "create_only_not_enabled"
    return _finish(state, decision, observation)


def _finish(
    state: dict[str, Any], decision: dict[str, Any], observation: Observation
) -> tuple[dict[str, Any], dict[str, Any]]:
    state["last_observation"] = dataclasses.asdict(observation)
    state["last_decision"] = decision
    return state, decision


class JsonClient:
    def __init__(self, token: str, base_url: str, auth_scheme: str = "Bearer") -> None:
        self.token = token
        self.base_url = base_url.rstrip("/")
        self.auth_scheme = auth_scheme
        # Response metadata is per thread: one client may serve several
        # concurrent reads, and each caller inspects its own response.
        self._local = threading.local()

    @property
    def last_headers(self) -> dict[str, str]:
        if not hasattr(self._local, "headers"):
            self._local.headers = {}
        return self._local.headers

    @last_headers.setter
    def last_headers(self, value: dict[str, str]) -> None:
        self._local.headers = value

    @property
    def last_status(self) -> int | None:
        return getattr(self._local, "status", None)

    @last_status.setter
    def last_status(self, value: int | None) -> None:
        self._local.status = value

    @contextlib.contextmanager
    def prioritized(self) -> Iterator[None]:
        """Let reads on this thread use the slot kept for scale-up demand."""
        previous = getattr(self._local, "priority", False)
        self._local.priority = True
        try:
            yield
        finally:
            self._local.priority = previous

    def get(self, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        budget_path = os.environ.get("ALSO_RAN_GITHUB_BUDGET")
        if self.base_url != "https://api.github.com" or not budget_path:
            return self._get(path, query)
        # One budget across broker pools, fast/maintenance loops and telemetry.
        # A few reads may be in flight at once (one slot lock each); the budget
        # file itself is only locked briefly to check and record it, so a slow
        # response never holds up the others. The file contains no credentials.
        with self._budget_slot(budget_path):
            start_at = self._admit(budget_path)
            delay = start_at - time.time()
            if delay > 0:
                time.sleep(delay)
            self.last_headers = {}
            self.last_status = None
            try:
                return self._get(path, query)
            finally:
                self._record(budget_path)

    @contextlib.contextmanager
    def _budget_slot(self, budget_path: str) -> Iterator[None]:
        slots = github_concurrency()
        first = 0 if getattr(self._local, "priority", False) else 1
        handles = []
        try:
            for slot in range(first, slots):
                handles.append(open(f"{budget_path}.slot{slot}.lock", "a", encoding="utf-8"))
            while True:
                for handle in handles:
                    try:
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        continue
                    try:
                        yield
                    finally:
                        fcntl.flock(handle, fcntl.LOCK_UN)
                    return
                time.sleep(0.05)
        finally:
            for handle in handles:
                handle.close()

    @staticmethod
    def _read_budget(budget_path: str) -> tuple[float, float, dict[str, Any]]:
        try:
            state = json.loads(pathlib.Path(budget_path).read_text())
        except FileNotFoundError:
            state = {}
        except (ValueError, OSError) as error:
            raise ObservationError("GitHub budget state is unreadable") from error
        try:
            blocked = float(state.get("blocked_until", 0))
            next_request = float(state.get("next_request_at", 0))
            if not math.isfinite(blocked) or not math.isfinite(next_request):
                raise ValueError("non-finite budget time")
        except (AttributeError, TypeError, ValueError) as error:
            raise ObservationError("GitHub budget state is malformed") from error
        return blocked, next_request, state

    def _admit(self, budget_path: str) -> float:
        """Check the shared budget and reserve a start time for one request."""
        with open(budget_path + ".lock", "a", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            blocked, next_request, state = self._read_budget(budget_path)
            now = time.time()
            if blocked > now:
                raise ObservationError("GitHub reads paused by shared backoff")
            try:
                remaining = int(state.get("remaining") or GITHUB_PACING_BELOW_REMAINING)
            except (TypeError, ValueError):
                remaining = 0
            if remaining >= GITHUB_PACING_BELOW_REMAINING:
                return now
            # Low allowance: bound this controller's share even when other
            # clients use the same account, one request per second.
            start_at = max(now, min(next_request, now + 2))
            atomic_write_json(budget_path, {**state, "next_request_at": start_at + 1})
            return start_at

    def _record(self, budget_path: str) -> None:
        with open(budget_path + ".lock", "a", encoding="utf-8") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            now = time.time()
            headers = self.last_headers
            blocked_until = 0.0
            try:
                remaining = int(headers.get("x-ratelimit-remaining", "5000"))
                reset = float(headers.get("x-ratelimit-reset", now + 60))
                if remaining < GITHUB_FLOOR_REMAINING:
                    blocked_until = max(now + 60, reset + 1)
                if "retry-after" in headers:
                    blocked_until = max(blocked_until, now + float(headers["retry-after"]))
            except ValueError:
                blocked_until = now + 60
            try:
                previous_blocked, next_request, _ = self._read_budget(budget_path)
            except ObservationError:
                previous_blocked, next_request = 0.0, 0.0
            # A concurrent read may already have opened a backoff; keep it.
            if previous_blocked > now:
                blocked_until = max(blocked_until, previous_blocked)
            atomic_write_json(budget_path, {
                "blocked_until": blocked_until,
                "next_request_at": next_request,
                "remaining": headers.get("x-ratelimit-remaining"),
                "reset": headers.get("x-ratelimit-reset"),
            })

    def _get(self, path: str, query: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        cache_path, cached = self._conditional_cache(url)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"{self.auth_scheme} {self.token}",
                "User-Agent": USER_AGENT,
                "X-GitHub-Api-Version": "2022-11-28",
            },
            method="GET",
        )
        if cached is not None:
            request.add_header("If-None-Match", cached["etag"])
        # A GET is idempotent, so a body cut off mid-read (IncompleteRead, a
        # reset or a dropped keep-alive) is retried once before it counts as a
        # failure. Timeouts are not retried: another 20s would hold the shared
        # budget lock that every also-ran process waits on.
        with self._representation_lock(cache_path):
            if cache_path is not None:
                # Re-read under the lock: a concurrent read of this URL may
                # just have stored the representation this one can revalidate.
                cache_path, cached = self._conditional_cache(url)
                # Always drop the first read's validator: a concurrent prune may
                # have removed the entry, and a 304 needs the body it names.
                request.remove_header("If-none-match")
                if cached is not None:
                    request.add_header("If-None-Match", cached["etag"])
            for attempt in range(2):
                try:
                    return self._get_once(request, path, cache_path, cached)
                except (http.client.HTTPException, ConnectionError) as error:
                    if attempt:
                        self.last_headers["retry-after"] = "60"
                        raise ObservationError(f"GET {path} failed: {error!r}") from error
        raise AssertionError("unreachable")

    @staticmethod
    @contextlib.contextmanager
    def _representation_lock(cache_path: pathlib.Path | None) -> Iterator[None]:
        """Serialize reads of one URL so concurrent callers revalidate.

        Different URLs proceed in parallel. One of 16 lock stripes is chosen by
        the cache key, which keeps the number of lock files bounded.
        """
        if cache_path is None:
            yield
            return
        stripe = int(cache_path.stem[:2], 16) % 16
        try:
            handle = open(cache_path.parent / f"stripe{stripe:02d}.lock", "a", encoding="utf-8")
        except OSError:
            yield
            return
        with handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            yield

    def _get_once(
        self,
        request: urllib.request.Request,
        path: str,
        cache_path: pathlib.Path | None,
        cached: dict | None,
    ) -> dict[str, Any]:
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                self.last_headers = {
                    key.lower(): value for key, value in response.headers.items()
                }
                self.last_status = response.status
                payload = json.load(response)
                etag = self.last_headers.get("etag")
                if cache_path is not None and etag and isinstance(payload, dict):
                    entry = {"etag": etag, "body": payload}
                    if len(json.dumps(entry, sort_keys=True, indent=2).encode()) + 1 <= 1024 * 1024:
                        with contextlib.suppress(OSError):
                            atomic_write_json(str(cache_path), entry)
                            self._prune_conditional_cache(cache_path.parent)
                return payload
        except urllib.error.HTTPError as error:
            self.last_headers = {key.lower(): value for key, value in error.headers.items()}
            self.last_status = error.code
            if error.code == 304 and cached is not None:
                # Reuse only after GitHub confirms that this exact authorized
                # representation is current. Never serve stale data on errors.
                error.close()
                return cached["body"]
            if error.code in (403, 429):
                self.last_headers.setdefault("retry-after", "60")
            body = error.read(2048).decode("utf-8", errors="replace")
            raise ObservationError(
                f"GET {path} returned HTTP {error.code}: {body}"
            ) from error
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            self.last_headers["retry-after"] = "60"
            raise ObservationError(f"GET {path} failed: {error}") from error


    def _conditional_cache(self, url: str) -> tuple[pathlib.Path | None, dict | None]:
        budget = os.environ.get("ALSO_RAN_GITHUB_BUDGET")
        if self.base_url != "https://api.github.com" or not budget:
            return None, None
        root = pathlib.Path(budget + ".cache")
        try:
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
        except OSError:
            return None, None
        # Bind the representation to the credential without persisting it.
        key = hmac.new(self.token.encode(), (self.auth_scheme + url).encode(), hashlib.sha256).hexdigest()
        path = root / (key + ".json")
        try:
            if path.stat().st_size > 1024 * 1024:
                return path, None
            cached = json.loads(path.read_text())
        except (OSError, ValueError):
            return path, None
        if (not isinstance(cached, dict)
                or not isinstance(cached.get("etag"), str)
                or not cached["etag"] or len(cached["etag"]) > 512
                or "\r" in cached["etag"] or "\n" in cached["etag"]
                or not isinstance(cached.get("body"), dict)):
            return path, None
        return path, cached

    @staticmethod
    def _prune_conditional_cache(root: pathlib.Path) -> None:
        # Writers are serialized per URL only; a concurrent prune may remove a
        # file first, which the caller tolerates as an OSError.
        entries = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime)
        for path in entries[:-256]:
            path.unlink()


class GitHubObserver:
    def __init__(
        self,
        client: JsonClient,
        repositories: str | tuple[str, ...],
        max_pages: int = 10,
        max_active_runs: int = 50,
    ) -> None:
        self.client = client
        self.repositories = (
            (repositories,) if isinstance(repositories, str) else tuple(repositories)
        )
        self.max_pages = max_pages
        self.max_active_runs = max_active_runs

    def observe(
        self, policy: Policy, now: dt.datetime
    ) -> tuple[int, int, int, int | None]:
        # Keyed by (repository, run id); the safety cap applies per repository
        # so adding a repository cannot push an existing one over it.
        runs: dict[tuple[str, int], dict[str, Any]] = {}
        created_filter = ">=" + (now - dt.timedelta(days=1)).date().isoformat()
        for repository in self.repositories:
            repository_runs: dict[int, dict[str, Any]] = {}
            for status in ("queued", "in_progress"):
                for run in self._pages(
                    f"/repos/{repository}/actions/runs",
                    "workflow_runs",
                    {"status": status, "created": created_filter},
                ):
                    repository_runs[int(run["id"])] = run
            if len(repository_runs) > self.max_active_runs:
                raise ObservationError(
                    f"GitHub returned {len(repository_runs)} active runs for "
                    f"{repository}; safe inspection cap is {self.max_active_runs}"
                )
            for run_id, run in repository_runs.items():
                runs[(repository, run_id)] = run

        queued_ages: list[int] = []
        active_burst = 0
        for (repository, run_id), run in runs.items():
            for job in self._pages(
                f"/repos/{repository}/actions/runs/{run_id}/jobs",
                "jobs",
                {"filter": "latest"},
            ):
                labels = set(job.get("labels") or [])
                if policy.workflow_label not in labels:
                    continue
                status = job.get("status")
                if status == "queued":
                    queued_at = job.get("created_at") or run.get("created_at")
                    if queued_at:
                        queued_ages.append(
                            max(0, int((now - parse_time(queued_at)).total_seconds()))
                        )
                prefixes = (
                    policy.burst_runner_prefix,
                    *policy.additional_burst_runner_prefixes,
                )
                if status == "in_progress" and str(
                    job.get("runner_name") or ""
                ).startswith(prefixes):
                    active_burst += 1

        remaining = self.client.last_headers.get("x-ratelimit-remaining")
        return (
            len(queued_ages),
            max(queued_ages, default=0),
            active_burst,
            int(remaining) if remaining else None,
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
        raise ObservationError(
            f"GitHub pagination exceeded {self.max_pages} pages for {path}"
        )


class HetznerObserver:
    def __init__(
        self, client: JsonClient, label_selector: str, max_pages: int = 10
    ) -> None:
        self.client = client
        self.label_selector = label_selector
        self.max_pages = max_pages

    def observe(self) -> tuple[Server, ...]:
        result: list[Server] = []
        for page in range(1, self.max_pages + 1):
            payload = self.client.get(
                "/servers",
                {"label_selector": self.label_selector, "per_page": 50, "page": page},
            )
            servers = payload.get("servers")
            if not isinstance(servers, list):
                raise ObservationError("Hetzner response omitted servers")
            for server in servers:
                result.append(
                    Server(
                        server_id=int(server["id"]),
                        name=str(server["name"]),
                        status=str(server["status"]),
                        labels={
                            str(k): str(v)
                            for k, v in (server.get("labels") or {}).items()
                        },
                        created=server.get("created"),
                    )
                )
            pagination = payload.get("meta", {}).get("pagination", {})
            if pagination.get("next_page") is None:
                return tuple(result)
        raise ObservationError(f"Hetzner pagination exceeded {self.max_pages} pages")


class AzureObserver:
    def __init__(
        self,
        client: JsonClient,
        subscription_id: str,
        resource_group: str,
        required_tags: dict[str, str],
    ) -> None:
        self.client = client
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.required_tags = required_tags

    def observe(self) -> tuple[Server, ...]:
        base = (
            f"/subscriptions/{self.subscription_id}/resourceGroups/{self.resource_group}"
            "/providers/Microsoft.Compute/virtualMachines"
        )
        payload = self.client.get(base, {"api-version": "2024-11-01"})
        virtual_machines = payload.get("value")
        if not isinstance(virtual_machines, list):
            raise ObservationError("Azure response omitted virtual machines")
        result: list[Server] = []
        for vm in virtual_machines:
            tags = {
                str(key): str(value) for key, value in (vm.get("tags") or {}).items()
            }
            if any(tags.get(key) != value for key, value in self.required_tags.items()):
                continue
            instance = self.client.get(
                f"{base}/{urllib.parse.quote(str(vm['name']))}/instanceView",
                {"api-version": "2024-11-01"},
            )
            codes = [str(item.get("code", "")) for item in instance.get("statuses", [])]
            status = self._status(
                codes, str(vm.get("properties", {}).get("provisioningState", ""))
            )
            result.append(
                Server(
                    server_id=str(vm["id"]),
                    name=str(vm["name"]),
                    status=status,
                    labels=tags,
                    created=vm.get("properties", {}).get("timeCreated"),
                )
            )
        return tuple(result)

    @staticmethod
    def _status(codes: list[str], provisioning_state: str) -> str:
        power = next(
            (
                code.removeprefix("PowerState/")
                for code in codes
                if code.startswith("PowerState/")
            ),
            None,
        )
        if power == "running":
            return "running"
        if power in {"starting", "creating"}:
            return "starting"
        if power in {"stopped", "deallocated", "deallocating"}:
            return "off"
        if provisioning_state in {"Creating", "Updating"}:
            return "initializing"
        return power or provisioning_state.lower() or "unknown"


def azure_access_token(config: dict[str, Any]) -> str:
    tenant = urllib.parse.quote(str(config["tenant_id"]), safe="")
    body = urllib.parse.urlencode(
        {
            "client_id": config["client_id"],
            "client_secret": read_secret(config["client_secret_file"]),
            "grant_type": "client_credentials",
            "scope": "https://management.azure.com/.default",
        }
    ).encode()
    request = urllib.request.Request(
        f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token",
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            token = json.load(response).get("access_token")
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
        raise ObservationError(f"Azure token request failed: {error}") from error
    if not token:
        raise ObservationError("Azure token response omitted access_token")
    return str(token)


def read_secret(path: str) -> str:
    value = pathlib.Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise RuntimeError(f"credential file is empty: {path}")
    return value


def load_json(
    path: str, default: Callable[[], dict[str, Any]] | None = None
) -> dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except FileNotFoundError:
        if default is not None:
            return default()
        raise
    if not isinstance(payload, dict):
        raise RuntimeError(f"expected a JSON object in {path}")
    return payload


def atomic_write_json(
    path: str,
    payload: dict[str, Any],
    before_replace: Callable[[], None] | None = None,
) -> None:
    """Write `payload` durably, then swap it in.

    `before_replace` runs once the bytes are on disk but before they become
    the state: a full disk fails before it runs, and if it raises, the old
    state stays in place.
    """
    destination = pathlib.Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=destination.name + ".", dir=destination.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        if before_replace is not None:
            before_replace()
        os.replace(temporary, destination)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def github_concurrency() -> int:
    try:
        value = int(os.environ.get("ALSO_RAN_GITHUB_CONCURRENCY", GITHUB_CONCURRENCY_DEFAULT))
    except ValueError:
        return GITHUB_CONCURRENCY_DEFAULT
    return max(2, min(value, GITHUB_CONCURRENCY_MAXIMUM))


@contextlib.contextmanager
def exclusive_lock(path: str) -> Iterator[None]:
    with open(path, "w", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("another also-ran invocation holds the lock") from error
        yield


def load_policy(config: dict[str, Any]) -> Policy:
    queue = config["scale_up"]
    down = config["scale_down"]
    return Policy(
        workflow_label=config["workflow_label"],
        burst_runner_prefix=config["burst_runner_prefix"],
        additional_burst_runner_prefixes=tuple(
            str(value) for value in config.get("additional_burst_runner_prefixes", [])
        ),
        oldest_queue_seconds=int(queue["oldest_matching_queue_seconds"]),
        consecutive_observations=int(queue["consecutive_observations"]),
        queued_job_count_immediate=int(queue["queued_job_count_immediate"]),
        continuous_idle_minutes=int(down["continuous_idle_minutes"]),
        maximum_extra_hosts=int(config["maximum_extra_hosts"]),
    )


def validate_config(config: dict[str, Any]) -> None:
    if config.get("mode") != "observe":
        raise RuntimeError(
            "version 0.1.0 supports only mode=observe; provider mutations are unavailable"
        )
    if config.get("automatic_delete", False):
        raise RuntimeError(
            "automatic deletion is blocked until the graceful-drain acceptance gate passes"
        )
    policy = load_policy(config)
    configured_repositories(config)
    if policy.maximum_extra_hosts not in (0, 1):
        raise RuntimeError("maximum_extra_hosts must be 0 or 1")
    required = config.get("required_server_labels")
    expected = {
        "managed-by": "runner-autoscaler",
        "fleet": FLEET,
        "lifecycle": "burst",
        "autoscale-slot": "burst-01",
    }
    if required != expected:
        raise RuntimeError(
            "required_server_labels must exactly match the RFC burst-01 identity"
        )
    azure = config.get("azure")
    if azure:
        expected_azure = {
            "tenant_id": "00000000-0000-4000-8000-000000000001",
            "subscription_id": "00000000-0000-4000-8000-000000000002",
            "resource_group": PROJECT_ID,
        }
        if any(azure.get(key) != value for key, value in expected_azure.items()):
            raise RuntimeError(
                "Azure observer boundary does not match the approved runner project"
            )
        if not azure.get("client_id") or not azure.get("client_secret_file"):
            raise RuntimeError("Azure observer credentials are incomplete")


def collect_observation(
    config: dict[str, Any], policy: Policy, now: dt.datetime
) -> Observation:
    github_complete = True
    hetzner_complete = True
    github_error = None
    hetzner_error = None
    azure_error = None
    queued_count = oldest = active = 0
    rate_remaining = None
    servers: tuple[Server, ...] = ()
    try:
        github = GitHubObserver(
            JsonClient(
                read_secret(config["github_token_file"]), "https://api.github.com"
            ),
            configured_repositories(config),
        )
        queued_count, oldest, active, rate_remaining = github.observe(policy, now)
    except (ObservationError, OSError, RuntimeError) as error:
        github_complete = False
        github_error = str(error)
    try:
        hetzner = HetznerObserver(
            JsonClient(
                read_secret(config["hetzner_token_file"]),
                "https://api.hetzner.cloud/v1",
            ),
            ",".join(
                f"{key}={value}"
                for key, value in config["required_server_labels"].items()
            ),
        )
        servers = hetzner.observe()
    except (ObservationError, OSError, RuntimeError) as error:
        hetzner_complete = False
        hetzner_error = str(error)
    azure_complete = True
    azure_config = config.get("azure")
    if azure_config:
        try:
            azure = AzureObserver(
                JsonClient(
                    azure_access_token(azure_config),
                    "https://management.azure.com",
                ),
                azure_config["subscription_id"],
                azure_config["resource_group"],
                config["required_server_labels"],
            )
            servers += azure.observe()
        except (ObservationError, OSError, RuntimeError) as error:
            azure_complete = False
            azure_error = str(error)
    return Observation(
        observed_at=now.isoformat().replace("+00:00", "Z"),
        queued_count=queued_count,
        oldest_queue_seconds=oldest,
        active_burst_jobs=active,
        servers=servers,
        github_complete=github_complete,
        hetzner_complete=hetzner_complete,
        azure_complete=azure_complete,
        github_rate_remaining=rate_remaining,
        github_error=github_error,
        hetzner_error=hetzner_error,
        azure_error=azure_error,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=DEFAULT_CONFIG)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--lock", default=DEFAULT_LOCK)
    parser.add_argument(
        "--once", action="store_true", help="perform one reconciliation"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="assert that provider mutation is disabled",
    )
    parser.add_argument(
        "--status", action="store_true", help="print the last durable status"
    )
    args = parser.parse_args(argv)
    if args.status:
        print(
            json.dumps(load_json(args.state, initial_state), sort_keys=True, indent=2)
        )
        return 0
    if not args.once:
        parser.error("one of --once or --status is required")

    started = time.monotonic()
    with exclusive_lock(args.lock):
        config = load_json(args.config)
        validate_config(config)
        if not args.dry_run:
            raise RuntimeError("observe mode must be invoked with --dry-run")
        policy = load_policy(config)
        previous = load_json(args.state, initial_state)
        now = dt.datetime.now(dt.timezone.utc)
        observation = collect_observation(config, policy, now)
        state, decision = decide(
            policy, previous, observation, config["required_server_labels"]
        )
        decision["mode"] = "observe"
        decision["dry_run"] = True
        decision["duration_ms"] = int((time.monotonic() - started) * 1000)
        state["last_decision"] = decision
        atomic_write_json(args.state, state)
        print(json.dumps(decision, sort_keys=True))
        return 0 if observation.complete else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # one structured terminal error for systemd/journald
        print(
            json.dumps(
                {"controller_version": VERSION, "result": "error", "error": str(error)}
            )
        )
        raise SystemExit(1)
