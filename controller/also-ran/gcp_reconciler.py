#!/usr/bin/env python3
"""Run the GCP broker's pools and loops as independent tasks in one process.

RFC 0005 stage 2. One long-running service replaces the scale-up and
maintenance timers and the gcp_pools.py fan-out:

- a demand task refreshes the shared GitHub demand snapshot on a rate-aware
  interval and wakes the scale-up tasks when new data lands;
- each pool's scale-up task runs the broker's --scale-up-only tick on every
  new snapshot (and at least every 30 seconds), so it never fetches demand
  itself;
- each pool's maintenance task runs the --maintenance-only tick at least every
  60 seconds, so a slow drain or delete never holds up scale-up or the other
  pool. While a host is being torn down, or the scale-up tick sees idle
  surplus beyond the scale-down hold to drain, it runs right after that pool's scale-up tick instead, no
  more than every 20 seconds. Following a fresh demand snapshot lets its
  pre-drain refresh reuse that snapshot rather than spend GitHub budget.

Every tick is the unchanged broker tick with the same state, lock and snapshot
paths as the timers used, so either mode can take over from the other.
"""
from __future__ import annotations

from deployment_identity import PROJECT_ID, PROJECT_NUMBER, RESOURCE_PREFIX, FLEET

import argparse
import dataclasses
import datetime as dt
import io
import json
import os
import signal
import socket
import sys
import threading
import time
from typing import Any, Callable

import gcp_broker
from also_ran import (
    configured_repositories,
    configured_retired_organizations,
    configured_retired_repositories,
    configured_runner_scope,
    load_json,
    read_secret,
)

VERSION = "0.2.0"
SERVICE = "also-ran-gcp-reconciler"
POOLS = ("heavy", "general")
# Demand refresh cadence by remaining GitHub allowance (lowest bound first).
# The allowance belongs to the token's account and is shared with every other
# client of it, so the cadence backs off on what GitHub reports, not on what
# this process spent. 20s is about the old 30s snapshot age at the same cost.
DEMAND_INTERVALS = ((1500, 60.0), (3000, 30.0), (None, 20.0))
# Reuse a snapshot another tick fetched this recently instead of refetching.
DEMAND_REUSE = dt.timedelta(seconds=5)
SCALE_UP_MAX_WAIT = 30.0
MAINTENANCE_INTERVAL = 60.0
# The tightest maintenance cadence, used while a pool has teardown work: a
# drain to request, a drain receipt to collect or a delete to make. Each wait
# is a wait an idle host bills for.
TEARDOWN_INTERVAL = 20.0
MINIMUM_GAP = 5.0
FAILURE_BACKOFF_FIRST = 15.0
FAILURE_BACKOFF_MAXIMUM = 300.0
# The old TimeoutStartSec of the timer-driven units.
TICK_DEADLINE = 15 * 60.0
WATCHDOG_PERIOD = 20.0
LOCK_BUSY = "another also-ran invocation holds the lock"


class LineWriter(io.TextIOBase):
    """Write whole lines atomically so concurrent tasks never interleave JSON."""

    def __init__(self, target: Any) -> None:
        self.target = target
        self.lock = threading.Lock()
        self.local = threading.local()

    def writable(self) -> bool:
        return True

    def write(self, text: str) -> int:
        buffer = getattr(self.local, "buffer", "") + text
        lines, newline, rest = buffer.rpartition("\n")
        self.local.buffer = rest
        if newline:
            with self.lock:
                self.target.write(lines + "\n")
                self.target.flush()
        return len(text)

    def flush(self) -> None:
        with self.lock:
            self.target.flush()


def emit(payload: dict[str, Any], stream: Any = None) -> None:
    print(json.dumps(payload, sort_keys=True), file=stream or sys.stdout, flush=True)


def alert(task: str, error: str) -> None:
    # The same line also-ran-failure@ writes for a failed timer unit, at err
    # priority, so whatever watches for failed also-ran units still sees it.
    print(f"<3>also-ran unit failed: {SERVICE}/{task}: {error}", file=sys.stderr, flush=True)


def sd_notify(message: str) -> None:
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(address)
            sock.sendall(message.encode())
    except OSError as error:
        # A missed ping is retried next period; the watchdog decides.
        emit({"reconciler_version": VERSION, "warning": "sd_notify failed",
              "error": str(error)}, sys.stderr)


def state_path(args: argparse.Namespace, pool: str, scale_up: bool) -> str:
    stem = "gcp" if pool == "heavy" else "gcp-general"
    suffix = "-scale-up" if scale_up else ""
    return f"{args.state_directory}/{stem}{suffix}-state.json"


def read_state(path: str) -> dict[str, Any]:
    """A tick's last decision, or {} when there is none to read."""
    try:
        state = load_json(path)
    except (OSError, ValueError):
        return {}
    return state if isinstance(state, dict) else {}


def teardown_pending(state: dict[str, Any]) -> bool:
    """A maintenance decision left hosts on their way out."""
    return any(
        state.get(key)
        for key in ("drain_requested_slots", "drain_request_slots", "delete_slots")
    )


def surplus_to_drain(
    state: dict[str, Any], maintenance_state: dict[str, Any] | None = None
) -> bool:
    """A scale-up decision saw more serving hosts than maintenance would keep.

    Maintenance keeps the larger of current demand and its scale-down hold
    (the recent demand peak, `scale_down_hold_hosts`). The fast loop does not
    know the hold, so without it every tick of a hold would read as surplus
    and wake maintenance every 20 seconds to drain nothing. The hold is the
    last maintenance tick's; if it has since lapsed, the regular 60-second
    tick drains.
    """
    try:
        serving = int(state.get("serving_hosts", 0))
        desired_serving = int(state.get("desired_serving_hosts", 0))
        nonserving = max(0, int(state.get("desired_hosts", 0)) - desired_serving)
        hold = int((maintenance_state or {}).get("scale_down_hold_hosts") or 0)
    except (TypeError, ValueError):
        return False
    return serving > max(desired_serving, hold - nonserving)


def broker_argv(args: argparse.Namespace, pool: str, scale_up: bool) -> list[str]:
    """The broker arguments gcp_pools.py passes for this pool and loop."""
    stem = "gcp" if pool == "heavy" else "gcp-general"
    suffix = "-scale-up" if scale_up else ""
    argv = [
        "--policy", args.policy,
        "--market-state", args.market_state,
        "--github-token-file", args.github_token_file,
        "--lifecycle-script", args.lifecycle_script,
        "--once",
        "--scale-up-only" if scale_up else "--maintenance-only",
        "--pool", pool,
        "--state", state_path(args, pool, scale_up),
        "--lock", f"{args.lock_directory}/{stem}{suffix}-lock",
        "--demand-snapshot", demand_snapshot_path(args),
        "--" + getattr(args, pool + "_mode"),
    ]
    if not scale_up:
        argv.extend(
            ["--scale-up-state", state_path(args, pool, True)]
        )
    return argv


def demand_snapshot_path(args: argparse.Namespace) -> str:
    return f"{args.state_directory}/github-demand-snapshot.json"


def demand_interval(rate_remaining: int | None) -> float:
    for bound, interval in DEMAND_INTERVALS:
        if bound is None or (rate_remaining is not None and rate_remaining < bound):
            return interval
    raise AssertionError("unreachable")


@dataclasses.dataclass
class DemandRefresh:
    observed_at: dt.datetime
    rate_remaining: int | None


def refresh_demand(args: argparse.Namespace) -> DemandRefresh:
    """Refresh the shared snapshot the way a broker tick reads it."""
    started = time.monotonic()
    policy = load_json(args.policy)
    repositories = configured_repositories(policy, None)
    client = gcp_broker.CountingJsonClient(
        read_secret(args.github_token_file), "https://api.github.com"
    )
    observer = gcp_broker.GitHubDemandObserver(
        client,
        repositories,
        runner_scope=configured_runner_scope(policy, repositories),
        retired_repositories=configured_retired_repositories(policy, repositories),
        retired_organizations=configured_retired_organizations(policy),
    )
    store = gcp_broker.DemandSnapshotStore(demand_snapshot_path(args))
    now = dt.datetime.now(dt.timezone.utc)
    active, observed_at, age_ms, wait_ms = store.read_or_fetch(observer, now, DEMAND_REUSE)
    emit(
        {
            "controller_version": gcp_broker.VERSION,
            "reconciler_version": VERSION,
            "loop": "demand",
            "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
            "demand_snapshot_age_ms": age_ms,
            "demand_snapshot_wait_ms": wait_ms,
            "github_requests": client.requests,
            "github_ms": client.request_ms,
            "github_rate_remaining": active["rate_remaining"],
            "duration_ms": int((time.monotonic() - started) * 1000),
        }
    )
    return DemandRefresh(observed_at, active["rate_remaining"])


class Task:
    """One repeating unit of work with failure backoff and a liveness deadline."""

    def __init__(
        self,
        name: str,
        tick: Callable[[], Any],
        wait: Callable[["Task"], None],
        stop: threading.Event,
        deadline: float = TICK_DEADLINE,
    ) -> None:
        self.name = name
        self.tick = tick
        self.wait = wait
        self.stop = stop
        self.deadline = deadline
        self.failures = 0
        self.running_since: float | None = None
        self.last_started: float | None = None
        self.last_result: Any = None
        self.thread = threading.Thread(target=self.run, name=name, daemon=True)

    def run(self) -> None:
        while not self.stop.is_set():
            self.wait(self)
            if self.stop.is_set():
                return
            self.run_once()
            if self.failures:
                backoff = min(
                    FAILURE_BACKOFF_MAXIMUM,
                    FAILURE_BACKOFF_FIRST * 2 ** (self.failures - 1),
                )
                self.stop.wait(backoff)

    def run_once(self) -> None:
        self.running_since = self.last_started = time.monotonic()
        try:
            self.last_result = self.tick()
            self.failures = 0
        except Exception as error:  # noqa: BLE001 - one task must not stop the others
            self.record_failure(error)
        except SystemExit as error:
            # argparse and the broker's own guards exit rather than raise.
            self.record_failure(error)
        finally:
            self.running_since = None

    def record_failure(self, error: BaseException) -> None:
        # Every broker exception lands here, so this one cap covers them all:
        # an unbounded message would split the journal line it rides on.
        message = gcp_broker.bounded_text(str(error) or type(error).__name__)
        if isinstance(error, RuntimeError) and LOCK_BUSY in message:
            # A timer-driven pass from before the switch still holds the lock.
            emit({"reconciler_version": VERSION, "task": self.name, "result": "skipped",
                  "reason": "lock_busy"})
            return
        self.failures += 1
        emit(
            {
                "controller_version": gcp_broker.VERSION,
                "reconciler_version": VERSION,
                "task": self.name,
                "result": "error",
                "error": message,
                "consecutive_failures": self.failures,
            },
            sys.stderr,
        )
        alert(self.name, message)

    def healthy(self, now: float) -> bool:
        return self.running_since is None or now - self.running_since <= self.deadline


class Reconciler:
    def __init__(self, args: argparse.Namespace, stop: threading.Event | None = None) -> None:
        self.args = args
        self.stop = stop or threading.Event()
        self.wakeups = {pool: threading.Event() for pool in POOLS}
        self.maintenance_wakeups = {pool: threading.Event() for pool in POOLS}
        self.teardown = {pool: False for pool in POOLS}
        self.last_demand: dt.datetime | None = None
        self.tasks: list[Task] = [
            Task("demand", self.demand_tick, self.wait_demand, self.stop),
        ]
        for pool in POOLS:
            self.tasks.append(
                Task(
                    f"{pool}-scale-up",
                    lambda pool=pool: self.scale_up_tick(pool),
                    lambda task, pool=pool: self.wait_scale_up(task, pool),
                    self.stop,
                )
            )
            self.tasks.append(
                Task(
                    f"{pool}-maintenance",
                    lambda pool=pool: self.maintenance_tick(pool),
                    lambda task, pool=pool: self.wait_maintenance(task, pool),
                    self.stop,
                )
            )

    def scale_up_tick(self, pool: str) -> int:
        try:
            return gcp_broker.main(broker_argv(self.args, pool, True))
        finally:
            # Hand maintenance this fresh snapshot while it has work to do.
            state = read_state(state_path(self.args, pool, True))
            if self.teardown[pool] or surplus_to_drain(
                state, read_state(state_path(self.args, pool, False))
            ):
                self.maintenance_wakeups[pool].set()

    def maintenance_tick(self, pool: str) -> int:
        self.maintenance_wakeups[pool].clear()
        try:
            return gcp_broker.main(broker_argv(self.args, pool, False))
        finally:
            self.teardown[pool] = teardown_pending(
                read_state(state_path(self.args, pool, False))
            )

    def demand_tick(self) -> DemandRefresh:
        refresh = refresh_demand(self.args)
        if self.last_demand is None or refresh.observed_at > self.last_demand:
            self.last_demand = refresh.observed_at
            for event in self.wakeups.values():
                event.set()
        return refresh

    def wait_demand(self, task: Task) -> None:
        if task.last_started is None:
            return
        remaining = task.last_result.rate_remaining if task.last_result else None
        self.wait_until(task.last_started + demand_interval(remaining))

    def wait_scale_up(self, task: Task, pool: str) -> None:
        event = self.wakeups[pool]
        if task.last_started is not None:
            self.wait_until(task.last_started + MINIMUM_GAP)
            event.wait(SCALE_UP_MAX_WAIT)
        event.clear()

    def wait_maintenance(self, task: Task, pool: str) -> None:
        if task.last_started is None:
            return
        self.wait_until(
            max(task.last_started + TEARDOWN_INTERVAL, time.monotonic() + MINIMUM_GAP)
        )
        remaining = task.last_started + MAINTENANCE_INTERVAL - time.monotonic()
        if remaining > 0:
            self.maintenance_wakeups[pool].wait(remaining)

    def wait_until(self, deadline: float) -> None:
        delay = deadline - time.monotonic()
        if delay > 0:
            self.stop.wait(delay)

    def healthy(self) -> list[str]:
        now = time.monotonic()
        return [task.name for task in self.tasks if not task.healthy(now)]

    def shutdown(self) -> None:
        self.stop.set()
        for event in (*self.wakeups.values(), *self.maintenance_wakeups.values()):
            event.set()

    def serve(self) -> int:
        for task in self.tasks:
            task.thread.start()
        emit({"reconciler_version": VERSION, "controller_version": gcp_broker.VERSION,
              "event": "started", "tasks": [task.name for task in self.tasks]})
        sd_notify("READY=1")
        reported: set[str] = set()
        while not self.stop.wait(WATCHDOG_PERIOD):
            stuck = self.healthy()
            if stuck:
                # Stop the watchdog ping; systemd restarts the process.
                for name in set(stuck) - reported:
                    alert(name, f"tick exceeded {int(TICK_DEADLINE)}s")
                reported.update(stuck)
                continue
            reported.clear()
            sd_notify("WATCHDOG=1")
        sd_notify("STOPPING=1")
        # Let in-flight ticks finish; systemd bounds this with TimeoutStopSec.
        for task in self.tasks:
            task.thread.join()
        emit({"reconciler_version": VERSION, "event": "stopped"})
        return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", default=gcp_broker.DEFAULT_POLICY)
    parser.add_argument("--market-state", default=gcp_broker.DEFAULT_MARKET_STATE)
    parser.add_argument("--github-token-file", required=True)
    parser.add_argument(
        "--lifecycle-script", default="/opt/also-ran/gcp-runner-lifecycle.py"
    )
    parser.add_argument("--heavy-mode", choices=("apply", "dry-run"), required=True)
    parser.add_argument("--general-mode", choices=("apply", "dry-run"), required=True)
    parser.add_argument("--state-directory", default="/var/lib/also-ran")
    parser.add_argument("--lock-directory", default="/run/also-ran")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sys.stdout = LineWriter(sys.stdout)
    sys.stderr = LineWriter(sys.stderr)
    reconciler = Reconciler(args)
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: reconciler.shutdown())
    return reconciler.serve()


if __name__ == "__main__":
    sys.exit(main())
