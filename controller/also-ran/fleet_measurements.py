"""Interval accounting shared by the collector and invoice reconciliation.

All elapsed work is the union of wall-clock intervals, never summed test-case
CPU durations. Unknown evidence stays unknown; polling disappearance is an
upper bound on lifetime, not the provider's billing stop timestamp.
"""

from __future__ import annotations
import datetime as dt
import re


def timestamp(value):
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include a timezone")
    return parsed.timestamp()


def interval(start, end):
    a, b = timestamp(start), timestamp(end)
    return (a, b) if a is not None and b is not None and b >= a else None


def merge(intervals, bounds=None):
    pieces = []
    for item in intervals:
        if item is None:
            continue
        a, b = item
        if bounds:
            a, b = max(a, bounds[0]), min(b, bounds[1])
        if b > a:
            pieces.append((a, b))
    result = []
    for a, b in sorted(pieces):
        if result and a <= result[-1][1]:
            result[-1] = (result[-1][0], max(b, result[-1][1]))
        else:
            result.append((a, b))
    return result


def elapsed(intervals, bounds=None):
    return sum(b - a for a, b in merge(intervals, bounds))


# Deliberately conservative: combined commands remain mixed/unknown rather
# than contributing their entire duration to the dedicated-test denominator.
CLASSIFICATION_VERSION = "dedicated_step_v1"
STEP_RULES = [
    ("mixed", r"(test.*(build|lint|type.?check)|(?:build|lint|type.?check).*test)"),
    (
        "test",
        r"^(run (combined frontend|orchard integration|repository tooling) tests(?:.*)?|suite shard, timed|only the asked tests|test the changed owner portal|run pnpm --filter @also-ran/api test(?::integration)?|measure frontend unit coverage)$",
    ),
    (
        "test",
        r"^(run )?(unit tests?|integration tests?|frontend tests?|vitest tests?|playwright tests?|e2e tests?)(\b|$)|^(run )?tests?$",
    ),
    ("checkout", r"^(?:run )?(checkout|check out|actions/checkout)"),
    ("report", r"(upload.*(artifact|report)|telemetry|test results|publish.*report)"),
    (
        "environment",
        r"(install|setup|set up|dependencies|start.*(docker|api|functions|preview)|prepare.*environment)",
    ),
    ("build", r"\bbuild\b"),
    ("check", r"(lint|type.?check|contract|check|validate)"),
]


def step_kind(name):
    text = str(name or "").lower()
    for kind, pattern in STEP_RULES:
        if re.search(pattern, text):
            return kind
    return "other"


def step_fields(job):
    steps = job.get("steps")
    bounds = interval(job.get("started_at"), job.get("completed_at"))
    completed = [s for s in (steps or []) if s.get("conclusion") != "skipped"]
    valid = [
        s for s in completed if interval(s.get("started_at"), s.get("completed_at"))
    ]
    result = {
        "step_classification_version": CLASSIFICATION_VERSION,
        "steps_status": (
            "unavailable"
            if steps is None
            else "complete" if len(valid) == len(completed) else "partial"
        ),
        "steps_expected": len(completed),
        "steps_timed": len(valid),
    }
    for kind in [
        "test",
        "checkout",
        "report",
        "environment",
        "build",
        "check",
        "mixed",
        "other",
    ]:
        result[kind + "_step_seconds"] = (
            elapsed(
                [
                    interval(s.get("started_at"), s.get("completed_at"))
                    for s in valid
                    if step_kind(s.get("name")) == kind
                ],
                bounds,
            )
            if steps is not None and bounds
            else None
        )
    result["steps_unmeasured_seconds"] = (
        max(
            0,
            bounds[1]
            - bounds[0]
            - elapsed(
                [interval(s.get("started_at"), s.get("completed_at")) for s in valid],
                bounds,
            ),
        )
        if bounds and steps is not None
        else None
    )
    return result


def host_fields(host, jobs):
    bounds = interval(
        host.get("created_at"),
        host.get("delete_completed_at") or host.get("deleted_observed_at"),
    )
    if not bounds:
        return {}
    pieces = merge(
        [interval(j.get("started_at"), j.get("completed_at")) for j in jobs], bounds
    )
    lifetime = bounds[1] - bounds[0]
    busy = elapsed(pieces)
    tests = []
    for job in jobs:
        tests.extend(
            merge(
                [
                    interval(step.get("started_at"), step.get("completed_at"))
                    for step in (job.get("steps") or [])
                    if step.get("conclusion") != "skipped"
                    and step_kind(step.get("name")) == "test"
                ],
                interval(job.get("started_at"), job.get("completed_at")),
            )
        )
    coverage = sum(j.get("steps") is not None for j in jobs)
    return {
        "lifetime_stop_source": (
            "delete_api_complete"
            if host.get("delete_completed_at")
            else "polling_disappearance_upper_bound"
        ),
        "job_busy_seconds": busy,
        "dedicated_test_seconds": elapsed(tests, bounds),
        "test_step_jobs_covered": coverage,
        "test_step_jobs_expected": len(jobs),
        "outside_job_seconds": max(0, lifetime - busy),
        "pre_first_job_seconds": pieces[0][0] - bounds[0] if pieces else lifetime,
        "between_job_seconds": sum(
            pieces[i][0] - pieces[i - 1][1] for i in range(1, len(pieces))
        ),
        "after_last_job_seconds": bounds[1] - pieces[-1][1] if pieces else 0,
        "zero_job_host": not pieces,
    }


def readiness_fields(host, bootstrap, event, jobs=()):
    """Join listener evidence by provider generation and boot, never slot name."""
    unknown = {
        "readiness_status": "not_observed",
        "runner_online_at": None,
        "boot_to_runner_online_seconds": None,
        "create_to_runner_online_seconds": None,
        "ready_to_first_job_seconds": None,
    }
    if not event:
        return unknown
    if (
        str(event.get("host_instance_id")) != str(host.get("instance_id"))
        or not bootstrap.get("boot_id")
        or event.get("boot_id") != bootstrap.get("boot_id")
        or event.get("readiness_source") != "actions_listener_session_log"
    ):
        return {**unknown, "readiness_status": "identity_mismatch_or_unverified"}
    try:
        online = timestamp(event.get("runner_online_at"))
        boot = timestamp(event.get("boot_started_at"))
        created = timestamp(host.get("created_at"))
        observed = timestamp(event.get("observed_at"))
        stopped = timestamp(
            host.get("delete_completed_at") or host.get("deleted_observed_at")
        )
        first = min(
            (timestamp(j["started_at"]) for j in jobs if j.get("started_at")),
            default=None,
        )
        if (
            None in (online, boot, created, observed)
            or online < max(boot, created)
            or online > observed
            or (stopped is not None and online > stopped)
            # GitHub job start timestamps have one-second precision; a runner
            # can establish its session and accept work in that same second.
            or (first is not None and online >= first + 1)
        ):
            return {**unknown, "readiness_status": "invalid_or_late_evidence"}
    except (TypeError, ValueError):
        return {**unknown, "readiness_status": "invalid_or_late_evidence"}
    return {
        "readiness_status": "observed",
        "runner_online_at": event["runner_online_at"],
        "boot_to_runner_online_seconds": online - boot,
        "create_to_runner_online_seconds": online - created,
        "ready_to_first_job_seconds": (
            max(0, first - online) if first is not None else None
        ),
    }


def required_check_fields(checks, required, origin, complete):
    """Explicit check names for one repository/SHA/check-set, including non-GCP.

    Caller must fetch *all* check-run and legacy status pages. Omitted controls,
    incomplete pages, pending checks or ambiguous app identities fail closed.
    latest per (name,app_id) handles reruns without mixing check providers.
    """
    latest = {}
    for check in checks:
        key = (check.get("name"), check.get("app_id"))
        previous = latest.get(key)
        if previous is None or check.get("id", 0) > previous.get("id", 0):
            latest[key] = check
    selected, missing = [], []
    for control in required:
        matches = [
            c
            for (name, app), c in latest.items()
            if name == control["name"]
            and (control.get("app_id") is None or app == control["app_id"])
        ]
        if len(matches) != 1:
            missing.append(control["name"])
        else:
            selected.append(matches[0])
    finished = [
        c for c in selected if c.get("completed_at") and c.get("status") == "completed"
    ]
    known = (
        bool(required) and complete and not missing and len(finished) == len(required)
    )
    last = max(finished, key=lambda c: timestamp(c["completed_at"])) if known else None
    start = timestamp(origin)
    stop = timestamp(last["completed_at"]) if last else None
    valid = known and start is not None and stop is not None and stop >= start
    return {
        "required_checks_status": "complete" if valid else "unknown",
        "required_checks_missing": missing,
        "required_checks_expected": len(required),
        "required_checks_completed": len(finished),
        "required_checks_wall_seconds": stop - start if valid else None,
        "critical_check": last.get("name") if valid else None,
        "required_checks_passed": (
            all(
                c.get("conclusion") in ("success", "neutral", "skipped")
                for c in finished
            )
            if valid
            else None
        ),
    }
