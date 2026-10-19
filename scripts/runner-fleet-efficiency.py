#!/usr/bin/env python3
"""Read-only joined UTC-day runner accounting and warm-pool comparison.

Offline: --state STATE --day YYYY-MM-DD [--evidence FILE] [--billing FILE]
Live evidence/billing: --fetch (tokens inherited, never printed or saved).
Outputs JSON; --events emits compact, deduplicable daily events to stdout.
"""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import math
import os
import pathlib
import re
import subprocess
import sys
import urllib.request

sys.path.insert(
    0, str(pathlib.Path(__file__).resolve().parents[1] / "controller/also-ran")
)
from fleet_measurements import (
    elapsed,
    interval,
    merge,
    required_check_fields,
    readiness_fields,
    step_kind,
    timestamp,
)

from deployment_identity import PROJECT_ID, RESOURCE_PREFIX
BILLING_TABLE = os.environ.get("ALSO_RAN_BILLING_TABLE", "")



def request(url, token, payload=None, headers=None):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
            **(headers or {}),
        },
    )
    with urllib.request.urlopen(req, timeout=40) as response:
        return json.load(response)


def command_token(command):
    return subprocess.check_output(
        command, text=True, stderr=subprocess.DEVNULL
    ).strip()


def fetch_billing(day):
    if not re.fullmatch(r"[a-z][a-z0-9-]+\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+", BILLING_TABLE):
        raise ValueError("set ALSO_RAN_BILLING_TABLE to project.dataset.table")
    dt.date.fromisoformat(day)
    # UTC-day usage; partition pruning also includes late-arriving corrections.
    sql = f"""SELECT CASE
      WHEN resource.name LIKE '%{RESOURCE_PREFIX}-runner-gcp-general-%' OR resource.global_name LIKE '%{RESOURCE_PREFIX}-runner-gcp-general-%' THEN 'general'
      WHEN resource.name LIKE '%{RESOURCE_PREFIX}-runner-gcp-prod-%' OR resource.global_name LIKE '%{RESOURCE_PREFIX}-runner-gcp-prod-%' THEN 'heavy' ELSE 'other' END pool,
      sku.description sku, usage.unit unit, SUM(usage.amount) usage_amount,
      SUM(cost) gross_usd,
      SUM(cost+IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c),0)) net_usd,
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', MAX(usage_end_time), 'UTC') latest_usage_end
      FROM `{BILLING_TABLE}`
      WHERE _PARTITIONTIME >= TIMESTAMP('{day}') AND DATE(usage_start_time) = DATE('{day}')
        AND project.id='{PROJECT_ID}' GROUP BY 1,2,3"""
    token = command_token(["gcloud", "auth", "print-access-token"])
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{BILLING_TABLE.split(chr(46))[0]}/queries"
    result = request(
        url,
        token,
        {
            "query": sql,
            "useLegacySql": False,
            "maximumBytesBilled": "1000000000",
            "timeoutMs": 20000,
        },
    )
    if not result.get("jobComplete"):
        raise ValueError("billing query still running; retry after completion")
    names = [f["name"] for f in result["schema"]["fields"]]
    rows = []
    while True:
        rows.extend(
            dict(zip(names, [v.get("v") for v in row["f"]]))
            for row in result.get("rows", [])
        )
        if not result.get("pageToken"):
            return rows
        ref = result["jobReference"]
        result = request(
            f"https://bigquery.googleapis.com/bigquery/v2/projects/{ref['projectId']}/queries/{ref['jobId']}?pageToken={result['pageToken']}&location={ref.get('location','US')}",
            token,
        )


def axiom_rows(apl, token):
    result = request(
        "https://api.axiom.co/v1/datasets/_apl?format=legacy",
        token,
        {"apl": apl},
        {"X-Axiom-Org-Id": os.environ["AXIOM_ORG_ID"]},
    )
    if result.get("status", {}).get("isPartial"):
        raise ValueError("Axiom returned partial evidence")
    return [row["data"] for row in result.get("matches", [])]


def fetch_evidence(start, end):
    token = os.environ.get("AXIOM_PERSONAL_ACCESS_TOKEN")
    if not token:
        raise ValueError("set AXIOM_PERSONAL_ACCESS_TOKEN for --fetch")
    # Fixed hourly pages bounded well below 20k. Refuse truncation rather than
    # silently reporting low test hours. Include previous day's boot/deletion
    # evidence; a separate replay can supply older host lifecycles if needed.
    events = []
    for hour in range(-24, 25):
        a = dt.datetime.fromtimestamp(start + hour * 3600, dt.timezone.utc).isoformat()
        b = dt.datetime.fromtimestamp(
            start + (hour + 1) * 3600, dt.timezone.utc
        ).isoformat()
        query = f"""['vm-logs'] | where _time >= datetime({a}) and _time < datetime({b})
        | where tostring(body.MESSAGE) startswith '{{'
        | extend s=parse_json(tostring(body.MESSAGE))
        | where tostring(s.event) in ('runner_lifecycle_metric','runner_bootstrap_metric','runner_readiness_metric','runner_retention_metric','runner_drain_reason','runner_host_sample')
        | project s | limit 20000"""
        rows = axiom_rows(query, token)
        if len(rows) >= 20000:
            raise ValueError("lifecycle evidence truncated; narrow query window")
        events.extend(row["s"] for row in rows)
    a = dt.datetime.fromtimestamp(start, dt.timezone.utc).isoformat()
    b = dt.datetime.fromtimestamp(end + 3600, dt.timezone.utc).isoformat()
    rows = axiom_rows(
        f"['ci-runs'] | where _time >= datetime({a}) and _time < datetime({b}) and event_type == 'ci_job' | limit 20000",
        token,
    )
    if len(rows) >= 20000:
        raise ValueError("CI evidence truncated; narrow query window")
    return events + rows


def fetch_required_checks(state, configs, day, maximum_sets=20):
    from gcp_run_telemetry import GitHubClient, github_budget_available

    token = os.environ.get("GITHUB_TOKEN") or command_token(["gh", "auth", "token"])
    client = GitHubClient(token)
    start = timestamp(day + "T00:00:00Z")
    end = start + 86400
    groups = {}
    for run in state["runs"].values():
        if not start <= (timestamp(run.get("created_at")) or 0) < end:
            continue
        for config in configs:
            if (
                run.get("repository") != config["repository"]
                or run.get("event") not in config["events"]
            ):
                continue
            scope_known = bool(run.get("head_branch")) or not config.get(
                "branch_prefix"
            )
            if (
                scope_known
                and config.get("branch_prefix")
                and not str(run.get("head_branch") or "").startswith(
                    config["branch_prefix"]
                )
            ):
                continue
            key = (config["repository"], run["head_sha"], config["name"])
            current = groups.get(key)
            if current is None or timestamp(run["created_at"]) < timestamp(
                current["origin_at"]
            ):
                groups[key] = {
                    "repository": config["repository"],
                    "sha": run["head_sha"],
                    "set_name": config["name"],
                    "origin_at": run["created_at"],
                    "origin_source": "first_observed_workflow_created_at",
                    "controls": config["controls"],
                    "pages_complete": False,
                    "scope_status": "known" if scope_known else "branch_unavailable",
                }
    checks = []
    cache = {}
    for index, record in enumerate(groups.values()):
        key = (record["repository"], record["sha"])
        if record.get("scope_status") != "known":
            continue
        if key in cache:
            record["pages_complete"] = cache[key]
            continue
        if index >= maximum_sets or not github_budget_available(client):
            continue
        gathered = []
        complete = False
        for page in range(1, 11):
            if not github_budget_available(client):
                break
            result = client.get(
                f"/repos/{key[0]}/commits/{key[1]}/check-runs",
                {"filter": "all", "per_page": 100, "page": page},
            )
            items = result.get("check_runs")
            if not isinstance(items, list) or not isinstance(
                result.get("total_count"), int
            ):
                raise ValueError("malformed required-check page")
            gathered.extend(items)
            if len(gathered) >= result["total_count"]:
                complete = True
                break
            if not items:
                break
        # Read legacy status contexts too; required controls may originate from
        # external CI. Completion needs both APIs, never just Actions jobs.
        legacy = []
        legacy_complete = False
        for page in range(1, 11):
            if not github_budget_available(client):
                break
            from also_ran import JsonClient

            legacy_client = JsonClient(token, "https://api.github.com")
            raw = legacy_client.get(
                f"/repos/{key[0]}/commits/{key[1]}/statuses",
                {"per_page": 100, "page": page},
            )
            if not isinstance(raw, list):
                raise ValueError("malformed legacy status page")
            remaining = legacy_client.last_headers.get("x-ratelimit-remaining")
            if remaining:
                client.rate_remaining = int(remaining)
            legacy.extend(raw)
            if len(raw) < 100:
                legacy_complete = True
                break
        for item in gathered:
            checks.append(
                {
                    "repository": key[0],
                    "sha": key[1],
                    "id": item["id"],
                    "name": item["name"],
                    "app_id": item.get("app", {}).get("id"),
                    "status": item["status"],
                    "completed_at": item.get("completed_at"),
                    "conclusion": item.get("conclusion"),
                }
            )
        for item in legacy:
            checks.append(
                {
                    "repository": key[0],
                    "sha": key[1],
                    "id": item["id"],
                    "name": item["context"],
                    "app_id": None,
                    "status": "completed" if item["state"] != "pending" else "pending",
                    "completed_at": (
                        item.get("updated_at") if item["state"] != "pending" else None
                    ),
                    "conclusion": (
                        "success" if item["state"] == "success" else item["state"]
                    ),
                }
            )
        record["pages_complete"] = cache[key] = complete and legacy_complete
    return list(groups.values()), checks


def quantile(values, p):
    values = sorted(v for v in values if v is not None and math.isfinite(v))
    if not values:
        return None
    position = (len(values) - 1) * p
    i = int(position)
    return values[i] + (values[min(i + 1, len(values) - 1)] - values[i]) * (
        position - i
    )


def billing_fields(rows, pool):
    selected = [r for r in rows if r["pool"] == pool]
    # CPU core-seconds is the one VM-time denominator. RAM/SSD quantities are
    # different units and must not add to billed VM hours. SKU whitelist fails
    # closed if machine type changes or invoice schema no longer matches.
    cpu = [
        r
        for r in selected
        if re.search(r"C4.*(?:Core|CPU)", r["sku"], re.I) and r["unit"] == "seconds"
    ]
    cores = {"heavy": 8, "general": 4}[pool]
    return {
        "billed_vm_seconds": (
            sum(float(r["usage_amount"]) / cores for r in cpu) if cpu else None
        ),
        "gross_usd": sum(float(r["gross_usd"]) for r in selected) if selected else None,
        "net_usd": sum(float(r["net_usd"]) for r in selected) if selected else None,
        "billing_latest_usage_end": max(
            (r["latest_usage_end"] for r in cpu), default=None
        ),
        "billing_status": "exported_provisional" if cpu else "unavailable",
    }


def retention_fields(samples, pool, bounds):
    # Attribute aggregate retained capacity, not individual hosts. A floor can
    # include a booting VM; this is policy evidence rather than causal waste.
    by_time = {}
    for sample in samples:
        if sample.get("pool") != pool or not sample.get("observed_at"):
            continue
        t = timestamp(sample["observed_at"])
        if t not in by_time or sample.get("loop") == "maintenance":
            by_time[t] = sample
    ordered = sorted(by_time.items())
    coverage = floor = hold = 0.0
    for i, (t, sample) in enumerate(ordered):
        stop = min(ordered[i + 1][0] if i + 1 < len(ordered) else bounds[1], t + 180)
        span = elapsed([(t, stop)], bounds)
        serving = float(sample.get("serving_hosts") or 0)
        active = float(sample.get("active_owned_jobs") or 0)
        minimum = float(sample.get("minimum_hosts") or 0)
        held = float(sample.get("scale_down_hold_hosts") or 0)
        coverage += span
        floor += span * max(0, min(minimum, serving) - active)
        hold += span * max(0, min(held, serving) - max(active, minimum))
    return {
        "retention_observed_seconds": coverage,
        "estimated_floor_idle_vm_seconds": floor,
        "estimated_hold_extra_vm_seconds": hold,
        "retention_estimate_source": "aggregate_capacity_samples_max_180s_gap",
    }


def report(state, day, evidence=(), billing=(), required_sets=(), checks=()):
    start = dt.datetime.fromisoformat(day).replace(tzinfo=dt.timezone.utc).timestamp()
    end = start + 86400
    bounds = start, end
    hosts = {k: dict(v) for k, v in state["hosts"].items()}
    boots, ready, summaries, retention, drains, health = {}, {}, {}, [], [], []
    latest = {}
    unkeyed = []
    for event in evidence:
        key = event.get("event_id")
        if not key:
            unkeyed.append(event)
            continue
        previous = latest.get(key)
        time = (
            lambda record: record.get("measured_at")
            or record.get("observed_at")
            or record.get("completed_at")
            or ""
        )
        if previous is None or time(event) >= time(previous):
            latest[key] = event
    for event in list(latest.values()) + unkeyed:
        kind = event.get("event")
        host_id = str(event.get("host_instance_id"))
        if kind == "runner_lifecycle_metric" and host_id in hosts:
            hosts[host_id].update(
                {
                    k: event[k]
                    for k in (
                        "delete_requested_at",
                        "delete_completed_at",
                        "requested_at",
                        "api_completed_at",
                    )
                    if event.get(k)
                }
            )
        elif kind == "runner_bootstrap_metric":
            boots[host_id] = event
        elif kind == "runner_readiness_metric":
            ready[(host_id, event.get("boot_id"))] = event
        elif kind == "runner_retention_metric":
            retention.append(event)
        elif kind == "runner_host_sample":
            health.append(event)
        elif kind == "runner_drain_reason":
            drains.append(event)
        elif event.get("event_type") == "ci_job" and event.get("github_job_id"):
            summaries[str(event["github_job_id"])] = event
    billing = [row for row in billing if not row.get("day") or row["day"] == day]
    all_jobs = list(state["jobs"].values())
    jobs_by_host = {}
    for job in all_jobs:
        jobs_by_host.setdefault(str(job.get("host_instance_id")), []).append(job)
    groups = {}
    cohorts = {}
    unmatched = 0
    readiness_rows = []
    for pool in ("heavy", "general"):
        fleet = {k: h for k, h in hosts.items() if h.get("pool") == pool}
        sums = dict(
            observed_vm_seconds=0.0,
            job_seconds=0.0,
            dedicated_test_seconds=0.0,
            other_job_seconds=0.0,
            pre_first_job_seconds=0.0,
            between_job_seconds=0.0,
            after_last_job_seconds=0.0,
            zero_job_seconds=0.0,
        )
        counts = dict(
            hosts=0,
            zero_job_hosts=0,
            jobs=0,
            jobs_with_steps=0,
            jobs_with_ci_phase_identity=0,
            exact_delete_hosts=0,
            bootstrap_hosts=0,
            readiness_hosts=0,
            jobs_with_test_selection=0,
        )
        startup_times = []
        for host_id, host in fleet.items():
            lifetime = interval(
                host.get("created_at"),
                host.get("delete_completed_at")
                or host.get("deleted_observed_at")
                or state.get("observed_at"),
            )
            if not lifetime or not elapsed([lifetime], bounds):
                continue
            counts["hosts"] += 1
            counts["exact_delete_hosts"] += bool(host.get("delete_completed_at"))
            counts["bootstrap_hosts"] += host_id in boots
            active_bounds = max(start, lifetime[0]), min(end, lifetime[1])
            sums["observed_vm_seconds"] += elapsed([lifetime], bounds)
            jobs = jobs_by_host.get(host_id, [])
            boot = boots.get(host_id, {})
            startup = readiness_fields(
                host, boot, ready.get((host_id, boot.get("boot_id"))), jobs
            )
            readiness_rows.append(
                {
                    "host_instance_id": host_id,
                    "boot_id": boot.get("boot_id"),
                    "pool": pool,
                    **startup,
                }
            )
            counts["readiness_hosts"] += startup["readiness_status"] == "observed"
            startup_times.append(startup["create_to_runner_online_seconds"])
            pieces = merge(
                [interval(j.get("started_at"), j.get("completed_at")) for j in jobs],
                active_bounds,
            )
            busy = elapsed(pieces)
            sums["job_seconds"] += busy
            tests = []
            for j in jobs:
                job_bounds = interval(j.get("started_at"), j.get("completed_at"))
                if not job_bounds or not elapsed([job_bounds], active_bounds):
                    continue
                counts["jobs"] += 1
                counts["jobs_with_steps"] += j.get("steps") is not None
                summary = summaries.get(str(j["github_job_id"]))
                selection = (summary or {}).get("test_selection_mode", "unknown")
                if selection not in ("full", "related", "static", "skip"):
                    selection = "unknown"
                counts["jobs_with_test_selection"] += selection != "unknown"
                if summary and interval(
                    summary.get("test_started_at"), summary.get("test_finished_at")
                ):
                    tests.append(
                        interval(
                            summary["test_started_at"], summary["test_finished_at"]
                        )
                    )
                    counts["jobs_with_ci_phase_identity"] += 1
                else:
                    tests.extend(
                        merge(
                            [
                                interval(s.get("started_at"), s.get("completed_at"))
                                for s in (j.get("steps") or [])
                                if s.get("conclusion") != "skipped"
                                and step_kind(s.get("name")) == "test"
                            ],
                            job_bounds,
                        )
                    )
                first = min(
                    (x.get("started_at") for x in jobs if x.get("started_at")),
                    default=None,
                )
                run = state["runs"].get(str(j["workflow_run_id"]), {})
                cohort = (
                    "first_job_proxy" if j.get("started_at") == first else "reused_host"
                )
                # Include the effective suite selector, even when a related
                # requested tier escalates to full. Historical evidence is unknown.
                key = "|".join(
                    str(v or "unknown")
                    for v in (
                        pool,
                        run.get("repository"),
                        run.get("workflow_name"),
                        j.get("job_name"),
                        run.get("event"),
                        host.get("region"),
                        boots.get(host_id, {}).get("image_source_commit")
                        or host.get("image_source_commit"),
                        j.get("workflow_run_attempt"),
                        selection,
                        cohort,
                    )
                )
                item = cohorts.setdefault(
                    key,
                    {
                        "jobs": 0,
                        "queue": [],
                        "runtime": [],
                        "test_selection_mode": selection,
                    },
                )
                item["jobs"] += 1
                queue = interval(j.get("created_at"), j.get("started_at"))
                item["queue"].append(elapsed([queue]) if queue else None)
                item["runtime"].append(job_bounds[1] - job_bounds[0])
            tests = merge(tests, active_bounds)
            # Test intervals cannot count outside an observed job.
            test_seconds = sum(elapsed(tests, piece) for piece in pieces)
            sums["dedicated_test_seconds"] += test_seconds
            sums["other_job_seconds"] += max(0, busy - test_seconds)
            if not pieces:
                sums["zero_job_seconds"] += active_bounds[1] - active_bounds[0]
                counts["zero_job_hosts"] += 1
            else:
                sums["pre_first_job_seconds"] += pieces[0][0] - active_bounds[0]
                sums["between_job_seconds"] += sum(
                    pieces[i][0] - pieces[i - 1][1] for i in range(1, len(pieces))
                )
                sums["after_last_job_seconds"] += active_bounds[1] - pieces[-1][1]
        invoice = billing_fields(billing, pool)
        denominator = invoice["billed_vm_seconds"]
        test = sums["dedicated_test_seconds"]
        sums.update(invoice)
        sums["billed_to_test_ratio"] = (
            denominator / test if denominator is not None and test else None
        )
        sums["test_utilization_percent"] = (
            100 * test / denominator if denominator else None
        )
        sums["job_utilization_percent"] = (
            100 * sums["job_seconds"] / denominator if denominator else None
        )
        sums["observed_minus_billed_vm_seconds"] = (
            sums["observed_vm_seconds"] - denominator
            if denominator is not None
            else None
        )
        sums["coverage"] = counts
        sums.update(retention_fields(retention, pool, bounds))
        sums["create_to_runner_online_p50_seconds"] = quantile(startup_times, 0.5)
        sums["create_to_runner_online_p95_seconds"] = quantile(startup_times, 0.95)
        samples = [
            h
            for h in health
            if h.get("runner_pool") == pool
            and start <= (timestamp(h.get("observed_at")) or 0) < end
        ]
        sums["health_samples"] = len(samples)
        sums["maximum_memory_pressure_some_avg10"] = max(
            (float(h.get("memory_pressure_some_avg10") or 0) for h in samples),
            default=None,
        )
        sums["minimum_memory_available_bytes"] = min(
            (
                float(h["memory_available_bytes"])
                for h in samples
                if h.get("memory_available_bytes") is not None
            ),
            default=None,
        )
        groups[pool] = sums
    for j in all_jobs:
        piece = interval(j.get("started_at"), j.get("completed_at"))
        unmatched += bool(
            piece and elapsed([piece], bounds) and not j.get("host_instance_id")
        )
    required = []
    for record in required_sets:
        if not (start <= (timestamp(record.get("origin_at")) or 0) < end):
            continue
        selected = [
            c
            for c in checks
            if c.get("repository") == record["repository"]
            and c.get("sha") == record["sha"]
        ]
        required.append(
            {
                **record,
                **required_check_fields(
                    selected,
                    record["controls"],
                    record["origin_at"],
                    record.get("pages_complete", False),
                ),
            }
        )
    workflow_wall = []
    for r in state["runs"].values():
        if (
            not r.get("relevant_jobs")
            or not start <= (timestamp(r.get("created_at")) or 0) < end
        ):
            continue
        span = (
            interval(r.get("created_at"), r.get("updated_at"))
            if r.get("status") == "completed"
            else None
        )
        workflow_wall.append(
            {
                "repository": r.get("repository"),
                "workflow": r.get("workflow_name"),
                "event": r.get("event"),
                "run_id": r.get("workflow_run_id"),
                "attempt": r.get("workflow_run_attempt"),
                "sha": r.get("head_sha"),
                "conclusion": r.get("conclusion"),
                "wall_seconds_updated_at_proxy": elapsed([span]) if span else None,
            }
        )
    cohort_rows = [
        {
            "cohort": key,
            "test_selection_mode": v["test_selection_mode"],
            "jobs": v["jobs"],
            "queue_p50_seconds": quantile(v["queue"], 0.5),
            "queue_p95_seconds": quantile(v["queue"], 0.95),
            "runtime_p95_seconds": quantile(v["runtime"], 0.95),
        }
        for key, v in sorted(cohorts.items())
    ]
    return {
        "schema_version": 2,
        "day": day,
        "collection_observed_at": state.get("observed_at"),
        "pending_job_runs": sum(
            not run.get("jobs_observed") for run in state["runs"].values()
        ),
        "pools": groups,
        "cohorts": cohort_rows,
        "workflows": workflow_wall,
        "required_check_sets": required,
        "bootstrap_phases": list(boots.values()),
        "runner_readiness": readiness_rows,
        "retention_samples": retention,
        "drain_reasons": drains,
        "unmatched_jobs": unmatched,
        "billing_other_net_usd": sum(
            float(r["net_usd"]) for r in billing if r["pool"] == "other"
        ),
        "measurement_notes": [
            "Invoice export remains provisional and can receive corrections.",
            "Observed VM time ends at API deletion when available; polling disappearance is an upper bound.",
            "Dedicated test time is a covered lower bound; mixed steps and unknown work are excluded.",
            "First job is a cold-start proxy, not a measured runner-online timestamp.",
            "Runner-online timing requires a generation/boot-matched listener session event; missing coverage remains unknown.",
            "Test selection is explicit per job; historical and uninstrumented lanes remain unknown.",
            "Required-check latency is unknown unless an explicit complete check set is supplied.",
        ],
    }


def compare(reports, slo, minimum_samples):
    result = []
    for label, data in reports.items():
        required = data.get("required_check_sets", [])
        complete = [r for r in required if r["required_checks_status"] == "complete"]
        values = [
            r["required_checks_wall_seconds"]
            for r in complete
            if r.get("required_checks_passed")
        ]
        p95 = quantile(values, 0.95)
        eligible = (
            len(values) >= minimum_samples
            and len(complete) == len(required)
            and bool(required)
        )
        billed = [p.get("billed_vm_seconds") for p in data["pools"].values()]
        costs = [p.get("net_usd") for p in data["pools"].values()]
        result.append(
            {
                "variant": label,
                "successful_required_sets": len(values),
                "failed_required_sets": sum(
                    item.get("required_checks_passed") is False for item in complete
                ),
                "required_set_failure_rate": (
                    sum(
                        item.get("required_checks_passed") is False for item in complete
                    )
                    / len(complete)
                    if complete
                    else None
                ),
                "observed_required_sets": len(required),
                "required_wall_p95_seconds": p95,
                "latency_slo_seconds": slo,
                "decision": (
                    "insufficient_evidence"
                    if not eligible or slo is None
                    else "within_slo" if p95 <= slo else "exceeds_slo"
                ),
                "billed_vm_hours": (
                    sum(billed) / 3600 if all(v is not None for v in billed) else None
                ),
                "net_usd": sum(costs) if all(v is not None for v in costs) else None,
                "cohorts": data.get("cohorts", []),
            }
        )
    return {
        "comparisons": result,
        "causality": "Observational; randomize matched time blocks and change one policy parameter at a time.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--axiom-token-file")
    parser.add_argument("--github-token-file")
    parser.add_argument("--state")
    parser.add_argument("--day", type=dt.date.fromisoformat)
    parser.add_argument("--evidence")
    parser.add_argument("--billing")
    parser.add_argument("--checks")
    parser.add_argument("--required-sets")
    parser.add_argument("--check-config")
    parser.add_argument("--maximum-check-sets", type=int, default=20)
    parser.add_argument("--fetch", action="store_true")
    parser.add_argument("--events", action="store_true")
    parser.add_argument("--compare", nargs="+", metavar="LABEL=REPORT")
    parser.add_argument("--slo-seconds", type=float)
    parser.add_argument("--minimum-samples", type=int, default=20)
    args = parser.parse_args()
    for argument, variable in (
        (args.axiom_token_file, "AXIOM_PERSONAL_ACCESS_TOKEN"),
        (args.github_token_file, "GITHUB_TOKEN"),
    ):
        if argument:
            os.environ[variable] = pathlib.Path(argument).read_text().strip()
    load = lambda p: json.loads(pathlib.Path(p).read_text()) if p else []
    if args.compare:
        data = compare(
            {
                item.split("=", 1)[0]: load(item.split("=", 1)[1])
                for item in args.compare
            },
            args.slo_seconds,
            args.minimum_samples,
        )
    else:
        if not args.state or not args.day:
            parser.error("--state and --day required")
        day = args.day.isoformat()
        start = dt.datetime.combine(args.day, dt.time(), dt.timezone.utc).timestamp()
        evidence = load(args.evidence)
        billing = load(args.billing)
        if args.fetch:
            evidence += fetch_evidence(start, start + 86400)
            billing = fetch_billing(day)
        state = load(args.state)
        required = load(args.required_sets)
        checks = load(args.checks)
        if args.check_config:
            required, checks = fetch_required_checks(
                state, load(args.check_config), day, args.maximum_check_sets
            )
        data = report(state, day, evidence, billing, required, checks)
    if args.events and not args.compare:
        for pool, fields in data["pools"].items():
            print(
                json.dumps(
                    {
                        "event": "runner_fleet_efficiency",
                        "event_version": 1,
                        "event_id": f"fleet-day:{data['day']}:{pool}",
                        "day": data["day"],
                        "pool": pool,
                        **fields,
                    },
                    sort_keys=True,
                )
            )
        for fields in data["cohorts"]:
            digest = hashlib.sha256(fields["cohort"].encode()).hexdigest()[:24]
            print(
                json.dumps(
                    {
                        "event": "runner_ci_cohort",
                        "event_version": 1,
                        "event_id": f"cohort:{data['day']}:{digest}",
                        "day": data["day"],
                        **fields,
                    },
                    sort_keys=True,
                )
            )
        for fields in data["required_check_sets"]:
            print(
                json.dumps(
                    {
                        "event": "runner_required_checks",
                        "event_version": 1,
                        "event_id": f"required:{fields['repository']}:{fields['sha']}:{fields.get('set_name','explicit')}",
                        **fields,
                    },
                    sort_keys=True,
                )
            )
    else:
        print(json.dumps(data, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
