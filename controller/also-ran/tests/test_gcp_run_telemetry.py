import datetime as dt
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import unittest
import urllib.error
from unittest import mock


CONTROLLER_DIR = pathlib.Path(__file__).parents[1]
sys.path.insert(0, str(CONTROLLER_DIR))
CONTROLLER = CONTROLLER_DIR / "gcp_run_telemetry.py"
SPEC = importlib.util.spec_from_file_location("gcp_run_telemetry", CONTROLLER)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


NOW = dt.datetime(2026, 9, 19, 12, 0, tzinfo=dt.timezone.utc)


def host(instance_id="123", deleted="2026-09-19T12:00:00Z"):
    return {
        "instance_id": instance_id,
        "instance_name": "also-ran-gcp-general-01",
        "pool": "general",
        "scale_set": "also-ran-gcp-general-01",
        "zone": "europe-west9-a",
        "region": "europe-west9",
        "created_at": "2026-09-19T11:00:00Z",
        "deleted_observed_at": deleted,
        "modeled_hourly_usd": 0.6,
    }


def job(job_id, duration_minutes, run_id=10, host_id="123"):
    return {
        "github_job_id": job_id,
        "workflow_run_id": run_id,
        "job_name": f"job-{job_id}",
        "pool": "general",
        "status": "completed",
        "conclusion": "success",
        "created_at": "2026-09-19T11:00:00Z",
        "started_at": "2026-09-19T11:00:00Z",
        "completed_at": MODULE.isoformat(
            dt.datetime(2026, 9, 19, 11, 0, tzinfo=dt.timezone.utc)
            + dt.timedelta(minutes=duration_minutes)
        ),
        "runner_name": "also-ran-gcp-general-01-runner",
        "html_url": "https://example.invalid/job",
        "host_instance_id": host_id,
    }


def run():
    return {
        "workflow_run_id": 10,
        "workflow_name": "CI",
        "event": "pull_request",
        "head_sha": "abc",
        "status": "completed",
        "conclusion": "success",
        "created_at": "2026-09-19T10:59:00Z",
        "run_started_at": "2026-09-19T11:00:00Z",
        "updated_at": "2026-09-19T11:31:00Z",
        "html_url": "https://example.invalid/run",
        "jobs_observed": True,
        "relevant_jobs": 2,
    }


class PriceModelTests(unittest.TestCase):
    def test_derives_general_shape_from_heavy_components(self):
        market = {
            "price_observed_at": "2026-09-19T11:00:00Z",
            "regions": [
                {
                    "name": "europe-west9",
                    "price": {
                        "components_hourly_usd": {
                            "core": 0.08,
                            "ram": 0.03,
                            "local_ssd": 0.02,
                        }
                    },
                }
            ],
        }

        prices = MODULE.price_models(market, NOW, 72)

        self.assertAlmostEqual(prices["europe-west9"]["heavy"], 0.13)
        self.assertAlmostEqual(prices["europe-west9"]["general"], 0.075)

    def test_stale_price_returns_no_cost_model(self):
        market = {"price_observed_at": "2026-09-16T11:59:59Z", "regions": []}
        self.assertEqual(MODULE.price_models(market, NOW, 72), {})


class AttributionTests(unittest.TestCase):
    def state(self):
        state = MODULE.empty_state()
        state["hosts"] = {"123": host()}
        state["jobs"] = {"1": job(1, 10), "2": job(2, 20)}
        state["runs"] = {"10": run()}
        return state

    def test_host_lifetime_cost_is_allocated_by_job_runtime(self):
        allocations, total = MODULE.host_allocations(self.state(), host())

        self.assertAlmostEqual(total, 0.6)
        self.assertAlmostEqual(allocations["1"], 0.2)
        self.assertAlmostEqual(allocations["2"], 0.4)

    def test_job_event_has_wall_clock_and_final_allocated_cost(self):
        state = self.state()

        event = MODULE.build_job_event(state, state["jobs"]["1"], "o/r", NOW)

        self.assertEqual(event["queue_seconds"], 0)
        self.assertEqual(event["job_duration_seconds"], 600)
        self.assertEqual(event["workflow_wall_seconds"], 1920)
        self.assertEqual(event["cost_status"], "final")
        self.assertAlmostEqual(event["modeled_marginal_compute_usd"], 0.1)
        self.assertAlmostEqual(event["modeled_allocated_host_usd"], 0.2)

    def test_workflow_event_sums_job_costs(self):
        state = self.state()

        event = MODULE.build_workflow_event(state, state["runs"]["10"], "o/r", NOW)

        self.assertEqual(event["cost_status"], "final")
        self.assertAlmostEqual(event["modeled_marginal_compute_usd"], 0.3)
        self.assertAlmostEqual(event["modeled_allocated_host_usd"], 0.6)

    def test_open_host_cost_remains_provisional(self):
        state = self.state()
        state["hosts"]["123"]["deleted_observed_at"] = None

        event = MODULE.build_workflow_event(state, state["runs"]["10"], "o/r", NOW)

        self.assertEqual(event["cost_status"], "provisional")
        self.assertIsNone(event["modeled_allocated_host_usd"])
        self.assertAlmostEqual(event["modeled_marginal_compute_usd"], 0.3)

    def test_event_is_reemitted_only_when_material_fields_change(self):
        state = self.state()
        first = MODULE.build_job_event(state, state["jobs"]["1"], "o/r", NOW)
        later = NOW + dt.timedelta(minutes=2)
        duplicate = MODULE.build_job_event(state, state["jobs"]["1"], "o/r", later)

        self.assertIsNotNone(first)
        self.assertIsNone(duplicate)


class HostGenerationTests(unittest.TestCase):
    def test_matches_runner_to_host_generation_by_lifetime(self):
        old = host("old", "2026-09-19T10:00:00Z")
        old["created_at"] = "2026-09-19T09:00:00Z"
        new = host("new", None)
        target = job(1, 10, host_id=None)

        self.assertEqual(MODULE.matching_host(target, {"old": old, "new": new}), "new")

    def test_overlapping_poll_windows_choose_the_newest_host_generation(self):
        target = job(1, 10, host_id=None)
        target["started_at"] = "2026-09-19T12:05:30Z"
        old = host("old", "2026-09-19T12:06:00Z")
        new = host("new", None)
        new["created_at"] = "2026-09-19T12:05:00Z"

        self.assertEqual(MODULE.matching_host(target, {"old": old, "new": new}), "new")

    def test_missing_runner_does_not_guess_a_host(self):
        target = job(1, 10, host_id=None)
        target["runner_name"] = None
        self.assertIsNone(MODULE.matching_host(target, {"123": host()}))


class GithubObservationTests(unittest.TestCase):
    def test_existing_state_loads_with_empty_run_scan_progress(self):
        state = MODULE.empty_state()
        state.pop("run_scans")

        self.assertEqual(MODULE.validate_state(state)["run_scans"], {})

    def test_collects_every_rerun_attempt(self):
        state = MODULE.empty_state()
        state["hosts"] = {"123": host(deleted=None)}
        raw_run = {
            "id": 10,
            "run_attempt": 2,
            "name": "CI",
            "event": "pull_request",
            "head_sha": "abc",
            "status": "completed",
            "conclusion": "success",
            "created_at": "2026-09-19T10:59:00Z",
            "run_started_at": "2026-09-19T11:00:00Z",
            "updated_at": "2026-09-19T11:31:00Z",
            "html_url": "https://example.invalid/run",
        }
        raw_job = {
            "id": 1,
            "run_attempt": 1,
            "name": "test",
            "labels": [MODULE.GCP_LABELS["general"]],
            "status": "completed",
            "conclusion": "failure",
            "created_at": "2026-09-19T11:00:00Z",
            "started_at": "2026-09-19T11:00:00Z",
            "completed_at": "2026-09-19T11:10:00Z",
            "runner_name": "also-ran-gcp-general-01-runner",
            "html_url": "https://example.invalid/job",
        }

        with mock.patch.object(
            MODULE, "discover_runs", return_value=[raw_run]
        ):
            client = mock.Mock()
            client.get.return_value = {"jobs": [raw_job]}
            MODULE.observe_github(state, client, "o/r", NOW, NOW)

        self.assertEqual(client.get.call_args.args[1]["filter"], "all")
        self.assertEqual(state["jobs"]["1"]["workflow_run_attempt"], 1)

    def test_run_scan_splits_githubs_thousand_result_search_limit(self):
        client = mock.Mock()
        client.get.side_effect = [
            {"total_count": 1000, "workflow_runs": []},
            {"total_count": 1, "workflow_runs": [{"id": 1}]},
            {"total_count": 1, "workflow_runs": [{"id": 2}]},
        ]
        since = NOW - dt.timedelta(seconds=3)
        scan = {"ranges": [{"since": MODULE.isoformat(since),
                             "until": MODULE.isoformat(NOW), "page": 1}]}

        result = MODULE.discover_runs(client, "o/r", scan)

        self.assertEqual(result, [{"id": 1}, {"id": 2}])
        self.assertEqual(scan["ranges"], [])
        self.assertEqual(client.get.call_args_list[1].args[1]["created"],
                         f"{MODULE.isoformat(NOW - dt.timedelta(seconds=1))}..{MODULE.isoformat(NOW)}")

    def test_run_scan_saves_page_after_request_budget(self):
        client = mock.Mock()
        client.get.side_effect = [
            {"total_count": 1000, "workflow_runs": []}
        ] + [{"total_count": 950, "workflow_runs": [{"id": 1}] * 100}] * 9
        scan = {"ranges": [{"since": MODULE.isoformat(NOW - dt.timedelta(hours=6)),
                             "until": MODULE.isoformat(NOW), "page": 1}]}

        first = MODULE.discover_runs(client, "o/r", scan)

        self.assertEqual(len(first), 900)
        self.assertEqual(scan["ranges"][-1]["page"], 10)
        self.assertEqual(client.get.call_count, 10)

    def test_job_scan_resumes_after_full_page_without_publishing_partial_run(self):
        state = MODULE.empty_state()
        raw_run = {
            "id": 10, "run_attempt": 1, "name": "CI", "event": "push",
            "head_sha": "abc", "status": "completed", "conclusion": "success",
            "created_at": MODULE.isoformat(NOW - dt.timedelta(minutes=5)),
            "updated_at": MODULE.isoformat(NOW),
        }
        raw_job = {
            "id": 1, "labels": [MODULE.GCP_LABELS["general"]],
            "status": "completed", "conclusion": "success",
            "created_at": MODULE.isoformat(NOW - dt.timedelta(minutes=4)),
            "started_at": MODULE.isoformat(NOW - dt.timedelta(minutes=3)),
            "completed_at": MODULE.isoformat(NOW - dt.timedelta(minutes=2)),
        }
        client = mock.Mock()
        client.get.side_effect = [
            {"jobs": [raw_job] + [{"id": number, "labels": []} for number in range(2, 101)]},
            {"jobs": []},
        ]
        with mock.patch.object(MODULE, "discover_runs", side_effect=[
            [raw_run], []
        ]), mock.patch.object(MODULE, "JOB_PAGES_PER_TICK", 1):
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)
            self.assertEqual(state["runs"]["10"]["job_page"], 2)
            self.assertFalse(state["runs"]["10"]["jobs_observed"])
            self.assertIsNone(MODULE.build_workflow_event(state, state["runs"]["10"], "o/r", NOW))
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)

        self.assertTrue(state["runs"]["10"]["jobs_observed"])
        self.assertEqual(state["runs"]["10"]["relevant_jobs"], 1)
        self.assertEqual(client.get.call_args.args[1]["page"], 2)

    def test_rate_reserve_keeps_completed_scan_progress(self):
        client = mock.Mock()
        client.rate_remaining = 751

        def one_page(*_args):
            client.rate_remaining = 750
            return {"total_count": 200, "workflow_runs": [{"id": 1}] * 100}

        client.get.side_effect = one_page
        scan = {"ranges": [{"since": MODULE.isoformat(NOW - dt.timedelta(hours=6)),
                             "until": MODULE.isoformat(NOW), "page": 1}]}

        self.assertEqual(len(MODULE.discover_runs(client, "o/r", scan)), 100)
        self.assertEqual(scan["ranges"][0]["page"], 2)
        self.assertEqual(client.get.call_count, 1)

    def test_completed_runs_get_job_budget_before_active_runs(self):
        state = MODULE.empty_state()
        raw_active = {
            "id": 10, "run_attempt": 1, "name": "CI", "event": "push",
            "head_sha": "abc", "status": "in_progress", "conclusion": None,
            "created_at": MODULE.isoformat(NOW - dt.timedelta(minutes=5)),
            "updated_at": MODULE.isoformat(NOW),
        }
        raw_complete = {
            **raw_active, "id": 11, "status": "completed",
            "conclusion": "success",
            "created_at": MODULE.isoformat(NOW - dt.timedelta(minutes=2)),
        }
        client = mock.Mock()
        client.get.return_value = {"jobs": []}
        with mock.patch.object(MODULE, "discover_runs", return_value=[raw_active, raw_complete]), \
             mock.patch.object(MODULE, "JOB_PAGES_PER_TICK", 1):
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)

        self.assertIn("/runs/11/jobs", client.get.call_args.args[0])
        self.assertTrue(state["runs"]["11"]["jobs_observed"])
        self.assertFalse(state["runs"]["10"]["jobs_observed"])

    def test_deleted_pending_run_does_not_poison_later_ticks(self):
        state = MODULE.empty_state()
        raw_run = {
            "id": 10, "run_attempt": 1, "name": "CI", "event": "push",
            "head_sha": "abc", "status": "completed", "conclusion": "success",
            "created_at": MODULE.isoformat(NOW - dt.timedelta(minutes=5)),
            "updated_at": MODULE.isoformat(NOW),
        }
        client = mock.Mock()
        http_error = urllib.error.HTTPError("https://api.github.com", 404, "gone", {}, None)
        client.get.side_effect = MODULE.ObservationError("deleted")
        client.get.side_effect.__cause__ = http_error
        with mock.patch.object(MODULE, "discover_runs", return_value=[raw_run]):
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)

        self.assertEqual(client.get.call_count, 1)
        self.assertTrue(state["runs"]["10"]["jobs_unavailable"])
        self.assertIsNone(MODULE.build_workflow_event(state, state["runs"]["10"], "o/r", NOW))



class MultiRepositoryTests(unittest.TestCase):
    RAW_RUN = {
        "id": 10,
        "run_attempt": 1,
        "name": "test",
        "event": "pull_request",
        "head_sha": "abc",
        "status": "completed",
        "conclusion": "success",
        "created_at": "2026-09-19T10:59:00Z",
        "run_started_at": "2026-09-19T11:00:00Z",
        "updated_at": "2026-09-19T11:31:00Z",
        "html_url": "https://example.invalid/run",
    }

    def raw_job(self, job_id):
        return {
            "id": job_id,
            "name": f"job-{job_id}",
            "labels": [MODULE.GCP_LABELS["general"]],
            "status": "completed",
            "conclusion": "success",
            "created_at": "2026-09-19T11:00:00Z",
            "started_at": "2026-09-19T11:00:00Z",
            "completed_at": "2026-09-19T11:10:00Z",
            "runner_name": "also-ran-gcp-general-01-runner",
            "html_url": "https://example.invalid/job",
        }

    def test_each_repository_is_observed_and_its_records_name_it(self):
        state = MODULE.empty_state()
        state["hosts"] = {"123": host(deleted=None)}
        runs = {
            "o/also-ran": [dict(self.RAW_RUN, id=10)],
            "o/another-repo": [dict(self.RAW_RUN, id=20)],
        }
        jobs = {10: [self.raw_job(1)], 20: [self.raw_job(2)]}

        def fake_discover(_client, repo, _scan):
            return runs[repo]

        client = mock.Mock()
        client.get.side_effect = lambda path, _query: {"jobs": jobs[int(path.split("/")[-2])]}
        with mock.patch.object(MODULE, "discover_runs", side_effect=fake_discover):
            MODULE.observe_github(state, client, ("o/also-ran", "o/another-repo"), NOW, NOW)

        self.assertEqual(state["runs"]["10"]["repository"], "o/also-ran")
        self.assertEqual(state["runs"]["20"]["repository"], "o/another-repo")
        self.assertEqual(state["jobs"]["1"]["repository"], "o/also-ran")
        self.assertEqual(state["jobs"]["2"]["repository"], "o/another-repo")

    def test_events_carry_their_own_repository_not_the_collectors(self):
        state = MODULE.empty_state()
        state["hosts"] = {"123": host()}
        state["runs"] = {"10": dict(run(), repository="o/another-repo")}
        state["jobs"] = {"1": dict(job(1, 10), repository="o/another-repo")}

        job_event = MODULE.build_job_event(state, state["jobs"]["1"], None, NOW)
        run_event = MODULE.build_workflow_event(state, state["runs"]["10"], None, NOW)

        self.assertEqual(job_event["repository"], "o/another-repo")
        self.assertEqual(run_event["repository"], "o/another-repo")

    def test_host_shared_by_two_repositories_names_both_and_neither_alone(self):
        state = MODULE.empty_state()
        state["hosts"] = {"123": host()}
        state["jobs"] = {
            "1": dict(job(1, 10), repository="o/also-ran"),
            "2": dict(job(2, 20), repository="o/another-repo"),
        }

        event = MODULE.build_host_event(state, host(), None, NOW)

        self.assertIsNone(event["repository"])
        self.assertEqual(event["repositories"], ["o/also-ran", "o/another-repo"])

    def test_host_serving_one_repository_is_attributed_to_it(self):
        state = MODULE.empty_state()
        state["hosts"] = {"123": host()}
        state["jobs"] = {"1": dict(job(1, 10), repository="o/another-repo")}

        event = MODULE.build_host_event(state, host(), None, NOW)

        self.assertEqual(event["repository"], "o/another-repo")
        self.assertEqual(event["repositories"], ["o/another-repo"])

    def test_single_repository_collector_keeps_its_name_on_an_idle_host(self):
        state = MODULE.empty_state()
        event = MODULE.build_host_event(state, host(), "o/also-ran", NOW)
        self.assertEqual(event["repository"], "o/also-ran")
        self.assertEqual(event["repositories"], [])

    def test_state_from_before_multi_repository_is_named_once(self):
        state = MODULE.empty_state()
        state["runs"] = {"10": run()}
        state["jobs"] = {"1": job(1, 10), "2": dict(job(2, 10), repository="o/x")}

        MODULE.backfill_repository(state, "o/also-ran")

        self.assertEqual(state["runs"]["10"]["repository"], "o/also-ran")
        self.assertEqual(state["jobs"]["1"]["repository"], "o/also-ran")
        self.assertEqual(state["jobs"]["2"]["repository"], "o/x")



def pending_run(run_id, name, created_minutes_ago=5):
    return {
        "id": run_id, "run_attempt": 1, "name": name, "event": "push",
        "head_sha": "abc", "status": "completed", "conclusion": "success",
        "created_at": MODULE.isoformat(NOW - dt.timedelta(minutes=created_minutes_ago)),
        "updated_at": MODULE.isoformat(NOW),
    }


def observed_run(run_id, name, relevant):
    return {
        **run(), "workflow_run_id": run_id, "workflow_name": name,
        "repository": "o/r", "relevant_jobs": relevant,
    }


class BacklogTests(unittest.TestCase):
    """2026-10-02: the job backlog grew all day and emission lagged ~13h."""

    def test_catchup_pages_drain_a_backlog_while_the_budget_is_healthy(self):
        state = MODULE.empty_state()
        client = mock.Mock()
        client.rate_remaining = 4000
        client.get.return_value = {"jobs": []}
        runs = [pending_run(100 + n, "CI", 60 - n) for n in range(5)]
        with mock.patch.object(MODULE, "discover_runs", return_value=runs), \
             mock.patch.object(MODULE, "JOB_PAGES_PER_TICK", 2), \
             mock.patch.object(MODULE, "JOB_CATCHUP_PAGES_PER_TICK", 2):
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)

        self.assertEqual(client.get.call_count, 4)
        self.assertEqual(
            sum(not item["jobs_observed"] for item in state["runs"].values()), 1
        )

    def test_catchup_pages_are_not_spent_from_a_low_budget(self):
        state = MODULE.empty_state()
        client = mock.Mock()
        client.rate_remaining = MODULE.CATCHUP_MINIMUM_RATE_REMAINING - 1
        client.get.return_value = {"jobs": []}
        runs = [pending_run(100 + n, "CI", 60 - n) for n in range(5)]
        with mock.patch.object(MODULE, "discover_runs", return_value=runs), \
             mock.patch.object(MODULE, "JOB_PAGES_PER_TICK", 2), \
             mock.patch.object(MODULE, "JOB_CATCHUP_PAGES_PER_TICK", 2):
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)

        self.assertEqual(client.get.call_count, 2)

    def test_workflows_that_use_gcp_are_fetched_before_ones_that_never_do(self):
        state = MODULE.empty_state()
        for n in range(MODULE.IRRELEVANT_WORKFLOW_MINIMUM_RUNS):
            state["runs"][str(n)] = observed_run(n, "coderabbit-gate", 0)
        state["runs"]["50"] = observed_run(50, "Tests", 3)
        client = mock.Mock()
        client.get.return_value = {"jobs": []}
        # The irrelevant run is oldest, so plain age order would take it first.
        runs = [
            pending_run(101, "coderabbit-gate", 60),
            pending_run(102, "Brand new workflow", 30),
            pending_run(103, "Tests", 10),
        ]
        with mock.patch.object(MODULE, "discover_runs", return_value=runs), \
             mock.patch.object(MODULE, "JOB_PAGES_PER_TICK", 2):
            MODULE.observe_repository(state, client, "o/r", NOW - dt.timedelta(hours=6), NOW)

        fetched = [call.args[0] for call in client.get.call_args_list]
        self.assertEqual(
            fetched,
            ["/repos/o/r/actions/runs/103/jobs", "/repos/o/r/actions/runs/102/jobs"],
        )
        self.assertFalse(state["runs"]["101"]["jobs_observed"])

    def test_one_gcp_run_makes_a_workflow_relevant_again(self):
        state = MODULE.empty_state()
        for n in range(MODULE.IRRELEVANT_WORKFLOW_MINIMUM_RUNS):
            state["runs"][str(n)] = observed_run(n, "Deploy", 0)
        self.assertEqual(MODULE.workflow_relevance(state, "o/r"), {"Deploy": False})
        state["runs"]["99"] = observed_run(99, "Deploy", 1)
        self.assertEqual(MODULE.workflow_relevance(state, "o/r"), {"Deploy": True})

    def test_index_matches_the_unindexed_events(self):
        state = AttributionTests().state()
        index = MODULE.JobIndex(state)
        self.assertEqual(
            MODULE.host_allocations(state, host(), index),
            MODULE.host_allocations(state, host()),
        )
        indexed = MODULE.build_workflow_event(
            state, state["runs"]["10"], "o/r", NOW, index
        )
        state["event_fingerprints"].clear()
        plain = MODULE.build_workflow_event(state, state["runs"]["10"], "o/r", NOW)
        self.assertEqual(indexed, plain)

    def test_a_failed_state_write_prints_no_events(self):
        argv = [
            "gcp_run_telemetry.py", "--policy", "p", "--market-state", "m",
            "--state", "s", "--lock", "l", "--github-token-file", "t",
        ]
        event = {"event": "runner_job_metric", "event_id": "github-job:1"}
        with mock.patch.object(sys, "argv", argv), \
             mock.patch.object(MODULE, "exclusive_lock"), \
             mock.patch.object(MODULE, "load_state", return_value=MODULE.empty_state()), \
             mock.patch.object(MODULE, "load_json", return_value={}), \
             mock.patch.object(MODULE, "configured_repositories", return_value=("o/r",)), \
             mock.patch.object(MODULE, "configured_retired_repositories", return_value=()), \
             mock.patch.object(MODULE, "read_secret", return_value="token"), \
             mock.patch.object(MODULE, "collect", return_value=([event], {})), \
             mock.patch.object(
                 MODULE, "atomic_write_json",
                 side_effect=OSError(28, "No space left on device"),
             ), \
             mock.patch("builtins.print") as printed:
            self.assertEqual(MODULE.main(), 1)

        stdout = [call for call in printed.call_args_list if "file" not in call.kwargs]
        self.assertEqual(stdout, [])

    def test_a_failed_print_keeps_the_old_state_so_events_re_emit(self):
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "state.json")
            MODULE.atomic_write_json(path, {"generation": 1})

            def broken_print():
                raise OSError(5, "Input/output error")

            with self.assertRaises(OSError):
                MODULE.atomic_write_json(
                    path, {"generation": 2}, before_replace=broken_print
                )
            with open(path, encoding="utf-8") as handle:
                self.assertEqual(json.load(handle), {"generation": 1})
            self.assertEqual(os.listdir(root), ["state.json"])

            printed = []
            MODULE.atomic_write_json(
                path, {"generation": 2}, before_replace=lambda: printed.append(1)
            )
            self.assertEqual(printed, [1])
            with open(path, encoding="utf-8") as handle:
                self.assertEqual(json.load(handle), {"generation": 2})


if __name__ == "__main__":
    unittest.main()
