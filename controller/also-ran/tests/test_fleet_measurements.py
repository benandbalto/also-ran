import contextlib
import importlib.util
import io
import pathlib
import sys
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "controller/also-ran"))
import fleet_measurements as m
import gcp_broker as broker

SPEC = importlib.util.spec_from_file_location(
    "fleet_report", ROOT / "scripts/runner-fleet-efficiency.py"
)
r = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(r)


class IntervalTests(unittest.TestCase):
    def test_union_clips_overlap_and_never_counts_parallel_time_twice(self):
        self.assertEqual(m.elapsed([(0, 100), (50, 150), (190, 250)], (25, 200)), 135)
        self.assertIsNone(m.interval("2026-10-09T00:00:05Z", "2026-10-09T00:00:00Z"))
        with self.assertRaises(ValueError):
            m.timestamp("2026-10-09T00:00:00")

    def test_combined_step_is_not_dedicated_test_time(self):
        self.assertEqual(m.step_kind("Run tests and build"), "mixed")
        self.assertEqual(m.step_kind("Run integration tests"), "test")
        self.assertEqual(m.step_kind("Build frontend"), "build")
        self.assertEqual(m.step_kind("Normalize test telemetry"), "report")

    def test_missing_steps_stays_unknown(self):
        self.assertEqual(m.step_fields({})["steps_status"], "unavailable")
        self.assertIsNone(m.step_fields({})["test_step_seconds"])

    def test_required_controls_fail_closed_on_ambiguity_and_pending(self):
        check = dict(
            id=1,
            name="test",
            app_id=15368,
            status="completed",
            completed_at="2026-10-09T00:01:00Z",
            conclusion="success",
        )
        required = [{"name": "test", "app_id": 15368}]
        start = "2026-10-09T00:00:00Z"
        result = m.required_check_fields([check], required, start, True)
        self.assertEqual(result["required_checks_wall_seconds"], 60)
        self.assertTrue(result["required_checks_passed"])
        self.assertIsNone(
            m.required_check_fields([check], required, start, False)[
                "required_checks_wall_seconds"
            ]
        )
        self.assertIsNone(
            m.required_check_fields(
                [
                    check,
                    {**check, "id": 2, "status": "in_progress", "completed_at": None},
                ],
                required,
                start,
                True,
            )["required_checks_wall_seconds"]
        )
        self.assertIsNone(
            m.required_check_fields(
                [check, {**check, "app_id": 1}], [{"name": "test"}], start, True
            )["required_checks_wall_seconds"]
        )
        self.assertIsNone(
            m.required_check_fields([], [], start, True)["required_checks_wall_seconds"]
        )


class AccountingTests(unittest.TestCase):
    def state(self):
        return {
            "observed_at": "2026-10-10T00:00:00Z",
            "hosts": {
                "111": {
                    "instance_id": "111",
                    "pool": "general",
                    "created_at": "2026-10-09T00:00:00Z",
                    "deleted_observed_at": "2026-10-09T01:01:00Z",
                }
            },
            "jobs": {
                "7": {
                    "github_job_id": 7,
                    "host_instance_id": "111",
                    "workflow_run_id": 9,
                    "started_at": "2026-10-09T00:10:00Z",
                    "completed_at": "2026-10-09T00:30:00Z",
                    "steps": [
                        {
                            "name": "Run unit tests",
                            "started_at": "2026-10-09T00:15:00Z",
                            "completed_at": "2026-10-09T00:25:00Z",
                        }
                    ],
                }
            },
            "runs": {},
        }

    def billing(self):
        return [
            {
                "pool": "general",
                "sku": "Spot C4 Instance Core",
                "unit": "seconds",
                "usage_amount": "14400",
                "net_usd": ".5",
                "gross_usd": "1",
                "latest_usage_end": "2026-10-09T01:00:00Z",
            },
            {
                "pool": "general",
                "sku": "Spot C4 RAM",
                "unit": "byte-seconds",
                "usage_amount": "9999999",
                "net_usd": ".1",
                "gross_usd": ".2",
                "latest_usage_end": "2026-10-09T01:00:00Z",
            },
        ]

    def test_invoice_uses_core_seconds_once_and_net_credits(self):
        result = r.report(self.state(), "2026-10-09", billing=self.billing())["pools"][
            "general"
        ]
        self.assertEqual(result["billed_vm_seconds"], 3600)
        self.assertEqual(result["job_seconds"], 1200)
        self.assertEqual(result["dedicated_test_seconds"], 600)
        self.assertEqual(result["billed_to_test_ratio"], 6)
        self.assertEqual(result["observed_minus_billed_vm_seconds"], 60)
        self.assertAlmostEqual(result["net_usd"], 0.6)
        self.assertEqual(result["billing_status"], "exported_provisional")

    def test_exact_delete_replaces_polling_upper_bound_and_delivery_dedupes(self):
        evidence = [
            {
                "event": "runner_lifecycle_metric",
                "event_id": "gcp-delete:111",
                "host_instance_id": "111",
                "delete_completed_at": "2026-10-09T01:00:00Z",
            }
        ] * 2
        result = r.report(self.state(), "2026-10-09", evidence=evidence)["pools"][
            "general"
        ]
        self.assertEqual(result["observed_vm_seconds"], 3600)
        self.assertEqual(result["coverage"]["exact_delete_hosts"], 1)
        self.assertIsNone(result["billed_to_test_ratio"])

    def test_ci_intervals_override_step_proxy_but_clip_to_job(self):
        evidence = [
            {
                "event_type": "ci_job",
                "github_job_id": 7,
                "test_started_at": "2026-10-09T00:00:00Z",
                "test_finished_at": "2026-10-09T00:20:00Z",
            }
        ]
        result = r.report(self.state(), "2026-10-09", evidence=evidence)["pools"][
            "general"
        ]
        self.assertEqual(result["dedicated_test_seconds"], 600)
        self.assertEqual(result["coverage"]["jobs_with_ci_phase_identity"], 1)

    def test_readiness_is_generation_boot_matched_and_deduplicated(self):
        boot = {
            "event": "runner_bootstrap_metric",
            "event_id": "bootstrap:111:boot",
            "host_instance_id": "111",
            "boot_id": "boot",
        }
        ready = {
            "event": "runner_readiness_metric",
            "event_id": "ready:111:boot",
            "host_instance_id": "111",
            "boot_id": "boot",
            "runner_online_at": "2026-10-09T00:01:00Z",
            "boot_started_at": "2026-10-09T00:00:10Z",
            "observed_at": "2026-10-09T00:01:01Z",
            "readiness_source": "actions_listener_session_log",
        }
        result = r.report(self.state(), "2026-10-09", evidence=[boot, ready, ready])
        self.assertEqual(result["pools"]["general"]["coverage"]["readiness_hosts"], 1)
        self.assertEqual(
            result["pools"]["general"]["create_to_runner_online_p95_seconds"], 60
        )
        self.assertEqual(
            result["runner_readiness"][0]["boot_to_runner_online_seconds"], 50
        )
        self.assertEqual(
            result["runner_readiness"][0]["ready_to_first_job_seconds"], 540
        )
        same_second = {
            **ready,
            "runner_online_at": "2026-10-09T00:10:00.125Z",
            "observed_at": "2026-10-09T00:10:01Z",
        }
        simultaneous = r.report(
            self.state(), "2026-10-09", evidence=[boot, same_second]
        )
        self.assertEqual(
            simultaneous["pools"]["general"]["coverage"]["readiness_hosts"], 1
        )
        self.assertEqual(
            simultaneous["runner_readiness"][0]["ready_to_first_job_seconds"], 0
        )
        for bad in (
            {**ready, "boot_id": "old"},
            {**ready, "host_instance_id": "222"},
            {**ready, "runner_online_at": "2026-10-09T00:11:00Z"},
            {**ready, "runner_online_at": "garbled"},
        ):
            unknown = r.report(self.state(), "2026-10-09", evidence=[boot, bad])
            self.assertEqual(
                unknown["pools"]["general"]["coverage"]["readiness_hosts"], 0
            )
            self.assertIsNone(
                unknown["runner_readiness"][0]["create_to_runner_online_seconds"]
            )

    def test_effective_selection_separates_cohorts_and_legacy_stays_unknown(self):
        state = self.state()
        state["jobs"]["8"] = {**state["jobs"]["7"], "github_job_id": 8}
        evidence = [
            {
                "event_type": "ci_job",
                "github_job_id": 7,
                "test_selection_mode": "full",
                "test_tier": "related",
            },
            {
                "event_type": "ci_job",
                "github_job_id": 8,
                "test_selection_mode": "related",
                "test_tier": "related",
            },
        ]
        report = r.report(state, "2026-10-09", evidence=evidence)
        self.assertEqual(
            {c["test_selection_mode"] for c in report["cohorts"]}, {"full", "related"}
        )
        self.assertEqual(len(report["cohorts"]), 2)
        self.assertEqual(
            report["pools"]["general"]["coverage"]["jobs_with_test_selection"], 2
        )
        legacy = r.report(state, "2026-10-09")
        self.assertEqual(legacy["cohorts"][0]["test_selection_mode"], "unknown")
        self.assertEqual(
            legacy["pools"]["general"]["coverage"]["jobs_with_test_selection"], 0
        )

    def test_retention_excludes_unobserved_long_gaps(self):
        samples = [
            {
                "pool": "heavy",
                "observed_at": "2026-10-09T00:00:00Z",
                "serving_hosts": 8,
                "active_owned_jobs": 0,
                "minimum_hosts": 8,
            }
        ]
        start = m.timestamp("2026-10-09T00:00:00Z")
        result = r.retention_fields(samples, "heavy", (start, start + 3600))
        self.assertEqual(result["retention_observed_seconds"], 180)
        self.assertEqual(result["estimated_floor_idle_vm_seconds"], 1440)

    def test_comparison_never_recommends_from_missing_required_latency(self):
        result = r.compare({"baseline": r.report(self.state(), "2026-10-09")}, 600, 20)
        self.assertEqual(result["comparisons"][0]["decision"], "insufficient_evidence")

    def test_compact_events_omit_provider_payload_and_preserve_partial_success(self):
        stream = io.StringIO()
        with contextlib.redirect_stdout(stream):
            broker.emit_mutations(
                [
                    {
                        "ok": True,
                        "slot": 1,
                        "result": {
                            "id": "123",
                            "requested_at": "t",
                            "api_completed_at": "u",
                            "private": "never",
                        },
                    },
                    {"ok": False, "slot": 2, "error": "sensitive"},
                ],
                "create",
                "heavy",
            )
        self.assertIn("gcp-create:123", stream.getvalue())
        self.assertNotIn("never", stream.getvalue())
        self.assertNotIn("sensitive", stream.getvalue())


class RequiredApiTests(unittest.TestCase):
    def run_state(self, branch="trunk-merge/example"):
        return {
            "runs": {
                "1": {
                    "repository": "o/r",
                    "event": "push",
                    "head_branch": branch,
                    "head_sha": "abc",
                    "created_at": "2026-10-09T00:00:00Z",
                }
            }
        }

    def config(self):
        return [
            {
                "name": "queue",
                "repository": "o/r",
                "events": ["push"],
                "branch_prefix": "trunk-merge/",
                "controls": [{"name": "test", "app_id": 15368}],
            }
        ]

    @mock.patch.object(r, "command_token", return_value="test-only")
    @mock.patch("gcp_run_telemetry.GitHubClient")
    def test_unknown_branch_is_retained_as_missing_scope(self, client_class, token):
        sets, checks = r.fetch_required_checks(
            self.run_state(None), self.config(), "2026-10-09"
        )
        self.assertEqual(len(sets), 1)
        self.assertEqual(sets[0]["scope_status"], "branch_unavailable")
        self.assertFalse(sets[0]["pages_complete"])
        client_class.return_value.get.assert_not_called()

    @mock.patch.object(r, "command_token", return_value="test-only")
    @mock.patch("also_ran.JsonClient")
    @mock.patch("gcp_run_telemetry.GitHubClient")
    def test_paginated_check_runs_and_legacy_statuses_both_needed(
        self, client_class, legacy_class, token
    ):
        client = client_class.return_value
        client.rate_remaining = 5000
        client.get.side_effect = [
            {
                "total_count": 2,
                "check_runs": [
                    {
                        "id": 1,
                        "name": "test",
                        "status": "completed",
                        "conclusion": "success",
                        "completed_at": "2026-10-09T00:01:00Z",
                        "app": {"id": 15368},
                    }
                ],
            },
            {
                "total_count": 2,
                "check_runs": [
                    {
                        "id": 2,
                        "name": "extra",
                        "status": "completed",
                        "conclusion": "success",
                        "completed_at": "2026-10-09T00:02:00Z",
                        "app": {"id": 15368},
                    }
                ],
            },
        ]
        legacy_class.return_value.get.return_value = []
        legacy_class.return_value.last_headers = {"x-ratelimit-remaining": "4000"}
        sets, checks = r.fetch_required_checks(
            self.run_state(), self.config(), "2026-10-09"
        )
        self.assertTrue(sets[0]["pages_complete"])
        self.assertEqual(len(checks), 2)
        self.assertEqual(client.get.call_count, 2)
        self.assertEqual(client.rate_remaining, 4000)

    @mock.patch.object(r, "command_token", return_value="test-only")
    @mock.patch("also_ran.JsonClient")
    @mock.patch("gcp_run_telemetry.GitHubClient")
    def test_pagination_limit_never_claims_complete(
        self, client_class, legacy_class, token
    ):
        client = client_class.return_value
        client.rate_remaining = 5000
        client.get.return_value = {"total_count": 100, "check_runs": []}
        legacy_class.return_value.get.return_value = []
        legacy_class.return_value.last_headers = {}
        sets, checks = r.fetch_required_checks(
            self.run_state(), self.config(), "2026-10-09"
        )
        self.assertFalse(sets[0]["pages_complete"])
        self.assertEqual(checks, [])


if __name__ == "__main__":
    unittest.main()
