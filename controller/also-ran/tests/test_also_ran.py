import dataclasses
import importlib.util
import pathlib
import sys
import tempfile
import unittest


MODULE_PATH = pathlib.Path(__file__).parents[1] / "also_ran.py"
SPEC = importlib.util.spec_from_file_location("also_ran", MODULE_PATH)
also_ran = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules[SPEC.name] = also_ran
SPEC.loader.exec_module(also_ran)


LABELS = {
    "managed-by": "runner-autoscaler",
    "fleet": "also-ran-ci",
    "lifecycle": "burst",
    "autoscale-slot": "burst-01",
}
NOW = "2026-09-16T12:00:00Z"


class FakeClient:
    def __init__(self, responder):
        self.responder = responder
        self.last_headers = {"x-ratelimit-remaining": "4999"}
        self.calls = []

    def get(self, path, query=None):
        self.calls.append((path, query))
        return self.responder(path, query or {})


class DecisionTests(unittest.TestCase):
    def setUp(self):
        self.policy = also_ran.Policy(
            workflow_label="outrunner-hetzner-linux-x64",
            burst_runner_prefix="also-ran-hetzner-burst-01-",
        )

    def observation(self, **changes):
        values = {
            "observed_at": NOW,
            "queued_count": 0,
            "oldest_queue_seconds": 0,
            "active_burst_jobs": 0,
            "servers": (),
        }
        values.update(changes)
        return also_ran.Observation(**values)

    def test_two_old_queue_samples_propose_create(self):
        state, first = also_ran.decide(
            self.policy,
            also_ran.initial_state(),
            self.observation(queued_count=1, oldest_queue_seconds=46),
            LABELS,
        )
        self.assertEqual(first["action"], "none")
        state, second = also_ran.decide(
            self.policy,
            state,
            self.observation(queued_count=1, oldest_queue_seconds=106),
            LABELS,
        )
        self.assertEqual(second["action"], "create")
        self.assertEqual(second["suppression_reason"], "create_only_not_enabled")

    def test_two_queued_jobs_propose_create_immediately(self):
        _, decision = also_ran.decide(
            self.policy,
            also_ran.initial_state(),
            self.observation(queued_count=2),
            LABELS,
        )
        self.assertEqual(decision["action"], "create")

    def test_incomplete_observation_preserves_state(self):
        previous = also_ran.initial_state()
        previous["state"] = "BUSY"
        previous["server_id"] = 42
        state, decision = also_ran.decide(
            self.policy,
            previous,
            self.observation(github_complete=False, github_error="rate limited"),
            LABELS,
        )
        self.assertEqual(state["state"], "BUSY")
        self.assertEqual(state["server_id"], 42)
        self.assertEqual(decision["suppression_reason"], "observation_incomplete")

    def test_duplicate_servers_fail_closed(self):
        servers = tuple(
            also_ran.Server(number, f"burst-{number}", "running", LABELS)
            for number in (1, 2)
        )
        state, decision = also_ran.decide(
            self.policy,
            also_ran.initial_state(),
            self.observation(servers=servers),
            LABELS,
        )
        self.assertEqual(state["state"], "ERROR")
        self.assertEqual(decision["suppression_reason"], "duplicate_burst_servers")

    def test_label_mismatch_fails_closed(self):
        server = also_ran.Server(1, "burst", "running", {**LABELS, "fleet": "wrong"})
        state, decision = also_ran.decide(
            self.policy,
            also_ran.initial_state(),
            self.observation(servers=(server,)),
            LABELS,
        )
        self.assertEqual(state["state"], "ERROR")
        self.assertEqual(decision["suppression_reason"], "server_identity_mismatch")

    def test_active_burst_job_is_busy_and_resets_idle(self):
        server = also_ran.Server(1, "burst", "running", LABELS)
        previous = also_ran.initial_state()
        previous["idle_since"] = "2026-09-16T11:00:00Z"
        state, _ = also_ran.decide(
            self.policy,
            previous,
            self.observation(servers=(server,), active_burst_jobs=1),
            LABELS,
        )
        self.assertEqual(state["state"], "BUSY")
        self.assertIsNone(state["idle_since"])

    def test_idle_threshold_never_requests_delete(self):
        server = also_ran.Server(1, "burst", "running", LABELS)
        previous = also_ran.initial_state()
        previous["server_id"] = 1
        previous["idle_since"] = "2026-09-16T11:00:00Z"
        state, decision = also_ran.decide(
            self.policy, previous, self.observation(servers=(server,)), LABELS
        )
        self.assertEqual(state["state"], "IDLE")
        self.assertEqual(decision["action"], "none")
        self.assertEqual(
            decision["suppression_reason"],
            "automatic_deletion_not_implemented_without_drain",
        )

    def test_zero_maximum_does_not_reject_existing_slot(self):
        policy = dataclasses.replace(self.policy, maximum_extra_hosts=0)
        server = also_ran.Server(1, "burst", "running", LABELS)
        state, decision = also_ran.decide(
            policy,
            also_ran.initial_state(),
            self.observation(servers=(server,)),
            LABELS,
        )
        self.assertEqual(state["state"], "IDLE")
        self.assertIsNone(decision["suppression_reason"])

    def test_off_server_fails_closed(self):
        server = also_ran.Server(1, "burst", "off", LABELS)
        state, decision = also_ran.decide(
            self.policy,
            also_ran.initial_state(),
            self.observation(servers=(server,)),
            LABELS,
        )
        self.assertEqual(state["state"], "ERROR")
        self.assertEqual(decision["suppression_reason"], "burst_server_off")


class PersistenceTests(unittest.TestCase):
    def test_atomic_state_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(pathlib.Path(directory) / "state.json")
            also_ran.atomic_write_json(path, {"state": "ABSENT"})
            self.assertEqual(also_ran.load_json(path), {"state": "ABSENT"})
            self.assertEqual(pathlib.Path(path).stat().st_mode & 0o777, 0o600)


class ProviderTests(unittest.TestCase):
    def test_github_observer_finds_matching_queue_and_rate_limit(self):
        def respond(path, query):
            if path.endswith("/actions/runs"):
                runs = (
                    [{"id": 10, "created_at": "2026-09-16T11:58:00Z"}]
                    if query["status"] == "queued"
                    else []
                )
                return {"workflow_runs": runs}
            return {
                "jobs": [
                    {
                        "status": "queued",
                        "labels": ["self-hosted", "outrunner-hetzner-linux-x64"],
                        "created_at": "2026-09-16T11:58:00Z",
                    }
                ]
            }

        client = FakeClient(respond)
        observer = also_ran.GitHubObserver(client, "example-org/example-repo")
        policy = also_ran.Policy(
            workflow_label="outrunner-hetzner-linux-x64",
            burst_runner_prefix="also-ran-hetzner-burst-01-",
        )
        result = observer.observe(policy, also_ran.parse_time(NOW))
        self.assertEqual(result, (1, 120, 0, 4999))

    def test_github_pagination_is_bounded_and_complete(self):
        def respond(_path, query):
            count = 100 if query["page"] == 1 else 1
            return {"items": [{"id": number} for number in range(count)]}

        client = FakeClient(respond)
        observer = also_ran.GitHubObserver(client, "owner/repo")
        items = list(observer._pages("/items", "items", {}))
        self.assertEqual(len(items), 101)
        self.assertEqual(len(client.calls), 2)

    def test_github_provider_error_is_not_treated_as_empty(self):
        def fail(_path, _query):
            raise also_ran.ObservationError("rate limited")

        observer = also_ran.GitHubObserver(FakeClient(fail), "owner/repo")
        with self.assertRaisesRegex(also_ran.ObservationError, "rate limited"):
            list(observer._pages("/items", "items", {}))

    def test_github_active_run_cap_fails_closed_before_job_queries(self):
        def respond(path, query):
            if path.endswith("/actions/runs"):
                return {
                    "workflow_runs": (
                        [{"id": number, "created_at": NOW} for number in range(1, 4)]
                        if query["status"] == "queued"
                        else []
                    )
                }
            self.fail("job API must not run after the active-run cap is exceeded")

        observer = also_ran.GitHubObserver(
            FakeClient(respond), "owner/repo", max_active_runs=2
        )
        policy = also_ran.Policy("label", "runner-prefix")
        with self.assertRaisesRegex(also_ran.ObservationError, "safe inspection cap"):
            observer.observe(policy, also_ran.parse_time(NOW))

    def test_hetzner_observer_follows_pagination(self):
        def respond(_path, query):
            page = query["page"]
            return {
                "servers": [
                    {
                        "id": page,
                        "name": f"burst-{page}",
                        "status": "running",
                        "labels": LABELS,
                    }
                ],
                "meta": {"pagination": {"next_page": 2 if page == 1 else None}},
            }

        observer = also_ran.HetznerObserver(FakeClient(respond), "fleet=also-ran-ci")
        self.assertEqual([server.server_id for server in observer.observe()], [1, 2])

    def test_azure_observer_maps_running_owned_vm(self):
        vm_id = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/burst"

        def respond(path, _query):
            if path.endswith("/instanceView"):
                return {"statuses": [{"code": "PowerState/running"}]}
            return {
                "value": [
                    {
                        "id": vm_id,
                        "name": "burst",
                        "tags": LABELS,
                        "properties": {"provisioningState": "Succeeded"},
                    },
                    {
                        "id": vm_id + "-unowned",
                        "name": "unowned",
                        "tags": {**LABELS, "fleet": "other"},
                        "properties": {"provisioningState": "Succeeded"},
                    },
                ]
            }

        observer = also_ran.AzureObserver(FakeClient(respond), "sub", "rg", LABELS)
        servers = observer.observe()
        self.assertEqual(len(servers), 1)
        self.assertEqual(servers[0].server_id, vm_id)
        self.assertEqual(servers[0].status, "running")

    def test_azure_eviction_maps_to_absence_not_idle_host(self):
        self.assertEqual(
            also_ran.AzureObserver._status(["PowerState/deallocated"], "Succeeded"),
            "off",
        )

    def test_automatic_delete_configuration_is_rejected(self):
        config = {
            "mode": "observe",
            "automatic_delete": True,
            "workflow_label": "outrunner-hetzner-linux-x64",
            "burst_runner_prefix": "also-ran-hetzner-burst-01-",
            "maximum_extra_hosts": 1,
            "scale_up": {
                "oldest_matching_queue_seconds": 45,
                "consecutive_observations": 2,
                "queued_job_count_immediate": 2,
            },
            "scale_down": {"continuous_idle_minutes": 30},
            "required_server_labels": LABELS,
        }
        with self.assertRaisesRegex(RuntimeError, "graceful-drain"):
            also_ran.validate_config(config)



class RepositoryConfigurationTests(unittest.TestCase):
    def test_single_repository_key_still_configures_one_repository(self):
        self.assertEqual(
            also_ran.configured_repositories({"github_repository": "o/a"}),
            ("o/a",),
        )

    def test_repository_list_configures_several_in_order(self):
        self.assertEqual(
            also_ran.configured_repositories({"github_repositories": ["o/a", "o/b"]}),
            ("o/a", "o/b"),
        )

    def test_flag_configures_one_repository_when_config_names_none(self):
        self.assertEqual(also_ran.configured_repositories({}, "o/a"), ("o/a",))

    def test_two_sources_are_refused_rather_than_merged(self):
        for config, flag in (
            ({"github_repository": "o/a", "github_repositories": ["o/a"]}, None),
            ({"github_repositories": ["o/a"]}, "o/a"),
            ({"github_repository": "o/a"}, "o/b"),
        ):
            with self.subTest(config=config, flag=flag):
                with self.assertRaisesRegex(RuntimeError, "one place"):
                    also_ran.configured_repositories(config, flag)

    def test_there_is_no_default_repository(self):
        with self.assertRaisesRegex(RuntimeError, "no GitHub repository"):
            also_ran.configured_repositories({})

    def test_malformed_lists_fail_closed(self):
        for value in ([], "o/a", ["o/a", "O/A"], ["o/a", 3], ["no-slash"], ["o/a/b"]):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    also_ran.configured_repositories({"github_repositories": value})

    def test_runner_scope_defaults_to_repository(self):
        self.assertEqual(also_ran.configured_runner_scope({}, ("o/a",)), "repository")

    def test_organization_scope_needs_one_owner(self):
        self.assertEqual(
            also_ran.configured_runner_scope(
                {"github_runner_scope": "organization"}, ("o/a", "O/b")
            ),
            "organization",
        )
        with self.assertRaisesRegex(RuntimeError, "one owner"):
            also_ran.configured_runner_scope(
                {"github_runner_scope": "organization"}, ("o/a", "p/b")
            )
        with self.assertRaisesRegex(RuntimeError, "github_runner_scope"):
            also_ran.configured_runner_scope(
                {"github_runner_scope": "enterprise"}, ("o/a",)
            )

    def test_retired_repositories_are_distinct_and_not_served(self):
        self.assertEqual(also_ran.configured_retired_repositories({}, ("o/a",)), ())
        self.assertEqual(
            also_ran.configured_retired_repositories(
                {"github_retired_repositories": ["o/b"]}, ("o/a",)
            ),
            ("o/b",),
        )
        for value in (["O/A"], ["o/b", "o/b"], "o/b", ["bad"]):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    also_ran.configured_retired_repositories(
                        {"github_retired_repositories": value}, ("o/a",)
                    )

    def test_retired_organizations_are_owner_names(self):
        self.assertEqual(
            also_ran.configured_retired_organizations(
                {"github_retired_runner_organizations": ["o"]}
            ),
            ("o",),
        )
        for value in (["o/a"], ["o", "O"], "o"):
            with self.subTest(value=value):
                with self.assertRaises(RuntimeError):
                    also_ran.configured_retired_organizations(
                        {"github_retired_runner_organizations": value}
                    )

    def test_burst_observer_sums_queues_across_repositories(self):
        def respond(path, query):
            if path.endswith("/actions/runs"):
                if query["status"] != "queued":
                    return {"workflow_runs": []}
                return {"workflow_runs": [{"id": 1, "created_at": NOW}]}
            created = (
                "2026-09-16T11:58:00Z" if "/o/a/" in path else "2026-09-16T11:55:00Z"
            )
            return {
                "jobs": [
                    {
                        "status": "queued",
                        "labels": ["outrunner-hetzner-linux-x64"],
                        "created_at": created,
                    }
                ]
            }

        observer = also_ran.GitHubObserver(FakeClient(respond), ("o/a", "o/b"))
        policy = also_ran.Policy(
            workflow_label="outrunner-hetzner-linux-x64",
            burst_runner_prefix="also-ran-hetzner-burst-01-",
        )
        self.assertEqual(
            observer.observe(policy, also_ran.parse_time(NOW)), (2, 300, 0, 4999)
        )


if __name__ == "__main__":
    unittest.main()
