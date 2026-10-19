import contextlib
import datetime as dt
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

import test_gcp_broker as broker  # noqa: E402

MODULE = broker.MODULE
UTC = dt.timezone.utc
NOW = dt.datetime(2026, 10, 8, 17, 30, tzinfo=UTC)


def z(at):
    return at.isoformat().replace("+00:00", "Z")


def demand(queued=0, active=0, slots=()):
    return MODULE.Demand(
        queued=queued,
        active_owned=active,
        rate_remaining=4000,
        active_slots=frozenset(slots),
    )


def deleting_production(hold_minutes=5):
    config = broker.production()
    config["automatic_delete"] = True
    config["scale_down_hold_minutes"] = hold_minutes
    return config


class HoldPolicyTests(unittest.TestCase):
    def test_hold_is_required_and_bounded_in_both_pools(self):
        for pool, key in (("heavy", "production"), ("general", "general_production")):
            for value in (0, 5, 30):
                policy = broker.broker_policy()
                policy[key]["scale_down_hold_minutes"] = value
                self.assertEqual(
                    MODULE.validate_policy(policy, pool)["scale_down_hold_minutes"],
                    value,
                )
            for value in (None, -1, 31, 5.0, "5", True):
                policy = broker.broker_policy()
                if value is None:
                    del policy[key]["scale_down_hold_minutes"]
                else:
                    policy[key]["scale_down_hold_minutes"] = value
                with self.assertRaises(MODULE.BrokerError, msg=f"{pool} {value!r}"):
                    MODULE.validate_policy(policy, pool)

    def test_committed_policy_holds_five_minutes_in_both_pools(self):
        policy = json.loads((ROOT / "gcp-acceptance-policy.json").read_text())
        self.assertEqual(policy["production"]["scale_down_hold_minutes"], 5)
        self.assertEqual(policy["general_production"]["scale_down_hold_minutes"], 5)


class DemandHistoryTests(unittest.TestCase):
    def test_merges_samples_and_states_within_the_window(self):
        prior = {
            "demand_history": [
                {"at": z(NOW - dt.timedelta(minutes=6)), "demand": 20},
                {"at": z(NOW - dt.timedelta(minutes=4)), "demand": 9},
                {"at": z(NOW - dt.timedelta(minutes=2)), "demand": 3},
            ]
        }
        history = MODULE.demand_history(
            5, NOW, [(NOW, 2), (NOW - dt.timedelta(seconds=20), 4)], prior
        )
        self.assertEqual(
            history,
            [
                {"at": z(NOW - dt.timedelta(minutes=4)), "demand": 9},
                {"at": z(NOW - dt.timedelta(minutes=2)), "demand": 3},
                {"at": z(NOW - dt.timedelta(seconds=20)), "demand": 4},
                {"at": z(NOW), "demand": 2},
            ],
        )
        self.assertEqual(MODULE.scale_down_hold(history), 9)

    def test_future_malformed_and_negative_entries_are_ignored(self):
        prior = {
            "demand_history": [
                {"at": z(NOW + dt.timedelta(minutes=1)), "demand": 50},
                {"at": "yesterday", "demand": 50},
                {"at": z(NOW), "demand": "many"},
                {"at": z(NOW), "demand": -3},
                "not an entry",
            ]
        }
        history = MODULE.demand_history(5, NOW, [(None, 7)], prior, None)
        self.assertEqual(history, [])
        self.assertEqual(MODULE.scale_down_hold(history), 0)

    def test_zero_minutes_disables_the_history(self):
        self.assertEqual(MODULE.demand_history(0, NOW, [(NOW, 12)]), [])

    def test_a_loop_state_becomes_one_sample(self):
        state = {"observed_at": z(NOW), "queued_jobs": 3, "active_owned_jobs": 4}
        self.assertEqual(MODULE.state_demand_sample(state), (NOW, 7))
        self.assertEqual(MODULE.state_demand_sample({}), (None, 0))
        self.assertEqual(MODULE.state_demand_sample(None), (None, 0))


class HoldDecisionTests(unittest.TestCase):
    def test_hold_keeps_idle_hosts_from_a_recent_burst(self):
        hosts = tuple(broker.instance(slot) for slot in range(1, 11))
        result = MODULE.decide(
            deleting_production(), demand(active=2, slots=[1, 2]), hosts, hold=8
        )
        self.assertEqual(result["desired_hosts"], 2)
        self.assertEqual(result["scale_down_hold_hosts"], 8)
        self.assertEqual(result["drain_request_slots"], [9, 10])
        self.assertEqual(result["create_slots"], [])

    def test_hold_never_creates_hosts(self):
        hosts = (broker.instance(1),)
        result = MODULE.decide(deleting_production(), demand(), hosts, hold=6)
        self.assertEqual(result["create_slots"], [])
        self.assertEqual(result["drain_request_slots"], [])

    def test_without_a_hold_idle_surplus_drains_immediately(self):
        hosts = tuple(broker.instance(slot) for slot in range(1, 6))
        result = MODULE.decide(
            deleting_production(), demand(active=1, slots=[1]), hosts
        )
        self.assertEqual(result["drain_request_slots"], [2, 3, 4, 5])

    def test_hold_does_not_shield_quarantined_hosts(self):
        hosts = tuple(broker.instance(slot) for slot in range(1, 4))
        result = MODULE.decide(
            deleting_production(),
            demand(),
            hosts,
            quarantined={3: "malformed receipt"},
            hold=3,
        )
        self.assertEqual(result["drain_request_slots"], [3])

    def test_hold_does_not_delay_deleting_broken_or_drained_hosts(self):
        hosts = tuple(broker.instance(slot) for slot in range(1, 8))
        stalled = {"7": {"since": z(NOW - dt.timedelta(minutes=50)),
                         "instance_id": hosts[6].instance_id}}
        result = MODULE.decide(
            deleting_production(),
            demand(),
            hosts,
            drained={4: {"completed_jobs": 5, "max_jobs": 5}},
            now=NOW,
            drain_stall_ledger=stalled,
            hold=10,
        )
        self.assertEqual(result["delete_slots"], [4, 7])
        self.assertEqual(result["drain_request_slots"], [])

    def test_the_larger_of_the_warm_floor_and_the_hold_applies(self):
        hosts = tuple(broker.instance(slot) for slot in range(1, 11))
        for floor, hold in ((8, 3), (3, 8)):
            result = MODULE.decide(
                deleting_production(), demand(), hosts, floor=floor, hold=hold
            )
            self.assertEqual(result["drain_request_slots"], [9, 10], (floor, hold))

    def test_hold_counts_a_job_finishing_on_a_drain_requested_host(self):
        hosts = (
            broker.instance(1, metadata={"runner-drain-request": "1001"}),
        ) + tuple(
            broker.instance(slot) for slot in range(2, 7)
        )
        result = MODULE.decide(
            deleting_production(), demand(active=1, slots=[1]), hosts, hold=4
        )
        self.assertEqual(result["active_nonserving_slots"], [1])
        # Hold 4 less the one active non-serving host keeps 3 of 5 serving.
        self.assertEqual(result["drain_request_slots"], [5, 6])

    def test_hold_is_capped_by_the_pool_maximum(self):
        hosts = tuple(broker.instance(slot) for slot in range(1, 29))
        result = MODULE.decide(deleting_production(), demand(), hosts, hold=40)
        self.assertEqual(result["drain_request_slots"], [])


class MaintenanceTickTests(unittest.TestCase):
    def run_tick(self, prior, scale_up_state=None, hold_minutes=5):
        candidate = json.loads((ROOT / "gcp-acceptance-policy.json").read_text())
        candidate["production"]["automatic_delete"] = True
        candidate["production"]["retention_mode"] = "demand_only"
        candidate["production"].pop("warm_pool", None)
        candidate["production"]["scale_down_hold_minutes"] = hold_minutes

        class Clock(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return NOW if tz is None else NOW.astimezone(tz)

        frozen = mock.MagicMock(wraps=dt)
        frozen.datetime = Clock
        frozen.timezone = dt.timezone
        frozen.timedelta = dt.timedelta
        hosts = tuple(broker.instance(slot) for slot in range(1, 7))
        with tempfile.TemporaryDirectory() as root:
            policy_path = pathlib.Path(root) / "policy.json"
            state_path = pathlib.Path(root) / "state.json"
            scale_up_path = pathlib.Path(root) / "scale-up.json"
            policy_path.write_text(json.dumps(candidate))
            (pathlib.Path(root) / "market.json").write_text("{}")
            state_path.write_text(json.dumps(prior))
            args = [
                "--policy", str(policy_path),
                "--market-state", str(pathlib.Path(root) / "market.json"),
                "--state", str(state_path), "--lock", root + "/lock",
                "--github-token-file", "unused",
                "--once", "--dry-run", "--maintenance-only",
            ]
            if scale_up_state is not None:
                scale_up_path.write_text(json.dumps(scale_up_state))
                args += ["--scale-up-state", str(scale_up_path)]
            with (mock.patch.object(MODULE, "dt", frozen),
                  mock.patch.object(MODULE, "read_secret", return_value="test-only"),
                  mock.patch.object(MODULE.GitHubDemandObserver, "observe",
                                    return_value=demand()),
                  mock.patch.object(MODULE, "observe_instances", return_value=hosts),
                  mock.patch.object(MODULE, "observe_drained_instances",
                                    return_value=({}, {}, set(), set())),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(MODULE.main(args), 0)
            return json.loads(state_path.read_text())

    def test_recent_peak_holds_hosts_and_is_recorded(self):
        prior = {
            "demand_history": [
                {"at": z(NOW - dt.timedelta(minutes=3)), "demand": 4}
            ]
        }
        state = self.run_tick(prior)
        self.assertEqual(state["scale_down_hold_hosts"], 4)
        self.assertEqual(state["drain_request_slots"], [5, 6])
        self.assertIn(
            {"at": z(NOW - dt.timedelta(minutes=3)), "demand": 4},
            state["demand_history"],
        )

    def test_the_fast_loop_observation_counts(self):
        scale_up = {
            "observed_at": z(NOW - dt.timedelta(seconds=30)),
            "queued_jobs": 2,
            "active_owned_jobs": 3,
        }
        state = self.run_tick({}, scale_up_state=scale_up)
        self.assertEqual(state["scale_down_hold_hosts"], 5)
        self.assertEqual(state["drain_request_slots"], [6])

    def test_an_expired_peak_releases_the_hosts(self):
        prior = {
            "demand_history": [
                {"at": z(NOW - dt.timedelta(minutes=6)), "demand": 6}
            ]
        }
        state = self.run_tick(prior)
        self.assertEqual(state["scale_down_hold_hosts"], 0)
        self.assertEqual(state["drain_request_slots"], [1, 2, 3, 4, 5, 6])

    def test_zero_minutes_restores_immediate_scale_down(self):
        prior = {
            "demand_history": [
                {"at": z(NOW - dt.timedelta(minutes=1)), "demand": 6}
            ]
        }
        state = self.run_tick(prior, hold_minutes=0)
        self.assertEqual(state["scale_down_hold_hosts"], 0)
        self.assertEqual(state["demand_history"], [])
        self.assertEqual(state["drain_request_slots"], [1, 2, 3, 4, 5, 6])


if __name__ == "__main__":
    unittest.main()
