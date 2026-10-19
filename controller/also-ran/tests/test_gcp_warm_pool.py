import copy
import datetime as dt
import importlib.util
import json
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(pathlib.Path(__file__).parent))

import test_gcp_broker as broker  # noqa: E402

MODULE = broker.MODULE
UTC = dt.timezone.utc
# 2026-10-05 is a Monday; 08:00 PDT is 15:00Z.
MONDAY_OPEN = dt.datetime(2026, 10, 5, 15, 0, tzinfo=UTC)
# Two hours into Monday's window.
TICK_AT = MONDAY_OPEN + dt.timedelta(hours=2)


def warm(**overrides):
    config = {
        "timezone": "America/Los_Angeles",
        "days": ["mon", "tue", "wed", "thu", "fri"],
        "start": "08:00",
        "end": "18:00",
        "minimum_hosts": 8,
        "idle_minutes": 60,
    }
    config.update(overrides)
    return config


def warm_production(**overrides):
    config = broker.production()
    config["automatic_delete"] = True
    config["retention_mode"] = "peak_warm_pool"
    config["warm_pool"] = warm(**overrides)
    return config


def demand(queued=0, active=0, slots=()):
    return MODULE.Demand(
        queued=queued,
        active_owned=active,
        rate_remaining=4000,
        active_slots=frozenset(slots),
    )


class RetentionPolicyTests(unittest.TestCase):
    def test_demand_only_stays_valid_and_rejects_a_stray_warm_pool(self):
        config = broker.production()
        self.assertTrue(MODULE.valid_retention(config))
        config["warm_pool"] = warm()
        self.assertFalse(MODULE.valid_retention(config))

    def test_peak_warm_pool_is_accepted_by_both_validators(self):
        policy = broker.broker_policy()
        policy["production"]["retention_mode"] = "peak_warm_pool"
        policy["production"]["warm_pool"] = warm()
        policy["general_production"]["retention_mode"] = "peak_warm_pool"
        policy["general_production"]["warm_pool"] = warm(minimum_hosts=4)
        self.assertEqual(
            MODULE.validate_policy(policy)["warm_pool"]["minimum_hosts"], 8
        )
        self.assertEqual(
            MODULE.validate_policy(policy, "general")["warm_pool"]["minimum_hosts"], 4
        )

    def test_malformed_warm_pools_are_rejected(self):
        cases = {
            "unknown timezone": warm(timezone="Mars/Olympus"),
            "no days": warm(days=[]),
            "unknown day": warm(days=["mon", "funday"]),
            "repeated day": warm(days=["mon", "mon"]),
            "bad clock": warm(start="8:00"),
            "end before start": warm(start="18:00", end="08:00"),
            "empty window": warm(start="08:00", end="08:00"),
            "floor above maximum": warm(minimum_hosts=29),
            "zero floor": warm(minimum_hosts=0),
            "boolean floor": warm(minimum_hosts=True),
            "idle too short": warm(idle_minutes=1),
            "idle too long": warm(idle_minutes=600),
        }
        for name, candidate in cases.items():
            with self.subTest(name):
                config = broker.production()
                config["retention_mode"] = "peak_warm_pool"
                config["warm_pool"] = candidate
                self.assertFalse(MODULE.valid_retention(config))
        config = broker.production()
        config["retention_mode"] = "peak_warm_pool"
        config["warm_pool"] = {**warm(), "extra": 1}
        self.assertFalse(MODULE.valid_retention(config))
        config["warm_pool"] = None
        self.assertFalse(MODULE.valid_retention(config))

    def test_midnight_is_a_valid_end(self):
        config = broker.production()
        config["retention_mode"] = "peak_warm_pool"
        config["warm_pool"] = warm(start="06:00", end="24:00")
        self.assertTrue(MODULE.valid_retention(config))


class WarmFloorTests(unittest.TestCase):
    def test_demand_only_has_no_floor(self):
        self.assertEqual(MODULE.warm_floor(broker.production(), MONDAY_OPEN, None), 0)

    def test_window_opening_warms_the_pool_without_demand(self):
        config = warm_production()
        self.assertEqual(MODULE.warm_floor(config, MONDAY_OPEN, None), 8)
        # Work from yesterday does not shorten today's first hour.
        yesterday = MONDAY_OPEN - dt.timedelta(hours=20)
        self.assertEqual(
            MODULE.warm_floor(config, MONDAY_OPEN + dt.timedelta(minutes=59), yesterday),
            8,
        )

    def test_an_idle_hour_releases_the_floor_until_work_returns(self):
        config = warm_production()
        later = MONDAY_OPEN + dt.timedelta(minutes=60)
        self.assertEqual(MODULE.warm_floor(config, later, None), 0)
        worked = MONDAY_OPEN + dt.timedelta(minutes=50)
        self.assertEqual(MODULE.warm_floor(config, later, worked), 8)
        self.assertEqual(
            MODULE.warm_floor(config, worked + dt.timedelta(minutes=60), worked), 0
        )

    def test_outside_the_window_or_day_there_is_no_floor(self):
        config = warm_production()
        before = MONDAY_OPEN - dt.timedelta(minutes=1)
        closing = MONDAY_OPEN + dt.timedelta(hours=10)  # 18:00 local
        saturday = MONDAY_OPEN - dt.timedelta(days=2)
        for when in (before, closing, saturday):
            with self.subTest(when=when):
                self.assertEqual(MODULE.warm_floor(config, when, when), 0)
        self.assertEqual(
            MODULE.warm_floor(config, closing - dt.timedelta(seconds=1), closing), 8
        )

    def test_floor_never_exceeds_the_pool_maximum(self):
        config = warm_production(minimum_hosts=28)
        config["maximum_hosts"] = 12
        self.assertEqual(MODULE.warm_floor(config, MONDAY_OPEN, None), 12)

    def test_maintenance_holds_the_floor_a_margin_past_release(self):
        config = warm_production()
        closing = MONDAY_OPEN + dt.timedelta(hours=10)
        margin = MODULE.WARM_POOL_RELEASE_MARGIN
        busy = closing - dt.timedelta(minutes=5)
        self.assertEqual(MODULE.warm_floor(config, closing, busy), 0)
        self.assertEqual(MODULE.maintenance_warm_floor(config, closing, busy), 8)
        self.assertEqual(
            MODULE.maintenance_warm_floor(config, closing + margin, busy), 0
        )
        worked = MONDAY_OPEN + dt.timedelta(minutes=30)
        released = worked + dt.timedelta(minutes=60)
        self.assertEqual(MODULE.warm_floor(config, released, worked), 0)
        self.assertEqual(MODULE.maintenance_warm_floor(config, released, worked), 8)
        self.assertEqual(
            MODULE.maintenance_warm_floor(config, released + margin, worked), 0
        )


class LastDemandTests(unittest.TestCase):
    def state(self, value):
        return {"warm_pool": {"last_demand_at": value}}

    def test_current_work_and_recorded_work_both_count(self):
        now = dt.datetime.now(UTC).replace(microsecond=0)
        earlier = (now - dt.timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
        self.assertEqual(MODULE.last_demand_seen(demand(queued=1), now, now), now)
        self.assertEqual(
            MODULE.last_demand_seen(demand(), now, now, self.state(earlier)),
            now - dt.timedelta(minutes=5),
        )
        self.assertIsNone(MODULE.last_demand_seen(demand(), now, now, {}, None))

    def test_a_peer_observation_newer_than_this_one_counts(self):
        now = dt.datetime.now(UTC).replace(microsecond=0)
        opening = now - dt.timedelta(seconds=30)
        peer = now.isoformat().replace("+00:00", "Z")
        self.assertEqual(
            MODULE.last_demand_seen(demand(), opening, now, self.state(peer)), now
        )

    def test_future_and_malformed_times_are_ignored(self):
        now = dt.datetime.now(UTC)
        future = (now + dt.timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        self.assertIsNone(
            MODULE.last_demand_seen(
                demand(), now, now, self.state(future), self.state("yesterday"),
                self.state(17), {"warm_pool": None},
            )
        )


class FloorDecisionTests(unittest.TestCase):
    def test_maintenance_keeps_idle_hosts_up_to_the_floor(self):
        config = warm_production(minimum_hosts=3)
        hosts = tuple(broker.instance(slot) for slot in range(1, 6))
        result = MODULE.decide(config, demand(active=1, slots=[1]), hosts, floor=3)
        self.assertEqual(result["desired_hosts"], 3)
        self.assertEqual(result["minimum_hosts"], 3)
        self.assertEqual(result["drain_request_slots"], [4, 5])
        self.assertEqual(result["retention_mode"], "peak_warm_pool")

    def test_demand_above_the_floor_still_scales(self):
        config = warm_production(minimum_hosts=2)
        result = MODULE.decide(config, demand(queued=4), (), floor=2)
        self.assertEqual(result["desired_hosts"], 4)
        self.assertEqual(len(result["create_slots"]), 4)

    def test_released_floor_drains_to_zero(self):
        config = warm_production(minimum_hosts=3)
        hosts = tuple(broker.instance(slot) for slot in range(1, 4))
        result = MODULE.decide(config, demand(), hosts, floor=0)
        self.assertEqual(result["drain_request_slots"], [1, 2, 3])

    def test_fast_loop_fills_the_floor(self):
        config = warm_production(minimum_hosts=4)
        result = MODULE.fast_scale_up_decision(
            config, demand(), (broker.instance(1),), floor=4
        )
        self.assertEqual(result["create_slots"], [2, 3, 4])
        self.assertEqual(result["minimum_hosts"], 4)

    def test_without_a_floor_the_fast_loop_is_demand_only(self):
        result = MODULE.fast_scale_up_decision(
            warm_production(), demand(), (broker.instance(1),)
        )
        self.assertEqual(result["create_slots"], [])


class MaintenanceTickTests(unittest.TestCase):
    def run_tick(self, prior):
        import contextlib
        import io
        import tempfile
        from unittest import mock

        candidate = json.loads((ROOT / "gcp-acceptance-policy.json").read_text())
        candidate["production"]["automatic_delete"] = True
        candidate["production"]["retention_mode"] = "peak_warm_pool"
        candidate["production"]["warm_pool"] = warm(minimum_hosts=4)

        class Clock(dt.datetime):
            @classmethod
            def now(cls, tz=None):
                return TICK_AT if tz is None else TICK_AT.astimezone(tz)

        frozen = mock.MagicMock(wraps=dt)
        frozen.datetime = Clock
        frozen.timezone = dt.timezone
        frozen.timedelta = dt.timedelta
        hosts = tuple(broker.instance(slot) for slot in range(1, 7))
        with tempfile.TemporaryDirectory() as root:
            policy_path = pathlib.Path(root) / "policy.json"
            state_path = pathlib.Path(root) / "state.json"
            policy_path.write_text(json.dumps(candidate))
            (pathlib.Path(root) / "market.json").write_text("{}")
            state_path.write_text(json.dumps(prior))
            with (mock.patch.object(MODULE, "dt", frozen),
                  mock.patch.object(MODULE, "read_secret", return_value="test-only"),
                  mock.patch.object(MODULE.GitHubDemandObserver, "observe",
                                    return_value=demand()),
                  mock.patch.object(MODULE, "observe_instances", return_value=hosts),
                  mock.patch.object(MODULE, "observe_drained_instances",
                                    return_value=({}, {}, set(), set())),
                  contextlib.redirect_stdout(io.StringIO())):
                self.assertEqual(MODULE.main([
                    "--policy", str(policy_path),
                    "--market-state", str(pathlib.Path(root) / "market.json"),
                    "--state", str(state_path), "--lock", root + "/lock",
                    "--github-token-file", "unused",
                    "--once", "--dry-run", "--maintenance-only"]), 0)
            return json.loads(state_path.read_text())

    def test_recent_work_holds_the_floor_and_drains_only_the_surplus(self):
        recent = TICK_AT - dt.timedelta(minutes=5)
        state = self.run_tick(
            {"warm_pool": {"last_demand_at": recent.isoformat().replace("+00:00", "Z")}}
        )
        self.assertEqual(state["warm_pool"]["floor"], 4)
        self.assertEqual(state["warm_pool"]["decided_at"], "2026-10-05T17:00:00Z")
        self.assertEqual(state["minimum_hosts"], 4)
        self.assertEqual(state["drain_request_slots"], [5, 6])
        # The time carries forward while no new work is seen.
        self.assertEqual(
            state["warm_pool"]["last_demand_at"],
            recent.isoformat().replace("+00:00", "Z"),
        )

    def test_an_idle_hour_drains_the_pool_to_zero(self):
        stale = TICK_AT - dt.timedelta(minutes=90)
        state = self.run_tick(
            {"warm_pool": {"last_demand_at": stale.isoformat().replace("+00:00", "Z")}}
        )
        self.assertEqual(state["warm_pool"]["floor"], 0)
        self.assertEqual(state["drain_request_slots"], [1, 2, 3, 4, 5, 6])


class StaleDrainGuardTests(unittest.TestCase):
    # Both loops reused the 07:59:50 PDT snapshot: equal observed_at.
    SNAPSHOT = "2026-10-05T14:59:50Z"

    def decision(self, floor, decided_at="2026-10-05T14:59:58Z"):
        return {
            "observed_at": self.SNAPSHOT,
            "warm_pool": {"floor": floor, "last_demand_at": None,
                          "decided_at": decided_at},
            "drain_request_slots": [1, 2, 3],
            "delete_slots": [],
            "create_slots": [],
            "action": "drain",
        }

    def peer(self, floor, decided_at="2026-10-05T15:00:02Z", observed_at=SNAPSHOT):
        return {
            "observed_at": observed_at,
            "queued_jobs": 0,
            "active_owned_jobs": 0,
            "warm_pool": {"floor": floor, "last_demand_at": None,
                          "decided_at": decided_at},
        }

    def test_a_floor_raised_on_the_same_snapshot_vetoes_the_stale_drain(self):
        # Maintenance planned just before 08:00 PDT with no floor; the window
        # opened and the fast loop, on the same snapshot, now holds 8 hosts.
        decision = self.decision(0)
        self.assertTrue(MODULE.suppress_stale_scale_down(decision, self.peer(8)))
        self.assertEqual(decision["drain_request_slots"], [])
        self.assertEqual(decision["scale_down_suppressed_reason"], "newer_scale_up_demand")

    def test_the_wave_guard_also_sees_the_raised_floor(self):
        self.assertTrue(MODULE.newer_peer_has_demand(self.decision(0), self.peer(8)))

    def test_same_or_lower_or_earlier_floors_do_not_veto(self):
        self.assertFalse(MODULE.newer_peer_has_demand(self.decision(8), self.peer(8)))
        self.assertFalse(MODULE.newer_peer_has_demand(self.decision(8), self.peer(0)))
        self.assertFalse(MODULE.newer_peer_has_demand(
            self.decision(0), self.peer(8, decided_at="2026-10-05T14:59:00Z")))
        malformed = self.peer(8)
        malformed["warm_pool"]["decided_at"] = "soon"
        self.assertFalse(MODULE.newer_peer_has_demand(self.decision(0), malformed))
        unparsable = self.peer(8)
        unparsable["warm_pool"]["floor"] = "x"
        self.assertFalse(MODULE.newer_peer_has_demand(self.decision(0), unparsable))


class RepositoryPolicyTests(unittest.TestCase):
    def test_committed_policy_validates_and_describes_its_window(self):
        policy = json.loads((ROOT / "gcp-acceptance-policy.json").read_text())
        for pool in ("heavy", "general"):
            with self.subTest(pool=pool):
                config = MODULE.validate_policy(copy.deepcopy(policy), pool)
                self.assertTrue(MODULE.valid_retention(config))


if __name__ == "__main__":
    unittest.main()
