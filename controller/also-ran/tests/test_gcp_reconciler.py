import argparse
import datetime as dt
import importlib.util
import io
import pathlib
import json
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))


def load(name):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


BROKER = load("gcp_broker")
POOLS = load("gcp_pools")
MODULE = load("gcp_reconciler")


def arguments(**overrides):
    values = {
        "policy": "/etc/also-ran/gcp-policy.json",
        "market_state": "/var/lib/also-ran/gcp-market-state.json",
        "github_token_file": "/run/credentials/token",
        "lifecycle_script": "/opt/also-ran/gcp-runner-lifecycle.py",
        "heavy_mode": "apply",
        "general_mode": "dry-run",
        "state_directory": "/var/lib/also-ran",
        "lock_directory": "/run/also-ran",
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class BrokerArgumentTests(unittest.TestCase):
    def test_ticks_use_the_paths_the_timers_used(self):
        common = [
            "--policy", "/etc/also-ran/gcp-policy.json",
            "--market-state", "/var/lib/also-ran/gcp-market-state.json",
            "--github-token-file", "/run/credentials/token",
            "--lifecycle-script", "/opt/also-ran/gcp-runner-lifecycle.py",
            "--once",
        ]
        for scale_up in (True, False):
            with mock.patch.object(POOLS.subprocess, "run",
                                   return_value=mock.Mock(returncode=0)) as run:
                POOLS.main([*common, "--heavy-mode", "apply", "--general-mode", "dry-run",
                            "--scale-up-only" if scale_up else "--maintenance-only"])
            timer_commands = {
                call.args[0][call.args[0].index("--pool") + 1]: call.args[0][2:]
                for call in run.call_args_list
            }
            for pool in MODULE.POOLS:
                with self.subTest(pool=pool, scale_up=scale_up):
                    self.assertEqual(
                        sorted(MODULE.broker_argv(arguments(), pool, scale_up)),
                        sorted(timer_commands[pool]),
                    )

    def test_broker_accepts_every_tick_argument_set(self):
        for pool in MODULE.POOLS:
            for scale_up in (True, False):
                with self.subTest(pool=pool, scale_up=scale_up), \
                        mock.patch.object(BROKER, "exclusive_lock",
                                          side_effect=RuntimeError("parsed")):
                    with self.assertRaisesRegex(RuntimeError, "parsed"):
                        BROKER.main(MODULE.broker_argv(arguments(), pool, scale_up))


class DemandIntervalTests(unittest.TestCase):
    def test_interval_stretches_as_allowance_falls(self):
        self.assertEqual(MODULE.demand_interval(None), 20.0)
        self.assertEqual(MODULE.demand_interval(4000), 20.0)
        self.assertEqual(MODULE.demand_interval(2999), 30.0)
        self.assertEqual(MODULE.demand_interval(1499), 60.0)


class RefreshDemandTests(unittest.TestCase):
    def test_broker_ticks_reuse_the_snapshot_the_demand_task_wrote(self):
        requests = []

        def github(path, query):
            requests.append(path)
            if path.endswith("/actions/runs"):
                runs = {"queued": [{"id": 1}], "in_progress": []}
                return {"workflow_runs": runs[query["status"]] if query["page"] == 1 else []}
            return {"jobs": [{"labels": ["heavy"], "status": "queued", "runner_name": None}]
                    if query["page"] == 1 else []}

        with tempfile.TemporaryDirectory() as root:
            policy = pathlib.Path(root, "policy.json")
            policy.write_text(json.dumps({"github_repositories": ["example-org/example-repo"]}))
            token = pathlib.Path(root, "token")
            token.write_text("token")
            args = arguments(policy=str(policy), github_token_file=str(token),
                             state_directory=root)
            with mock.patch.object(BROKER.JsonClient, "_get", side_effect=github), \
                    mock.patch.object(sys, "stdout", io.StringIO()) as stdout:
                refresh = MODULE.refresh_demand(args)
            self.assertEqual(len(requests), 3)
            self.assertEqual(json.loads(stdout.getvalue())["github_requests"], 3)

            # A broker tick moments later reads the same snapshot without GitHub.
            observer = BROKER.GitHubDemandObserver(
                mock.Mock(side_effect=AssertionError("fetched")), "example-org/example-repo"
            )
            demand = observer.observe(
                "heavy", "also-ran-gcp-prod-", dt.datetime.now(dt.timezone.utc),
                BROKER.DemandSnapshotStore(MODULE.demand_snapshot_path(args)),
                BROKER.DEMAND_SNAPSHOT_MAX_AGE,
            )
            self.assertEqual(demand.queued, 1)
            self.assertEqual(demand.observed_at, refresh.observed_at)


    def test_each_pass_reads_the_token_file_again(self):
        # An App token is replaced in place every 20 minutes; the service never restarts.
        with tempfile.TemporaryDirectory() as root:
            policy = pathlib.Path(root, "policy.json")
            policy.write_text(json.dumps({"github_repositories": ["example-org/example-repo"]}))
            token = pathlib.Path(root, "token")
            args = arguments(policy=str(policy), github_token_file=str(token),
                             state_directory=root)
            client = mock.Mock(side_effect=RuntimeError("stop"))
            with mock.patch.object(BROKER, "CountingJsonClient", client):
                for value in ("first", "second"):
                    token.write_text(value + "\n")
                    with self.assertRaisesRegex(RuntimeError, "stop"):
                        MODULE.refresh_demand(args)
            self.assertEqual([call.args[0] for call in client.call_args_list],
                             ["first", "second"])


class TaskTests(unittest.TestCase):
    def task(self, tick):
        return MODULE.Task("heavy-scale-up", tick, lambda task: None, threading.Event())

    def test_failure_is_reported_and_counted(self):
        task = self.task(mock.Mock(side_effect=BROKER.BrokerError("create failed")))
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            task.run_once()
            task.run_once()
        self.assertEqual(task.failures, 2)
        self.assertIn('"result": "error"', stderr.getvalue())
        self.assertIn(
            "<3>also-ran unit failed: also-ran-gcp-reconciler/heavy-scale-up: create failed",
            stderr.getvalue(),
        )
        task.tick = mock.Mock(return_value=0)
        task.run_once()
        self.assertEqual(task.failures, 0)

    def test_a_huge_failure_message_stays_one_journal_line(self):
        task = self.task(mock.Mock(side_effect=BROKER.BrokerError("x" * 200_000)))
        stderr = io.StringIO()
        with mock.patch.object(sys, "stderr", stderr):
            task.run_once()
        for line in stderr.getvalue().splitlines():
            self.assertLess(len(line), 48 * 1024)
        self.assertIn("more chars truncated", stderr.getvalue())

    def test_exit_from_a_tick_is_a_failure_not_a_dead_thread(self):
        task = self.task(mock.Mock(side_effect=SystemExit(2)))
        with mock.patch.object(sys, "stderr", io.StringIO()):
            task.run_once()
        self.assertEqual(task.failures, 1)
        self.assertIsNone(task.running_since)

    def test_lock_held_by_a_timer_pass_is_skipped_quietly(self):
        task = self.task(mock.Mock(side_effect=RuntimeError(MODULE.LOCK_BUSY)))
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(sys, "stdout", stdout), mock.patch.object(sys, "stderr", stderr):
            task.run_once()
        self.assertEqual(task.failures, 0)
        self.assertIn("lock_busy", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    def test_a_tick_past_its_deadline_is_unhealthy(self):
        task = self.task(lambda: None)
        now = time.monotonic()
        self.assertTrue(task.healthy(now))
        task.running_since = now - MODULE.TICK_DEADLINE - 1
        self.assertFalse(task.healthy(now))


class ReconcilerTests(unittest.TestCase):
    def test_scale_up_wakes_only_on_newer_demand(self):
        reconciler = MODULE.Reconciler(arguments())
        first = dt.datetime(2026, 10, 2, 5, 0, tzinfo=dt.timezone.utc)
        refreshes = [
            MODULE.DemandRefresh(first, 4000),
            MODULE.DemandRefresh(first, 4000),
            MODULE.DemandRefresh(first + dt.timedelta(seconds=15), 4000),
        ]
        with mock.patch.object(MODULE, "refresh_demand", side_effect=refreshes):
            reconciler.demand_tick()
            self.assertTrue(all(event.is_set() for event in reconciler.wakeups.values()))
            for event in reconciler.wakeups.values():
                event.clear()
            reconciler.demand_tick()
            self.assertFalse(any(event.is_set() for event in reconciler.wakeups.values()))
            reconciler.demand_tick()
            self.assertTrue(all(event.is_set() for event in reconciler.wakeups.values()))

    def test_serve_runs_pools_independently_and_stops_cleanly(self):
        calls = []
        lock = threading.Lock()
        heavy_maintenance_started = threading.Event()
        release = threading.Event()
        observed = [dt.datetime(2026, 10, 2, 5, 0, tzinfo=dt.timezone.utc)]

        def broker(argv):
            name = argv[argv.index("--pool") + 1] + (
                "-scale-up" if "--scale-up-only" in argv else "-maintenance"
            )
            with lock:
                calls.append(name)
            if name == "heavy-maintenance":
                # A slow drain/delete in one pool and loop.
                heavy_maintenance_started.set()
                release.wait(5)
            return 0

        def refresh(_args):
            observed[0] += dt.timedelta(seconds=15)
            return MODULE.DemandRefresh(observed[0], 4000)

        reconciler = MODULE.Reconciler(arguments())
        notices = []
        with mock.patch.object(MODULE.gcp_broker, "main", side_effect=broker), \
                mock.patch.object(MODULE, "refresh_demand", side_effect=refresh), \
                mock.patch.object(MODULE, "sd_notify", side_effect=notices.append), \
                mock.patch.object(MODULE, "WATCHDOG_PERIOD", 0.05), \
                mock.patch.object(MODULE, "MINIMUM_GAP", 0.01), \
                mock.patch.object(MODULE, "SCALE_UP_MAX_WAIT", 0.05), \
                mock.patch.object(MODULE, "DEMAND_INTERVALS", ((None, 0.05),)), \
                mock.patch.object(sys, "stdout", io.StringIO()):
            server = threading.Thread(target=lambda: notices.append(reconciler.serve()))
            server.start()
            self.assertTrue(heavy_maintenance_started.wait(5))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                with lock:
                    if calls.count("heavy-scale-up") >= 3 and calls.count("general-maintenance") >= 1:
                        break
                time.sleep(0.01)
            release.set()
            reconciler.shutdown()
            server.join(5)
        self.assertFalse(server.is_alive())
        # Scale-up kept running while heavy maintenance was stuck in its tick.
        self.assertGreaterEqual(calls.count("heavy-scale-up"), 3)
        self.assertGreaterEqual(calls.count("general-scale-up"), 3)
        self.assertGreaterEqual(calls.count("general-maintenance"), 1)
        self.assertEqual(notices[0], "READY=1")
        self.assertIn("WATCHDOG=1", notices)
        self.assertEqual(notices[-1], 0)

    def write_state(self, directory, pool, scale_up, state):
        path = MODULE.state_path(arguments(state_directory=directory), pool, scale_up)
        pathlib.Path(path).write_text(json.dumps(state))

    def test_scale_up_hands_surplus_to_maintenance(self):
        with tempfile.TemporaryDirectory() as directory:
            reconciler = MODULE.Reconciler(arguments(state_directory=directory))
            self.write_state(directory, "heavy", True,
                             {"serving_hosts": 3, "desired_serving_hosts": 1})
            self.write_state(directory, "general", True,
                             {"serving_hosts": 2, "desired_serving_hosts": 2})
            with mock.patch.object(MODULE.gcp_broker, "main", return_value=0):
                reconciler.scale_up_tick("heavy")
                reconciler.scale_up_tick("general")
            self.assertTrue(reconciler.maintenance_wakeups["heavy"].is_set())
            self.assertFalse(reconciler.maintenance_wakeups["general"].is_set())

    def test_surplus_within_the_scale_down_hold_does_not_wake_maintenance(self):
        with tempfile.TemporaryDirectory() as directory:
            reconciler = MODULE.Reconciler(arguments(state_directory=directory))
            self.write_state(directory, "heavy", False, {"scale_down_hold_hosts": 6})
            self.write_state(directory, "heavy", True,
                             {"serving_hosts": 6, "desired_hosts": 2,
                              "desired_serving_hosts": 2})
            with mock.patch.object(MODULE.gcp_broker, "main", return_value=0):
                reconciler.scale_up_tick("heavy")
            self.assertFalse(reconciler.maintenance_wakeups["heavy"].is_set())
            # Surplus beyond the hold still wakes it.
            self.write_state(directory, "heavy", True,
                             {"serving_hosts": 7, "desired_hosts": 2,
                              "desired_serving_hosts": 2})
            with mock.patch.object(MODULE.gcp_broker, "main", return_value=0):
                reconciler.scale_up_tick("heavy")
            self.assertTrue(reconciler.maintenance_wakeups["heavy"].is_set())

    def test_the_hold_counts_hosts_finishing_work_after_a_drain_request(self):
        # One active job sits on a drain-requested host: desired 3 hosts, 2 of
        # them serving. A hold of 5 therefore keeps 4 serving hosts.
        fast = {"serving_hosts": 4, "desired_hosts": 3, "desired_serving_hosts": 2}
        self.assertFalse(MODULE.surplus_to_drain(fast, {"scale_down_hold_hosts": 5}))
        self.assertTrue(MODULE.surplus_to_drain(fast, {"scale_down_hold_hosts": 4}))
        self.assertTrue(MODULE.surplus_to_drain(fast))
        self.assertTrue(
            MODULE.surplus_to_drain(fast, {"scale_down_hold_hosts": "x"}) is False
        )

    def test_pending_teardown_keeps_maintenance_on_the_snapshot_cadence(self):
        with tempfile.TemporaryDirectory() as directory:
            reconciler = MODULE.Reconciler(arguments(state_directory=directory))
            self.write_state(directory, "general", False,
                             {"drain_requested_slots": [3], "delete_slots": []})
            self.write_state(directory, "general", True,
                             {"serving_hosts": 0, "desired_serving_hosts": 0})
            with mock.patch.object(MODULE.gcp_broker, "main", return_value=0):
                reconciler.maintenance_tick("general")
                self.assertTrue(reconciler.teardown["general"])
                self.assertFalse(reconciler.maintenance_wakeups["general"].is_set())
                reconciler.scale_up_tick("general")
            self.assertTrue(reconciler.maintenance_wakeups["general"].is_set())
            self.write_state(directory, "general", False,
                             {"drain_requested_slots": [], "delete_slots": []})
            with mock.patch.object(MODULE.gcp_broker, "main", return_value=0):
                reconciler.maintenance_tick("general")
                reconciler.scale_up_tick("general")
            self.assertFalse(reconciler.teardown["general"])
            self.assertFalse(reconciler.maintenance_wakeups["general"].is_set())

    def test_a_failed_tick_still_records_what_it_left(self):
        with tempfile.TemporaryDirectory() as directory:
            reconciler = MODULE.Reconciler(arguments(state_directory=directory))
            self.write_state(directory, "heavy", False, {"drain_request_slots": [1]})
            with mock.patch.object(MODULE.gcp_broker, "main",
                                   side_effect=RuntimeError("boom")), \
                    self.assertRaises(RuntimeError):
                reconciler.maintenance_tick("heavy")
            self.assertTrue(reconciler.teardown["heavy"])

    def test_maintenance_wakes_early_only_after_the_teardown_interval(self):
        reconciler = MODULE.Reconciler(arguments())
        task = mock.Mock(last_started=time.monotonic())
        reconciler.maintenance_wakeups["heavy"].set()
        with mock.patch.object(MODULE, "TEARDOWN_INTERVAL", 0.2), \
                mock.patch.object(MODULE, "MAINTENANCE_INTERVAL", 5.0), \
                mock.patch.object(MODULE, "MINIMUM_GAP", 0.0):
            reconciler.wait_maintenance(task, "heavy")
        waited = time.monotonic() - task.last_started
        self.assertGreaterEqual(waited, 0.2)
        self.assertLess(waited, 2.0)

    def test_quiet_maintenance_waits_the_full_interval(self):
        reconciler = MODULE.Reconciler(arguments())
        task = mock.Mock(last_started=time.monotonic())
        with mock.patch.object(MODULE, "TEARDOWN_INTERVAL", 0.05), \
                mock.patch.object(MODULE, "MAINTENANCE_INTERVAL", 0.4), \
                mock.patch.object(MODULE, "MINIMUM_GAP", 0.0):
            reconciler.wait_maintenance(task, "general")
        self.assertGreaterEqual(time.monotonic() - task.last_started, 0.4)

    def test_state_helpers_tolerate_missing_and_malformed_state(self):
        self.assertEqual(MODULE.read_state("/nonexistent/state.json"), {})
        self.assertFalse(MODULE.teardown_pending({}))
        self.assertFalse(MODULE.surplus_to_drain({"serving_hosts": "x"}))
        self.assertTrue(MODULE.teardown_pending({"delete_slots": [2]}))

    def test_watchdog_stops_while_a_tick_is_stuck(self):
        reconciler = MODULE.Reconciler(arguments())
        reconciler.tasks[1].running_since = time.monotonic() - MODULE.TICK_DEADLINE - 1
        self.assertEqual(reconciler.healthy(), ["heavy-scale-up"])


class LineWriterTests(unittest.TestCase):
    def test_lines_from_concurrent_writers_never_interleave(self):
        target = io.StringIO()
        writer = MODULE.LineWriter(target)

        def write(tag):
            for _ in range(200):
                print("{" + tag * 50, end="", file=writer)
                print("}", file=writer)

        threads = [threading.Thread(target=write, args=(tag,)) for tag in "ab"]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        lines = target.getvalue().splitlines()
        self.assertEqual(len(lines), 400)
        self.assertTrue(all(line in ("{" + "a" * 50 + "}", "{" + "b" * 50 + "}") for line in lines))


if __name__ == "__main__":
    unittest.main()
