"""
Regression test for a replica crash in
KeyspaceEventManager::RemoveSubscription when LoadFromRDB fails mid-load.

Without the fix, workers parked in ScheduleMutation can hold the last strong
ref to the staged IndexSchema. When the load fails and the main thread drops
its ref, the destructor runs on a worker thread, calls MarkAsDestructing,
hits the main-thread CHECK in subscriptions_.Get(), and aborts.

The fix (absl::Cleanup in LoadFromRDB) ensures MarkAsDestructing runs on the
main thread on every error path, so the worker-thread destructor sees
is_destructing_ == true and skips the main-thread-only cleanup.

The test reproduces the bug window deterministically by:
  - parking workers at the mutation_processing pause point (they hold strong
    refs while parked),
  - tripping the ForceRDBLoadFailure controlled variable mid-load,
  - using diskless replica load (the only path where the engine survives an
    aux_load failure - on disk-based load the engine exits before we can
    observe the worker-thread crash).
"""
import struct
import time

import pytest

from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from ft_info_parser import FTInfoParser


class TestRdbLoadErrorCleanup(ValkeySearchTestCaseDebugMode):
    """Regression test for the worker-thread-destruction crash."""

    INDEX_NAME = "idx"
    PAUSEPOINT_NAME = "mutation_processing"
    CONTROLLED_VAR = "ForceRDBLoadFailure"
    NUM_VECTORS = 50
    SIM_ERROR_LOG = "Simulated IO error during RDB load"

    def get_config_file_lines(self, testdir, port):
        # Diskless sync on both ends - required for the engine to survive
        # an aux_load failure (cancelReplicationHandshake instead of exit(1)).
        # `flush-before-load` (not `on-empty-db` or `swapdb`) is required so
        # that the second sync also uses diskless: `on-empty-db` falls back
        # to disk once the replica is populated, and `swapdb` requires every
        # loaded module to opt into VALKEYMODULE_OPTIONS_HANDLE_REPL_ASYNC_LOAD
        # (the JSON module doesn't).
        return super().get_config_file_lines(testdir, port) + [
            "repl-diskless-sync yes",
            "repl-diskless-sync-delay 0",
            "repl-diskless-load flush-before-load",
            "repl-timeout 10",
        ]

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        # LoadIndexExtension only runs when both ends use V2; otherwise the
        # supplemental section is skipped during load.
        args["search.rdb-write-v2"] = "yes"
        args["search.rdb-read-v2"] = "yes"
        return args

    def _populate_primary_with_index(self, primary):
        """Create an HNSW index with NUM_VECTORS vectors on the primary."""
        primary.client.execute_command(
            "FT.CREATE", self.INDEX_NAME,
            "PREFIX", "1", "vec:",
            "SCHEMA", "vector", "VECTOR", "HNSW", "8",
            "TYPE", "FLOAT32", "DIM", "3",
            "DISTANCE_METRIC", "L2", "INITIAL_CAP", str(self.NUM_VECTORS))

        for i in range(self.NUM_VECTORS):
            v = struct.pack('<3f', float(i), float(i + 1), float(i + 2))
            primary.client.hset(f"vec:{i}", mapping={"vector": v})

    def _trigger_full_sync(self, primary, replica):
        """Force a full sync by changing the primary's replication id.

        change-repl-id forces the next handshake to be a full sync rather
        than a partial resync.
        """
        primary.client.execute_command("DEBUG", "change-repl-id")
        replica.client.execute_command("REPLICAOF", "NO", "ONE")
        time.sleep(0.5)
        replica.client.execute_command(
            "REPLICAOF", primary.server.bind_ip, str(primary.server.port))

    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 1}], indirect=True
    )
    def test_rdb_load_error_with_inflight_mutations_does_not_crash(self):
        primary = self.rg.primary
        replica = self.rg.replicas[0]

        self._populate_primary_with_index(primary)

        # Wait for initial sync to settle so the index is on the replica.
        waiters.wait_for_true(
            lambda: self.INDEX_NAME.encode() in replica.client.execute_command(
                "FT._LIST"),
            timeout=30)
        waiters.wait_for_equal(
            lambda: FTInfoParser(replica.client.execute_command(
                "FT.INFO", self.INDEX_NAME)).num_docs,
            self.NUM_VECTORS,
            timeout=30)

        # Arm the trap: park workers at mutation_processing, then trip
        # ForceRDBLoadFailure mid-load.
        replica.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", self.PAUSEPOINT_NAME)
        replica.client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            self.CONTROLLED_VAR, "yes")

        self._trigger_full_sync(primary, replica)

        # The bug window requires both: workers holding strong refs (parked)
        # AND the main thread having unwound from LoadFromRDB (error logged).
        # Either alone can pass without exercising the bug.
        def error_logged():
            return replica.does_logfile_contains(self.SIM_ERROR_LOG)

        def workers_paused():
            try:
                n = replica.client.execute_command(
                    "FT._DEBUG", "PAUSEPOINT", "TEST", self.PAUSEPOINT_NAME)
                return int(n) >= 1
            except (TypeError, ValueError):
                return False

        try:
            waiters.wait_for_true(
                lambda: error_logged() and workers_paused(),
                timeout=60)
        except Exception:
            # If the replica died during the waiter loop, that IS the crash
            # we're testing for. Detect it here so we fail loudly instead of
            # falling through to pytest.skip below.
            try:
                replica.client.ping()
            except Exception as exc:
                pytest.fail(
                    "Replica died before the bug window could be verified. "
                    "This is the crash the test is designed to catch, but it "
                    f"happened during setup instead of at the release point: "
                    f"{exc!r}")
            err = error_logged()
            paused = workers_paused()
            load_ran = replica.does_logfile_contains("Loading Index Extension")
            # Reset the trap before exiting.
            try:
                replica.client.execute_command(
                    "FT._DEBUG", "PAUSEPOINT", "RESET", self.PAUSEPOINT_NAME)
            except Exception:
                pass
            try:
                replica.client.execute_command(
                    "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
                    self.CONTROLLED_VAR, "no")
            except Exception:
                pass
            if load_ran and not err:
                pytest.fail(
                    "LoadIndexExtension ran but ForceRDBLoadFailure never "
                    "tripped - test infrastructure is broken (likely "
                    "--debug-mode missing on the replica).")
            pytest.skip(
                f"Bug window not engaged (err={err}, paused={paused}, "
                f"load_ran={load_ran}). The replica likely did a partial "
                "resync, or workers drained before parking.")

        # Release the workers. The last one to drop its shared_ptr runs
        # ~IndexSchema(). With the fix is_destructing_ is already true, so
        # the destructor skips MarkAsDestructing and the replica survives.
        # Without the fix the destructor calls MarkAsDestructing on the
        # worker thread, hits the main-thread CHECK, and aborts with SIGABRT.
        replica.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", self.PAUSEPOINT_NAME)
        time.sleep(2)  # let workers drain and run destructors

        crash_msg = (
            "Replica process died after RDB load error. IndexSchema "
            "destructor ran on a worker thread without MarkAsDestructing "
            "being called on the main thread - the fix is missing.")

        # Liveness check with retry over a window.
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                replica.client.ping()
            except Exception as exc:
                pytest.fail(f"{crash_msg} ({exc!r})")
            time.sleep(0.25)

        # Disarm the failure injection so the engine's automatic post-failure
        # resync can succeed, then verify sync recovers and the index is
        # repopulated. This proves the replica is not just alive but
        # functional after the crash window.
        replica.client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            self.CONTROLLED_VAR, "no")

        # Wait for the post-failure resync to succeed.
        self.rg._wait_for_replication()

        # And verify the index is fully repopulated.
        waiters.wait_for_equal(
            lambda: FTInfoParser(replica.client.execute_command(
                "FT.INFO", self.INDEX_NAME)).num_docs,
            self.NUM_VECTORS,
            timeout=60)
