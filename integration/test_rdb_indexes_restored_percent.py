"""
Verify search_rdb_indexes_restored_percent reports 100% after successful RDB
restore in both standalone (CMD) and cluster/coordinator (CME) modes.

This metric was previously broken in CME mode because the GLOBAL_METADATA RDB
section was counted in the total but never incremented the completed counter,
causing it to report 66.67% with 2 indexes (2/3 sections completed). That was
first worked around by having the metadata section increment the completed
counter too. Both the total and the completed counter now track index schemas
only: the total comes from the RDB_SECTION_SNAPSHOT_INFO section written at
save time (falling back to section arithmetic for older RDBs), and the metadata
section no longer increments the completed counter.
"""

import base64
import os
import threading

from indexes import Index, Numeric, Tag, Vector
from valkey_search_test_case import (
    LOGS_DIR,
    ValkeySearchClusterTestCaseDebugMode,
    ValkeySearchTestCaseCommon,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


index_1 = Index("idx_pct_1", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
index_2 = Index("idx_pct_2", [Vector("v", 3, type="HNSW", m=2, efc=1), Tag("t")])
index_3 = Index("idx_pct_3", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
NUM_DOCS = 500


class TestIndexesRestoredPercentStandalone(ValkeySearchTestCaseDebugMode):
    """
    CMD (standalone) mode: the index total comes from SNAPSHOT_INFO and counts
    only index schemas. After restore, the metric should progress monotonically
    and reach 100%.
    """

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.max-mutation-queue-size-on-restore"] = "1"
        return args

    def test_indexes_restored_percent_is_100_after_restore(self):
        # Create indexes and load data
        print(f"[Standalone] Creating indexes and loading {NUM_DOCS} docs each...")
        index_1.create(self.client, True)
        index_2.create(self.client, True)
        index_1.load_data(self.client, NUM_DOCS)
        index_2.load_data(self.client, NUM_DOCS)

        waiters.wait_for_true(lambda: index_1.backfill_complete(self.client))
        waiters.wait_for_true(lambda: index_2.backfill_complete(self.client))

        # Baseline the counters we expect the save/restore cycle to move.
        before = self.client.info("search")
        assert before["search_number_of_indexes"] == 2, \
            f"Expected 2 indexes before save, got {before['search_number_of_indexes']}"
        save_success_before = int(before.get("search_rdb_save_success_cnt", 0))
        load_success_before = int(before.get("search_rdb_load_success_cnt", 0))

        # Save RDB
        self.client.execute_command("SAVE")

        after_save = self.client.info("search")
        assert int(after_save["search_rdb_save_success_cnt"]) == save_success_before + 1, \
            "Expected search_rdb_save_success_cnt to increment after SAVE"
        assert int(after_save.get("search_rdb_save_failure_cnt", 0)) == 0, \
            "Expected no RDB save failures"

        print("[Standalone] RDB saved, starting DEBUG RELOAD with metric monitoring...")

        # Monitor the metric from a separate connection during DEBUG RELOAD
        monitor_client = self.server.get_new_client()
        monitor_ready = threading.Event()
        done = threading.Event()
        observed_percents = []
        observed_index_counts = []
        monitor_error = [None]

        def monitor():
            try:
                # Signal readiness after confirming the connection works
                monitor_client.info("search")
                monitor_ready.set()

                while not done.is_set():
                    info = monitor_client.info("search")
                    if info.get("search_rdb_restore_in_progress", 0):
                        pct = float(info["search_rdb_indexes_restored_percent"])
                        observed_percents.append(pct)
                        num_indexes = int(info["search_number_of_indexes"])
                        observed_index_counts.append(num_indexes)
                        keys_pct = float(info["search_rdb_current_index_keys_restored_percent"])
                        print(f"[Standalone][Monitor] restore_in_progress=1, "
                              f"indexes_restored_percent={pct:.2f}%, "
                              f"number_of_indexes={num_indexes}, "
                              f"current_index_keys_restored_percent={keys_pct:.2f}%")
                        # Verify the metric never exceeds 100%
                        if pct > 100.0:
                            monitor_error[0] = f"Metric exceeded 100%: {pct}"
                            return
                        if keys_pct > 100.0:
                            monitor_error[0] = f"Key restore metric exceeded 100%: {keys_pct}"
                            return
                        # Indexes must never appear to vanish mid-restore.
                        if num_indexes < 2:
                            monitor_error[0] = (
                                f"number_of_indexes dropped to {num_indexes} during restore"
                            )
                            return
                    done.wait(0.001)
            except Exception as exc:
                monitor_error[0] = f"Monitor exception: {exc}"
                monitor_ready.set()

        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
        assert monitor_ready.wait(timeout=5), "Monitor thread failed to start"

        try:
            self.client.execute_command("DEBUG", "RELOAD")
        finally:
            done.set()
            thread.join(timeout=5)

        assert not thread.is_alive(), "Monitor thread did not exit cleanly"

        print(f"[Standalone] DEBUG RELOAD complete. Observed {len(observed_percents)} progress samples during restore.")
        if observed_percents:
            print(f"[Standalone] Progress samples: {observed_percents}")

        # Verify no errors observed during monitoring
        assert monitor_error[0] is None, monitor_error[0]

        # Verify the metric progressed monotonically (non-decreasing)
        if len(observed_percents) > 1:
            for i in range(1, len(observed_percents)):
                assert observed_percents[i] >= observed_percents[i-1], \
                    f"Metric decreased: {observed_percents[i-1]} -> {observed_percents[i]}"

        if observed_index_counts:
            print(f"[Standalone] number_of_indexes samples: {observed_index_counts}")

        # After restore completes, verify the metric is 100%
        info = self.client.info("search")
        assert info["search_rdb_restore_in_progress"] == 0
        percent = float(info["search_rdb_indexes_restored_percent"])
        print(f"[Standalone] Final: restore_in_progress={info['search_rdb_restore_in_progress']}, "
              f"indexes_restored_percent={percent:.2f}%")
        assert percent == 100.0, \
            f"Expected search_rdb_indexes_restored_percent=100.0 after restore, got {percent}"

        # The remaining restore counters must agree with a clean, complete load.
        assert int(info["search_rdb_load_success_cnt"]) == load_success_before + 1, \
            "Expected search_rdb_load_success_cnt to increment after DEBUG RELOAD"
        assert int(info.get("search_rdb_load_failure_cnt", 0)) == 0, \
            "Expected no RDB load failures"
        assert float(info["search_rdb_current_index_keys_restored_percent"]) == 100.0, \
            "Expected per-index key restore to report 100% after restore"
        assert info["search_number_of_indexes"] == 2, \
            f"Expected 2 indexes after restore, got {info['search_number_of_indexes']}"

        # The recorded total must follow the real number of index schemas
        # rather than being a fixed value or the section count: save/reload at
        # a higher and then a lower index count and confirm the restored state
        # tracks it in both directions, completing at 100% each time.
        index_3.create(self.client, True)
        waiters.wait_for_true(lambda: index_3.backfill_complete(self.client))
        assert self.client.info("search")["search_number_of_indexes"] == 3

        self.client.execute_command("SAVE")
        self.client.execute_command("DEBUG", "RELOAD")
        info = self.client.info("search")
        assert float(info["search_rdb_indexes_restored_percent"]) == 100.0, \
            "Expected 100% after restoring 3 indexes"
        assert info["search_number_of_indexes"] == 3, \
            f"Expected 3 indexes after restore, got {info['search_number_of_indexes']}"

        self.client.execute_command("FT.DROPINDEX", index_2.name)
        self.client.execute_command("FT.DROPINDEX", index_3.name)
        assert self.client.info("search")["search_number_of_indexes"] == 1

        self.client.execute_command("SAVE")
        self.client.execute_command("DEBUG", "RELOAD")
        info = self.client.info("search")
        assert float(info["search_rdb_indexes_restored_percent"]) == 100.0, \
            "Expected 100% after restoring 1 index"
        assert info["search_number_of_indexes"] == 1, \
            f"Expected 1 index after restore, got {info['search_number_of_indexes']}"

    def test_restore_without_indexes(self):
        """
        With no indexes there is no ValkeySearch aux payload at all, so a
        reload must not invoke the aux load callback. This guards the AuxSave2
        "write nothing when empty" behaviour that the new section must not
        disturb: rdb_load_success_cnt only moves when PerformRDBLoad runs.
        """
        before = self.client.info("search")
        assert before["search_number_of_indexes"] == 0
        load_success_before = int(before.get("search_rdb_load_success_cnt", 0))

        self.client.execute_command("SAVE")
        self.client.execute_command("DEBUG", "RELOAD")

        info = self.client.info("search")
        assert int(info.get("search_rdb_load_success_cnt", 0)) == load_success_before, \
            "Expected no aux payload (and hence no aux load) for an empty index set"
        assert int(info.get("search_rdb_load_failure_cnt", 0)) == 0
        assert info["search_rdb_restore_in_progress"] == 0
        assert float(info["search_rdb_indexes_restored_percent"]) == 100.0
        assert info["search_number_of_indexes"] == 0


class TestIndexesRestoredPercentCluster(ValkeySearchClusterTestCaseDebugMode):
    """
    CME (cluster/coordinator) mode: rdb_section_count includes both
    INDEX_SCHEMA and GLOBAL_METADATA sections. After restore, the metric
    should still be 100%.
    """

    def test_indexes_restored_percent_is_100_after_restore(self):
        node0 = self.new_client_for_primary(0)
        print("[Cluster] Creating 2 indexes in CME mode...")

        # Create indexes on the cluster
        assert node0.execute_command(
            "FT.CREATE", index_1.name,
            "ON", "HASH",
            "PREFIX", "1", f"{index_1.name}:",
            "SCHEMA",
            "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
            "n", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command(
            "FT.CREATE", index_2.name,
            "ON", "HASH",
            "PREFIX", "1", f"{index_2.name}:",
            "SCHEMA",
            "v", "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
            "t", "TAG"
        ) == b"OK"

        # Load some data
        cluster = self.new_cluster_client()
        for i in range(NUM_DOCS):
            cluster.execute_command(
                "HSET", f"{index_1.name}:{i}",
                "v", b'\x00\x00\x80\x3f' * 3,
                "n", str(i)
            )
        print(f"[Cluster] Loaded {NUM_DOCS} docs")

        # Save RDB on all primaries
        for idx, rg in enumerate(self.replication_groups):
            rg.primary.client.execute_command("SAVE")

        # Restart all primaries (triggers RDB load with GLOBAL_METADATA section)
        print(f"[Cluster] Restarting all {len(self.replication_groups)} primaries...")
        for idx, rg in enumerate(self.replication_groups):
            rg.primary.server.restart(remove_rdb=False)
            print(f"[Cluster] Restarted primary node {idx}")

        # Verify the metric on each primary node after restore
        for idx in range(len(self.replication_groups)):
            # Get a fresh client — the Node.client reference is stale after restart
            client = self.new_client_for_primary(idx)

            # Wait for server to be ready and restore to finish
            def server_restored(c=client):
                try:
                    return c.info("search").get("search_rdb_restore_in_progress", 1) == 0
                except Exception:
                    return False

            waiters.wait_for_true(server_restored)

            client.execute_command("CONFIG", "SET", "search.info-developer-visible", "yes")

            info = client.info("search")
            percent = float(info["search_rdb_indexes_restored_percent"])
            rdb_load_success = info.get("search_rdb_load_success_cnt", "N/A")
            num_indexes = info.get("search_number_of_indexes", "N/A")
            print(f"[Cluster] Node {idx}: restore_in_progress={info['search_rdb_restore_in_progress']}, "
                  f"indexes_restored_percent={percent:.2f}%, "
                  f"rdb_load_success_cnt={rdb_load_success}, number_of_indexes={num_indexes}")
            assert percent == 100.0, \
                f"Node {idx}: Expected search_rdb_indexes_restored_percent=100.0 after restore, got {percent}"

            # 100% pins down both halves of the ratio in coordinated mode.
            # If the metadata section were still counted in the total this
            # would read 66.67%; if it still incremented the completed counter
            # it would read 150%.
            assert num_indexes == 2, \
                f"Node {idx}: Expected 2 indexes after restore, got {num_indexes}"
            assert int(info["search_rdb_load_success_cnt"]) >= 1, \
                f"Node {idx}: Expected at least one successful RDB load"
            assert int(info.get("search_rdb_load_failure_cnt", 0)) == 0, \
                f"Node {idx}: Expected no RDB load failures"

        print("[Cluster] All nodes verified: indexes_restored_percent=100.0%")


class TestIndexesRestoredPercentLegacyRDB(ValkeySearchTestCaseCommon):
    """
    Backward compatibility: an RDB written by a pre-SnapshotInfo module (no
    RDB_SECTION_SNAPSHOT_INFO section) must load correctly with the
    SnapshotInfo-aware code, exercising the legacy fallback that derives the
    index total from section arithmetic. The metric must land at exactly
    100%, not 66.67% (metadata counted in total) or 150% (metadata counted
    as completed).

    The fixture below is a committed base64 RDB generated by a module build
    from before the SnapshotInfo section existed. It contains two HASH
    indexes (idx_legacy_1: HNSW vector DIM=3 + NUMERIC, idx_legacy_2: HNSW
    vector DIM=3 + TAG) with 3 documents each.
    """

    RDB_FILENAME = "dump.rdb"

    # Legacy RDB (no SnapshotInfo section) with 2 indexes, 3 docs each
    RDB_BASE64 = (
        "VkFMS0VZMDgw+gp2YWxrZXktdmVyBTkuMS4y+gpyZWRpcy1iaXRzwED6BWN0aW1lwoyypGr6CHVz"
        "ZWQtbWVtwqgBiAD6CGFvZi1iYXNlwAD+APsGAAQOaWR4X2xlZ2FjeV8xOjECAXYMAACAPwAAgD8A"
        "AIA/AW7AAQQOaWR4X2xlZ2FjeV8yOjECAXYMAACAPwAAgD8AAIA/AXQEdGFnMQQOaWR4X2xlZ2Fj"
        "eV8xOjICAXYMAACAPwAAgD8AAIA/AW7AAgQOaWR4X2xlZ2FjeV8yOjICAXYMAACAPwAAgD8AAIA/"
        "AXQEdGFnMgQOaWR4X2xlZ2FjeV8xOjMCAXYMAACAPwAAgD8AAIA/AW7AAwQOaWR4X2xlZ2FjeV8y"
        "OjMCAXYMAACAPwAAgD8AAIA/AXQEdGFnM/eBVk+SearchAECAgKAAAEAAAICBcNBE0EmFggBEAQa"
        "nwIKDGlkeF9sZWdhY3lfMhIN4AMNHzoYASABLQAAgD8yDQoBdBIBdBoFGgMKASwyGwoBdhIBBnYa"
        "EwoRCANAJR8ogFAyBggCEAIYCjoCCANAAEoeLC48Pnt9W10iJzo7IR9AIyQlXiYqKCktKz1+L1x8"
        "P1ABWgFhWgJpc1oDdGhlWgcCYW5aA2FuZCAEAHJADQBzIBEIdFoCYmVaA2J1QAgFeVoDZm9yIDAA"
        "ZiADBm5aBGludG8gCSAaBW5vWgNubyAIAG8gGgFvbiADCnJaBHN1Y2haBHRoIEQABSBfAGkgEiAG"
        "AG5gDCBgQBMBc2VgEwB5QAUghgACIE8BA3cgeAQEd2lsbEAFA3RoYAQFEwgBEg8KDQoBdBIBdBoF"
        "GgMKASwFAAUhCAESHQobCgF2EgF2GhMKEQgDGAEgASiAUDIGCAIQAhgKBSAKHhCAUBgDICgoKDAU"
        "OAJIAlAEWAJh/oIrZUcV9z9oAgXDGioGCigCAAAAASADQAfgAQABgD/gAQNAAAEAAAXDFyoDCigC"
        "AIAAwAeAAAGAP8ADAAFgEAEAAAXDGCoGCigCAAAAASAD4AUAAYA/wANAHyAAAAAFCgoIGAAAAAAA"
        "AAAFwwoaAgoYAOAMAAEAAAUKCggAAAAAAAAAAAUKCggAAAAAAAAAAAUABSEIAhodChsKAXYSAXYa"
        "EwoRCAMYASABKIBQMgYIAhACGAoFGQoXCg5pZHhfbGVnYWN5XzI6MhABHdiz3T8FFwoVCg5pZHhf"
        "bGVnYWN5XzI6MR3Ys90/BRkKFwoOaWR4X2xlZ2FjeV8yOjMQAh3Ys90/BQAFBggDIgIIAQUKCggD"
        "AAAAAAAAAAUQCg5pZHhfbGVnYWN5XzI6MwUQCg5pZHhfbGVnYWN5XzI6MQUQCg5pZHhfbGVnYWN5"
        "XzI6MgUKCggAAAAAAAAAAAUKCggAAAAAAAAAAAUABcNBEEEjFggBEAQanAIKDGlkeF9sZWdhY3lf"
        "MRIN4AMNHzoYASABLQAAgD8yCgoBbhIBbhoCEgAyGwoBdhIBdhoTAwoRCANAIh8ogFAyBggCEAIY"
        "CjoCCANAAEoeLC48Pnt9W10iJzo7IR9AIyQlXiYqKCktKz1+L1x8P1ABWgFhWgJpc1oDdGhlWgcC"
        "YW5aA2FuZCAEAHJADQBzIBEIdFoCYmVaA2J1QAgFeVoDZm9yIDAAZiADBm5aBGludG8gCSAaBW5v"
        "WgNubyAIAG8gGgFvbiADCnJaBHN1Y2haBHRoIEQABSBfAGkgEiAGAG5gDCBgQBMBc2VgEwB5QAUg"
        "hgACIE8BA3cgeAQEd2lsbEAFA3RoYAQFEAgBEgwKCgoBbhIBbhoCEgAFAAUhCAESHQobCgF2EgF2"
        "GhMKEQgDGAEgASiAUDIGCAIQAhgKBSAKHhCAUBgDICgoKDAUOAJIAlAEWAJh/oIrZUcV9z9oAgXD"
        "GioGCigCAAAAASADQAfgAQABgD/gAQNAAAEAAAXDFyoDCigCAIAAwAeAAAGAP8ADAAFgEAEAAAXD"
        "GCoGCigCAAAAASAD4AUAAYA/wANAHyAAAAAFCgoIGAAAAAAAAAAFwwoaAgoYAOAMAAEAAAUKCggA"
        "AAAAAAAAAAUKCggAAAAAAAAAAAUABSEIAhodChsKAXYSAXYaEwoRCAMYASABKIBQMgYIAhACGAoF"
        "GQoXCg5pZHhfbGVnYWN5XzE6MhABHdiz3T8FFwoVCg5pZHhfbGVnYWN5XzE6MR3Ys90/BRkKFwoO"
        "aWR4X2xlZ2FjeV8xOjMQAh3Ys90/BQAFBggDIgIIAQUKCggDAAAAAAAAAAUQCg5pZHhfbGVnYWN5"
        "XzE6MwUQCg5pZHhfbGVnYWN5XzE6MQUQCg5pZHhfbGVnYWN5XzE6MgUKCggAAAAAAAAAAAUKCggA"
        "AAAAAAAAAAUAAP8ysRPkVTlRgA=="
    )

    def _setup_rdb_in_testdir(self, testdir):
        """Decode the legacy RDB and write it into testdir as dump.rdb."""
        os.makedirs(testdir, exist_ok=True)
        rdb_path = os.path.join(testdir, self.RDB_FILENAME)
        with open(rdb_path, "wb") as f:
            f.write(base64.b64decode(self.RDB_BASE64))

    def test_legacy_rdb_without_snapshot_info_restores_100_percent(self):
        server_path = os.getenv("VALKEY_SERVER_PATH")
        testdir = f"{LOGS_DIR}/legacy_rdb_snapshot_info_fallback"
        port = self.get_bind_port()

        # Place the legacy RDB in testdir before starting
        self._setup_rdb_in_testdir(testdir)

        lines = [
            "enable-debug-command yes",
            f"dbfilename {self.RDB_FILENAME}",
            f"dir {testdir}",
            f"port {port}",
            f"loadmodule {os.getenv('JSON_MODULE_PATH')}",
            f"loadmodule {os.getenv('MODULE_PATH')}",
        ]

        conf_file = os.path.join(testdir, f"valkey_{port}.conf")
        with open(conf_file, "w") as f:
            for line in lines:
                f.write(f"{line}\n")

        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            port=port,
            conf_file=conf_file,
            args={
                "logfile": f"logfile_{port}",
                "dbfilename": self.RDB_FILENAME,
                # The rdb restore metrics are developer-visible fields
                "search.info-developer-visible": "yes",
            },
        )

        try:
            assert server.is_alive(), "Server failed to start with legacy RDB"

            # Wait for the restore to complete
            def restore_finished():
                return (
                    client.info("search").get("search_rdb_restore_in_progress", 1)
                    == 0
                )

            waiters.wait_for_true(restore_finished)

            info = client.info("search")

            # The legacy RDB has no aux SnapshotInfo section, so the loader
            # must have derived the index total via the fallback. A correct
            # total means completed/total is exactly 100%.
            percent = float(info["search_rdb_indexes_restored_percent"])
            assert percent == 100.0, (
                f"Expected search_rdb_indexes_restored_percent=100.0 after "
                f"restoring a legacy RDB, got {percent}"
            )

            assert int(info["search_rdb_load_success_cnt"]) == 1, (
                "Expected exactly one successful RDB load, got "
                f"{info['search_rdb_load_success_cnt']}"
            )
            assert int(info.get("search_rdb_load_failure_cnt", 0)) == 0, (
                "Expected no RDB load failures"
            )
            assert info["search_number_of_indexes"] == 2, (
                f"Expected 2 indexes after restore, got "
                f"{info['search_number_of_indexes']}"
            )

            indices = client.execute_command("FT._LIST")
            assert b"idx_legacy_1" in indices, "idx_legacy_1 missing from FT._LIST"
            assert b"idx_legacy_2" in indices, "idx_legacy_2 missing from FT._LIST"
        finally:
            server.exit()
