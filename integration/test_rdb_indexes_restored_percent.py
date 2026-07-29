"""
Verify search_rdb_indexes_restored_percent reports 100% after successful RDB
restore in both standalone (CMD) and cluster/coordinator (CME) modes.

This metric was previously broken in CME mode because the GLOBAL_METADATA RDB
section was counted in the total but never incremented the completed counter,
causing it to report 66.67% with 2 indexes (2/3 sections completed).
"""

import time
import threading
from valkey_search_test_case import ValkeySearchTestCaseDebugMode, ValkeySearchClusterTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from indexes import *


index_1 = Index("idx_pct_1", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
index_2 = Index("idx_pct_2", [Vector("v", 3, type="HNSW", m=2, efc=1), Tag("t")])
NUM_DOCS = 500


class TestIndexesRestoredPercentStandalone(ValkeySearchTestCaseDebugMode):
    """
    CMD (standalone) mode: rdb_section_count only includes INDEX_SCHEMA sections.
    After restore, the metric should progress monotonically and reach 100%.
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

        # Save RDB
        self.client.execute_command("SAVE")
        print("[Standalone] RDB saved, starting DEBUG RELOAD with metric monitoring...")

        # Monitor the metric from a separate connection during DEBUG RELOAD
        monitor_client = self.server.get_new_client()
        done = threading.Event()
        observed_percents = []
        monitor_error = [None]

        def monitor():
            while not done.is_set():
                try:
                    info = monitor_client.info("search")
                    if info.get("search_rdb_restore_in_progress", 0):
                        pct = float(info["search_rdb_indexes_restored_percent"])
                        observed_percents.append(pct)
                        print(f"[Standalone][Monitor] restore_in_progress=1, indexes_restored_percent={pct:.2f}%")
                        # Verify the metric never exceeds 100%
                        if pct > 100.0:
                            monitor_error[0] = f"Metric exceeded 100%: {pct}"
                            return
                except Exception:
                    pass
                time.sleep(0.001)

        threading.Thread(target=monitor, daemon=True).start()
        self.client.execute_command("DEBUG", "RELOAD")
        done.set()

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

        # After restore completes, verify the metric is 100%
        info = self.client.info("search")
        assert info["search_rdb_restore_in_progress"] == 0
        percent = float(info["search_rdb_indexes_restored_percent"])
        print(f"[Standalone] Final: restore_in_progress={info['search_rdb_restore_in_progress']}, "
              f"indexes_restored_percent={percent:.2f}%")
        assert percent == 100.0, \
            f"Expected search_rdb_indexes_restored_percent=100.0 after restore, got {percent}"


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

        print("[Cluster] All nodes verified: indexes_restored_percent=100.0%")
