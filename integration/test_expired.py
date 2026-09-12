import threading
import time

import pytest
from ft_info_parser import FTInfoParser
from indexes import *
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCaseDebugMode
from util import waiters
from valkeytestframework.conftest import resource_port_tracker


class TestExpired(ValkeySearchTestCaseBase):
    def test_expired_hashes_should_remove_from_index(self):
        """
        Test CMD flushall logic
        """
        index_name = "my_index"
        client: Valkey = self.server.get_new_client()
        hnsw_index = Index(
            index_name, [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")]
        )

        num_of_docs = 500
        hnsw_index.create(client)
        hnsw_index.load_data(client, num_of_docs)
        ft_info = hnsw_index.info(client)
        assert num_of_docs == ft_info.num_docs

        for i in range(0, num_of_docs):
            # expire half the keys
            if i % 2:
                client.pexpire(hnsw_index.keyname(i), 1)  # 1 ms

        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            num_of_docs / 2,
            timeout=5,
        )


class TestExpiredCluster(ValkeySearchClusterTestCaseDebugMode):

    @pytest.mark.skip(reason="This test is designed to run for an extended period with high concurrency to detect crashes. It is not suitable for regular test runs and should be run manually when needed.")
    @pytest.mark.parametrize("setup_test", [{"replica_count": 1}], indirect=True)
    def test_concurrent_workload(self, setup_test):
        """
            Test commandstats timing with search and concurrent operations.
            In order to reproduce the num_ops code, a numeric/tag search with
            concurrent expirations with a short TTL (e.g. 1) was effective.
        """
        rg = self.get_replication_group(0)
        primary = rg.get_primary_connection()
        index = Index("idx", [Numeric("price"), Tag("tags")])
        index.create(primary)
        index.wait_for_backfill_complete(primary)
        num_writers = 50
        num_searchers = 75
        num_expirers = 75
        ops_per_client = 10000
        num_keys = 1000
        write_clients = [self.new_cluster_client() for _ in range(num_writers)]
        search_clients = [self.new_client_for_primary(0) for _ in range(num_searchers)]
        expire_clients = [self.new_cluster_client() for _ in range(num_expirers)]

        crash_detected = threading.Event()
        stop_watchdog = threading.Event()
        def watchdog():
            while not stop_watchdog.is_set():
                for node in self.nodes:
                    try:
                        node.client.ping()
                    except Exception as e:
                        if not crash_detected.is_set():
                            crash_detected.set()
                            print(f"\nWatchdog detected crash on port {node.server.port}: {e}")
                        stop_watchdog.set()
                        return
                time.sleep(0.1)
        def writer(client_id):
            for i in range(ops_per_client):
                if crash_detected.is_set():
                    return
                key = f"doc:{i % num_keys}"
                write_clients[client_id].execute_command("HSET", key, "price", str(100 + i), "tags", f"tag{i % 5}")
        def searcher(client_id):
            for i in range(ops_per_client):
                if crash_detected.is_set():
                    return
                search_clients[client_id].execute_command("FT.SEARCH", "idx", f"@price:[{90 + i} {110 + i}]")
                search_clients[client_id].execute_command("FT.SEARCH", "idx", f"@tags:{{tag{i % 5}}}")
        def expirer(client_id):
            for i in range(ops_per_client):
                if crash_detected.is_set():
                    return
                key = f"doc:{i % num_keys}"
                expire_clients[client_id].execute_command("EXPIRE", key, "1")
        watchdog_thread = threading.Thread(target=watchdog, daemon=True)
        watchdog_thread.start()
        threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_writers)]
        threads += [threading.Thread(target=searcher, args=(i,)) for i in range(num_searchers)]
        threads += [threading.Thread(target=expirer, args=(i,)) for i in range(num_expirers)]
        for t in threads:
            t.start()
        # Check for crash while threads run
        while any(t.is_alive() for t in threads):
            if crash_detected.is_set():
                stop_watchdog.set()
                pytest.fail("Server crash detected during test execution")
            time.sleep(0.1)
        stop_watchdog.set()
        watchdog_thread.join(timeout=1)
        index.wait_for_backfill_complete(primary)
        # Collect stats from all primaries
        total_hset_calls = 0
        total_expire_calls = 0
        total_search_calls = 0
        for i in range(self.CLUSTER_SIZE):
            node = self.new_client_for_primary(i)
            info = node.info("commandstats")
            hset_stats = info.get("cmdstat_hset", {})
            expire_stats = info.get("cmdstat_expire", {})
            search_stats = info.get("cmdstat_FT.SEARCH", {})
            total_hset_calls += hset_stats.get("calls", 0)
            total_expire_calls += expire_stats.get("calls", 0)
            total_search_calls += search_stats.get("calls", 0)
        print("\nFinal ping check...")
        for node in self.nodes:
            node.client.ping()
        assert total_hset_calls > 0, "Expected HSET calls in commandstats"
        assert total_expire_calls > 0, "Expected EXPIRE calls in commandstats"
        assert total_search_calls > 0, "Expected FT.SEARCH calls in commandstats"
