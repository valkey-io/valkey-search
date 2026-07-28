"""
Overload reproduction test in cluster mode.
Tests the overload protection fixes under fan-out.

Run with:
  export VALKEY_SERVER_PATH=/home/karsubba/valkey/src/valkey-server
  export MODULE_PATH=/home/karsubba/valkey-search-local-priority/.build-release/libsearch.so
  export LD_LIBRARY_PATH=/home/karsubba/.local/GCCStandalone/lib64
  cd /home/karsubba/valkey-search-local-priority
  python3 -m pytest integration/test_overload_repro.py -v -s
"""

import time
import threading
import pytest
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode, Node
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkey.exceptions import ResponseError


class TestOverloadRepro(ValkeySearchClusterTestCaseDebugMode):
    """Reproduce the queue growth death spiral in cluster mode with fan-out."""

    CLUSTER_SIZE = 5
    REPLICAS_COUNT = 1  # 5 shards × 2 = 10 nodes total

    def _load_data(self, cluster, num_docs=50000):
        """Load documents across the cluster."""
        pipe = cluster.pipeline()
        for i in range(num_docs):
            t1 = "common" if i % 10 != 0 else "rare"
            t3 = "active" if i % 5 == 0 else "inactive"
            pipe.hset(
                f"d:{i}",
                mapping={
                    "title": f"device asset {i} description",
                    "tag1": t1,
                    "tag2": "standard",
                    "tag3": t3,
                    "num1": str(i),
                },
            )
            if i % 1000 == 0:
                pipe.execute()
        pipe.execute()

    def _continuous_writes(self, cluster, stop_event, num_docs=50000):
        """Background writer thread."""
        import random
        while not stop_event.is_set():
            try:
                pipe = cluster.pipeline()
                for _ in range(50):
                    doc_id = random.randint(0, num_docs - 1)
                    pipe.hset(f"d:{doc_id}", mapping={"tag1": "updated", "num1": str(random.randint(0, 9999))})
                pipe.execute()
            except Exception:
                pass
            time.sleep(0.05)

    def _query_flood(self, client, stop_event, results, query="@tag1:{common}"):
        """Flood queries from a single thread, collecting success/fail counts."""
        successes = 0
        failures = 0
        timeouts = 0
        while not stop_event.is_set():
            try:
                result = client.execute_command("FT.SEARCH", "idx", query, "LIMIT", "0", "10")
                successes += 1
            except ResponseError as e:
                if "timeout" in str(e).lower() or "overloaded" in str(e).lower():
                    timeouts += 1
                else:
                    failures += 1
            except Exception:
                failures += 1
        results["successes"] = successes
        results["failures"] = failures
        results["timeouts"] = timeouts

    def test_overload_without_fix(self):
        """Reproduce: fan-out queries timeout under load, successful_requests stuck."""
        cluster = self.new_cluster_client()
        node0 = self.new_client_for_primary(0)

        # Create index
        node0.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "title", "TEXT", "tag1", "TAG", "tag2", "TAG", "tag3", "TAG", "num1", "NUMERIC"
        )
        time.sleep(1)

        # Load data
        self._load_data(cluster, num_docs=20000)
        time.sleep(2)

        # Configure: protections OFF, low reader threads, inject delay
        for node in self.nodes:
            c = Valkey(host=node.server.bind_ip, port=node.server.port)
            c.execute_command("CONFIG", "SET", "search.reader-threads", "2")
            c.execute_command("CONFIG", "SET", "search.default-timeout-ms", "2000")
            c.execute_command("CONFIG", "SET", "search.local-search-max-priority", "no")
            c.execute_command("CONFIG", "SET", "search.max-query-queue-depth", "0")
            c.execute_command("FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "ForceQueryDelayMs", "1000")
            c.close()

        # Get baseline
        info_before = node0.info("MODULES")
        q_before = info_before.get("search_query_queue_size", 0)

        # Start writes + queries
        stop = threading.Event()
        write_thread = threading.Thread(target=self._continuous_writes, args=(cluster, stop, 20000))
        write_thread.start()

        # 10 query threads to flood
        query_results = []
        query_threads = []
        for _ in range(10):
            r = {}
            query_results.append(r)
            # Connect to a replica for reads (like the customer)
            replica_node = self.replication_groups[0].replicas[0] if self.replication_groups[0].replicas else self.replication_groups[0].primary
            c = Valkey(host=replica_node.server.bind_ip, port=replica_node.server.port)
            t = threading.Thread(target=self._query_flood, args=(c, stop, r))
            query_threads.append(t)
            t.start()

        # Let it run for 20 seconds
        time.sleep(20)
        stop.set()

        for t in query_threads:
            t.join(timeout=5)
        write_thread.join(timeout=5)

        # Check queue and stats
        info_after = node0.info("MODULES")
        q_after = info_after.get("search_query_queue_size", 0)
        timeouts = info_after.get("search_cancel-timeouts", 0)
        successful = info_after.get("search_successful_requests_count", 0)

        total_successes = sum(r.get("successes", 0) for r in query_results)
        total_timeouts = sum(r.get("timeouts", 0) for r in query_results)

        print(f"\n=== WITHOUT FIX ===")
        print(f"Queue: {q_before} -> {q_after}")
        print(f"Server timeouts: {timeouts}")
        print(f"Server successful: {successful}")
        print(f"Client successes: {total_successes}")
        print(f"Client timeouts: {total_timeouts}")

        # Assert the overload pattern: high timeouts, queue not empty
        assert timeouts > 0, "Expected timeouts under overload"
        assert q_after > 0 or total_timeouts > 0, "Expected queue buildup or client timeouts"

    def test_overload_with_queue_depth_rejection(self):
        """Fix: max-query-queue-depth rejects before fan-out, queries fail fast."""
        cluster = self.new_cluster_client()
        node0 = self.new_client_for_primary(0)

        # Create index
        node0.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "title", "TEXT", "tag1", "TAG", "tag2", "TAG", "tag3", "TAG", "num1", "NUMERIC"
        )
        time.sleep(1)
        self._load_data(cluster, num_docs=20000)
        time.sleep(2)

        # Configure: queue depth rejection ON at 50, inject delay
        for node in self.nodes:
            c = Valkey(host=node.server.bind_ip, port=node.server.port)
            c.execute_command("CONFIG", "SET", "search.reader-threads", "2")
            c.execute_command("CONFIG", "SET", "search.default-timeout-ms", "2000")
            c.execute_command("CONFIG", "SET", "search.local-search-max-priority", "no")
            c.execute_command("CONFIG", "SET", "search.max-query-queue-depth", "50")
            c.execute_command("FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "ForceQueryDelayMs", "1000")
            c.close()

        stop = threading.Event()
        write_thread = threading.Thread(target=self._continuous_writes, args=(cluster, stop, 20000))
        write_thread.start()

        query_results = []
        query_threads = []
        for _ in range(10):
            r = {}
            query_results.append(r)
            replica_node = self.replication_groups[0].replicas[0] if self.replication_groups[0].replicas else self.replication_groups[0].primary
            c = Valkey(host=replica_node.server.bind_ip, port=replica_node.server.port)
            t = threading.Thread(target=self._query_flood, args=(c, stop, r))
            query_threads.append(t)
            t.start()

        time.sleep(20)
        stop.set()
        for t in query_threads:
            t.join(timeout=5)
        write_thread.join(timeout=5)

        info_after = node0.info("MODULES")
        q_after = info_after.get("search_query_queue_size", 0)

        total_successes = sum(r.get("successes", 0) for r in query_results)
        total_timeouts = sum(r.get("timeouts", 0) for r in query_results)
        total_failures = sum(r.get("failures", 0) for r in query_results)

        print(f"\n=== WITH QUEUE DEPTH REJECTION (max=50) ===")
        print(f"Queue after: {q_after}")
        print(f"Client successes: {total_successes}")
        print(f"Client timeouts: {total_timeouts}")
        print(f"Client failures (rejected): {total_failures}")

        # With rejection, queue should be bounded
        assert q_after <= 100, f"Queue should be bounded with rejection, got {q_after}"

    def test_overload_with_local_priority(self):
        """Fix: local-search-max-priority=yes, partial results work under overload."""
        cluster = self.new_cluster_client()
        node0 = self.new_client_for_primary(0)

        node0.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "title", "TEXT", "tag1", "TAG", "tag2", "TAG", "tag3", "TAG", "num1", "NUMERIC"
        )
        time.sleep(1)
        self._load_data(cluster, num_docs=20000)
        time.sleep(2)

        # Configure: local priority ON, no queue rejection
        for node in self.nodes:
            c = Valkey(host=node.server.bind_ip, port=node.server.port)
            c.execute_command("CONFIG", "SET", "search.reader-threads", "2")
            c.execute_command("CONFIG", "SET", "search.default-timeout-ms", "2000")
            c.execute_command("CONFIG", "SET", "search.local-search-max-priority", "yes")
            c.execute_command("CONFIG", "SET", "search.max-query-queue-depth", "0")
            c.execute_command("FT._DEBUG", "CONTROLLED_VARIABLE", "SET", "ForceQueryDelayMs", "1000")
            c.close()

        stop = threading.Event()
        write_thread = threading.Thread(target=self._continuous_writes, args=(cluster, stop, 20000))
        write_thread.start()

        query_results = []
        query_threads = []
        for _ in range(10):
            r = {}
            query_results.append(r)
            replica_node = self.replication_groups[0].replicas[0] if self.replication_groups[0].replicas else self.replication_groups[0].primary
            c = Valkey(host=replica_node.server.bind_ip, port=replica_node.server.port)
            t = threading.Thread(target=self._query_flood, args=(c, stop, r))
            query_threads.append(t)
            t.start()

        time.sleep(20)
        stop.set()
        for t in query_threads:
            t.join(timeout=5)
        write_thread.join(timeout=5)

        info_after = node0.info("MODULES")
        successful = info_after.get("search_successful_requests_count", 0)

        total_successes = sum(r.get("successes", 0) for r in query_results)
        total_timeouts = sum(r.get("timeouts", 0) for r in query_results)

        print(f"\n=== WITH LOCAL PRIORITY (partial results should work) ===")
        print(f"Server successful: {successful}")
        print(f"Client successes: {total_successes}")
        print(f"Client timeouts: {total_timeouts}")

        # With local priority, some queries should succeed (local shard)
        # even if remote partitions timeout
        assert total_successes > 0, "Expected some successes with local priority enabled"
