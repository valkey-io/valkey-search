"""
Integration tests for search overload protection.

Configs tested:
  - search.max-query-queue-depth: Rejects queries when queue exceeds limit.
  - search.local-search-max-priority: Local shard gets kMax thread pool priority.
  - search.queue-depth-scaling-factor: Scales queue limit by cluster size.
"""

import time
import threading
import pytest
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.client import Valkey
from valkey.exceptions import ResponseError, ConnectionError as ValkeyConnectionError


class TestOverloadProtection(ValkeySearchClusterTestCaseDebugMode):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 0

    def _create_index(self):
        client = self.new_client_for_primary(0)
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "tag1", "TAG", "num1", "NUMERIC"
        )
        time.sleep(2)
        cluster = self.new_cluster_client()
        for i in range(100):
            cluster.hset(f"d:{i}", mapping={"tag1": "common", "num1": str(i)})
        time.sleep(1)

    def _config_all(self, key, value):
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command(
                "CONFIG", "SET", key, str(value))

    def _primary_client(self, idx=0):
        node = self.replication_groups[idx].primary
        return Valkey(host=node.server.bind_ip, port=node.server.port,
                      socket_timeout=10)

    def test_queue_depth_rejection(self):
        """Queries are rejected with proper error when queue exceeds limit."""
        self._create_index()

        # Set a very low limit so concurrent queries exceed it.
        self._config_all("search.reader-threads", 1)
        self._config_all("search.max-query-queue-depth", 2)
        self._config_all("search.default-timeout-ms", 10000)

        # Use a pausepoint to hold the single reader thread busy.
        node0 = self.new_client_for_primary(0)
        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        results = {"success": 0, "rejected": 0, "other_error": 0}
        lock = threading.Lock()

        def query():
            try:
                c = self._primary_client(0)
                c.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                with lock:
                    results["success"] += 1
                c.close()
            except ResponseError as e:
                with lock:
                    if "queue depth exceeded" in str(e).lower():
                        results["rejected"] += 1
                    else:
                        results["other_error"] += 1
            except (ValkeyConnectionError, OSError):
                with lock:
                    results["other_error"] += 1

        # Fire 8 queries concurrently.
        threads = [threading.Thread(target=query) for _ in range(8)]
        for t in threads:
            t.start()
        time.sleep(2)

        # Release pausepoint so threads complete.
        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        for t in threads:
            t.join(timeout=15)

        assert results["rejected"] > 0, (
            f"Expected queue depth rejections. Got: {results}")

    def test_queue_depth_scaling_factor(self):
        """Scaling factor reduces effective limit based on shard count."""
        self._create_index()

        # With 3 shards and scaling_factor=100:
        # effective = 6 / (1 + 1.0*(3-1)) = 6/3 = 2
        self._config_all("search.reader-threads", 1)
        self._config_all("search.max-query-queue-depth", 6)
        self._config_all("search.queue-depth-scaling-factor", 100)
        self._config_all("search.default-timeout-ms", 10000)

        node0 = self.new_client_for_primary(0)
        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        results = {"success": 0, "rejected": 0, "other_error": 0}
        lock = threading.Lock()

        def query():
            try:
                c = self._primary_client(0)
                c.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                with lock:
                    results["success"] += 1
                c.close()
            except ResponseError as e:
                with lock:
                    if "queue depth exceeded" in str(e).lower():
                        results["rejected"] += 1
                    else:
                        results["other_error"] += 1
            except (ValkeyConnectionError, OSError):
                with lock:
                    results["other_error"] += 1

        # With effective limit=2, even 5 queries should trigger rejections.
        threads = [threading.Thread(target=query) for _ in range(5)]
        for t in threads:
            t.start()
        time.sleep(2)

        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        for t in threads:
            t.join(timeout=15)

        assert results["rejected"] > 0, (
            f"Expected rejections with scaled limit. Got: {results}")

    def test_local_search_max_priority(self):
        """Local shard gets kMax priority, completing even when remote is slow."""
        self._create_index()

        self._config_all("search.reader-threads", 2)
        self._config_all("search.default-timeout-ms", 2000)
        self._config_all("search.max-query-queue-depth", 0)

        # Pause remote primaries to simulate slow shards.
        self.new_client_for_primary(1).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")
        self.new_client_for_primary(2).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        node0 = self.new_client_for_primary(0)

        # Without fix: queries may timeout waiting for remote shards.
        self._config_all("search.local-search-max-priority", "no")
        timeouts_off = 0
        for _ in range(3):
            try:
                node0.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
            except ResponseError:
                timeouts_off += 1

        # With fix: local shard completes via kMax, partial results returned.
        self._config_all("search.local-search-max-priority", "yes")
        successes_on = 0
        for _ in range(3):
            try:
                node0.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                successes_on += 1
            except ResponseError:
                pass

        # Cleanup pausepoints.
        self.new_client_for_primary(1).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        self.new_client_for_primary(2).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        time.sleep(3)

        assert successes_on > 0, (
            f"Expected successes with local-search-max-priority=yes. "
            f"Without fix timeouts={timeouts_off}, with fix successes={successes_on}")
