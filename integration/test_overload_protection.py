"""
Integration tests for search overload protection.

Configs tested:
  - search.max-query-queue-depth: Rejects queries when queue exceeds limit.
  - search.local-search-max-priority: Local shard gets kMax thread pool priority.
  - search.queue-depth-scaling-factor: Scales queue limit by cluster size.

These features only activate on the coordinator fan-out path (replicas),
so tests MUST send queries to replicas, not primaries.
"""

import time
import threading
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.client import Valkey
from valkey.exceptions import ResponseError, ConnectionError as ValkeyConnectionError
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from utils import IndexingTestHelper


class TestOverloadProtection(ValkeySearchClusterTestCaseDebugMode):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 1

    def _create_index(self):
        client = self.new_client_for_primary(0)
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "tag1", "TAG", "num1", "NUMERIC"
        )
        cluster = self.new_cluster_client()
        for i in range(100):
            cluster.hset(f"d:{i}", mapping={"tag1": "common", "num1": str(i)})
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")

    def _config_all(self, key, value):
        for rg in self.replication_groups:
            rg.primary.client.execute_command("CONFIG", "SET", key, str(value))
            for replica in rg.replicas:
                replica.client.execute_command("CONFIG", "SET", key, str(value))

    def _replica_client(self):
        """Connect to the first replica (coordinator node)."""
        node = self.replication_groups[0].replicas[0]
        return Valkey(host=node.server.bind_ip, port=node.server.port,
                      socket_timeout=15)

    def _pausepoint_all_primaries(self, action):
        """SET or RESET pausepoint on all primaries."""
        for rg in self.replication_groups:
            rg.primary.client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", action, "background_search_completing")

    def test_queue_depth_rejection(self):
        """Queries rejected when queue exceeds limit on coordinator path."""
        self._create_index()
        self._config_all("search.max-query-queue-depth", 2)
        self._config_all("search.queue-depth-scaling-factor", "0.0")
        self._config_all("search.default-timeout-ms", 10000)

        # Pause all primaries so partition requests queue up on the replica.
        self._pausepoint_all_primaries("SET")

        errors = []

        def query():
            try:
                c = self._replica_client()
                c.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
            except ResponseError as e:
                errors.append(str(e))
            except (ValkeyConnectionError, OSError):
                pass

        threads = [threading.Thread(target=query) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        self._pausepoint_all_primaries("RESET")
        time.sleep(2)

        rejected = [e for e in errors if "queue depth exceeded" in e.lower()]
        assert len(rejected) > 0, (
            f"Expected queue depth rejections. Errors: {errors[:5]}")

    def test_queue_depth_scaling_factor(self):
        """Scaling factor reduces effective limit based on shard count."""
        self._create_index()
        # 3 shards, scaling=1.0: effective = 6 / (1 + 1.0*(3-1)) = 2
        self._config_all("search.max-query-queue-depth", 6)
        self._config_all("search.queue-depth-scaling-factor", "1.0")
        self._config_all("search.default-timeout-ms", 10000)

        self._pausepoint_all_primaries("SET")

        errors = []

        def query():
            try:
                c = self._replica_client()
                c.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
            except ResponseError as e:
                errors.append(str(e))
            except (ValkeyConnectionError, OSError):
                pass

        threads = [threading.Thread(target=query) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)

        self._pausepoint_all_primaries("RESET")
        time.sleep(2)

        rejected = [e for e in errors if "queue depth exceeded" in e.lower()]
        assert len(rejected) > 0, (
            f"Expected rejections with scaled limit. Errors: {errors[:5]}")

    def test_local_search_max_priority(self):
        """Local shard completes via kMax priority when remote shards are paused."""
        self._create_index()
        self._config_all("search.default-timeout-ms", 2000)
        self._config_all("search.max-query-queue-depth", 0)

        # Pause only remote primaries (not the one whose replica we query).
        self.replication_groups[1].primary.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")
        self.replication_groups[2].primary.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        # Without fix: queries timeout waiting for remote shards.
        self._config_all("search.local-search-max-priority", "no")
        timeouts_off = 0
        for _ in range(3):
            try:
                c = self._replica_client()
                c.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
            except ResponseError:
                timeouts_off += 1

        # With fix: local shard completes via kMax, partial results returned.
        self._config_all("search.local-search-max-priority", "yes")
        successes_on = 0
        for _ in range(3):
            try:
                c = self._replica_client()
                c.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                successes_on += 1
            except ResponseError:
                pass

        # Cleanup.
        self.replication_groups[1].primary.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        self.replication_groups[2].primary.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        time.sleep(2)

        assert timeouts_off > 0, (
            f"Expected timeouts without fix. timeouts_off={timeouts_off}")
        assert successes_on > 0, (
            f"Expected successes with local-search-max-priority=yes. "
            f"timeouts_off={timeouts_off}, successes_on={successes_on}")
