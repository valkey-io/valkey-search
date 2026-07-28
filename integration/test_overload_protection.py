"""
Integration tests for search overload protection.

Configs tested:
  - search.max-query-queue-depth: Rejects queries when queue exceeds limit.
  - search.local-search-max-priority: Local shard gets kMax thread pool priority.
  - search.queue-depth-scaling-factor: Scales queue limit by cluster size.
"""

import threading
import pytest
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.client import Valkey
from valkey.exceptions import ResponseError, ConnectionError as ValkeyConnectionError
from utils import IndexingTestHelper


class TestOverloadProtection(ValkeySearchClusterTestCaseDebugMode):
    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 0

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
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command(
                "CONFIG", "SET", key, str(value))

    def test_queue_depth_rejection(self):
        """Queries rejected with proper error when queue exceeds limit."""
        self._create_index()
        self._config_all("search.reader-threads", 1)
        self._config_all("search.max-query-queue-depth", 2)
        self._config_all("search.queue-depth-scaling-factor", 0)
        self._config_all("search.default-timeout-ms", 10000)

        node0 = self.new_client_for_primary(0)
        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        errors = []

        def query():
            try:
                c = self.new_client_for_primary(0)
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
            t.join(timeout=15)

        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")

        rejected = [e for e in errors if "queue depth exceeded" in e.lower()]
        assert len(rejected) > 0, f"Expected queue depth rejections. Errors: {errors[:5]}"

    def test_queue_depth_scaling_factor(self):
        """Scaling factor reduces effective limit based on shard count."""
        self._create_index()
        # 3 shards, scaling=100: effective = 6 / (1 + 1.0*(3-1)) = 2
        self._config_all("search.reader-threads", 1)
        self._config_all("search.max-query-queue-depth", 6)
        self._config_all("search.queue-depth-scaling-factor", 100)
        self._config_all("search.default-timeout-ms", 10000)

        node0 = self.new_client_for_primary(0)
        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        errors = []

        def query():
            try:
                c = self.new_client_for_primary(0)
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
            t.join(timeout=15)

        node0.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")

        rejected = [e for e in errors if "queue depth exceeded" in e.lower()]
        assert len(rejected) > 0, f"Expected rejections with scaled limit. Errors: {errors[:5]}"

    def test_local_search_max_priority(self):
        """Local shard completes via kMax priority when remote shards are paused."""
        self._create_index()
        self._config_all("search.reader-threads", 2)
        self._config_all("search.default-timeout-ms", 2000)
        self._config_all("search.max-query-queue-depth", 0)

        # Pause remote primaries.
        self.new_client_for_primary(1).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")
        self.new_client_for_primary(2).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        node0 = self.new_client_for_primary(0)

        # Without fix: queries timeout waiting for remote shards.
        self._config_all("search.local-search-max-priority", "no")
        timeouts_off = 0
        for _ in range(3):
            try:
                node0.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
            except ResponseError:
                timeouts_off += 1

        # With fix: local shard completes, partial results returned.
        self._config_all("search.local-search-max-priority", "yes")
        successes_on = 0
        for _ in range(3):
            try:
                node0.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                successes_on += 1
            except ResponseError:
                pass

        # Cleanup.
        self.new_client_for_primary(1).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        self.new_client_for_primary(2).execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")

        assert successes_on > 0, (
            f"Expected successes with local-search-max-priority=yes. "
            f"timeouts_off={timeouts_off}, successes_on={successes_on}")
