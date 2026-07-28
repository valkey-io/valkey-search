"""
Integration tests for search overload protection mechanisms.

Tests:
1. max-query-queue-depth: Reject queries when queue exceeds threshold
2. local-search-max-priority: Local shard gets kMax priority during fan-out
"""

import time
import threading
import pytest
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.client import Valkey
from valkey.exceptions import ResponseError, ConnectionError as ValkeyConnectionError


class TestOverloadProtection(ValkeySearchClusterTestCaseDebugMode):
    """Tests for search overload protection in cluster mode."""

    CLUSTER_SIZE = 3
    REPLICAS_COUNT = 0

    def _setup_index(self):
        """Create index and load data."""
        node0 = self.new_client_for_primary(0)
        node0.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "tag1", "TAG", "num1", "NUMERIC"
        )
        time.sleep(2)
        cluster = self.new_cluster_client()
        for j in range(100):
            cluster.hset(f"d:{j}", mapping={"tag1": "common", "num1": str(j)})
        time.sleep(1)

    def _config_all(self, key, value):
        for i in range(self.CLUSTER_SIZE):
            self.new_client_for_primary(i).execute_command("CONFIG", "SET", key, str(value))

    def test_max_query_queue_depth_rejects(self):
        """
        When queue depth exceeds max-query-queue-depth, new queries are rejected
        with 'Search query queue depth exceeded' before fan-out.
        """
        self._setup_index()

        node0 = self.new_client_for_primary(0)

        # Set queue depth limit to 2, use pausepoint to hold queries in the reader thread
        self._config_all("search.reader-threads", 1)
        self._config_all("search.default-timeout-ms", 10000)
        self._config_all("search.max-query-queue-depth", 2)
        self._config_all("search.local-search-max-priority", "no")

        # Set pausepoint to block the reader thread — queries will pile up in the queue
        node0.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        # Pre-create connections
        host, port = self.replication_groups[0].primary.server.bind_ip, self.replication_groups[0].primary.server.port
        clients = [Valkey(host=host, port=port, socket_timeout=12) for _ in range(8)]

        results = []
        errors = []

        def query_worker(idx):
            try:
                r = clients[idx].execute_command("FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                results.append("ok")
            except ResponseError as e:
                errors.append(str(e))
            except (ValkeyConnectionError, OSError, TimeoutError) as e:
                errors.append(f"conn:{type(e).__name__}")

        # Launch 8 queries concurrently — with 1 thread paused, queue fills up
        threads = [threading.Thread(target=query_worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()

        # Give time for queries to arrive and queue to fill
        time.sleep(2)

        # Release the pausepoint so threads can complete
        node0.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")

        for t in threads:
            t.join(timeout=15)

        for c in clients:
            try:
                c.close()
            except Exception:
                pass

        queue_errors = [e for e in errors if "queue depth exceeded" in e.lower()]
        print(f"\nResults: {len(results)} succeeded, {len(queue_errors)} queue-rejected, "
              f"{len(errors) - len(queue_errors)} other errors")
        print(f"  Errors sample: {errors[:3]}")

        # With queue limit=2 and 8 concurrent queries, most should be rejected
        assert len(queue_errors) > 0, \
            f"Expected queue depth rejections. Got {len(results)} successes, errors: {errors[:5]}"

    def test_local_search_max_priority(self):
        """
        With local-search-max-priority=yes and remote shards slow (via pausepoint),
        the local shard completes via kMax priority, enabling partial results.
        """
        self._setup_index()

        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        node2 = self.new_client_for_primary(2)

        # Short timeout, enable partial results
        self._config_all("search.reader-threads", 2)
        self._config_all("search.default-timeout-ms", 1500)
        self._config_all("search.max-query-queue-depth", 0)
        self._config_all("search.enable-partial-results", "yes")

        # Pause remote nodes (simulates slow primaries)
        node1.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")
        node2.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")

        # --- Without fix: local shard has same priority as remote, may not complete in time ---
        self._config_all("search.local-search-max-priority", "no")
        time.sleep(0.5)

        timeouts_off = 0
        successes_off = 0
        for _ in range(3):
            try:
                node0.execute_command("FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                successes_off += 1
            except ResponseError:
                timeouts_off += 1

        # --- With fix: local shard gets kMax priority, completes even when remote paused ---
        self._config_all("search.local-search-max-priority", "yes")
        time.sleep(0.5)

        timeouts_on = 0
        successes_on = 0
        for _ in range(3):
            try:
                node0.execute_command("FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                successes_on += 1
            except ResponseError:
                timeouts_on += 1

        # Release pausepoints
        node1.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        node2.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "background_search_completing")
        time.sleep(2)

        print(f"\nWithout fix: successes={successes_off}, timeouts={timeouts_off}")
        print(f"With fix:    successes={successes_on}, timeouts={timeouts_on}")

        assert successes_on > successes_off, \
            f"Expected more successes with local-search-max-priority. " \
            f"Off={successes_off}, On={successes_on}"
