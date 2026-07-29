"""
Integration tests for search overload protection.

Configs tested:
  - search.max-query-queue-depth: Rejects queries when queue exceeds limit.
  - search.queue-depth-scaling-factor: Scales queue limit by cluster size.
  - search.local-search-max-priority: Local shard gets kMax thread pool priority.
"""

from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkey.exceptions import ResponseError
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from utils import IndexingTestHelper


class TestQueueDepthSingleNode(ValkeySearchTestCaseDebugMode):
    """Queue depth rejection on a single node (no fanout, direct SearchAsync)."""

    def test_queue_depth_single_node(self):
        """Queue depth check fires on non-fanout path."""
        client = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "tag1", "TAG"
        )
        for i in range(10):
            client.hset(f"d:{i}", mapping={"tag1": "common"})
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        client.execute_command("CONFIG", "SET",
                               "search.max-query-queue-depth", "100")
        client.execute_command("CONFIG", "SET",
                               "search.queue-depth-scaling-factor", "0.0")
        # Force rejection.
        client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceQueueDepthExceeded", "yes")
        rejected = None
        try:
            client.execute_command(
                "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
        except ResponseError as e:
            rejected = str(e)
        assert rejected is not None, "Expected rejection on single node"
        assert "queue depth exceeded" in rejected.lower(), (
            f"Wrong error: {rejected}")
        # Disable force -- should succeed.
        client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceQueueDepthExceeded", "no")
        result = client.execute_command(
            "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
        assert result is not None
        # Disabled when limit=0.
        client.execute_command("CONFIG", "SET",
                               "search.max-query-queue-depth", "0")
        client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceQueueDepthExceeded", "yes")
        try:
            client.execute_command(
                "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
        except ResponseError as e:
            assert False, f"Should not reject when limit=0: {e}"
        finally:
            client.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
                "ForceQueueDepthExceeded", "no")


class TestOverloadProtectionCluster(ValkeySearchClusterTestCaseDebugMode):
    """Queue depth and partial results on cluster (fanout/coordinator path)."""
    CLUSTER_SIZE = 2
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
        """Get the first replica's existing client (coordinator)."""
        return self.replication_groups[0].replicas[0].client

    def test_queue_depth_cluster(self):
        """Queue depth rejection and scaling on coordinator path."""
        self._create_index()
        replica = self._replica_client()
        # Rejection fires with correct error message.
        self._config_all("search.max-query-queue-depth", 100)
        self._config_all("search.queue-depth-scaling-factor", "0.0")
        replica.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceQueueDepthExceeded", "yes")
        with_rejection = None
        try:
            replica.execute_command(
                "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
        except ResponseError as e:
            with_rejection = str(e)
        assert with_rejection is not None, "Expected rejection when forced"
        assert "queue depth exceeded" in with_rejection.lower(), (
            f"Wrong error message: {with_rejection}")
        # Disable force -- query should succeed.
        replica.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceQueueDepthExceeded", "no")
        result = replica.execute_command(
            "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
        assert result is not None
        # Scaling factor: 2 shards, scaling=1.0: effective = 6/(1+1*(2-1)) = 3.
        self._config_all("search.max-query-queue-depth", 6)
        self._config_all("search.queue-depth-scaling-factor", "1.0")
        replica.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceQueueDepthExceeded", "yes")
        rejected = False
        try:
            replica.execute_command(
                "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
        except ResponseError as e:
            if "queue depth exceeded" in str(e).lower():
                rejected = True
        replica.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceQueueDepthExceeded", "no")
        assert rejected, "Expected rejection with scaling active"

    def test_local_search_max_priority(self):
        """Local shard returns partial results when remote shard times out."""
        self._create_index()
        self._config_all("search.coordinator-query-timeout-secs", 1)
        self._config_all("search.default-timeout-ms", 3000)
        self._config_all("search.max-query-queue-depth", 0)
        self._config_all("search.local-search-max-priority", "yes")
        # Pause the ENTIRE remote shard (both primary and replica).
        self.replication_groups[1].primary.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "search_entries_fetcher")
        self.replication_groups[1].replicas[0].client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "search_entries_fetcher")
        replica = self._replica_client()
        successes = 0
        errors = []
        for _ in range(3):
            try:
                result = replica.execute_command(
                    "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "1")
                successes += 1
            except ResponseError as e:
                errors.append(str(e))
        # Cleanup.
        self.replication_groups[1].primary.client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "search_entries_fetcher")
        self.replication_groups[1].replicas[0].client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "search_entries_fetcher")
        assert successes == 3, (
            f"Expected all queries to return partial results. "
            f"successes={successes}, errors={errors[:3]}")
