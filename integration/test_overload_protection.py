"""
Integration tests for search overload protection.

Configs tested:
  - search.max-query-queue-depth: Rejects queries when queue exceeds limit.
"""

import pytest
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkey.exceptions import ResponseError
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from utils import IndexingTestHelper


class TestOverloadProtectionSingleNode(ValkeySearchTestCaseDebugMode):
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
            pytest.fail(f"Should not reject when limit=0: {e}")
        finally:
            client.execute_command(
                "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
                "ForceQueueDepthExceeded", "no")


class TestOverloadProtectionCluster(ValkeySearchClusterTestCaseDebugMode):
    """Queue depth on cluster (fanout/coordinator path)."""
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
        """Queue depth rejection on coordinator path."""
        self._create_index()
        replica = self._replica_client()
        # Rejection fires with correct error message.
        self._config_all("search.max-query-queue-depth", 100)
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

    def test_server_side_queue_depth(self):
        """Server rejects incoming partition requests when its queue is full.
        Forces the remote shard's primary to reject partition requests via
        ForceServerQueueDepthExceeded. The coordinator should handle this
        gracefully — with enable_partial_results (default), it returns
        results from the local shard only.
        """
        self._create_index()
        self._config_all("search.max-query-queue-depth", 100)
        replica = self._replica_client()
        # Baseline: full results from both shards.
        full_result = replica.execute_command(
            "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "10")
        full_count = full_result[0]
        assert full_count > 0, "Baseline should return results"
        # Force remote shard's primary AND replica to reject (kRandom may
        # pick either).
        self.replication_groups[1].primary.client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceServerQueueDepthExceeded", "yes")
        self.replication_groups[1].replicas[0].client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceServerQueueDepthExceeded", "yes")
        # With remote shard rejecting, we get partial results (local only).
        partial_result = replica.execute_command(
            "FT.SEARCH", "idx", "@tag1:{common}", "LIMIT", "0", "10")
        partial_count = partial_result[0]
        # Cleanup.
        self.replication_groups[1].primary.client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceServerQueueDepthExceeded", "no")
        self.replication_groups[1].replicas[0].client.execute_command(
            "FT._DEBUG", "CONTROLLED_VARIABLE", "SET",
            "ForceServerQueueDepthExceeded", "no")
        # Partial count should be less than full (missing remote shard's docs).
        assert partial_count < full_count, (
            f"Expected partial < full. partial={partial_count}, full={full_count}")
        assert partial_count > 0, (
            f"Expected some results from local shard. partial={partial_count}")
