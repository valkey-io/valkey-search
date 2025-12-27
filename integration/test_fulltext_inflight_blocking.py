"""Integration tests for full-text query blocking on in-flight mutations."""

import struct
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode
)
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper, run_in_thread


class TestFullTextInFlightBlockingCMD(ValkeySearchTestCaseDebugMode):
    """Tests for CMD (standalone) mode."""

    def test_fulltext_inflight_blocking_with_pausepoint(self):
        """Test that full-text queries block when there are in-flight mutations."""
        client: Valkey = self.server.get_new_client()

        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT"
        )
        client.execute_command("HSET", "doc:1", "content", "hello world test document")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")
        assert client.execute_command("FT.SEARCH", "idx", "@content:hello")[0] == 1

        # Pause mutation processing to keep key in-flight
        client.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")

        # HSET blocks at pausepoint, run in background
        hset_thread, _, hset_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "content", "updated"
            )
        )

        waiters.wait_for_true(
            lambda: client.execute_command("FT._DEBUG PAUSEPOINT TEST mutation_processing") > 0,
            timeout=5
        )

        search_thread, search_res, search_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.SEARCH", "idx", "@content:hello"
            )
        )
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_fulltext_query_blocked_count"] >= 1
        )
        # Wait for multiple retries to verify retry mechanism
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_fulltext_query_retry_count"] >= 2
        )
        # Verify search is still blocked (hasn't returned yet)
        assert search_res[0] is None and search_thread.is_alive()

        # Release pausepoint and wait for completion
        client.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        hset_thread.join()
        search_thread.join()

        assert hset_err[0] is None
        assert search_err[0] is None and search_res[0] is not None

    def test_hybrid_query_with_text_predicate(self):
        """Test that hybrid queries (vector + text) DO block on in-flight mutations."""
        client: Valkey = self.server.get_new_client()

        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT", "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2"
        )
        vec1 = struct.pack('<4f', 0.0, 0.0, 0.0, 0.0)
        vec2 = struct.pack('<4f', 1.0, 1.0, 1.0, 1.0)
        client.execute_command("HSET", "doc:1", "content", "hello world", "vec", vec1)
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")

        hset_thread, _, _ = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "content", "updated", "vec", vec2
            )
        )

        waiters.wait_for_true(
            lambda: client.execute_command("FT._DEBUG PAUSEPOINT TEST mutation_processing") > 0,
            timeout=5
        )

        # Hybrid query with text component SHOULD block
        search_thread, search_res, search_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.SEARCH", "idx",
                "(@content:hello)=>[KNN 10 @vec $BLOB]",
                "PARAMS", "2", "BLOB", vec1,
                "DIALECT", "2"
            )
        )
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_fulltext_query_blocked_count"] >= 1
        )
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_fulltext_query_retry_count"] >= 2
        )
        assert search_res[0] is None and search_thread.is_alive()

        client.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        hset_thread.join()
        search_thread.join()

        assert search_err[0] is None and search_res[0] is not None

    def test_non_text_query_does_not_block(self):
        """Test that non-text queries on index with text field do NOT block."""
        client: Valkey = self.server.get_new_client()

        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT", "category", "TAG"
        )
        client.execute_command("HSET", "doc:1", "content", "hello world", "category", "news")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        client.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")

        hset_thread, _, _ = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "content", "updated", "category", "sports"
            )
        )

        waiters.wait_for_true(
            lambda: client.execute_command("FT._DEBUG PAUSEPOINT TEST mutation_processing") > 0,
            timeout=5
        )

        # TAG-only query should NOT block even though index has TEXT field
        result = client.execute_command("FT.SEARCH", "idx", "@category:{news}")
        assert result is not None
        assert client.info("SEARCH")["search_fulltext_query_blocked_count"] == 0

        client.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        hset_thread.join()


class TestFullTextInFlightBlockingCME(ValkeySearchClusterTestCaseDebugMode):
    """Tests for CME (cluster) mode."""

    def _find_key_for_node(self, node_client, prefix="doc:test:"):
        """Find a key that belongs to the given node using CLUSTER SLOTS."""
        node_port = node_client.connection_pool.connection_kwargs['port']
        slots_info = node_client.execute_command("CLUSTER", "SLOTS")
        for slot_range in slots_info:
            # slot_range: [start_slot, end_slot, [ip, port, node_id], ...]
            if slot_range[2][1] == node_port:
                for i in range(10000):
                    key = f"{prefix}{i}"
                    if slot_range[0] <= node_client.execute_command("CLUSTER", "KEYSLOT", key) <= slot_range[1]:
                        return key
        return None

    def _find_shard_keys(self, primary_clients):
        """Find one key per shard."""
        return [self._find_key_for_node(nc, f"doc:shard{i}:") for i, nc in enumerate(primary_clients)]

    def test_fulltext_inflight_blocking_cluster_with_pausepoint(self):
        """Test that full-text queries block in cluster mode."""
        client: Valkey = self.client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        primary_clients = self.get_all_primary_clients()
        num_shards = len(primary_clients)

        shard_keys = self._find_shard_keys(primary_clients)
        assert len(shard_keys) == num_shards

        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT"
        )
        for i, key in enumerate(shard_keys):
            cluster_client.execute_command("HSET", key, "content", f"hello world {i}")
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(primary_clients, "idx")
        assert client.execute_command("FT.SEARCH", "idx", "@content:hello")[0] == num_shards

        # Pause mutation processing on all nodes to simulate inflight keys
        for nc in primary_clients:
            nc.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")

        # Update all documents
        hset_threads = []
        for key in shard_keys:
            t, _, _ = run_in_thread(
                lambda k=key: cluster_client.execute_command("HSET", k, "content", "updated")
            )
            hset_threads.append(t)

        waiters.wait_for_true(
            lambda: all(nc.execute_command("FT._DEBUG PAUSEPOINT TEST mutation_processing") > 0
                       for nc in primary_clients),
        )

        search_thread, search_res, search_err = run_in_thread(
            lambda: client.execute_command("FT.SEARCH", "idx", "@content:hello")
        )
        waiters.wait_for_true(
            lambda: all(nc.info("SEARCH")["search_fulltext_query_blocked_count"] >= 1
                       for nc in primary_clients)
        )
        # Wait for multiple retries to verify retry mechanism
        waiters.wait_for_true(
            lambda: all(nc.info("SEARCH")["search_fulltext_query_retry_count"] >= 2
                       for nc in primary_clients)
        )
        # Verify search is still blocked (hasn't returned yet)
        assert search_res[0] is None and search_thread.is_alive()

        # Release pausepoints and wait
        for nc in primary_clients:
            nc.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        for t in hset_threads:
            t.join()
        search_thread.join()

        assert search_err[0] is None and search_res[0] is not None

    def test_blocking_only_on_remote_nodes(self):
        """Test when coordinator is not blocked but remote nodes are blocked."""
        coordinator: Valkey = self.client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        primary_clients = self.get_all_primary_clients()
        remote_clients = primary_clients[1:]

        # Find keys: one for coordinator, one for each remote node
        coordinator_key = self._find_key_for_node(coordinator, "doc:coord:")
        remote_keys = [self._find_key_for_node(nc, f"doc:remote{i}:") for i, nc in enumerate(remote_clients)]

        coordinator.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT"
        )
        cluster_client.execute_command("HSET", coordinator_key, "content", "hello coordinator")
        for key in remote_keys:
            cluster_client.execute_command("HSET", key, "content", "hello remote")
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(primary_clients, "idx")

        # Pause only on remote nodes (not coordinator)
        for nc in remote_clients:
            nc.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")

        # Update only remote keys
        hset_threads = []
        for key in remote_keys:
            t, _, _ = run_in_thread(
                lambda k=key: cluster_client.execute_command("HSET", k, "content", "updated")
            )
            hset_threads.append(t)

        waiters.wait_for_true(
            lambda: all(nc.execute_command("FT._DEBUG PAUSEPOINT TEST mutation_processing") > 0
                       for nc in remote_clients),
        )

        search_thread, search_res, search_err = run_in_thread(
            lambda: coordinator.execute_command("FT.SEARCH", "idx", "@content:hello")
        )
        waiters.wait_for_true(
            lambda: all(nc.info("SEARCH")["search_fulltext_query_blocked_count"] >= 1
                       for nc in remote_clients)
        )
        assert search_res[0] is None and search_thread.is_alive()
        # Coordinator should not be blocked
        assert coordinator.info("SEARCH")["search_fulltext_query_blocked_count"] == 0

        for nc in remote_clients:
            nc.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        for t in hset_threads:
            t.join()
        search_thread.join()

        assert search_err[0] is None and search_res[0] is not None

    def test_blocking_only_on_coordinator(self):
        """Test when coordinator is blocked but remote nodes are not blocked."""
        coordinator: Valkey = self.client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        primary_clients = self.get_all_primary_clients()
        remote_clients = primary_clients[1:]

        # Find keys: one for coordinator, one for each remote node
        coordinator_key = self._find_key_for_node(coordinator, "doc:coord:")
        remote_keys = [self._find_key_for_node(nc, f"doc:remote{i}:") for i, nc in enumerate(remote_clients)]

        coordinator.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT"
        )
        cluster_client.execute_command("HSET", coordinator_key, "content", "hello coordinator")
        for key in remote_keys:
            cluster_client.execute_command("HSET", key, "content", "hello remote")
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(primary_clients, "idx")

        # Pause only on coordinator
        coordinator.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")

        # Update only coordinator's key
        hset_thread, _, _ = run_in_thread(
            lambda: cluster_client.execute_command("HSET", coordinator_key, "content", "updated")
        )

        waiters.wait_for_true(
            lambda: coordinator.execute_command("FT._DEBUG PAUSEPOINT TEST mutation_processing") > 0,
        )

        search_thread, search_res, search_err = run_in_thread(
            lambda: coordinator.execute_command("FT.SEARCH", "idx", "@content:hello")
        )
        waiters.wait_for_true(
            lambda: coordinator.info("SEARCH")["search_fulltext_query_blocked_count"] >= 1
        )
        assert search_res[0] is None and search_thread.is_alive()
        # Remote nodes should not be blocked
        for nc in remote_clients:
            assert nc.info("SEARCH")["search_fulltext_query_blocked_count"] == 0

        coordinator.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        hset_thread.join()
        search_thread.join()

        assert search_err[0] is None and search_res[0] is not None
