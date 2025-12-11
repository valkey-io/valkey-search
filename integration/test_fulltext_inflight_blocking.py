"""Integration tests for full-text query blocking on in-flight mutations."""

import threading
import time
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode
)
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper


def run_in_thread(func):
    """Run func in thread, return (thread, result, error) for later inspection."""
    result, error = [None], [None]
    def wrapper():
        try:
            result[0] = func()
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=wrapper)
    t.start()
    return t, result, error


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
        time.sleep(0.5)
        # Verify search is still blocked (hasn't returned yet)
        assert search_res[0] is None and search_thread.is_alive()

        # Release pausepoint and wait for completion
        client.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        hset_thread.join()
        search_thread.join()

        assert hset_err[0] is None
        assert search_err[0] is None and search_res[0] is not None

        # Verify blocking metrics
        info = client.info("SEARCH")
        assert info["search_fulltext_query_blocked_count"] >= 1
        assert info["search_fulltext_query_retry_count"] >= 1

class TestFullTextInFlightBlockingCME(ValkeySearchClusterTestCaseDebugMode):
    """Tests for CME (cluster) mode."""

    def _find_shard_keys(self, client, num_shards):
        """Find one key per shard using CLUSTER SLOTS."""
        slots_info = client.execute_command("CLUSTER", "SLOTS")
        shard_keys = []
        for slot_range in slots_info[:num_shards]:
            for i in range(1000):
                key = f"doc:test:{i}"
                slot = client.execute_command("CLUSTER", "KEYSLOT", key)
                if slot_range[0] <= slot <= slot_range[1]:
                    shard_keys.append(key)
                    break
        return shard_keys

    def test_fulltext_inflight_blocking_cluster_with_pausepoint(self):
        """Test that full-text queries block in cluster mode."""
        client: Valkey = self.client_for_primary(0)
        cluster_client: ValkeyCluster = self.new_cluster_client()
        primary_clients = self.get_all_primary_clients()
        num_shards = len(primary_clients)

        shard_keys = self._find_shard_keys(client, num_shards)
        assert len(shard_keys) == num_shards

        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT"
        )
        for i, key in enumerate(shard_keys):
            cluster_client.execute_command("HSET", key, "content", f"hello world {i}")
        IndexingTestHelper.wait_for_indexing_complete_on_all_nodes(primary_clients, "idx")
        assert client.execute_command("FT.SEARCH", "idx", "@content:hello")[0] == num_shards

        # Pause mutation processing on all nodes to mimick inflight keys
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
        time.sleep(0.5)
        # Verify search is still blocked (hasn't returned yet)
        assert search_res[0] is None and search_thread.is_alive()

        # Release pausepoints and wait
        for nc in primary_clients:
            nc.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        for t in hset_threads:
            t.join()
        search_thread.join()

        assert search_err[0] is None and search_res[0] is not None

        # Verify blocking is done on all shards
        for nc in primary_clients:
            assert nc.info("SEARCH")["search_fulltext_query_blocked_count"] >= 1
            assert nc.info("SEARCH")["search_fulltext_query_retry_count"] >= 1