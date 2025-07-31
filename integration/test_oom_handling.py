from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
import struct
import pytest
from valkey.exceptions import OutOfMemoryError


class TestSearchOOMHandling(ValkeySearchClusterTestCase):
    """
    Test suite for search command OOM handling. We expect that
    when one node hits oom during fanout, whole command would be aborted.
    """

    index_name = "myIndex"

    def _run_search_query(self, client):
        search_vector = struct.pack("<3f", *[1.0, 2.0, 3.0])
        return client.execute_command(
            "FT.SEARCH",
            self.index_name,
            "*=>[KNN 2 @vector $query_vector]",
            "PARAMS",
            "2",
            "query_vector",
            search_vector,
        )

    def test_oom_one_shard_abort(self):
        cluster_client: ValkeyCluster = self.new_cluster_client()
        # Create index
        assert (
            cluster_client.execute_command(
                "FT.CREATE",
                self.index_name,
                "SCHEMA",
                "vector",
                "VECTOR",
                "HNSW",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "3",
                "DISTANCE_METRIC",
                "COSINE",
            )
            == b"OK"
        )
        # Insert vectors
        for i in range(10000):
            vector_bytes = struct.pack("<3f", *[float(i), float(i + 1), float(i + 2)])
            cluster_client.hset(f"vec:{i}", "vector", vector_bytes)

        # Expect command returns 2 vectors
        assert self._run_search_query(cluster_client)[0] == 2

        client_primary_1: Valkey = self.new_client_for_primary(1)
        current_used_memory = client_primary_1.info("memory")["used_memory"]

        # Update the maxmemory of the second primary node so we hit OOM
        client_primary_1.config_set("maxmemory", current_used_memory)

        # Get client for third primary, assert it has enough memory
        client_primary_2: Valkey = self.new_client_for_primary(2)
        maxmemory_client_2 = client_primary_2.info("memory")["maxmemory"]
        assert maxmemory_client_2 == 0  # Unlimited usage

        # Run search query using third primary, and expect to fail on OOM
        with pytest.raises(OutOfMemoryError):
            self._run_search_query(client_primary_2)
