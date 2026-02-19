from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase, ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
import struct
import pytest
from valkey.exceptions import OutOfMemoryError

INDEX_NAME = "myIndex"

def create_index(client: Valkey):
    assert (
        client.execute_command(
            "FT.CREATE",
            INDEX_NAME,
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

def insert_vectors(client: Valkey, num_vectors: int = 10000):
    for i in range(num_vectors):
        vector_bytes = struct.pack("<3f", *[float(i), float(i + 1), float(i + 2)])
        client.hset(f"vec:{i}", "vector", vector_bytes)

def run_search_query(client: Valkey):
    search_vector = struct.pack("<3f", *[1.0, 2.0, 3.0])
    return client.execute_command(
        "FT.SEARCH",
        INDEX_NAME,
        "*=>[KNN 2 @vector $query_vector]",
        "PARAMS",
        "2",
        "query_vector",
        search_vector,
    )

class TestSearchOOMHandlingCME(ValkeySearchClusterTestCase):
    """
    Test suite for search command OOM handling for CME. We expect that
    when one node hits oom during fanout, whole command would be aborted.
    """

    def test_search_oom_cme(self):
        cluster_client: ValkeyCluster = self.new_cluster_client()
        # Create index
        create_index(cluster_client)
        # Insert vectors
        insert_vectors(cluster_client)

        # Expect command returns 2 vectors
        assert run_search_query(cluster_client)[0] == 2

        client_primary_1: Valkey = self.new_client_for_primary(1)
        current_used_memory = client_primary_1.info("memory")["used_memory"]

        # Update the maxmemory of the second primary node so we hit OOM
        client_primary_1.config_set("maxmemory", current_used_memory)

        # Get client for third primary, assert it has enough memory
        client_primary_2: Valkey = self.new_client_for_primary(2)
        maxmemory_client_2 = client_primary_2.info("memory")["maxmemory"]
        assert maxmemory_client_2 == 0  # Unlimited usage
        
        # Expect to NOT OOM when trying to run search on second primary
        assert run_search_query(client_primary_1)[0] == 2

        # Run search query using third primary, and expect to NOT fail on OOM
        assert run_search_query(client_primary_2)[0] == 2

class TestSearchOOMHandlingCMD(ValkeySearchTestCaseBase):
    """
    Test suite for search command OOM handling for CMD.
    """
    
    def test_search_oom_cmd(self):
        client: Valkey = self.server.get_new_client()
        # Create index
        create_index(client)
        # Insert data
        insert_vectors(client)
        # Set maxmemory for current used memory of the node
        current_used_memory = client.info("memory")["used_memory"]
        client.config_set("maxmemory", current_used_memory)
        # Expect to NOT fail on OOM
        assert run_search_query(client)[0] == 2
