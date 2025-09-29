from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkey.exceptions import ResponseError
import threading
import pytest

class TestFTDropindexConsistency(ValkeySearchClusterTestCaseDebugMode):

    def test_dropindex_success(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command(
            "FT.DROPINDEX", index_name
        ) == b"OK"

    def test_duplicate_dropindex(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command(
            "FT.DROPINDEX", index_name
        ) == b"OK"

        with pytest.raises(ResponseError) as e:
            node0.execute_command("FT.DROPINDEX", index_name)
        err_msg = "Index with name '" + index_name + "' not found"
        assert err_msg in str(e)

    def test_concurrent_dropindex(self):

        def dropindex_on_node(node, node_id):
            barrier.wait()
            try:
                result = node.execute_command("FT.DROPINDEX", index_name)
                results[node_id] = result
            except Exception as e:
                exceptions[node_id] = e

        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        results = {}
        exceptions = {}

        # Barrier to synchronize thread execution
        barrier = threading.Barrier(2)
        # Create and start threads
        thread0 = threading.Thread(
            target=dropindex_on_node, 
            args=(node0, 0)
        )
        thread1 = threading.Thread(
            target=dropindex_on_node, 
            args=(node1, 1)
        )
        thread0.start()
        thread1.start()
        # Wait for both threads to complete
        thread0.join()
        thread1.join()

        res = next(iter(results.values()))
        err = next(iter(exceptions.values()))
        assert "OK" in str(res)
        err_msg = "Index with name '" + index_name + "' not found"
        assert err_msg in str(err)