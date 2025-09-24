from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from valkey.exceptions import ResponseError, ConnectionError
import threading
import time
import pytest

def create_index_on_node(node, node_id, index_name, results, exceptions, barrier):
    try:
        if barrier:
            barrier.wait()
        if node_id == 0:
            result = node.execute_command(
                "FT.CREATE", index_name,
                "ON", "HASH",
                "PREFIX", "1", "doc:",
                "SCHEMA", "price", "NUMERIC"
            )
        else:
            result = node.execute_command(
                "FT.CREATE", index_name,
                "ON", "HASH", 
                "PREFIX", "1", "doc:",
                "SCHEMA", "embedding", "VECTOR", "FLAT", 
                 "6", "TYPE", "FLOAT32", "DIM", "2", 
                "DISTANCE_METRIC", "L2"
        )
        results[node_id] = result
    except Exception as e:
        exceptions[node_id] = e

class TestFTCreateConsistency(ValkeySearchClusterTestCaseDebugMode):

    def test_success_creation(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
    
    def test_duplicate_creation(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"
        results = {}
        exceptions = {}
        
        create_index_on_node(node0, 0, index_name, results, exceptions, None)
        create_index_on_node(node1, 1, index_name, results, exceptions, None)
        res = str(results[0])
        err = str(exceptions[1])
        assert "OK" in res
        assert "Index index1 already exists" in err

    # create same index name with different schema on two nodes concurrently
    # one command should success and other one should fail
    def test_concurrent_creation(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"
        
        # Store results from both threads
        results = {}
        exceptions = {}
        
        # Barrier to synchronize thread execution
        barrier = threading.Barrier(2)
        # Create and start threads
        thread0 = threading.Thread(
            target=create_index_on_node, 
            args=(node0, 0, index_name, results, exceptions, barrier)
        )
        thread1 = threading.Thread(
            target=create_index_on_node, 
            args=(node1, 1, index_name, results, exceptions, barrier)
        )
        thread0.start()
        thread1.start()
        # Wait for both threads to complete
        thread0.join()
        thread1.join()

        res = next(iter(results.values()))
        err = next(iter(exceptions.values()))
        assert "OK" in str(res)
        assert "Index index1 already exists" in str(err)
    
    def test_create_shutdown(self):
        cluster = self.new_cluster_client()
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        index_name = "index1"

        try:
            node1.execute_command("SHUTDOWN", "NOSAVE")
        except:
            pass
        
        def is_node_down(node):
            try:
                node.ping()
                return False
            except ConnectionError:
                return True
    
        waiters.wait_for_true(lambda: is_node_down(node1), timeout=5)
        
        with pytest.raises(ResponseError) as excinfo:
            node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
            )

        assert "Unable to contact all cluster members" in str(excinfo.value)