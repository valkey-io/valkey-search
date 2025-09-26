from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from valkey.exceptions import ResponseError, ConnectionError
import pytest

MAX_RETRIES = "4294967295"

class TestFanoutBase(ValkeySearchClusterTestCaseDebugMode):

    # force retry by manually creating remote failure once
    def test_fanout_retry(self):
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

        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]

        # force remote node to fail once and trigger retry
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 1") == b"OK"

        node0.execute_command("FT.INFO", index_name, "PRIMARY")

        # check retry count
        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count_after == retry_count_before + 1, f"Expected retry_count increment by 1, got {retry_count_after - retry_count_before}"

    def test_fanout_shutdown(self):
        cluster = self.new_cluster_client()
        node0 = self.new_client_for_primary(0)
        node1 = self.new_client_for_primary(1)
        
        index_name = "index1"
        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
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
            node0.execute_command("FT.INFO", index_name, "CLUSTER")
        
        assert "Unable to contact all cluster members" in str(excinfo.value)
    
    def test_fanout_timeout(self):
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

        # force timeout by enabling continuous remote failure
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            MAX_RETRIES
        ) == b"OK"

        with pytest.raises(ResponseError) as ei:
            node0.execute_command("FT.INFO", index_name, "PRIMARY")
        assert "Unable to contact all cluster members" in str(ei.value)

        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 0", 
        ) == b"OK"