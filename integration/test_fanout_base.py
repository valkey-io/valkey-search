from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode, ValkeySearchClusterTestCase, Node
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from valkey.exceptions import ResponseError, ConnectionError
import pytest
from indexes import *

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

def search_command(index: str) -> list[str]:
    return [
        "FT.SEARCH",
        index,
        "*=>[KNN 10 @v $BLOB]",
        "PARAMS",
        "2",
        "BLOB",
        float_to_bytes([10.0, 10.0, 10.0]),
    ]

def index_on_node(client, name:str) -> bool:
    indexes = client.execute_command("FT._LIST")
    return name.encode() in indexes

def sum_of_remote_searches(nodes: list[Node]) -> int:
    return sum([n.client.info("search")["search_coordinator_server_search_index_partition_success_count"] for n in nodes])

class TestFanout(ValkeySearchClusterTestCase):
    @pytest.mark.parametrize(
        "setup_test", [{"replica_count": 2}], indirect=True
    )
    @pytest.mark.parametrize("threshold", [0, 100])
    def test_fanout_low_utilization_fanout(self, threshold):

        number_of_searches_to_run = 100
        rg = self.get_replication_group(0)
        primary = rg.get_primary_connection()
        assert(primary.info("replication")["role"] == "master")
        
        # Set the fanout low utilization threshold
        primary.execute_command("CONFIG", "SET", "search.local-fanout-queue-wait-threshold", threshold)
        
        index = Index("test", [Vector("v", 3, type="FLAT")], type=KeyDataType.HASH)
        index.create(primary)
        for node in self.get_nodes():
            waiters.wait_for_true(lambda: index_on_node(node.client, index.name))
        index.load_data(self.new_cluster_client(), 100)

        waiters.wait_for_true(lambda: self.replication_lag() == 0)

        # Assert replicas of the primary didn't run any search queries
        assert(sum_of_remote_searches(rg.replicas) == 0)

        # Verify the threshold was applied
        result = primary.execute_command("CONFIG", "GET", "search.local-fanout-queue-wait-threshold")
        assert result[1].decode() == str(threshold)

        # Execute searches
        for i in range(number_of_searches_to_run):
            result = primary.execute_command(*search_command(index.name))
            assert(len(result) > 1)
        
        if threshold:
            # threshold == 100 means we are always under utilize and prefer to do local search on the shard
            # Assert replicas of the primary didn't run any search queries
            assert(sum_of_remote_searches(rg.replicas) == 0)
        else:
            # threshold == 0 means we are always "too busy"
            # Assert replicas of the primary run some of the search queries 
            assert(sum_of_remote_searches(rg.replicas) > 0)

    def test_sample_queue_size_config(self):
        """Test thread-pool-wait-time-samples configuration parameter"""
        rg = self.get_replication_group(0)
        client = rg.get_primary_connection()
        
        # Test default value
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"100"
        
        # Test setting new value
        client.execute_command("CONFIG", "SET", "search.thread-pool-wait-time-samples", "200")
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"200"
        
        # Test boundary values
        client.execute_command("CONFIG", "SET", "search.thread-pool-wait-time-samples", "10")
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"10"
        
        client.execute_command("CONFIG", "SET", "search.thread-pool-wait-time-samples", "10000")
        result = client.execute_command("CONFIG", "GET", "search.thread-pool-wait-time-samples")
        assert result[1] == b"10000"
        