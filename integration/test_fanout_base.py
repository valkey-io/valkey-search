from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode, ValkeySearchClusterTestCase, Node
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from valkey.exceptions import ResponseError, ConnectionError
import pytest
from indexes import *
from ft_info_parser import FTInfoParser

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
    
    def test_fingerprint_version_create(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        assert node0.execute_command("CONFIG SET search.info-developer-visible yes") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)
        fingerprint = int(info["index_fingerprint"])
        assert fingerprint is not None
        version = int(info["index_version"])
        assert version == 0

        # drop and create index again
        assert node0.execute_command("FT.DROPINDEX", index_name) == b"OK"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")
        info = parser._parse_key_value_list(raw)
        assert fingerprint == int(info["index_fingerprint"])
        assert version < int(info["index_version"])

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
        number_of_searches_to_run = 10
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

def load_fingerprint_version_from_rdb(test):
    client = test.new_client_for_primary(0)
    index_name = "index1"

    assert client.execute_command("CONFIG SET search.info-developer-visible yes") == b"OK"

    assert client.execute_command(
        "FT.CREATE", index_name,
        "ON", "HASH",
        "PREFIX", "1", "doc:",
        "SCHEMA", "price", "NUMERIC"
    ) == b"OK"

    raw = client.execute_command("FT.INFO", index_name, "PRIMARY")
    parser = FTInfoParser([])
    info = parser._parse_key_value_list(raw)
    fingerprint = int(info["index_fingerprint"])
    assert fingerprint is not None
    version = int(info["index_version"])
    assert version == 0

    # drop and create index again
    assert client.execute_command("FT.DROPINDEX", index_name) == b"OK"

    assert client.execute_command(
        "FT.CREATE", index_name,
        "ON", "HASH",
        "PREFIX", "1", "doc:",
        "SCHEMA", "price", "NUMERIC"
    ) == b"OK"

    # Save RDB on all nodes
    for rg in test.replication_groups:
        rg.primary.client.execute_command("SAVE")
        
    # Restart all nodes in the cluster
    for rg in test.replication_groups:
        rg.primary.server.restart(remove_rdb=False)

    # Wait for all nodes to be ready
    def are_all_nodes_ready():
        try:
            for i in range(len(test.replication_groups)):
                client = test.new_client_for_primary(i)
                client.ping()
            return True
        except (ConnectionError, Exception):
            return False

    waiters.wait_for_true(are_all_nodes_ready, timeout=10)
    client = test.new_client_for_primary(0)
    client.execute_command("CONFIG SET search.info-developer-visible yes")

    raw = client.execute_command("FT.INFO", index_name, "PRIMARY")
    info = parser._parse_key_value_list(raw)
    assert fingerprint == int(info["index_fingerprint"])
    assert version < int(info["index_version"])
        
class TestLoadFingerprintVersionFromRDB_v1_v1(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "no"
        return args
    def test_load_fingerprint_version_from_rdb_v1_v1(self):
        load_fingerprint_version_from_rdb(self)

class TestLoadFingerprintVersionFromRDB_v1_v2(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "yes"
        return args
    def test_load_fingerprint_version_from_rdb_v1_v2(self):
        load_fingerprint_version_from_rdb(self)

class TestLoadFingerprintVersionFromRDB_v2_v1(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "no"
        return args
    def test_load_fingerprint_version_from_rdb_v2_v1(self):
        load_fingerprint_version_from_rdb(self)

class TestLoadFingerprintVersionFromRDB_v2_v2(ValkeySearchClusterTestCaseDebugMode):
    def append_startup_args(self, args):
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args
    def test_load_fingerprint_version_from_rdb_v2_v2(self):
        load_fingerprint_version_from_rdb(self)
