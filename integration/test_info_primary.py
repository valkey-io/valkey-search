from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser
from test_fanout_base import MAX_RETRIES
from typing import Any, Union
from valkey.exceptions import ResponseError, ConnectionError
import pytest

def verify_error_response(client, cmd, expected_err_reply):
    try:
        client.execute_command(cmd)
        assert False
    except Exception as e:
        assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
        assert str(e) == expected_err_reply, assert_error_msg
        return str(e)

class TestFTInfoPrimary(ValkeySearchClusterTestCaseDebugMode):

    def execute_primaries(self, command: Union[str, list[str]]) -> list[Any]:
        return [
            self.client_for_primary(i).execute_command(*command)
            for i in range(len(self.replication_groups))
        ]

    def is_indexing_complete(self, node, index_name, N):
        raw = node.execute_command("FT.INFO", index_name, "PRIMARY")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)
        if not info:
            return False
        num_docs = int(info["num_docs"])
        num_records = int(info["num_records"])
        return num_docs >= N and num_records >= N

    def test_ft_info_primary_success(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        waiters.wait_for_true(lambda: self.is_indexing_complete(node0, index_name, N))

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check primary info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "primary"
        assert int(info["num_docs"]) == N
        assert int(info["num_records"]) == N
        assert int(info["hash_indexing_failures"]) == 0

    
    def test_ft_info_primary_retry(self):
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

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        waiters.wait_for_true(lambda: self.is_indexing_complete(node0, index_name, N))

        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]

        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 1") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check retry count
        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count_after == retry_count_before + 1, f"Expected retry_count increment by 1, got {retry_count_after - retry_count_before}"

        # check primary info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "primary"
        assert int(info["num_docs"]) == N
        assert int(info["num_records"]) == N
        assert int(info["hash_indexing_failures"]) == 0
    
    # force one remote node to fail and test SOMESHARDS arg
    # expect partial results
    def test_ft_info_primary_someshards_one_shard_fail(self):
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

        N = 60
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        waiters.wait_for_true(lambda: self.is_indexing_complete(node0, index_name, N))

        # force one node to fail continuously
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            MAX_RETRIES
        ) == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY", "SOMESHARDS")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check partial primary info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "primary"
        assert int(info["num_docs"]) < N
        assert int(info["num_records"]) < N

        # reset remote fail to 0
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            0
        ) == b"OK"
    
    # force all nodes to fail
    # expect empty results
    def test_ft_info_primary_someshards_all_shards_fail(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        N = 60
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        waiters.wait_for_true(lambda: self.is_indexing_complete(node0, index_name, N))

        # force all remote nodes to fail continuously
        results = self.execute_primaries(["FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount", MAX_RETRIES])
        assert all(result == b"OK" for result in results)

        # force local node to fail continuously
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceLocalFailCount ", 
            MAX_RETRIES
        ) == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY", "SOMESHARDS")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check partial results should be 0
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "primary"
        assert int(info["num_docs"]) == 0
        assert int(info["num_records"]) == 0
        assert int(info["hash_indexing_failures"]) == 0

        # reset all remote nodes
        results = self.execute_primaries(["FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount", 0])
        assert all(result == b"OK" for result in results)

         # reset local node
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceLocalFailCount ", 
            0
        ) == b"OK"
    
    # force inconsistent errors
    # expect fail in CONSISTENT but success in INCONSISTENT arg
    def test_ft_info_primary_consistent_arg(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        N = 60
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        waiters.wait_for_true(lambda: self.is_indexing_complete(node0, index_name, N))

        # force inconsistent error
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceInfoPrimaryInconsistentError yes"
        ) == b"OK"

        # expect error in consistent mode
        with pytest.raises(ResponseError) as excinfo:
            node0.execute_command("FT.INFO", index_name, "PRIMARY", "ALLSHARDS", "CONSISTENT")
        assert "Unable to contact all cluster members" in str(excinfo.value)

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY", "ALLSHARDS", "INCONSISTENT")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # expect results in inconsistent mode
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "primary"
        assert int(info["num_docs"]) == N
        assert int(info["num_records"]) == N
        assert int(info["hash_indexing_failures"]) == 0

        # reset inconsistent error
        assert node0.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceInfoPrimaryInconsistentError no"
        ) == b"OK"

