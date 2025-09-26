from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser

def verify_error_response(client, cmd, expected_err_reply):
    try:
        client.execute_command(cmd)
        assert False
    except Exception as e:
        assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
        assert str(e) == expected_err_reply, assert_error_msg
        return str(e)

class TestFTInfoPrimary(ValkeySearchClusterTestCaseDebugMode):

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
