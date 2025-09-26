from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from test_info_primary import verify_error_response
from ft_info_parser import FTInfoParser

class TestFTInfoCluster(ValkeySearchClusterTestCaseDebugMode):

    def is_backfill_complete(self, node, index_name):
        raw = node.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)
        if not info:
            return False
        backfill_in_progress = int(info["backfill_in_progress"])
        state = info["state"]
        return backfill_in_progress == 0 and state == "ready"

    def test_ft_info_cluster_success(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check cluster info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"
    
    def test_ft_info_cluster_retry(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        index_name = "index1"

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        
        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))
        
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 1") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check retry count
        retry_count = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count == 1, f"Expected retry_count to be equal to 1, got {retry_count}"

        # check cluster info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"
