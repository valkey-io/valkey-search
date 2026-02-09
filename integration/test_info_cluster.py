from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
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

    def test_ft_info_cluster_force_index_name_error_retry(self):
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
        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceIndexNotFoundError 3") == b"OK"

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

        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]

        assert retry_count_before + 3 == retry_count_after, f"Expected retry_count increment by 3, got {retry_count_after - retry_count_before}"
    
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

        retry_count_before = node0.info("SEARCH")["search_info_fanout_retry_count"]
        
        assert node1.execute_command("FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount 1") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check retry count
        retry_count_after = node0.info("SEARCH")["search_info_fanout_retry_count"]
        assert retry_count_after == retry_count_before + 1, f"Expected retry_count increment by 1, got {retry_count_after - retry_count_before}"

        # check cluster info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"

    def test_ft_info_cluster_attribute(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        cluster.execute_command("HSET", "doc:1", "title", "hello", "body", "world")
        cluster.execute_command("HSET", "doc:2", "title", "valkey")

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "title", "TEXT",
            "body", "TEXT"
        ) == b"OK"

        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")
        parser = FTInfoParser([])
        info = parser._parse_key_value_list(raw)

        # check primary info results
        assert info is not None
        assert str(info.get("index_name")) == index_name
        assert str(info.get("mode")) == "cluster"
        assert int(info["backfill_in_progress"]) == 0
        assert float(info["backfill_complete_percent_max"]) == 1.000000
        assert float(info["backfill_complete_percent_min"]) == 1.000000
        assert str(info["state"]) == "ready"

        # Parse attributes into a dict keyed by attribute name
        attrs = {
            parsed["attribute"]: parsed
            for a in info["attributes"]
            if isinstance(parsed := parser._parse_key_value_list(a), dict)
        }
        assert attrs["title"]["user_indexed_memory"] > 0
        assert attrs["body"]["user_indexed_memory"] > 0