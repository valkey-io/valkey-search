import time
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
import pytest
from test_info_primary import _parse_info_kv_list, verify_error_response, is_index_on_all_nodes
import re

class TestFTInfoCluster(ValkeySearchClusterTestCaseDebugMode):

    def is_backfill_complete(self, node, index_name):
        raw = node.execute_command("FT.INFO", index_name, "CLUSTER")
        info = _parse_info_kv_list(raw)
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
        
        waiters.wait_for_true(lambda: is_index_on_all_nodes(self, index_name))
        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")
        info = _parse_info_kv_list(raw)

        assert info is not None
        mode = info.get("mode")
        index_name = info.get("index_name")
        assert (mode in (b"cluster", "cluster"))
        assert (index_name in (b"index1", "index1"))

        backfill_in_progress = int(info["backfill_in_progress"])
        backfill_complete_percent_max = float(info["backfill_complete_percent_max"])
        backfill_complete_percent_min = float(info["backfill_complete_percent_min"])
        state = info["state"]

        assert backfill_in_progress == 0
        assert backfill_complete_percent_max == 1.000000
        assert backfill_complete_percent_min == 1.000000
        assert state == "ready"
    
    def test_ft_info_cluster_retry(self):
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
        
        waiters.wait_for_true(lambda: is_index_on_all_nodes(self, index_name))
        waiters.wait_for_true(lambda: self.is_backfill_complete(node0, index_name))
        
        assert node0.execute_command("FT._DEBUG FANOUT_FORCE_REMOTE_FAIL yes") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "CLUSTER")

        # check retry count
        info_search_str = str(node0.execute_command("INFO SEARCH"))
        pattern = r'search_fanout_retry_count:(\d+)'
        match = re.search(pattern, info_search_str)
        if not match:
            assert False, f"search_fanout_retry_count not found in INFO SEARCH results!"
        retry_count = int(match.group(1))
        assert retry_count > 0, f"Expected retry_count to be greater than 0, got {retry_count}"

        # check cluster info results
        info = _parse_info_kv_list(raw)
        assert info is not None
        mode = info.get("mode")
        index_name = info.get("index_name")
        assert (mode in (b"cluster", "cluster"))
        assert (index_name in (b"index1", "index1"))
        backfill_in_progress = int(info["backfill_in_progress"])
        backfill_complete_percent_max = float(info["backfill_complete_percent_max"])
        backfill_complete_percent_min = float(info["backfill_complete_percent_min"])
        state = info["state"]
        assert backfill_in_progress == 0
        assert backfill_complete_percent_max == 1.000000
        assert backfill_complete_percent_min == 1.000000
        assert state == "ready"

        assert node0.execute_command("FT._DEBUG FANOUT_FORCE_REMOTE_FAIL no") == b"OK"