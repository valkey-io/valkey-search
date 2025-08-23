import time
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from test_info_primary import _parse_info_kv_list, verify_error_response
import pytest

@pytest.mark.skip("temporary")
class TestFTInfoCluster(ValkeySearchClusterTestCase):

    def is_indexing_complete(self, node, index_name):
        try:
            raw = node.execute_command("FT.INFO", index_name, "CLUSTER")
            info = _parse_info_kv_list(raw)
            if not info:
                return False
            backfill_in_progress = int(info.get("backfill_in_progress", 1))
            state = info.get("state", "")
            return backfill_in_progress == 0 and state == "ready"
        except Exception as e:
            if "Communication error between nodes found" in str(e):
                return False
            raise

    def test_ft_info_cluster_counts(self):
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
        
        waiters.wait_for_equal(lambda: self.is_indexing_complete(node0, index_name), True, timeout=10)

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

    def test_ft_info_non_existing_index(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        verify_error_response(
            node0,
            "FT.INFO index123 CLUSTER",
            "Index with name 'index123' not found",
        )
