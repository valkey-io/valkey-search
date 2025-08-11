import time
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker

def _parse_info_kv_list(reply):
    it = iter(reply)
    out = {}
    for k in it:
        v = next(it, None)
        out[k.decode() if isinstance(k, bytes) else k] = v
    return out

class TestFTInfoClusterCompleted(ValkeySearchClusterTestCase):

    def test_ft_info_cluster_counts(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        assert node0.execute_command(
            "FT.CREATE", "index1",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        time.sleep(0.2)

        raw = node0.execute_command("FT.INFO", "index1", "CLUSTER")
        info = _parse_info_kv_list(raw)

        assert info is not None
        mode = info.get("mode")
        index_name = info.get("index_name")
        assert (mode in (b"cluster", "cluster"))
        assert (index_name in (b"index1", "index1"))

        backfill_in_progress = int(info["backfill_in_progress"].decode() if isinstance(info["backfill_in_progress"], bytes) else info["backfill_in_progress"])
        backfill_complete_percent_max = float(info["backfill_complete_percent_max"].decode() if isinstance(info["backfill_complete_percent_max"], bytes) else info["backfill_complete_percent_max"])
        backfill_complete_percent_min = float(info["backfill_complete_percent_min"].decode() if isinstance(info["backfill_complete_percent_min"], bytes) else info["backfill_complete_percent_min"])
        state = info["state"].decode() if isinstance(info["state"], bytes) else info["state"]

        assert backfill_in_progress == 0
        assert backfill_complete_percent_max == 1.000000
        assert backfill_complete_percent_min == 1.000000
        assert state == "ready"

    def test_ft_info_non_existing_index(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        self.verify_error_response(
            node0,
            "FT.INFO index123 CLUSTER",
            "Index with name 'index123' not found",
        )
