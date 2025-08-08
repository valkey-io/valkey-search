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

class TestFTInfoPrimary(ValkeySearchClusterTestCase):

    def test_ft_info_primary_counts(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)

        assert node0.execute_command(
            "FT.CREATE", "index1",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        deadline = time.time() + 10
        last = None
        while time.time() < deadline:
            raw = node0.execute_command("FT.INFO", "index1", "PRIMARY")
            info = _parse_info_kv_list(raw)
            last = info

            if "num_docs" in info and "num_records" in info:
                num_docs = int(info["num_docs"].decode() if isinstance(info["num_docs"], bytes) else info["num_docs"])
                num_records = int(info["num_records"].decode() if isinstance(info["num_records"], bytes) else info["num_records"])
                if num_docs >= N and num_records >= N:
                    break
            time.sleep(0.2)

        assert last is not None
        mode = last.get("mode")
        index_name = last.get("index_name")
        assert (mode in (b"primary", "primary"))
        assert (index_name in (b"index1", "index1"))

        num_docs = int(last["num_docs"].decode() if isinstance(last["num_docs"], bytes) else last["num_docs"])
        num_records = int(last["num_records"].decode() if isinstance(last["num_records"], bytes) else last["num_records"])
        hash_fail = last.get("hash_indexing_failures", b"0")
        hash_fail = int(hash_fail.decode() if isinstance(hash_fail, bytes) else hash_fail)

        assert num_docs == N
        assert num_records == N
        assert hash_fail == 0

    def test_ft_info_non_existing_index(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        self.verify_error_response(
            node0,
            "FT.INFO index123 PRIMARY",
            "Index with name 'index123' not found",
        )
