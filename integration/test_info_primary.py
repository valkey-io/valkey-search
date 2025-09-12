import time
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
import pytest

def _parse_info_kv_list(reply):
    it = iter(reply)
    out = {}
    for k in it:
        v = next(it, None)
        out[k.decode() if isinstance(k, bytes) else k] = v.decode() if isinstance(v, bytes) else v
    return out

def verify_error_response(client, cmd, expected_err_reply):
    try:
        client.execute_command(cmd)
        assert False
    except Exception as e:
        assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
        assert str(e) == expected_err_reply, assert_error_msg
        return str(e)

@pytest.mark.skip("temporary")
class TestFTInfoPrimary(ValkeySearchClusterTestCase):

    def is_indexing_complete(self, node, index_name, N):
        raw = node.execute_command("FT.INFO", index_name, "PRIMARY")
        info = _parse_info_kv_list(raw)
        if not info:
            return False
        num_docs = int(info.get("num_docs", 0))
        num_records = int(info.get("num_records", 0))
        return num_docs >= N and num_records >= N

    def test_ft_info_primary_counts(self):
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

        waiters.wait_for_equal(lambda: self.is_indexing_complete(node0, index_name, N), True, timeout=5)

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")
        info = _parse_info_kv_list(raw)

        assert info is not None
        mode = info.get("mode")
        index_name = info.get("index_name")
        assert (mode in (b"primary", "primary"))
        assert (index_name in (b"index1", "index1"))

        num_docs = int(info["num_docs"])
        num_records = int(info["num_records"])
        hash_fail = int(info["hash_indexing_failures"])

        assert num_docs == N
        assert num_records == N
        assert hash_fail == 0

    def test_ft_info_non_existing_index(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        verify_error_response(
            node0,
            "FT.INFO index123 PRIMARY",
            "Index with name 'index123' not found",
        )
