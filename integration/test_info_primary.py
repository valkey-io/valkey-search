import time
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
import pytest
import re

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

def is_index_on_all_nodes(cur, index_name):
    """
    Returns True if index exists on all nodes, False otherwise
    """
    cluster_size = getattr(cur, 'CLUSTER_SIZE', 3)
    for i in range(cluster_size):
        rg = cur.get_replication_group(i)
        all_nodes = [rg.primary] + rg.replicas
        for j, node in enumerate(all_nodes):
            client = node.client if hasattr(node, 'client') else cur.new_client_for_primary(i)
            index_list = client.execute_command("FT._LIST")
            index_names = [idx.decode() if isinstance(idx, bytes) else str(idx) for idx in index_list]
            if index_name not in index_names:
                node_type = "primary" if j == 0 else f"replica-{j-1}"
                return False
    return True

class TestFTInfoPrimary(ValkeySearchClusterTestCase):

    def is_indexing_complete(self, node, index_name, N):
        raw = node.execute_command("FT.INFO", index_name, "PRIMARY")
        info = _parse_info_kv_list(raw)
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

        waiters.wait_for_true(lambda: is_index_on_all_nodes(self, index_name))

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        waiters.wait_for_true(lambda: self.is_indexing_complete(node0, index_name, N))

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
    
    def test_ft_info_primary_retry(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        node0: Valkey = self.new_client_for_primary(0)
        index_name = "index1"

        assert node0.execute_command(
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        waiters.wait_for_true(lambda: is_index_on_all_nodes(self, index_name))
        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        waiters.wait_for_true(lambda: self.is_indexing_complete(node0, index_name, N))

        assert node0.execute_command("CONFIG SET search.fanout-force-remote-fail yes") == b"OK"

        raw = node0.execute_command("FT.INFO", index_name, "PRIMARY")

        # check retry count
        info_search_str = str(node0.execute_command("INFO SEARCH"))
        pattern = r'search_fanout_retry_count:(\d+)'
        match = re.search(pattern, info_search_str)
        if not match:
            assert False, f"search_fanout_retry_count not found in INFO SEARCH results!"
        retry_count = int(match.group(1))
        assert retry_count > 0, f"Expected retry_count to be greater than 0, got {retry_count}"

        # check primary info results
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

        assert node0.execute_command("CONFIG SET search.fanout-force-remote-fail no") == b"OK"
