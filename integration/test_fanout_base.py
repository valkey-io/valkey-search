import time
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from test_info_primary import is_index_on_all_nodes
from valkey.exceptions import ResponseError
import pytest
import re
import threading
import time

class TestFanoutBase(ValkeySearchClusterTestCaseDebugMode):

    # force retry by manually creating remote failure
    def test_fanout_base_retry(self):
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

        assert node0.execute_command("CONFIG SET search.fanout-force-remote-fail yes") == b"OK"

        node0.execute_command("FT.INFO", index_name, "PRIMARY")
        info_search_str = str(node0.execute_command("INFO SEARCH"))

        pattern = r'search_fanout_retry_count:(\d+)'
        match = re.search(pattern, info_search_str)
        if not match:
            assert False, f"search_fanout_retry_count not found in INFO SEARCH results!"
        retry_count = int(match.group(1))

        assert retry_count > 0, f"Expected retry_count to be greater than 0, got {retry_count}"

        assert node0.execute_command("CONFIG SET search.fanout-force-remote-fail no") == b"OK"

    # force timeout by pausing remote calls
    def test_fanout_base_timeout(self):
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

        assert node0.execute_command("FT._DEBUG PAUSEPOINT SET fanout_remote_pausepoint") == b"OK"

        with pytest.raises(ResponseError) as ei:
            node0.execute_command("FT.INFO", index_name, "PRIMARY")
        assert "Request timed out" in str(ei.value)

        assert int(str(node0.execute_command("FT._DEBUG PAUSEPOINT TEST fanout_remote_pausepoint"))) > 0

        assert node0.execute_command("FT._DEBUG PAUSEPOINT RESET fanout_remote_pausepoint") == b"OK"

    # force timeout by shutting down a node
    def test_fanout_base_shutdown_timeout(self):
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

        waiters.wait_for_true(lambda: is_index_on_all_nodes(self, index_name))

        N = 5
        for i in range(N):
            cluster.execute_command("HSET", f"doc:{i}", "price", str(10 + i))

        set_res = node0.execute_command("FT._DEBUG PAUSEPOINT SET fanout_before_rpc")
        assert b"OK" in set_res

        def shutdown_and_release():
            waiters.wait_for_true(lambda: int(node0.execute_command("FT._DEBUG PAUSEPOINT TEST fanout_before_rpc")) > 0)
            try:
                node1.execute_command("SHUTDOWN")
            except Exception:
                pass
            node0.execute_command("FT._DEBUG PAUSEPOINT RESET fanout_before_rpc")

        threading.Thread(target=shutdown_and_release, daemon=True).start()

        with pytest.raises(ResponseError) as ei:
            node0.execute_command("FT.INFO", index_name, "CLUSTER")
        assert "Request timed out" in str(ei.value)