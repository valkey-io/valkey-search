from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkey.exceptions import ResponseError
from indexes import *
from test_cancel import Any
from valkeytestframework.util import waiters
from valkey import ResponseError
from test_fanout_base import MAX_RETRIES

class TestFTInfoPartitionConsistencyControls(ClusterTestUtils, ValkeySearchClusterTestCaseDebugMode):
    def run_info_command(self, client, index_name, enable_partial_results=False, require_consistency=False, expect_error=False):
        try:
            return client.execute_command(
                "FT.INFO",
                index_name,
                "PRIMARY",
                "SOMESHARDS" if enable_partial_results else "ALLSHARDS",
                "CONSISTENT" if require_consistency else "INCONSISTENT"
            )
        except ResponseError as e:
            if expect_error:
                assert str(e) == "Unable to contact all cluster members"
                return []
            else:
                raise

    def test_ft_info_consistency_controls(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()

        index_name = "hnsw"
        index = Index(index_name, [Vector("v", 3, type="HNSW"), Numeric("n")])
        index.create(client)
        index.load_data(client, 1000)
        assert self.sum_docs(index) == 1000

        # normal result without consistency check
        normal_result = self.run_info_command(client, index_name, require_consistency=False)

        # make commands to all nodes, force refresh cluster map
        node0 = self.new_client_for_primary(0)
        node0.execute_command("FT.INFO hnsw PRIMARY")
        node1 = self.new_client_for_primary(1)
        node1.execute_command("FT.INFO hnsw PRIMARY")
        node2 = self.new_client_for_primary(2)
        node2.execute_command("FT.INFO hnsw PRIMARY")

        # normal result with consistency check
        cur_result = self.run_info_command(client, index_name, require_consistency=True)
        assert cur_result == normal_result
        
        # force invalid invalid index fingerprint and version
        self.control_set("ForceInfoInvalidSlotFingerprint", "yes")
        self.config_set("search.ft-info-timeout-ms", "500")

        # enable consistency check, get error result
        cur_result = self.run_info_command(client, index_name, require_consistency=True, expect_error=True)
        assert cur_result == []

        # disable consistency check, get normal result
        cur_result = self.run_info_command(client, index_name, require_consistency=False, expect_error=False)
        assert cur_result == normal_result

        self.control_set("ForceInfoInvalidSlotFingerprint", "no")

    def test_ft_info_partition_controls(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()

        index_name = "hnsw"
        index = Index(index_name, [Vector("v", 3, type="HNSW"), Numeric("n")])
        index.create(client)
        index.load_data(client, 1000)
        assert self.sum_docs(index) == 1000

        # normal result without partition controls
        normal_result = self.run_info_command(client, index_name)

        # normal result with partition controls
        cur_result = self.run_info_command(client, index_name, enable_partial_results=True)
        assert cur_result == normal_result

        self.config_set("search.ft-info-timeout-ms", "500")

        node0: Valkey = self.new_client_for_primary(0)
        node1: Valkey = self.new_client_for_primary(1)
        
        # force timeout on a remote node
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            MAX_RETRIES
        ) == b"OK"

        # disable partial result, get error result
        cur_result = self.run_info_command(node0, index_name, enable_partial_results=False, expect_error=True)
        assert cur_result == []

        # enable partial results, get partial result
        cur_result = self.run_info_command(node0, index_name, enable_partial_results=True)
        print(cur_result)
        print(normal_result)
        assert cur_result != normal_result

        # reset timeout on remote node
        assert node1.execute_command(
            "FT._DEBUG CONTROLLED_VARIABLE SET ForceRemoteFailCount ", 
            0
        ) == b"OK"
