from valkey import ResponseError
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from indexes import *
from valkeytestframework.util import waiters
from test_cancel import search, search_command, Any

class TestFTSearchPartitionConsistencyControls(ClusterTestUtils, ValkeySearchClusterTestCaseDebugMode):
    def test_ft_search_partition_controls(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()
        self.check_info("search_cancel-timeouts", 0)
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW"), Numeric("n")])

        # create index and load data
        hnsw_index.create(client)
        hnsw_index.load_data(client, 1000)
        assert self.sum_docs(hnsw_index) == 1000

        # Nominal case
        nominal_hnsw_result = search(client, "hnsw", False)
        self.check_info_sum("search_test-counter-ForceCancels", 0)
        assert nominal_hnsw_result[0] == 10

        # Now, force timeouts quickly
        self.control_set("ForceTimeout", "yes")
        self.control_set("TimeoutPollFrequency", "0")

        # Disable partial results, get empty result due to timeout
        hnsw_result = search(client, "hnsw", True, None, enable_partial_results=False)
        assert hnsw_result == []
        self.check_info_sum("search_test-counter-ForceCancels", 3)

        # Enable and get partial results
        hnsw_result = search(client, "hnsw", False, None, enable_partial_results=True)
        self.check_info_sum("search_test-counter-ForceCancels", 6)
        assert hnsw_result[0] != nominal_hnsw_result[0]

        self.control_set("ForceTimeout", "no")
    
    def test_ft_search_consistency_controls(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()
        self.check_info("search_cancel-timeouts", 0)
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW"), Numeric("n")])

        # create index and load data
        hnsw_index.create(client)
        hnsw_index.load_data(client, 1000)
        assert self.sum_docs(hnsw_index) == 1000

        # Nominal case
        nominal_hnsw_result = search(client, "hnsw", False)
        self.check_info_sum("search_test-counter-ForceCancels", 0)
        assert nominal_hnsw_result[0] == 10

        # make commands to all nodes, force refresh cluster map
        node1 = self.new_client_for_primary(1)
        node1.execute_command("FT.INFO hnsw PRIMARY")
        node2 = self.new_client_for_primary(2)
        node2.execute_command("FT.INFO hnsw PRIMARY")
        
        # enable consistency check, get correct result
        hnsw_result = search(client, "hnsw", False, expect_consistency_error=False, enable_consistency=True)
        assert hnsw_result[0] == nominal_hnsw_result[0]

        # force invalid slot fingerprint
        self.control_set("ForceInvalidSlotFingerprint", "yes")

        # disable consistency check, get valid results
        hnsw_result = search(client, "hnsw", False, enable_consistency=False)
        assert hnsw_result[0] == nominal_hnsw_result[0]

        # enable consistency check, get empty result (failure)
        hnsw_result = search(client, "hnsw", False, expect_consistency_error=True, enable_consistency=True)
        assert hnsw_result == []

        # do not force invalid slot fingerprint
        self.control_set("ForceInvalidSlotFingerprint", "no")

        # force invalid index fingerprint on one node only
        client = self.new_client_for_primary(0)
        client.execute_command(
            "ft._debug", "CONTROLLED_VARIABLE", "set", "ForceInvalidIndexFingerprint", "yes"
        )

        # disable consistency check, get valid results
        hnsw_result = search(client, "hnsw", False, enable_consistency=False)
        assert hnsw_result[0] == nominal_hnsw_result[0]

        # enable consistency check, get empty result (failure)
        hnsw_result = search(client, "hnsw", False, expect_consistency_error=True, enable_consistency=True)
        assert hnsw_result == []
