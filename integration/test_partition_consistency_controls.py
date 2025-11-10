from valkey import ResponseError
from valkey.client import Valkey
from valkeytestframework.conftest import resource_port_tracker
from valkey_search_test_case import (
    ValkeySearchTestCaseDebugMode,
    ValkeySearchClusterTestCaseDebugMode,
)
from indexes import *
from test_cancel import *

class TestPartitionConsistencyControls(ValkeySearchClusterTestCaseDebugMode):

    def execute_primaries(self, command: Union[str, list[str]]) -> list[Any]:
        return [
            self.client_for_primary(i).execute_command(*command)
            for i in range(len(self.replication_groups))
        ]

    def config_set(self, config: str, value: str):
        assert self.execute_primaries(["config set", config, value]) == [True] * len(
            self.replication_groups
        )

    def control_set(self, key:str, value:str):
        assert all(
            [node.client.execute_command(*["ft._debug", "CONTROLLED_VARIABLE", "set", key, value]) == b"OK" for node in self.nodes]
        )

    def check_info(self, name: str, value: Union[str, int]):
        results = self.execute_primaries(["INFO", "SEARCH"])
        failed = False
        for ix, r in enumerate(results):
            if r[name] != value:
                print(
                    name,
                    " Expected:",
                    value,
                    " Received:",
                    r[name],
                    " on server:",
                    ix,
                )
                failed = True
        assert not failed
    
    def _check_info_sum(self, name: str) -> int:
        """Sum the values of a given info field across all servers"""
        results = self.execute_primaries(["INFO", "SEARCH"])
        return sum([int(r[name]) for r in results if name in r])

    def check_info_sum(self, name: str, sum_value: int):
        """Sum the values of a given info field across all servers"""
        waiters.wait_for_equal(
          lambda: self._check_info_sum(name), 
          sum_value, 
          timeout=5
        )
    
    def sum_docs(self, index: Index) -> int:
        return sum([index.info(self.client_for_primary(i)).num_docs for i in range(len(self.replication_groups))])

    def test_ft_search_partition_controls(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()
        self.check_info("search_cancel-timeouts", 0)
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW"), Numeric("n")])

        # create index and load data
        hnsw_index.create(client)
        hnsw_index.load_data(client, 100)
        waiters.wait_for_equal(lambda: self.sum_docs(hnsw_index), 100, timeout=3)

        # Nominal case
        nominal_hnsw_result = search(client, "hnsw", False)
        self.check_info_sum("search_test-counter-ForceCancels", 0)
        assert nominal_hnsw_result[0] == 10

        # Now, force timeouts quickly
        self.control_set("ForceTimeout", "yes")
        self.control_set("TimeoutPollFrequency", "1")

        # Disable partial results, get empty result due to timeout
        hnsw_result = search(client, "hnsw", True, None, enable_partial_results=False)
        assert hnsw_result == []
        self.check_info_sum("search_test-counter-ForceCancels", 3)

        # Enable partial results, get partial results
        hnsw_result = search(client, "hnsw", False, None, enable_partial_results=True)
        self.check_info_sum("search_test-counter-ForceCancels", 6)
        assert hnsw_result != nominal_hnsw_result
