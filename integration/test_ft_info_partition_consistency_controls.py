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

class TestFTInfoPartitionConsistencyControls(ValkeySearchClusterTestCaseDebugMode):

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

    def run_info_command(self, client, index_name, enable_partial_results=False, enable_consistency=False, expect_error=False):
        if expect_error:
            try:
                x = client.execute_command(
                    "FT.INFO",
                    index_name,
                    "PRIMARY",
                    "SOMESHARDS" if enable_partial_results else "ALLSHARDS",
                    "CONSISTENT" if enable_consistency else "INCONSISTENT"
                )
                assert False, "Expected error, but got result: " + str(x)
            except ResponseError as e:
                assert str(e) == "Unable to contact all cluster members"
            return []
        else:
            return client.execute_command(
                "FT.INFO",
                index_name,
                "PRIMARY",
                "SOMESHARDS" if enable_partial_results else "ALLSHARDS",
                "CONSISTENT" if enable_consistency else "INCONSISTENT"
            )

    def test_ft_info_consistency_controls(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()

        index_name = "hnsw"
        index = Index(index_name, [Vector("v", 3, type="HNSW"), Numeric("n")])
        index.create(client)
        index.load_data(client, 1000)
        waiters.wait_for_equal(lambda: self.sum_docs(index), 1000, timeout=3)

        # normal result without consistency check
        normal_result = self.run_info_command(client, index_name, enable_consistency=False)

        # normal result with consistency check
        cur_result = self.run_info_command(client, index_name, enable_consistency=True)
        assert cur_result == normal_result
        
        # force invalid invalid index fingerprint and version
        self.control_set("ForceInfoInvalidIndexFingerprint", "yes")

        # enable consistency check, get error result
        cur_result = self.run_info_command(client, index_name, enable_consistency=True, expect_error=True)
        assert cur_result == []

        # disable consistency check, get normal result
        cur_result = self.run_info_command(client, index_name, enable_consistency=False, expect_error=False)
        assert cur_result == normal_result

        self.control_set("ForceInfoInvalidIndexFingerprint", "no")

    def test_ft_info_partition_controls(self):
        self.execute_primaries(["flushall sync"])
        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()

        index_name = "hnsw"
        index = Index(index_name, [Vector("v", 3, type="HNSW"), Numeric("n")])
        index.create(client)
        index.load_data(client, 1000)
        waiters.wait_for_equal(lambda: self.sum_docs(index), 1000, timeout=3)

        # normal result without partition controls
        normal_result = self.run_info_command(client, index_name)

        # normal result with partition controls
        cur_result = self.run_info_command(client, index_name, enable_partial_results=True)
        assert cur_result == normal_result

        self.config_set("search.ft-info-timeout-ms", "1000")

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








        