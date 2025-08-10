from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging
from typing import Any

def canceller(client, client_id):
    my_id = client.execute_command("client id")
    assert my_id != client_id
    client.execute_command("client kill id ", client_id)

def search_command(index:str, filter: int|None) -> list[str]:
    predicate = "*" if filter is None else f"(@n:[0 {filter}])"
    return ["FT.SEARCH", index, predicate+"=>[KNN 10 @v $BLOB]", "PARAMS", "2", "BLOB", float_to_bytes([10.0, 10.0, 10.0])]
    
def search(client: valkey.client, index:str, timeout: bool, filter: int|None = None) -> list[tuple[str, float]]:
    print("Search command: ", search_command(index, filter))
    if not timeout:
        return client.execute_command(*search_command(index, filter))
    else:
        try:
            x = client.execute_command(*search_command(index, filter))
            assert False, "Expected timeout, but got result: " + str(x)
        except ResponseError as e:
            assert str(e) == "Search operation cancelled due to timeout"
        return []


class TestCancelCMD(ValkeySearchTestCaseBase):

    def test_timeoutCMD(self):
        """
            Test CMD timeout logic
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FLUSHALL SYNC")
        # po
        assert client.execute_command("CONFIG SET search.info-developer-visible yes") == b"OK"
        assert client.info("SEARCH")["search_cancel-timeouts"] == 0
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
        flat_index = Index("flat", [Vector("v", 3, type="FLAT"), Numeric("n")])
       
        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 1000)

        #
        # Nominal case
        #
        nominal_hnsw_result = search(client, "hnsw", False)
        nominal_flat_result = search(client, "flat", False)

        assert client.info("SEARCH")["search_cancel-timeouts"] == 0
        assert nominal_hnsw_result[0] == 10
        assert nominal_flat_result[0] == 10

        #
        # Now, force timeouts quickly
        #
        assert client.execute_command("CONFIG SET search.test-force-timeout yes") == b"OK"
        assert client.execute_command("CONFIG SET search.timeout-poll-frequency 1") == b"OK"

        #
        # Enable timeout path, no error but message result
        #
        assert client.execute_command("CONFIG SET search.enable-partial-results no") == b"OK"

        hnsw_result = search(client, "hnsw", True)
        assert client.info("SEARCH")["search_cancel-forced"] == 1

        flat_result = search(client, "flat", True)
        assert client.info("SEARCH")["search_cancel-forced"] == 2

        #
        # Enable partial results
        #
        assert client.execute_command("CONFIG SET search.enable-partial-results yes") == b"OK"

        hnsw_result = search(client, "hnsw", False)
        assert client.info("SEARCH")["search_cancel-forced"] == 3
        assert hnsw_result != nominal_hnsw_result

        flat_result = search(client, "flat", False)
        assert client.info("SEARCH")["search_cancel-forced"] == 4
        assert flat_result != nominal_flat_result

        #
        # Now, test pre-filtering case.
        #
        assert client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 0
        hnsw_result = search(client, "hnsw", False, 2)
        assert hnsw_result[0] == 2
        assert client.info("SEARCH")["search_cancel-forced"] == 5
        assert client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 1

        #
        # Disable partial results, and force timeout with pre-filtering
        #
        assert client.execute_command("CONFIG SET search.enable-partial-results no") == b"OK"
        assert client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 1
        hnsw_result = search(client, "hnsw", True, 2)
        assert client.info("SEARCH")["search_cancel-forced"] == 6
        assert client.info("SEARCH")["search_query_prefiltering_requests_cnt"] == 2
        assert hnsw_result != nominal_hnsw_result

class TestCancelCME(ValkeySearchClusterTestCase):

    def execute_all(self, command: str|list[str]) -> list[Any]:
        return [self.client_for_primary(i).execute_command(*command) for i in range(len(self.servers))]

    def config_set(self, config: str, value: str):
        assert self.execute_all(["config set", config, value]) == [True] * len(self.servers)

    def check_info(self, name: str, value: str|int):
        results = self.execute_all(["INFO","SEARCH"])
        failed = False
        for ix, r in enumerate(results):
            if r[name] != value:
                print(name, " Expected:", value, " Received:", r[name], " on server:", ix)
                failed = True
        assert not failed

    def check_info_sum(self, name: str, sum_value: int):
        '''Sum the values of a given info field across all servers'''
        results = self.execute_all(["INFO","SEARCH"])
        s = sum([int(r[name]) for r in results if name in r])
        assert s == sum_value, f"Expected sum of {name} to be {sum_value}, got {s}"


    def test_timeoutCME(self):
        self.execute_all(["flushall sync"])

        self.config_set("search.info-developer-visible", "yes")
        client: Valkey = self.new_cluster_client()
        self.check_info("search_cancel-timeouts", 0)

        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW"), Numeric("n")])
        flat_index = Index("flat", [Vector("v", 3, type="FLAT"), Numeric("n")])
       
        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 100)

        #
        # Nominal case
        #
        cluster = self.get_primary(0).get_new_client()
        # hnsw_result = search(cluster, "hnsw", False)
        # flat_result = search(cluster, "flat", False)

        self.check_info_sum("search_cancel-forced", 0)

        #assert hnsw_result[0] == 10
        #assert flat_result[0] == 10
        #
        # Now, force timeouts quickly
        #
        self.config_set("search.test-force-timeout", "yes")
        self.config_set("search.timeout-poll-frequency", "1")

        #
        # Enable timeout path, no error but message result
        #
        self.config_set("search.enable-partial-results", "no")

        #
        # Normal HNSW path
        #
        hnsw_result = search(client, "hnsw", True)

        self.check_info_sum("search_cancel-forced", 3)
        
        #
        # Pre-filtering HNSW path
        #
        self.check_info("search_query_prefiltering_requests_cnt", 0)
        hnsw_result = search(client, "hnsw", True, 10)
        self.check_info("search_query_prefiltering_requests_cnt", 1)
        self.check_info_sum("search_cancel-forced", 6)

        #
        # Flat path
        #
        flat_result = search(client, "flat", True)
        self.check_info_sum("search_cancel-forced", 9)
        self.check_info("search_query_prefiltering_requests_cnt", 1)
