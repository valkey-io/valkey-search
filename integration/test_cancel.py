from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging

def print_result(name:str, result):
    print(">>>>>", name, "<<<<<")
    print("Count: ", result[0])
    for i in range(1,len(result)):
        print("Row ",i, result[i])

class TestCMDCancel(ValkeySearchTestCaseBase):

    def search(self, client: valkey.client, index:str) -> list[tuple[str, float]]:
        return client.execute_command(*["FT.SEARCH", index, "*=>[KNN 10 @v $BLOB]", "PARAMS", "2", "BLOB", float_to_bytes([0.0, 0.0, 0.0])])

    def test_timeout(self):
        """
            Test CMD timeout logic
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FLUSHALL SYNC")
        # po
        assert client.execute_command("CONFIG SET search.info-developer-visible yes") == b"OK"
        info = client.info("SEARCH")
        logging.info(f"INFO Result: {info}")
        assert(info["search_cancel-timeouts"] == 0)
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW")])
        flat_index = Index("flat", [Vector("v", 3, type="FLAT")])
       
        hnsw_index.create(client)
        flat_index.create(client)
        hnsw_index.load_data(client, 100)

        hnsw_full_result = self.search(client, "hnsw")
        flat_full_result = self.search(client, "flat")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 0
        assert client.info("SEARCH")["search_QueryTimeouts"] == 0

        # assert client.execute_command("CONFIG SET search.enable-partial-results yes") == b"OK"

        client.execute_command("CONFIG SET search.debug-force-timeout yes") == b"OK"
        client.execute_command("CONFIG SET search.timeout-poll-frequency 5") == b"OK"
        client.execute_command("CONFIG SEt search.enable-partial-results no") == b"OK"

        hnsw_partial_result = self.search(client, "hnsw")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 1
        assert client.info("SEARCH")["search_QueryTimeouts"] == 1

        assert hnsw_partial_result == b"Request timed out"

        flat_partial_result = self.search(client, "flat")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 2
        assert client.info("SEARCH")["search_QueryTimeouts"] == 2

        assert flat_partial_result == b"Request timed out"



