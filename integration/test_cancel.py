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

def canceller(client, client_id):
    my_id = client.execute_command("client id")
    assert my_id != client_id
    client.execute_command("client kill id ", client_id)

class TestCancel(ValkeySearchTestCaseBase):

    def search_command(self, index:str) -> list[str]:
        return ["FT.SEARCH", index, "*=>[KNN 10 @v $BLOB]", "PARAMS", "2", "BLOB", float_to_bytes([0.0, 0.0, 0.0])]
    
    def search(self, client: valkey.client, index:str) -> list[tuple[str, float]]:
        return client.execute_command(*self.search_command(index))

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

        #
        # Nominal case
        #
        hnsw_result = self.search(client, "hnsw")
        flat_result = self.search(client, "flat")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 0
        assert client.info("SEARCH")["search_QueryTimeouts"] == 0

        assert hnsw_result[0] == 10
        assert flat_result[0] == 10

        #
        # Now, force timeouts quickly
        #
        client.execute_command("CONFIG SET search.test-force-timeout yes") == b"OK"
        client.execute_command("CONFIG SET search.timeout-poll-frequency 5") == b"OK"

        #
        # Enable timeout path, no error but message result
        #
        client.execute_command("CONFIG SET search.enable-partial-results no") == b"OK"

        hnsw_result = self.search(client, "hnsw")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 1
        assert client.info("SEARCH")["search_QueryTimeouts"] == 1

        assert hnsw_result == b"Request timed out"

        flat_result = self.search(client, "flat")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 2
        assert client.info("SEARCH")["search_QueryTimeouts"] == 2

        assert flat_result == b"Request timed out"

        #
        # Enable partial results
        #

        assert client.execute_command("CONFIG SET search.enable-partial-results yes") == b"OK"

        hnsw_result = self.search(client, "hnsw")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 3
        assert client.info("SEARCH")["search_QueryTimeouts"] == 2

        assert hnsw_result[0] == 10

        flat_result = self.search(client, "flat")

        assert client.info("SEARCH")["search_cancel-timeouts"] == 4
        assert client.info("SEARCH")["search_QueryTimeouts"] == 2

        assert flat_result[0] == 10

