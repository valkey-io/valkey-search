from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging, time
from typing import Any
from util import waiters

def num_docs(client: Valkey.client, index: str) -> dict[str, str]:
    res = client.execute_command("FT.INFO", index)
    print("Got info result of ", res)
    for i in range(len(res)):
        if res[i] == b'num_docs':
            print("Found ", res[i+1])
            return int(res[i+1].decode())
    assert False

class TestFlushAllCMD(ValkeySearchTestCaseBase):
    def test_flushallCMD(self):
        """
            Test CMD flushall logic
        """
        client: Valkey = self.server.get_new_client()
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
       
        hnsw_index.create(client)
        hnsw_index.load_data(client, 1000)
        assert 1000 == num_docs(client, hnsw_index.name)

        client.execute_command("FLUSHALL SYNC")

        assert client.execute_command("FT._LIST") == []

class TestFlushAllCME(ValkeySearchClusterTestCase):

    def sum_docs(self, index:str) -> int:
        return sum([num_docs(self.client_for_primary(i), index) for i in range(len(self.servers))])

    def test_flushallCME(self):
        """
            Test CMD flushall logic
        """
        client: Valkey = self.new_cluster_client()
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
        NUM_VECTORS = 100
       
        hnsw_index.create(client)
        hnsw_index.load_data(client, NUM_VECTORS)

        clients = [self.client_for_primary(i) for i in range(len(self.servers))]
        # Wait for all the docs to be indexed (up to 3 seconds)
        waiters.wait_for_equal(lambda: self.sum_docs(hnsw_index.name), NUM_VECTORS, timeout=3)
        for c in clients:
            c.execute_command("flushall sync")

        time.sleep(3)

        assert client.execute_command("FT._LIST") == [hnsw_index.name.encode()]
        assert 0 == self.sum_docs(hnsw_index.name)
