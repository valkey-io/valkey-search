from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
import logging, time
from typing import Any
from util import waiters
import pytest

class TestFlushAllCMD(ValkeySearchTestCaseBase):
    def test_flushallCMD(self):
        """
            Test CMD flushall logic
        """
        client: Valkey = self.server.get_new_client()
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
       
        hnsw_index.create(client)
        hnsw_index.load_data(client, 1000)
        assert 1000 == hnsw_index.info(client).num_docs

        client.execute_command("FLUSHALL SYNC")

        assert client.execute_command("FT._LIST") == []

@pytest.mark.skip(reason="temporary skipping cluster tests")
class TestFlushAllCME(ValkeySearchClusterTestCase):

    def sum_docs(self, index: Index) -> int:
        return sum([index.info(self.client_for_primary(i)).num_docs for i in range(len(self.replication_groups))])

    def test_flushallCME(self):
        """
            Test CMD flushall logic
        """
        client: Valkey = self.new_cluster_client()
        hnsw_index = Index("hnsw", [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")])
        NUM_VECTORS = 100
       
        hnsw_index.create(client)
        hnsw_index.load_data(client, NUM_VECTORS)

        clients = [self.client_for_primary(i) for i in range(len(self.replication_groups))]
        # Wait for all the docs to be indexed (up to 3 seconds)
        waiters.wait_for_equal(lambda: self.sum_docs(hnsw_index), NUM_VECTORS, timeout=5)
        for c in clients:
            c.execute_command("flushall sync")

        assert client.execute_command("FT._LIST") == [hnsw_index.name.encode()]
        waiters.wait_for_equal(lambda: self.sum_docs(hnsw_index), 0, timeout=5)
