from ft_info_parser import FTInfoParser
from indexes import *
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from util import waiters
from valkeytestframework.conftest import resource_port_tracker


class TestExpired(ValkeySearchTestCaseBase):
    def test_expired_hashes_should_remove_from_index(self):
        """
        Test CMD flushall logic
        """
        index_name = "my_index"
        client: Valkey = self.server.get_new_client()
        hnsw_index = Index(
            index_name, [Vector("v", 3, type="HNSW", m=2, efc=1), Numeric("n")]
        )

        num_of_docs = 500
        hnsw_index.create(client)
        hnsw_index.load_data(client, num_of_docs)
        ft_info = hnsw_index.info(client)
        assert num_of_docs == ft_info.num_docs

        for i in range(0, num_of_docs):
            # expire half the keys
            if i % 2:
                client.pexpire(hnsw_index.keyname(i), 1)  # 1 ms

        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            num_of_docs / 2,
            timeout=5,
        )
