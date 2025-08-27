from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker
from indexes import *
from util import waiters


class TestCopy(ValkeySearchTestCaseBase):
    def test_copyCMD(self):
        """
        Test CMD copy logic
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
            # copy half the keys
            if i % 2:
                client.copy(hnsw_index.keyname(i), hnsw_index.keyname(i + num_of_docs))

        waiters.wait_for_equal(
            lambda: hnsw_index.info(client).num_docs,
            int(num_of_docs * 1.5),
            timeout=5,
        )

        assert client.execute_command("FT._LIST") == [hnsw_index.name.encode()]
