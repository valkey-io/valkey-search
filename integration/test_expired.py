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

    def test_hash_field_expiration_should_update_index(self):
        """
        Test that HEXPIRE on indexed fields triggers keyspace notification
        and updates the index correctly (Hash Field Expiration - HFE)
        """
        index_name = "my_index"
        key_name = "test_key"
        client: Valkey = self.server.get_new_client()
        index = Index(index_name, [Tag("t"), Numeric("n")])

        index.create(client)
        client.hset(key_name, mapping={"t": "mytag", "n": "1"})
        assert index.info(client).num_docs == 1

        result = client.execute_command("FT.SEARCH", index_name, "@t:{mytag}")
        assert result[0] == 1

        # Expire the indexed tag field using HEXPIRE
        client.execute_command("HEXPIRE", key_name, 1, "FIELDS", 1, "t")

        waiters.wait_for_equal(
            lambda: client.execute_command("FT.SEARCH", index_name, "@t:{mytag}")[0],
            0,
            timeout=5,
        )
        assert index.info(client).num_docs == 1
