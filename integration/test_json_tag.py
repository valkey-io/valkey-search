import struct

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper


class TestJsonTag(ValkeySearchTestCaseBase):

    def set_release(self, client, release):
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", release) == b"OK"

    def create(self, client, *args):
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "d:", *args,
            "SCHEMA", "$.t", "AS", "t", "TAG",
            "$.v", "AS", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2") == b"OK"

    def keys(self, client, query):
        res = client.execute_command(
            "FT.SEARCH", "idx", query, "NOCONTENT", "DIALECT", "2")
        return set(res[1:])

    def test_array_indexed_per_element_from_1_3_0(self):
        client: Valkey = self.server.get_new_client()
        self.set_release(client, "1.2.0")
        self.create(client)
        client.execute_command("JSON.SET", "d:1", "$", '{"t":["x","y"]}')
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        assert self.keys(client, "@t:{x}") == set()

        self.set_release(client, "1.3.0")
        client.execute_command("JSON.SET", "d:2", "$", '{"t":["x","y"]}')
        assert self.keys(client, "@t:{x}") == {b"d:2"}
        client.execute_command("JSON.SET", "d:1", "$.t", '["y"]')
        assert self.keys(client, "@t:{y}") == {b"d:1", b"d:2"}

    def test_filter_and_knn_return(self):
        client: Valkey = self.server.get_new_client()
        self.create(client, "FILTER", "@t == 'a'")
        client.execute_command("JSON.SET", "d:1", "$", '{"t":"a","v":[0,0]}')
        client.execute_command("JSON.SET", "d:2", "$", '{"t":"b","v":[0,0]}')
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        assert self.keys(client, "@t:{a | b}") == {b"d:1"}

        res = client.execute_command(
            "FT.SEARCH", "idx", "*=>[KNN 1 @v $q]",
            "PARAMS", "2", "q", struct.pack("<2f", 0, 0),
            "RETURN", "1", "t", "DIALECT", "2")
        assert res == [1, b"d:1", [b"t", b"a"]]
