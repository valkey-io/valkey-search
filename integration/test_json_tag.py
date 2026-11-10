import struct

from valkey.client import Valkey
from valkey_search_test_case import (ValkeySearchTestCaseBase,
                                     ValkeySearchTestCaseDebugMode)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper, run_in_thread


class TestJsonTag(ValkeySearchTestCaseBase):

    def set_release(self, client, release):
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", release) == b"OK"

    def create(self, client, *args, tag_args=()):
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "d:", *args,
            "SCHEMA", "$.t", "AS", "t", "TAG", *tag_args,
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

    def test_invalid_value_drops_key_from_1_3_0(self):
        client: Valkey = self.server.get_new_client()
        self.set_release(client, "1.3.0")
        self.create(client)
        client.execute_command("JSON.SET", "d:1", "$", '{"t":"a","v":[0,0]}')
        client.execute_command("JSON.SET", "d:2", "$", '{"t":5,"v":[0,0]}')
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        knn = "*=>[KNN 10 @v $q]"
        params = ("PARAMS", "2", "q", struct.pack("<2f", 0, 0))
        res = client.execute_command(
            "FT.SEARCH", "idx", knn, *params, "NOCONTENT", "DIALECT", "2")
        assert set(res[1:]) == {b"d:1"}

        client.execute_command("JSON.SET", "d:2", "$.t", '"a"')
        client.execute_command("JSON.SET", "d:1", "$.t", '[true]')
        res = client.execute_command(
            "FT.SEARCH", "idx", knn, *params, "NOCONTENT", "DIALECT", "2")
        assert set(res[1:]) == {b"d:2"}

    def separator(self, client):
        attrs = client.execute_command("FT.INFO", "idx")
        attrs = attrs[attrs.index(b"attributes") + 1]
        tag = next(a for a in attrs if b"TAG" in a)
        return tag[tag.index(b"SEPARATOR") + 1]

    def test_no_split_without_separator_from_1_3_0(self):
        client: Valkey = self.server.get_new_client()
        self.set_release(client, "1.3.0")
        self.create(client)
        client.execute_command("JSON.SET", "d:1", "$", '{"t":"a,b"}')
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        assert self.separator(client) == b""
        assert self.keys(client, "@t:{a}") == set()
        assert self.keys(client, r"@t:{a\,b}") == {b"d:1"}

        client.execute_command("FT.DROPINDEX", "idx")
        self.create(client, tag_args=("SEPARATOR", ","))
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        assert self.separator(client) == b","
        assert self.keys(client, "@t:{a}") == {b"d:1"}


class TestJsonTagRevalidation(ValkeySearchTestCaseDebugMode):

    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.writer-threads"] = "2"
        args["search.reader-threads"] = "4"
        return args

    def run_across_mutation(self, client, query, mutation):
        """The result of `query`, parked behind `mutation` until it applies."""
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        mut_thread, _, mut_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(*mutation))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing") >= 1,
            timeout=5)
        blocked = client.info("SEARCH").get(
            "search_text_query_blocked_count", 0)
        q_thread, res, err = run_in_thread(query)
        # FT.HYBRID waits for the mutation; a tag-only FT.SEARCH does not.
        waiters.wait_for_true(
            lambda: not q_thread.is_alive()
            or client.info("SEARCH")["search_text_query_blocked_count"]
            >= blocked + 1,
            timeout=5)
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        mut_thread.join()
        q_thread.join()
        assert mut_err[0] is None
        assert err[0] is None
        return res[0]

    def test_revalidation_decodes_array(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", "1.3.0") == b"OK"
        client.execute_command(
            "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "d:",
            "SCHEMA", "$.t", "AS", "t", "TAG",
            "$.v", "AS", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32",
            "DIM", "2", "DISTANCE_METRIC", "L2")
        client.execute_command("JSON.SET", "d:1", "$", '{"t":[],"v":[9,9]}')
        client.execute_command("JSON.SET", "d:2", "$", '{"t":["x"],"v":[0,0]}')

        def search():
            res = self.server.get_new_client().execute_command(
                "FT.SEARCH", "idx", "@t:{x}")
            return set(res[1::2])

        def hybrid():
            # The VSIM arm finds d:2 only, so d:1 can come from SEARCH alone.
            res = self.server.get_new_client().execute_command(
                "FT.HYBRID", "idx", "SEARCH", "@t:{x}",
                "VSIM", "@v", "$q", "KNN", "2", "K", "1",
                "PARAMS", "2", "q", struct.pack("<2f", 0, 0))
            return {rec[rec.index(b"__key") + 1] for rec in res[1:]}

        for query in (search, hybrid):
            client.execute_command("JSON.SET", "d:1", "$.t", '["x","y"]')
            IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
            assert self.run_across_mutation(
                client, query, ("JSON.SET", "d:1", "$.t", '["z","x"]')
            ) == {b"d:1", b"d:2"}
            assert self.run_across_mutation(
                client, query, ("JSON.SET", "d:1", "$.t", '["z"]')
            ) == {b"d:2"}
