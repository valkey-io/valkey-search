"""FT.AGGREGATE honours VERBATIM, INORDER and SLOP (issue #1565 item 1)."""

import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper

FIX_RELEASE = "1.3.0"
LEGACY_RELEASE = "1.0.0"


def _vec(*values):
    return struct.pack(f"<{len(values)}f", *values)


def agg_keys(reply):
    keys = set()
    for row in reply[1:]:
        fields = dict(zip(row[::2], row[1::2]))
        keys.add(fields[b"__key"].decode())
    return keys


def search_keys(reply):
    return {key.decode() for key in reply[1:]}


class TestAggregateTextFlags(ValkeySearchTestCaseBase):
    DOCS = (
        ("d:1", "hello world"),
        ("d:2", "world hello"),
        ("d:3", "hello big wide world"),
        ("d:4", "worlds apart"),
    )
    # (query, flag arguments, keys RediSearch returns).
    CASES = (
        ("@s:worlds", ("VERBATIM",), {"d:4"}),
        ("@s:(world hello)", ("INORDER",), {"d:2"}),
        ("@s:(hello world)", ("SLOP", "0"), {"d:1", "d:2"}),
        ("@s:(hello wide)", ("SLOP", "0"), set()),
        ("@s:(hello wide)", ("SLOP", "1"), {"d:3"}),
        ("@s:(world hello)", ("SLOP", "0", "INORDER"), {"d:2"}),
    )

    def _client(self, release=LEGACY_RELEASE) -> Valkey:
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", release) == b"OK"
        return client

    def _populate(self, client: Valkey, *create_args):
        client.execute_command("FLUSHALL", "SYNC")
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", *create_args,
            "SCHEMA", "s", "TEXT",
            "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2",
        ) == b"OK"
        for i, (key, text) in enumerate(self.DOCS):
            client.execute_command("HSET", key, "s", text, "v", _vec(float(i), 0.0))
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")

    def test_flags_match_ft_search(self):
        for release in (LEGACY_RELEASE, FIX_RELEASE):
            client = self._client(release)
            self._populate(client)
            for query, flags, expected in self.CASES:
                requests = client.info("SEARCH")[
                    "search_nonvector_requests_count"]
                rows = agg_keys(client.execute_command(
                    "FT.AGGREGATE", "idx", query, *flags, "LOAD", "1", "@__key"))
                assert rows == expected, (release, query, flags)
                assert rows == search_keys(client.execute_command(
                    "FT.SEARCH", "idx", query, *flags, "NOCONTENT")), (
                        release, query, flags)
                # One for the aggregate, one for the search.
                assert client.info("SEARCH")[
                    "search_nonvector_requests_count"] == requests + 2

    def test_flag_applies_without_load(self):
        client = self._client()
        self._populate(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "@s:(world hello)", "INORDER",
            "GROUPBY", "0", "REDUCE", "COUNT", "0", "AS", "c")
        assert reply[1] == [b"c", b"1"]

    def test_reparse_counts_compatibility_once(self):
        client = self._client(LEGACY_RELEASE)
        self._populate(client)
        counters = ("search_compatibility-filter_parser_invalid_utf8_expression",
                    "search_compatibility-ft_search_apostrophe_phrase")
        query = b"@s:(hello\xff world's)"
        for flags in ((), ("VERBATIM",), ("INORDER",), ("SLOP", "1")):
            before = client.info("SEARCH")
            client.execute_command(
                "FT.AGGREGATE", "idx", query, *flags, "LOAD", "1", "@__key")
            after = client.info("SEARCH")
            deltas = [after.get(c, 0) - before.get(c, 0) for c in counters]
            if not flags:
                expected = deltas
                assert deltas[0] == 1 and deltas[1] >= 1, deltas
            assert deltas == expected, (flags, deltas)

    def test_inorder_with_knn(self):
        client = self._client()
        self._populate(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "@s:(world hello)=>[KNN 4 @v $q]", "INORDER",
            "LOAD", "2", "@__key", "@__v_score",
            "PARAMS", "2", "q", _vec(0.0, 0.0), "DIALECT", "2")
        assert agg_keys(reply) == {"d:2"}
        assert b"__v_score" in reply[1]

    def test_inorder_with_vector_range(self):
        client = self._client()
        self._populate(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx",
            "@s:(world hello) @v:[VECTOR_RANGE 100 $q]=>{$yield_distance_as: dist}",
            "INORDER", "LOAD", "2", "@__key", "@dist",
            "PARAMS", "2", "q", _vec(0.0, 0.0), "DIALECT", "2")
        assert agg_keys(reply) == {"d:2"}
        assert dict(zip(reply[1][::2], reply[1][1::2]))[b"dist"] == b"1"

    def test_inorder_and_slop_ignored_without_offsets(self):
        """Without offsets INORDER and SLOP are accepted but have no effect."""
        for release in (LEGACY_RELEASE, FIX_RELEASE):
            client = self._client(release)
            self._populate(client, "NOOFFSETS")
            for query, flags, expected in (
                ("@s:(hello world)", ("INORDER",), {"d:1", "d:2", "d:3"}),
                ("@s:(hello world)", ("SLOP", "1"), {"d:1", "d:2", "d:3"}),
                ("@s:(hello world)", ("INORDER", "SLOP", "1"),
                 {"d:1", "d:2", "d:3"}),
                ("@s:worlds", ("VERBATIM", "INORDER"), {"d:4"}),
                ("@s:worlds", ("VERBATIM",), {"d:4"}),
            ):
                assert agg_keys(client.execute_command(
                    "FT.AGGREGATE", "idx", query, *flags,
                    "LOAD", "1", "@__key")) == expected, (release, flags)


class TestAggregateTextFlagsMetrics(ValkeySearchTestCaseDebugMode):
    def test_operation_counters_follow_the_reparsed_tree(self):
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA", "s", "TEXT",
            "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2")
        client.execute_command("HSET", "d:1", "s", "hello world")
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        before = client.info("SEARCH")
        with pytest.raises(ResponseError, match="blob size"):
            client.execute_command(
                "FT.AGGREGATE", "idx", "@s:hello=>[KNN 1 @v $q]",
                "PARAMS", "2", "q", _vec(0.0, 0.0, 0.0), "DIALECT", "2")
        after = client.info("SEARCH")
        assert after.get("search_text_requests_count", 0) == before.get(
            "search_text_requests_count", 0)
        for release in (FIX_RELEASE, LEGACY_RELEASE):
            assert client.execute_command(
                "CONFIG", "SET", "search.emulate-release", release) == b"OK"
            before = client.info("SEARCH")
            client.execute_command(
                "FT.AGGREGATE", "idx", "@s:(hello world)", "INORDER",
                "LOAD", "1", "@__key")
            after = client.info("SEARCH")
            for name, delta in (
                ("search_text_requests_count", 1),
                ("search_query_text_term_count", 1),
                ("search_query_text_proximity_count", 1),
            ):
                assert after.get(name, 0) - before.get(name, 0) == delta, (
                    release, name)
