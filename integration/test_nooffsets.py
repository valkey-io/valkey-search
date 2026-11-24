"""Word-position queries on an index created with NOOFFSETS (issue #1638)."""

import struct

import pytest
from valkey.client import Valkey
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from utils import IndexingTestHelper

RELEASES = ("1.0.0", "1.3.0")
DOCS = (
    ("d:1", "hello world"),
    ("d:2", "world hello"),
    ("d:3", "hello big wide world"),
    ("d:4", "worlds apart"),
)
ALL3 = {"d:1", "d:2", "d:3"}


def _vec(*values):
    return struct.pack(f"<{len(values)}f", *values)


Q = _vec(0.0, 0.0)

# (command, query, flags, keys RediSearch returns without offsets, with offsets).
# Without offsets RediSearch matches a phrase's words in any order and at any
# distance, and ignores INORDER and SLOP.
CASES = (
    ("FT.SEARCH", '@s:"world hello"', (), ALL3, {"d:2"}),
    ("FT.SEARCH", '@s:"hello wide"', (), {"d:3"}, set()),
    ("FT.SEARCH", '@s:"world apart"', (), set(), set()),
    ("FT.SEARCH", '@s:"hello"', (), ALL3, ALL3),
    ("FT.SEARCH", '"world hello" big', (), {"d:3"}, set()),
    ("FT.SEARCH", '"world hello" | apart', (), ALL3 | {"d:4"}, {"d:2", "d:4"}),
    ("FT.SEARCH", '-"world hello"', (), {"d:4"}, {"d:1", "d:3", "d:4"}),
    ("FT.SEARCH", '@s:"worlds apart"', ("VERBATIM",), {"d:4"}, {"d:4"}),
    ("FT.SEARCH", '@s:"world hello"', ("INORDER",), ALL3, {"d:2"}),
    ("FT.SEARCH", "@s:(world hello)", ("INORDER",), ALL3, {"d:2"}),
    ("FT.SEARCH", "@s:(hello wide)", ("SLOP", "0"), {"d:3"}, set()),
    ("FT.SEARCH", "@s:(world hello)", ("INORDER", "SLOP", "0"), ALL3, {"d:2"}),
    ("FT.SEARCH", "@s:hello", ("INORDER",), ALL3, ALL3),
    ("FT.SEARCH", "*", ("INORDER",), ALL3 | {"d:4"}, ALL3 | {"d:4"}),
    ("FT.SEARCH", "(@s:(world hello))=>[KNN 2 @v $q]", ("INORDER",),
     {"d:1", "d:2"}, {"d:2"}),
    ("FT.AGGREGATE", '@s:"world hello"', (), ALL3, {"d:2"}),
    ("FT.AGGREGATE", '@s:"hello wide"', (), {"d:3"}, set()),
    # The vector arm adds d:1, the nearest document.
    ("FT.HYBRID", '@s:"world hello"', (), ALL3, {"d:1", "d:2"}),
    ("FT.HYBRID", '@s:"hello wide"', (), {"d:1", "d:3"}, {"d:1"}),
)


def _command(command, query, flags):
    if command == "FT.SEARCH":
        return ["FT.SEARCH", "idx", query, *flags, "NOCONTENT",
                "PARAMS", "2", "q", Q, "DIALECT", "2"]
    if command == "FT.AGGREGATE":
        return ["FT.AGGREGATE", "idx", query, *flags, "LOAD", "1", "@__key",
                "DIALECT", "2"]
    return ["FT.HYBRID", "idx", "SEARCH", query,
            "VSIM", "@v", "$q", "KNN", "2", "K", "1",
            "LIMIT", "0", "20", "PARAMS", "2", "q", Q]


def _keys(command, reply):
    if command == "FT.SEARCH":
        return {key.decode() for key in reply[1:]}
    return {dict(zip(row[::2], row[1::2]))[b"__key"].decode()
            for row in reply[1:]}


class _NoOffsetsTests:
    """Shared body. Subclasses provide _client() and _primaries()."""

    def _populate(self, client: Valkey, *create_args):
        for primary in self._primaries():
            primary.execute_command("FLUSHALL", "SYNC")
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", *create_args,
            "SCHEMA", "s", "TEXT",
            "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2",
        ) == b"OK"
        writer = self._writer()
        for i, (key, text) in enumerate(DOCS):
            writer.execute_command("HSET", key, "s", text, "v", _vec(float(i), 0.0))
        for primary in self._primaries():
            IndexingTestHelper.wait_for_indexing_complete_on_node(primary, "idx")

    def _check(self, create_args, column, releases):
        client = self._client()
        self._populate(client, *create_args)
        for release in releases:
            for primary in self._primaries():
                assert primary.execute_command(
                    "CONFIG", "SET", "search.emulate-release", release) == b"OK"
            for case in CASES:
                command, query, flags = case[:3]
                reply = client.execute_command(*_command(command, query, flags))
                assert _keys(command, reply) == case[column], (release, case)

    def test_position_queries_without_offsets(self):
        self._check(("NOOFFSETS",), 3, RELEASES)

    def test_position_queries_with_offsets_unchanged(self):
        self._check((), 4, RELEASES)

    @pytest.mark.parametrize("release", RELEASES)
    def test_apostrophe_phrase_without_offsets(self, release):
        """From 1.3.0 an apostrophe pair is a phrase; below it, punctuation.
        Either way the reply without offsets is the same."""
        client = self._client()
        self._populate(client, "NOOFFSETS")
        for primary in self._primaries():
            primary.execute_command("CONFIG", "SET", "search.emulate-release", release)
        reply = client.execute_command(
            "FT.SEARCH", "idx", "@s:'world hello'", "NOCONTENT")
        assert _keys("FT.SEARCH", reply) == ALL3


WARNING = "has no word offsets"


class TestNoOffsetsWarning(ValkeySearchTestCaseBase):
    """Each test starts a new server, so the rate limit has not been used."""

    def _query(self, create_args, *command):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", *create_args,
            "SCHEMA", "s", "TEXT") == b"OK"
        client.execute_command("HSET", "d:1", "s", "hello world")
        IndexingTestHelper.wait_for_indexing_complete_on_node(client, "idx")
        client.execute_command(*command)
        return self.rg.primary.does_logfile_contains(WARNING)

    @pytest.mark.parametrize("command", (
        ("FT.SEARCH", "idx", '@s:"world hello"'),
        ("FT.SEARCH", "idx", "@s:(world hello)", "INORDER"),
        ("FT.SEARCH", "idx", "@s:(world hello)", "SLOP", "0"),
        ("FT.AGGREGATE", "idx", "@s:(world hello)", "INORDER",
         "LOAD", "1", "@__key"),
    ))
    def test_logged_without_offsets(self, command):
        assert self._query(("NOOFFSETS",), *command)

    @pytest.mark.parametrize("create_args, command", (
        (("NOOFFSETS",), ("FT.SEARCH", "idx", "@s:(world hello)")),
        (("NOOFFSETS",), ("FT.SEARCH", "idx", '@s:"hello"')),
        ((), ("FT.SEARCH", "idx", '@s:"world hello"', "INORDER")),
    ))
    def test_not_logged_when_nothing_is_dropped(self, create_args, command):
        assert not self._query(create_args, *command)


class TestNoOffsets(_NoOffsetsTests, ValkeySearchTestCaseBase):
    def _client(self) -> Valkey:
        return self.server.get_new_client()

    def _writer(self):
        return self._client()

    def _primaries(self):
        return [self._client()]


class TestNoOffsetsCluster(_NoOffsetsTests, ValkeySearchClusterTestCase):
    def _client(self) -> Valkey:
        return self.new_client_for_primary(0)

    def _writer(self):
        return self.new_cluster_client()

    def _primaries(self):
        return self.get_all_primary_clients()
