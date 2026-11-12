"""A LOAD clause after GROUPBY: each record is then a group, not a document,
so there is nothing to load. FT.AGGREGATE rejects it from 1.3.0 on (see
COMPATIBILITY.md) and silently ignores it below, except a field outside the
schema, which was always rejected unless the command also has `LOAD *`.
FT.HYBRID ignores it at every release, as RediSearch does."""

import struct

import pytest
import valkey
from utils import IndexingTestHelper
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

COUNTER = "search_compatibility-ft_aggregate_load_after_groupby"
GROUPBY = ["GROUPBY", "1", "@g", "REDUCE", "COUNT", "0", "AS", "c"]


class TestAggregateLoadAfterGroupby(ValkeySearchTestCaseDebugMode):
    def _client(self, release) -> Valkey:
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", release) == b"OK"
        if not client.execute_command("FT._LIST"):
            client.execute_command(
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
                "SCHEMA", "g", "TAG", "n", "NUMERIC",
            )
            client.execute_command("HSET", "d:1", "g", "a", "n", "1", "extra", "e1")
            client.execute_command("HSET", "d:2", "g", "b", "n", "2", "extra", "e2")
        return client

    def _ticks(self, client, *args):
        before = client.info("SEARCH").get(COUNTER, 0)
        try:
            reply = client.execute_command("FT.AGGREGATE", "idx", "*", *args)
        except valkey.exceptions.ResponseError as e:
            reply = str(e)
        return reply, client.info("SEARCH").get(COUNTER, 0) - before

    def test_non_schema_field_is_rejected_at_every_release(self):
        for release in ("1.0.0", "1.3.0"):
            client = self._client(release)
            assert self._ticks(client, *GROUPBY, "LOAD", "1", "@extra") == (
                "Index field `extra` does not exist", 0)
            reply, ticks = self._ticks(
                client, *GROUPBY, "LOAD", "1", "@extra",
                "APPLY", "upper(@extra)", "AS", "u")
            assert "Index field `extra` does not exist" in reply and ticks == 0

    def test_schema_field_is_ignored_below_1_3_0(self):
        client = self._client("1.0.0")
        reply, ticks = self._ticks(
            client, *GROUPBY, "LOAD", "2", "@n", "@g", "SORTBY", "2", "@g", "ASC")
        assert reply == [2, [b"g", b"a", b"c", b"1"], [b"g", b"b", b"c", b"1"]]
        assert ticks == 1

    def test_schema_field_is_rejected_from_1_3_0(self):
        client = self._client("1.3.0")
        assert self._ticks(client, *GROUPBY, "LOAD", "2", "@n", "@g") == (
            "LOAD cannot be applied after GROUPBY", 0)

    def test_loadall_and_key_follow_the_schema_field_rule(self):
        late_loads = [
            [*GROUPBY, "LOAD", "*"],
            ["LOAD", "*", *GROUPBY, "LOAD", "1", "@n"],
            ["LOAD", "*", *GROUPBY, "LOAD", "1", "@extra"],
            [*GROUPBY, "LOAD", "1", "@__key"],
            [*GROUPBY, "LOAD", "*", "LOAD", "1", "@n"],
        ]
        sortby = ["SORTBY", "2", "@g", "ASC"]
        groups = [2, [b"g", b"a", b"c", b"1"], [b"g", b"b", b"c", b"1"]]
        for late in late_loads:
            client = self._client("1.0.0")
            assert self._ticks(client, *late, *sortby) == (groups, 1), late
            client = self._client("1.3.0")
            assert self._ticks(client, *late, *sortby) == (
                "LOAD cannot be applied after GROUPBY", 0), late

    def test_ignored_late_load_keeps_the_groupby_key_on_json(self):
        client = self._client("1.0.0")
        client.execute_command(
            "FT.CREATE", "jidx", "ON", "JSON", "PREFIX", "1", "j:",
            "SCHEMA", "$.g", "AS", "g", "TAG")
        client.execute_command("JSON.SET", "j:1", "$", '{"g":"a"}')
        client.execute_command("JSON.SET", "j:2", "$", '{"g":"b"}')
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "jidx")
        reply = client.execute_command(
            "FT.AGGREGATE", "jidx", "*", "LOAD", "*", *GROUPBY,
            "LOAD", "1", "@g", "SORTBY", "2", "@g", "ASC")
        assert [row[1::2] for row in reply[1:]] == [[b"a", b"1"], [b"b", b"1"]]

    def test_counter_ticks_once_per_command_across_cursor_pages(self):
        client = self._client("1.0.0")
        before = client.info("SEARCH").get(COUNTER, 0)
        reply, cursor = client.execute_command(
            "FT.AGGREGATE", "idx", "*", *GROUPBY, "LOAD", "1", "@n",
            "WITHCURSOR", "COUNT", "1")
        pages = 1
        while cursor:
            reply, cursor = client.execute_command(
                "FT.CURSOR", "READ", "idx", cursor)
            pages += 1
        assert pages > 1
        assert client.info("SEARCH").get(COUNTER, 0) - before == 1

    def test_hybrid_ignores_a_load_after_groupby_at_every_release(self):
        client = self._client("1.0.0")
        client.execute_command(
            "FT.CREATE", "h", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
            "title", "TEXT", "price", "NUMERIC", "color", "TAG",
            "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2")
        for i in range(4):
            client.execute_command(
                "HSET", f"doc:{i}", "title", "alpha", "price", str(i),
                "color", "red" if i < 2 else "blue", "extra", f"e{i}",
                "vec", struct.pack("<2f", float(i), 0.0))
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "h")

        def hybrid(*tail):
            return client.execute_command(
                "FT.HYBRID", "h", "SEARCH", "@title:alpha",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
                "GROUPBY", "1", "@color", "REDUCE", "COUNT", "0", "AS", "cnt",
                *tail, "SORTBY", "2", "@color", "ASC",
                "PARAMS", "2", "q", struct.pack("<2f", 0.0, 0.0))

        for release in ("1.0.0", "1.3.0"):
            client.execute_command(
                "CONFIG", "SET", "search.emulate-release", release)
            expected = hybrid()
            for late in (["LOAD", "1", "@price"], ["LOAD", "1", "@extra"],
                         ["LOAD", "*"], ["LOAD", "1", "@__key"]):
                before = client.info("SEARCH").get(COUNTER, 0)
                assert hybrid(*late) == expected, (release, late)
                assert client.info("SEARCH").get(COUNTER, 0) == before

    def test_late_rename_gets_the_late_load_error_not_a_name_error(self):
        client = self._client("1.3.0")
        assert self._ticks(client, "LOAD", "3", "@n", "AS", "x", *GROUPBY,
                           "LOAD", "3", "@g", "AS", "x") == (
            "LOAD cannot be applied after GROUPBY", 0)

    def test_hybrid_ignores_a_late_rename_onto_an_earlier_name(self):
        client = self._client("1.3.0")
        client.execute_command(
            "FT.CREATE", "hl", "ON", "HASH", "PREFIX", "1", "hl:", "SCHEMA",
            "title", "TEXT", "g", "TAG", "n", "NUMERIC",
            "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2")
        client.execute_command(
            "HSET", "hl:1", "title", "alpha", "g", "a", "n", "1",
            "vec", struct.pack("<2f", 1.0, 0.0))
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "hl")
        reply = client.execute_command(
            "FT.HYBRID", "hl", "SEARCH", "@title:alpha",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
            "LOAD", "3", "@n", "AS", "x", *GROUPBY, "LOAD", "3", "@g", "AS", "x",
            "PARAMS", "2", "q", struct.pack("<2f", 0.0, 0.0))
        assert reply[0] == 1 and dict(zip(reply[1][::2], reply[1][1::2]))[b"g"] == b"a"
        assert b"x" not in reply[1][::2]

    def test_hybrid_late_load_does_not_make_an_earlier_name_an_error(self):
        """An ignored late LOAD of an undeclared field loads nothing, so an
        ambiguous earlier name keeps its reply instead of being rejected."""
        client = self._client("1.3.0")
        client.execute_command(
            "FT.CREATE", "hd", "ON", "HASH", "PREFIX", "1", "hd:", "SCHEMA",
            "title", "TEXT", "g", "TAG", "n", "NUMERIC",
            "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2")
        client.execute_command(
            "HSET", "hd:1", "title", "alpha", "g", "a", "n", "1", "extra", "e",
            "vec", struct.pack("<2f", 1.0, 0.0))
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "hd")

        def hybrid(*late):
            return client.execute_command(
                "FT.HYBRID", "hd", "SEARCH", "@title:alpha",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
                "LOAD", "3", "@n", "AS", "q", "LOAD", "3", "@g", "AS", "q",
                "GROUPBY", "1", "@g", "REDUCE", "COUNT", "0", "AS", "c", *late,
                "PARAMS", "2", "q", struct.pack("<2f", 0.0, 0.0))

        assert hybrid("LOAD", "1", "@extra") == hybrid()

    def test_load_after_other_stages_still_loads(self):
        client = self._client("1.3.0")
        reply, ticks = self._ticks(
            client, "APPLY", "1", "AS", "y", "LOAD", "2", "@n", "@extra",
            "SORTBY", "2", "@n", "ASC")
        assert [dict(zip(r[::2], r[1::2])) for r in reply[1:]] == [
            {b"y": b"1", b"n": b"1", b"extra": b"e1"},
            {b"y": b"1", b"n": b"2", b"extra": b"e2"},
        ] and ticks == 0
