"""`LOAD *` together with named loads. From 1.3.0 every stored field appears
under its own name and a named load adds a column. Below 1.3.0 the reply is
1.2.1's: `LOAD *` alone. There `AS` is read as a field name, so a rename is
counted by the LOAD AS counter (see COMPATIBILITY.md)."""

import struct

import pytest
import valkey

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper

COUNTER = "search_compatibility-ft_aggregate_loadall_named_loads"
LOAD_AS_COUNTER = "search_compatibility-ft_aggregate_load_as"
SORTBY = ["SORTBY", "2", "@n", "ASC"]

# (LOAD clauses, rows below 1.3.0, rows from 1.3.0). Field order within a
# row is not compared (#1625).
CASES = [
    (["LOAD", "*", "LOAD", "3", "@extra", "AS", "x"],
     [{"n": "1", "g": "a", "extra": "e1"}, {"n": "2", "g": "b", "extra": "e2"}],
     [{"n": "1", "g": "a", "extra": "e1", "x": "e1"},
      {"n": "2", "g": "b", "extra": "e2", "x": "e2"}]),
    (["LOAD", "3", "@extra", "AS", "x", "LOAD", "*"],
     [{"n": "1", "g": "a", "extra": "e1"}, {"n": "2", "g": "b", "extra": "e2"}],
     [{"n": "1", "g": "a", "extra": "e1", "x": "e1"},
      {"n": "2", "g": "b", "extra": "e2", "x": "e2"}]),
    (["LOAD", "*", "LOAD", "3", "@n", "AS", "m"],
     [{"n": "1", "g": "a", "extra": "e1"}, {"n": "2", "g": "b", "extra": "e2"}],
     [{"n": "1", "m": "1", "g": "a", "extra": "e1"},
      {"n": "2", "m": "2", "g": "b", "extra": "e2"}]),
    (["LOAD", "*", "LOAD", "1", "@__key"],
     [{"n": "1", "g": "a", "extra": "e1"}, {"n": "2", "g": "b", "extra": "e2"}],
     [{"__key": "d:1", "n": "1", "g": "a", "extra": "e1"},
      {"__key": "d:2", "n": "2", "g": "b", "extra": "e2"}]),
]


def _rows(reply):
    return [{k.decode(): v.decode() for k, v in zip(r[::2], r[1::2])}
            for r in reply[1:]]


class TestAggregateLoadallNamedLoads(ValkeySearchTestCaseDebugMode):
    def _client(self, release) -> Valkey:
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", release) == b"OK"
        if not client.execute_command("FT._LIST"):
            client.execute_command(
                "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:",
                "SCHEMA", "g", "TAG", "n", "NUMERIC")
            client.execute_command("HSET", "d:1", "g", "a", "n", "1", "extra", "e1")
            client.execute_command("HSET", "d:2", "g", "b", "n", "2", "extra", "e2")
            IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx")
        return client

    def _run(self, client, *args):
        counters = (COUNTER, LOAD_AS_COUNTER)
        before = [client.info("SEARCH").get(c, 0) for c in counters]
        reply = client.execute_command("FT.AGGREGATE", "idx", "*", *args)
        after = [client.info("SEARCH").get(c, 0) for c in counters]
        return _rows(reply), tuple(a - b for a, b in zip(after, before))

    def test_legacy_reply_is_kept_and_counted_below_1_3_0(self):
        client = self._client("1.0.0")
        for loads, legacy, _ in CASES:
            counted = "@__key" in loads or "@n" in loads
            ticks = (int(counted), int("AS" in loads))
            assert self._run(client, *loads, *SORTBY) == (legacy, ticks), loads

    def test_reply_matches_redisearch_from_1_3_0(self):
        client = self._client("1.3.0")
        for loads, _, fixed in CASES:
            assert self._run(client, *loads, *SORTBY) == (fixed, (0, 0)), loads

    def test_ambiguous_name_keeps_the_reply_without_the_rule(self):
        # Two entries claim `x`, so the stored fields are not added beside
        # them; the reply is the one 1.3.0 gave before this rule.
        client = self._client("1.3.0")
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "*", "LOAD", "*", "LOAD", "3", "@n", "AS",
            "x", "LOAD", "3", "@g", "AS", "x", *SORTBY)
        rows = [sorted(zip(r[::2], r[1::2])) for r in reply[1:]]
        assert rows == [
            sorted([(b"x", b"1"), (b"x", b"a"), (b"extra", b"e1")]),
            sorted([(b"x", b"2"), (b"x", b"b"), (b"extra", b"e2")])]

    def test_addscores_score_rename_keeps_the_reply_without_the_rule(self):
        # ADDSCORES follows LOAD, so the parser cannot see the score name;
        # the rename onto it is ambiguous all the same.
        client = self._client("1.3.0")
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "*", "ADDSCORES", "LOAD", "*", "LOAD", "3",
            "@n", "AS", "__score", *SORTBY)
        assert [sorted(r[::2]) for r in reply[1:]] == [
            [b"__score", b"__score", b"extra", b"g"]] * 2

    def test_plain_named_load_is_not_counted(self):
        # A HASH text field reads the same at every release.
        client = self._client("1.0.0")
        rows, ticks = self._run(client, "LOAD", "*", "LOAD", "1", "@g", *SORTBY)
        assert rows == CASES[0][1] and ticks == (0, 0)

    def test_named_loads_add_nothing_below_1_3_0(self):
        """1.2.1 ignored a named load under `LOAD *`: a JSON path added no
        column and a NUMERIC field kept its stored text. A later stage still
        reads the field."""
        client = self._client("1.0.0")
        client.execute_command(
            "FT.CREATE", "j", "ON", "JSON", "PREFIX", "1", "j:", "SCHEMA",
            "$.n", "AS", "n", "NUMERIC", "$.v", "AS", "v", "VECTOR", "FLAT",
            "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2")
        client.execute_command(
            "JSON.SET", "j:1", "$", '{"n":1,"x":"a","v":[1,0]}')
        client.execute_command(
            "FT.CREATE", "v", "ON", "HASH", "PREFIX", "1", "v:", "SCHEMA",
            "price", "NUMERIC", "vec", "VECTOR", "FLAT", "6", "TYPE",
            "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2")
        client.execute_command(
            "HSET", "v:1", "price", "10.50", "vec", struct.pack("<2f", 1, 0))
        for index in ("j", "v"):
            IndexingTestHelper.wait_for_backfill_complete_on_node(client, index)

        def run(release, index, *args):
            client.execute_command(
                "CONFIG", "SET", "search.emulate-release", release)
            before = client.info("SEARCH").get(COUNTER, 0)
            reply = client.execute_command("FT.AGGREGATE", index, "*", *args)
            ticks = client.info("SEARCH").get(COUNTER, 0) - before
            return [dict(zip(r[::2], r[1::2])) for r in reply[1:]], ticks

        doc = b'{"n":1,"x":"a","v":[1,0]}'
        for load in ("$.x", "@n", "@v"):
            args = ("LOAD", "*", "LOAD", "1", load)
            assert run("1.0.0", "j", *args) == ([{b"$": doc}], 1), load
        assert run("1.3.0", "j", "LOAD", "*", "LOAD", "1", "$.x") == (
            [{b"$.x": b"a", b"$": doc}], 0)
        assert run("1.0.0", "j", "LOAD", "*", "LOAD", "1", "$.x", "GROUPBY",
                   "1", "@$.x", "REDUCE", "COUNT", "0", "AS", "c") == (
            [{b"$.x": b"a", b"c": b"1"}], 1)
        rows, ticks = run("1.0.0", "v", "LOAD", "*", "LOAD", "1", "@price")
        assert rows[0][b"price"] == b"10.50" and ticks == 1
        # On HASH `LOAD *` returns the vector, so naming it adds nothing.
        for release in ("1.0.0", "1.3.0"):
            rows, ticks = run(release, "v", "LOAD", "*", "LOAD", "1", "@vec")
            assert b"vec" in rows[0] and ticks == 0, release
        rows, _ = run("1.3.0", "v", "LOAD", "*", "LOAD", "3", "@vec", "AS", "w")
        assert rows[0][b"w"] == rows[0][b"vec"]
        # On JSON `LOAD *` is only the `$` root, so a named vector load cannot
        # be satisfied: from 1.3.0 it is rejected, as `LOAD @v` alone is.
        client.execute_command(
            "FT.CREATE", "jv", "ON", "JSON", "PREFIX", "1", "jv:", "SCHEMA",
            "$.v", "AS", "v", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM",
            "2", "DISTANCE_METRIC", "L2")
        client.execute_command("JSON.SET", "jv:1", "$", '{"v":[1,0]}')
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "jv")
        client.execute_command("CONFIG", "SET", "search.emulate-release", "1.3.0")
        for load in (("LOAD", "*", "LOAD", "1", "@v"),
                     ("LOAD", "*", "LOAD", "3", "$.v", "AS", "w")):
            with pytest.raises(valkey.exceptions.ResponseError):
                client.execute_command("FT.AGGREGATE", "jv", "*", *load)
        client.execute_command("CONFIG", "SET", "search.emulate-release", "1.0.0")
        reply = client.execute_command(
            "FT.AGGREGATE", "jv", "*", "LOAD", "*", "LOAD", "1", "@v")
        assert [dict(zip(r[::2], r[1::2])) for r in reply[1:]] == [
            {b"$": b'{"v":[1,0]}'}]

    def test_attribute_declared_under_another_name(self):
        """On HASH `tg AS t`: 1.2.1's `LOAD *` reply shows `tg`; from 1.3.0
        the named load adds `t`. The difference is counted."""
        client = self._client("1.0.0")
        client.execute_command(
            "FT.CREATE", "a", "ON", "HASH", "PREFIX", "1", "a:", "SCHEMA",
            "tg", "AS", "t", "TAG")
        client.execute_command("HSET", "a:1", "tg", "red")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "a")
        before = client.info("SEARCH").get(COUNTER, 0)
        reply = client.execute_command(
            "FT.AGGREGATE", "a", "*", "LOAD", "*", "LOAD", "1", "@t")
        assert reply[1] == [b"tg", b"red"]
        assert client.info("SEARCH").get(COUNTER, 0) - before == 1

    def test_counted_once_across_cursor_pages(self):
        client = self._client("1.0.0")
        before = client.info("SEARCH").get(COUNTER, 0)
        _, cursor = client.execute_command(
            "FT.AGGREGATE", "idx", "*", "LOAD", "*", "LOAD", "1", "@__key",
            "WITHCURSOR", "COUNT", "1")
        pages = 1
        while cursor:
            _, cursor = client.execute_command("FT.CURSOR", "READ", "idx", cursor)
            pages += 1
        assert pages > 1
        assert client.info("SEARCH").get(COUNTER, 0) - before == 1

    def test_hybrid_always_follows_the_1_3_0_rule(self):
        client = self._client("1.3.0")
        client.execute_command(
            "FT.CREATE", "h", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
            "title", "TEXT", "price", "NUMERIC",
            "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2")
        for i in range(2):
            client.execute_command(
                "HSET", f"doc:{i}", "title", "alpha", "price", str(i),
                "extra", f"e{i}", "vec", struct.pack("<2f", float(i), 0.0))
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "h")

        def hybrid(release, *loads):
            client.execute_command(
                "CONFIG", "SET", "search.emulate-release", release)
            reply = client.execute_command(
                "FT.HYBRID", "h", "SEARCH", "@title:alpha",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "10", *loads,
                "SORTBY", "2", "@price", "ASC",
                "PARAMS", "2", "q", struct.pack("<2f", 0.0, 0.0))
            return [{k.decode(): v for k, v in zip(r[::2], r[1::2])
                     if k != b"vec"} for r in reply[1:]]

        def stored(i):
            return {"title": b"alpha", "price": str(i).encode(),
                    "extra": f"e{i}".encode()}

        loads = ["LOAD", "*", "LOAD", "3", "@extra", "AS", "x"]
        assert hybrid("1.3.0", *loads) == [
            {**stored(i), "x": f"e{i}".encode()} for i in range(2)]
        # FT.HYBRID is new in 1.3.0 and always emulates at least 1.3.0, so
        # nothing is counted.
        before = client.info("SEARCH").get(COUNTER, 0)
        assert hybrid("1.0.0", *loads) == hybrid("1.3.0", *loads)
        assert client.info("SEARCH").get(COUNTER, 0) == before
