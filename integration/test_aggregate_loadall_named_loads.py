"""`LOAD *` together with a renamed field or `@__key`. From 1.3.0 every stored
field appears under its own name and a rename to a new name adds a column.
Below 1.3.0 the legacy reply is kept. There `AS` is read as a field name, so
only `@__key` reaches this counter; a rename is counted by the LOAD AS one
(see COMPATIBILITY.md)."""

import struct

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
            ticks = (1, 0) if "@__key" in loads else (0, 1)
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

    def test_plain_named_load_is_not_counted(self):
        # Without a rename or `__key` the reply is the same at every release.
        client = self._client("1.0.0")
        rows, ticks = self._run(client, "LOAD", "*", "LOAD", "1", "@n", *SORTBY)
        assert rows == CASES[0][1] and ticks == (0, 0)

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
