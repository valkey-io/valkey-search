"""FT.AGGREGATE with LOAD * gives the 1.2.1 reply below emulate-release 1.3.0.

1.2.1 fetched only the whole record under LOAD *: a named LOAD next to it added
nothing, and a stage read only what the record held under that name, which on
JSON is nothing but the `$` root. From 1.3.0 named and stage-read fields are
fetched as well.
"""

import json
import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchTestCaseBase,
)
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters

LEGACY = "1.0.0"
FIXED = "1.3.0"
COUNTER = "search_compatibility-ft_aggregate_loadall_fetch"
QUERY = "@price:[0 100]"


def _vec(*xs: float) -> bytes:
    return struct.pack(f"{len(xs)}f", *xs)


def _seed(client: Valkey, writer, key_type: str) -> None:
    vec = ("VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
           "DISTANCE_METRIC", "L2")
    if key_type == "HASH":
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA",
            "price", "NUMERIC", "SORTABLE", "q", "NUMERIC",
            "tg", "AS", "t", "TAG", "v", *vec)
        writer.hset("d:1", mapping={"price": "10.50", "q": "2", "tg": "red",
                                    "extra": "e1", "v": _vec(1, 0)})
        writer.hset("d:2", mapping={"price": "20", "q": "3", "tg": "blue",
                                    "extra": "e2", "v": _vec(0, 1)})
    else:
        client.execute_command(
            "FT.CREATE", "idx", "ON", "JSON", "PREFIX", "1", "d:", "SCHEMA",
            "$.price", "AS", "price", "NUMERIC", "$.t", "AS", "t", "TAG",
            "$.n", "AS", "n", "NUMERIC", "$.v", "AS", "v", *vec)
        writer.execute_command("JSON.SET", "d:1", "$", json.dumps(
            {"price": 10.5, "t": "red", "n": 5, "v": [1, 0]}))
        writer.execute_command("JSON.SET", "d:2", "$", json.dumps(
            {"price": 20, "t": "blue", "n": 7, "v": [0, 1]}))
    waiters.wait_for_true(
        lambda: client.execute_command(
            "FT.SEARCH", "idx", QUERY, "NOCONTENT", "LIMIT", "0", "0")[0] == 2,
        timeout=10)


def _rows(reply) -> list:
    rows = [dict(zip(r[::2], r[1::2])) for r in reply[1:]]
    return sorted(rows, key=lambda row: sorted(row.items()))


def _ticks(client: Valkey, release: str, *tail):
    """The rows (or error) of `FT.AGGREGATE idx QUERY *tail`, and the ticks."""
    client.execute_command("CONFIG", "SET", "search.emulate-release", release)
    before = client.info("SEARCH").get(COUNTER, 0)
    try:
        rows = _rows(client.execute_command("FT.AGGREGATE", "idx", QUERY, *tail))
    except ResponseError as e:
        rows = str(e)
    return rows, client.info("SEARCH").get(COUNTER, 0) - before


class TestAggregateLoadAllLegacy(ValkeySearchTestCaseBase):

    def _client(self, key_type: str) -> Valkey:
        client: Valkey = self.server.get_new_client()
        _seed(client, client, key_type)
        return client

    @pytest.mark.parametrize("named", [
        ("LOAD", "1", "@__key"),
        # NUMERIC: fetched by name it would print as 10.5.
        ("LOAD", "1", "@price"),
        # Outside the schema, a vector, and `AS` read as a field name: each an
        # error once fetched.
        ("LOAD", "1", "@extra"),
        ("LOAD", "1", "@v"),
        ("LOAD", "3", "@price", "AS", "p"),
    ])
    def test_named_load_adds_nothing_on_hash(self, named):
        client = self._client("HASH")
        whole, ticks = _ticks(client, LEGACY, "LOAD", "*")
        assert ticks == 0
        assert {r[b"price"] for r in whole} == {b"10.50", b"20"}
        rows, ticks = _ticks(client, LEGACY, "LOAD", "*", *named)
        assert (rows, ticks) == (whole, 1)
        _, ticks = _ticks(client, FIXED, "LOAD", "*", *named)
        assert ticks == 0

    def test_key_is_added_from_1_3_0(self):
        client = self._client("HASH")
        rows, _ = _ticks(client, FIXED, "LOAD", "*", "LOAD", "1", "@__key")
        assert {r[b"__key"] for r in rows} == {b"d:1", b"d:2"}

    def test_named_path_adds_nothing_on_json(self):
        client = self._client("JSON")
        rows, ticks = _ticks(client, LEGACY, "LOAD", "*", "LOAD", "1", "@price")
        assert [set(r) for r in rows] == [{b"$"}, {b"$"}]
        assert ticks == 1
        rows, _ = _ticks(client, FIXED, "LOAD", "*", "LOAD", "1", "@price")
        assert all(len(r) == 2 for r in rows)

    def test_stage_reads_nothing_on_json(self):
        client = self._client("JSON")
        rows, ticks = _ticks(client, LEGACY, "LOAD", "*", "FILTER", "@n>6")
        assert (rows, ticks) == ([], 1)
        rows, ticks = _ticks(client, LEGACY, "LOAD", "*",
                             "APPLY", "@n+1", "AS", "m")
        assert [set(r) for r in rows] == [{b"$"}, {b"$"}]
        assert ticks == 1
        rows, ticks = _ticks(client, FIXED, "LOAD", "*", "FILTER", "@n>6")
        assert len(rows) == 1 and ticks == 0

    def test_reducer_reading_an_earlier_reducers_name_ticks_on_json(self):
        # A reducer reads the input records, so `@n` here is the field `n`,
        # not the COUNT just named `n`.
        client = self._client("JSON")
        rows, ticks = _ticks(client, LEGACY, "LOAD", "*", "GROUPBY", "0",
                             "REDUCE", "COUNT", "0", "AS", "n",
                             "REDUCE", "SUM", "1", "@n", "AS", "s")
        assert rows[0][b"s"] == b"0"
        assert ticks == 1

    @pytest.mark.parametrize("key_type, tail", [
        ("HASH", ("LOAD", "*")),
        ("HASH", ("LOAD", "*", "LOAD", "1", "@t")),
        ("HASH", ("LOAD", "*", "APPLY", "@price+1", "AS", "m")),
        ("HASH", ("LOAD", "*", "SORTBY", "2", "@price", "DESC")),
        ("HASH", ("LOAD", "*", "LOAD", "1", "@__key",
                  "GROUPBY", "1", "@t", "REDUCE", "COUNT", "0", "AS", "c")),
        ("HASH", ("LOAD", "*", "LOAD", "1", "@price",
                  "APPLY", "7", "AS", "price")),
        ("HASH", ("LOAD", "*", "LOAD", "1", "@__key",
                  "APPLY", "3", "AS", "__key")),
        # A stage already reads the NUMERIC field as a number.
        ("HASH", ("LOAD", "*", "LOAD", "1", "@price",
                  "SORTBY", "2", "@price", "ASC")),
        ("JSON", ("LOAD", "*")),
        ("JSON", ("LOAD", "*", "APPLY", "@__key", "AS", "k")),
        ("JSON", ("LOAD", "*", "LOAD", "1", "@price",
                  "APPLY", "7", "AS", "price")),
    ])
    def test_no_tick_when_the_fetch_changes_nothing(self, key_type, tail):
        # Other gates may still change these replies at 1.3.0.
        client = self._client(key_type)
        _, ticks = _ticks(client, LEGACY, *tail)
        assert ticks == 0

    def test_key_read_by_a_stage_ticks_under_groupby(self):
        client = self._client("HASH")
        rows, ticks = _ticks(client, LEGACY, "LOAD", "*", "LOAD", "1", "@__key",
                             "GROUPBY", "1", "@__key",
                             "REDUCE", "COUNT", "0", "AS", "c")
        assert (rows, ticks) == ([{b"c": b"2"}], 1)

    def test_cursor_ticks_once(self):
        client = self._client("HASH")
        client.execute_command("CONFIG", "SET", "search.emulate-release",
                               LEGACY)
        before = client.info("SEARCH").get(COUNTER, 0)
        page, cursor = client.execute_command(
            "FT.AGGREGATE", "idx", QUERY, "LOAD", "*", "LOAD", "1", "@__key",
            "WITHCURSOR", "COUNT", "1")
        pages = 1
        while cursor:
            page, cursor = client.execute_command(
                "FT.CURSOR", "READ", "idx", cursor)
            pages += 1
        assert pages >= 2
        assert client.info("SEARCH").get(COUNTER, 0) - before == 1


class TestAggregateLoadAllLegacyCluster(ValkeySearchClusterTestCase):

    def test_named_path_adds_nothing_on_json(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        _seed(client, cluster, "JSON")
        rows, _ = _ticks(client, LEGACY, "LOAD", "*", "LOAD", "1", "@price")
        assert [set(r) for r in rows] == [{b"$"}, {b"$"}]
        rows, _ = _ticks(client, FIXED, "LOAD", "*", "LOAD", "1", "@price")
        assert all(len(r) == 2 for r in rows)

    def test_stage_reads_nothing_on_json(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        _seed(client, cluster, "JSON")
        rows, _ = _ticks(client, LEGACY, "LOAD", "*", "FILTER", "@n>6")
        assert rows == []
        rows, _ = _ticks(client, FIXED, "LOAD", "*", "FILTER", "@n>6")
        assert len(rows) == 1
