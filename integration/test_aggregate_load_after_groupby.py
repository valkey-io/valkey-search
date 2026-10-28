"""A LOAD clause after GROUPBY: each record is then a group, not a document,
so there is nothing to load. A field outside the schema is rejected at every
release, as before; a declared attribute is rejected from 1.3.0 on (see
COMPATIBILITY.md) and silently ignored below."""

import pytest
import valkey
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

    def test_load_after_other_stages_still_loads(self):
        client = self._client("1.3.0")
        reply, ticks = self._ticks(
            client, "APPLY", "1", "AS", "y", "LOAD", "2", "@n", "@extra",
            "SORTBY", "2", "@n", "ASC")
        assert [dict(zip(r[::2], r[1::2])) for r in reply[1:]] == [
            {b"y": b"1", b"n": b"1", b"extra": b"e1"},
            {b"y": b"1", b"n": b"2", b"extra": b"e2"},
        ] and ticks == 0
