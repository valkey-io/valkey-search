"""The `aggregate_nil_alias_named` compatibility counter ticks only for a nil
the 1.3.0 fix would name in the reply, not for a field the key lacks, which
both releases leave out."""

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

COUNTER = "search_compatibility-aggregate_nil_alias_named"


class TestAggregateNilCounter(ValkeySearchTestCaseBase):
    def test_counter_skips_missing_field(self):
        client: Valkey = self.server.get_new_client()
        assert client.execute_command(
            "CONFIG", "SET", "search.emulate-release", "1.0.0") == b"OK"
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "h:",
            "SCHEMA", "price", "NUMERIC", "qty", "NUMERIC",
        )
        client.execute_command("HSET", "h:1", "price", "10")

        def ticks(*args):
            before = client.info("SEARCH").get(COUNTER, 0)
            reply = client.execute_command("FT.AGGREGATE", "idx", "*", *args)
            return reply, client.info("SEARCH").get(COUNTER, 0) - before

        assert ticks("LOAD", "1", "@qty") == ([1, []], 0)
        assert ticks("LOAD", "1", "@extra") == ([1, []], 0)
        assert ticks("LOAD", "1", "@price", "APPLY", "@price+'x'", "AS", "s") == (
            [1, [b"price", b"10"]], 1)
