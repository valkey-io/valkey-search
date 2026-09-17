"""Integration tests for implicit loading of fields referenced by a pipeline
stage but absent from the LOAD clause (issue #919).

GROUPBY and SORTBY are covered by the compatibility suite, which compares
against captured Redisearch answers. APPLY and FILTER cannot be: Redisearch
rejects a reference to an un-loaded field outright ("Property `x` not loaded
nor in pipeline"), and the compatibility harness skips any case where
Redisearch itself errored. valkey-search loads those fields instead, which is
a deliberate extension, so it is asserted here directly.
"""

import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def rows(reply):
    """Convert an FT.AGGREGATE reply into a list of {field: value} dicts."""
    result = []
    for row in reply[1:]:
        fields = {}
        for i in range(0, len(row), 2):
            fields[row[i].decode()] = row[i + 1]
        result.append(fields)
    return result


class TestAggregateAutoLoad(ValkeySearchTestCaseBase):
    def _client(self) -> Valkey:
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "h:",
            "SCHEMA", "cat", "TAG", "price", "NUMERIC", "qty", "NUMERIC",
        )
        client.execute_command("HSET", "h:1", "cat", "a", "price", "10", "qty", "1")
        client.execute_command("HSET", "h:2", "cat", "b", "price", "20", "qty", "2")
        client.execute_command("HSET", "h:3", "cat", "a", "price", "30", "qty", "3")
        return client

    def test_apply_over_unloaded_field(self):
        """APPLY may reference a field the LOAD clause does not mention."""
        client = self._client()
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "@price:[-inf inf]",
            "LOAD", "1", "@__key", "APPLY", "@price*2", "AS", "dbl",
            "SORTBY", "2", "@__key", "ASC",
        )
        result = rows(reply)
        assert [r["dbl"] for r in result] == [b"20", b"40", b"60"]

    def test_apply_over_unloaded_field_with_no_load_clause(self):
        """... including when there is no LOAD clause at all."""
        client = self._client()
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "@price:[-inf inf]",
            "APPLY", "@price+@qty", "AS", "total", "SORTBY", "2", "@total", "ASC",
        )
        assert [r["total"] for r in rows(reply)] == [b"11", b"22", b"33"]

    def test_filter_over_unloaded_field(self):
        """FILTER may reference a field the LOAD clause does not mention.

        Without implicit loading the comparison sees Nil and every record is
        discarded, so this returned an empty result rather than an error.
        """
        client = self._client()
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "@price:[-inf inf]",
            "LOAD", "1", "@__key", "FILTER", "@price>15",
            "SORTBY", "2", "@__key", "ASC",
        )
        result = rows(reply)
        assert len(result) == 2
        assert [r["__key"] for r in result] == [b"h:2", b"h:3"]

    def test_derived_names_are_not_auto_loaded(self):
        """A name produced by the pipeline is not a stored field, so it must
        not be treated as loadable -- chained stages over a reducer alias have
        to keep working."""
        client = self._client()
        reply = client.execute_command(
            "FT.AGGREGATE", "idx", "@price:[-inf inf]",
            "GROUPBY", "1", "@cat", "REDUCE", "COUNT", "0", "AS", "cnt",
            "APPLY", "@cnt*10", "AS", "scaled",
        )
        result = sorted(rows(reply), key=lambda r: r["cat"])
        assert result == [
            {"cat": b"a", "cnt": b"2", "scaled": b"20"},
            {"cat": b"b", "cnt": b"1", "scaled": b"10"},
        ]

    def test_unknown_field_still_rejected(self):
        """Auto-loading must not turn a genuinely unknown field into a silent
        Nil -- it is not a declared attribute, so the query still fails."""
        client = self._client()
        with pytest.raises(Exception):
            client.execute_command(
                "FT.AGGREGATE", "idx", "@price:[-inf inf]",
                "GROUPBY", "1", "@nosuchfield", "REDUCE", "COUNT", "0", "AS", "cnt",
            )
