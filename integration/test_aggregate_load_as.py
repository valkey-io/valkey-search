"""Integration tests for the FT.AGGREGATE LOAD ... AS rename clause.

Honoring `AS <alias>` in the LOAD clause (and accepting a JSON path as the
loaded field) is a compatibility fix gated behind `search.emulate-release`
>= 1.3 (see COMPATIBILITY.md). These tests run under debug-mode so the
emulate-release ceiling can be lifted to the (as yet unreleased) fix version.
"""

import pytest
import valkey
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

FIX_RELEASE = "1.3.0"
LEGACY_RELEASE = "1.0.0"


def rows(reply):
    """Convert an FT.AGGREGATE reply into a list of {field: value} dicts."""
    result = []
    for row in reply[1:]:
        fields = {}
        for i in range(0, len(row), 2):
            fields[row[i].decode()] = row[i + 1]
        result.append(fields)
    return result


class TestAggregateLoadAs(ValkeySearchTestCaseDebugMode):
    def _client(self, emulate_release=FIX_RELEASE) -> Valkey:
        client: Valkey = self.server.get_new_client()
        assert (
            client.execute_command(
                f"CONFIG SET search.emulate-release {emulate_release}"
            )
            == b"OK"
        )
        return client

    def _make_hash(self, client):
        client.execute_command(
            "FT.CREATE", "idx_hash", "ON", "HASH", "PREFIX", "1", "h:",
            "SCHEMA", "price", "NUMERIC", "qty", "NUMERIC", "cat", "TAG",
        )
        client.execute_command("HSET", "h:1", "price", "10", "qty", "5", "cat", "a")

    def _make_json(self, client):
        client.execute_command(
            "FT.CREATE", "idx_json", "ON", "JSON", "PREFIX", "1", "j:",
            "SCHEMA", "$.price", "AS", "price", "NUMERIC",
            "$.qty", "AS", "qty", "NUMERIC",
        )
        client.execute_command("JSON.SET", "j:1", "$", '{"price":10,"qty":5}')

    def test_load_as_hash(self):
        """LOAD ... AS renames the emitted field for a HASH key."""
        client = self._client()
        self._make_hash(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
            "LOAD", "3", "@price", "AS", "cost",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"cost": b"10"}
        assert "price" not in result[0]

    def test_load_as_hash_used_in_apply(self):
        """A LOAD ... AS alias resolves in a later APPLY stage (HASH)."""
        client = self._client()
        self._make_hash(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
            "LOAD", "3", "@price", "AS", "cost",
            "APPLY", "@cost+1", "AS", "c2",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"cost": b"10", "c2": b"11"}

    def test_load_as_json_by_alias(self):
        """LOAD ... AS renames an indexed JSON field referenced by alias."""
        client = self._client()
        self._make_json(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]",
            "LOAD", "3", "@price", "AS", "cost",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"cost": b"10"}

    def test_load_as_json_by_path(self):
        """The loaded field may be given as a JSON path, with a rename."""
        client = self._client()
        self._make_json(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]",
            "LOAD", "3", "$.price", "AS", "cost",
            "APPLY", "@cost+1", "AS", "c2",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"cost": b"10", "c2": b"11"}

    def test_load_json_without_rename_still_appears(self):
        """A loaded JSON field without a rename still appears in the output.

        valkey-search emits it under its identifier (the JSON path), matching
        existing behavior; the AS parsing change must not regress this.
        """
        client = self._client()
        self._make_json(client)
        # By alias.
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]", "LOAD", "1", "@price",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"$.price": b"10"}
        # By path (no rename) -- also emitted under the path.
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]", "LOAD", "1", "$.price",
        )
        assert rows(reply)[0] == {"$.price": b"10"}

    def test_load_as_disabled_in_legacy_release(self):
        """Emulating a pre-1.2.1 release preserves the legacy behavior:
        AS is treated as an (unknown) field name and JSON paths are rejected."""
        client = self._client(emulate_release=LEGACY_RELEASE)
        self._make_hash(client)
        with pytest.raises(valkey.exceptions.ResponseError):
            client.execute_command(
                "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
                "LOAD", "3", "@price", "AS", "cost",
            )
        self._make_json(client)
        with pytest.raises(valkey.exceptions.ResponseError):
            client.execute_command(
                "FT.AGGREGATE", "idx_json", "@price:[-inf inf]",
                "LOAD", "3", "$.price", "AS", "cost",
            )

    def test_rename_hides_declared_attribute(self):
        """A rename can shadow a declared attribute: `@price AS qty` makes the
        name `qty` resolve to price's value, not the schema's qty."""
        for make, idx in ((self._make_hash, "idx_hash"), (self._make_json, "idx_json")):
            client = self._client()
            client.execute_command("FLUSHALL", "SYNC")
            make(client)
            reply = client.execute_command(
                "FT.AGGREGATE", idx, "@price:[-inf inf]",
                "LOAD", "3", "@price", "AS", "qty",
                "APPLY", "@qty+100", "AS", "r",
            )
            result = rows(reply)
            assert len(result) == 1
            # qty == price's value (10), NOT the schema qty (5); r == 110.
            assert result[0] == {"qty": b"10", "r": b"110"}

    def test_duplicate_output_name_errors(self):
        """Intentionally stricter than RediSearch (which keeps the first claim of
        an output name and drops the rest): a LOAD clause that names the same
        output twice is a syntax error whenever an AS rename is involved."""
        collisions = [
            # Two AS clauses targeting the same alias.
            ("LOAD", "6", "@price", "AS", "x", "@qty", "AS", "x"),
            # A rename onto the name of a field loaded earlier in the clause.
            ("LOAD", "4", "@qty", "@price", "AS", "qty"),
            # ... and the same collision in the opposite order.
            ("LOAD", "4", "@price", "AS", "qty", "@qty"),
            # A rename onto the key field when the key is also loaded.
            ("LOAD", "4", "@__key", "@price", "AS", "__key"),
        ]
        for make, idx in ((self._make_hash, "idx_hash"), (self._make_json, "idx_json")):
            client = self._client()
            client.execute_command("FLUSHALL", "SYNC")
            make(client)
            for load in collisions:
                with pytest.raises(valkey.exceptions.ResponseError, match="Duplicate"):
                    client.execute_command(
                        "FT.AGGREGATE", idx, "@price:[-inf inf]", *load,
                    )

    def test_plain_duplicate_load_is_deduplicated(self):
        """Loading the same field twice without a rename is not an error."""
        client = self._client()
        self._make_hash(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
            "LOAD", "2", "@price", "@price",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"price": b"10"}

    def test_rename_key_field(self):
        """`__key` may be renamed via AS, and the alias is usable downstream."""
        client = self._client()
        self._make_hash(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
            "LOAD", "3", "@__key", "AS", "mykey",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"mykey": b"h:1"}
