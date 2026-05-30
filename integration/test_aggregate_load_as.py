"""Integration tests for the FT.AGGREGATE LOAD ... AS rename clause.

Honoring `AS <alias>` in the LOAD clause (and accepting a JSON path as the
loaded field) is a compatibility fix gated behind `search.emulate-release`
>= 1.2.1 (see COMPATIBILITY.md). These tests run under debug-mode so the
emulate-release ceiling can be lifted to the (as yet unreleased) fix version.
"""

import pytest
import valkey
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

FIX_RELEASE = "1.2.1"
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
            "SCHEMA", "price", "NUMERIC", "cat", "TAG",
        )
        client.execute_command("HSET", "h:1", "price", "10", "cat", "a")

    def _make_json(self, client):
        client.execute_command(
            "FT.CREATE", "idx_json", "ON", "JSON", "PREFIX", "1", "j:",
            "SCHEMA", "$.price", "AS", "price", "NUMERIC",
        )
        client.execute_command("JSON.SET", "j:1", "$", '{"price":10}')

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
