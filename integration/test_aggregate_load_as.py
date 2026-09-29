"""Integration tests for the FT.AGGREGATE LOAD ... AS rename clause.

Honoring `AS <alias>` in the LOAD clause is a compatibility fix gated behind
`search.emulate-release` >= 1.3.0 (see COMPATIBILITY.md). These tests run under
debug-mode so the emulate-release ceiling can be lifted to the (as yet
unreleased) fix version.

Accepting a JSON path as the loaded field is deliberately not gated: before it
was supported such a load failed outright, so there is no prior behavior to
preserve.
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
        """A loaded JSON field without a rename still appears in the output,
        named by the field token as the query wrote it (issue #1243): `@price`
        comes back as `price`, a JSON path comes back as the path.
        """
        client = self._client()
        self._make_json(client)
        # By attribute name.
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]", "LOAD", "1", "@price",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"price": b"10"}
        # By path (no rename) -- emitted under the path, as written.
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]", "LOAD", "1", "$.price",
        )
        assert rows(reply)[0] == {"$.price": b"10"}

    def test_json_field_names_gated_by_release(self):
        """The #1243 naming fix is gated: emulating a pre-1.3.0 release keeps
        the old behavior of naming the column by the schema identifier."""
        client = self._client(emulate_release=LEGACY_RELEASE)
        self._make_json(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]", "LOAD", "1", "@price",
        )
        assert rows(reply)[0] == {"$.price": b"10"}

    def test_load_as_disabled_in_legacy_release(self):
        """Emulating a pre-1.3.0 release preserves the legacy behavior: AS is
        treated as an (unknown) field name. Both commands below still fail for
        that reason -- the JSON-path form resolves its path even here, but then
        trips over `AS` as an unknown field."""
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

    def test_same_field_under_two_output_names(self):
        """One LOAD clause may name the same field more than once under
        different output names; each name gets its own column, all reading the
        same source field. Verified against RediSearch, which emits a column
        per name.

        Cases involving a field loaded *without* a rename are asserted on HASH
        only: on JSON the un-renamed entry is emitted under the schema
        identifier (`$.price`) rather than the attribute name, which is a
        separate pre-existing defect and not what this test is about."""
        both = ((self._make_hash, "idx_hash"), (self._make_json, "idx_json"))
        hash_only = ((self._make_hash, "idx_hash"),)
        cases = [
            # Two renames of the same field -- no default output name involved,
            # so this holds for both key types.
            (both, ("LOAD", "6", "@price", "AS", "a", "@price", "AS", "b"),
             {"a": b"10", "b": b"10"}),
            # Plain load plus a rename of the same field.
            (hash_only, ("LOAD", "4", "@price", "@price", "AS", "b"),
             {"price": b"10", "b": b"10"}),
            # ... and the same pair in the opposite order.
            (hash_only, ("LOAD", "4", "@price", "AS", "a", "@price"),
             {"a": b"10", "price": b"10"}),
        ]
        for targets, load, expected in cases:
            for make, idx in targets:
                client = self._client()
                client.execute_command("FLUSHALL", "SYNC")
                make(client)
                reply = client.execute_command(
                    "FT.AGGREGATE", idx, "@price:[-inf inf]", *load,
                )
                result = rows(reply)
                assert len(result) == 1, f"{idx} {load}"
                assert result[0] == expected, f"{idx} {load}"

    def test_same_field_under_two_output_names_in_apply(self):
        """Both output names of a doubly-loaded field resolve independently in
        a later pipeline stage."""
        client = self._client()
        self._make_hash(client)
        reply = client.execute_command(
            "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
            "LOAD", "6", "@price", "AS", "a", "@price", "AS", "b",
            "APPLY", "@a+@b", "AS", "s",
        )
        result = rows(reply)
        assert len(result) == 1
        assert result[0] == {"a": b"10", "b": b"10", "s": b"20"}

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
