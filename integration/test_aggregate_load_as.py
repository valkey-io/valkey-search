"""Integration tests for the FT.AGGREGATE LOAD ... AS rename clause.

Honoring `AS <alias>` in the LOAD clause is a compatibility fix gated behind
`search.emulate-release` >= 1.3.0 (see COMPATIBILITY.md). These tests run under
debug-mode so the emulate-release ceiling can be lifted to the (as yet
unreleased) fix version.

Accepting a JSON path as the loaded field is deliberately not gated: before it
was supported such a load failed outright, so there is no prior behavior to
preserve.
"""

import struct

import pytest
import valkey
from utils import IndexingTestHelper
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker

FIX_RELEASE = "1.3.0"
LEGACY_RELEASE = "1.0.0"


def pairs(reply):
    """Each row as a list of (field, value) pairs; unlike rows(), keeps a
    repeated field visible."""
    return [[(row[i].decode(), row[i + 1]) for i in range(0, len(row), 2)]
            for row in reply[1:]]


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

    def test_load_as_legacy_error_matches_main(self):
        client = self._client(emulate_release=LEGACY_RELEASE)
        self._make_hash(client)
        self._make_json(client)
        counter = "search_compatibility-ft_aggregate_load_as"
        for index, field in (("idx_hash", "@price"), ("idx_json", "$.price")):
            before = client.info("SEARCH").get(counter, 0)
            with pytest.raises(valkey.exceptions.ResponseError,
                               match=r"^Index field `AS` does not exist$"):
                client.execute_command(
                    "FT.AGGREGATE", index, "@price:[-inf inf]",
                    "LOAD", "3", field, "AS", "cost",
                )
            assert client.info("SEARCH").get(counter, 0) - before == 1

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
        """Stricter than RediSearch (see COMPATIBILITY.md, "stricter input
        validation"): two LOAD entries that produce the same output name are
        rejected whenever an AS rename is involved, in one clause or across
        clauses."""
        collisions = [
            # Two AS clauses targeting the same alias.
            ("LOAD", "6", "@price", "AS", "x", "@qty", "AS", "x"),
            # A rename onto the name of a field loaded earlier in the clause.
            ("LOAD", "4", "@qty", "@price", "AS", "qty"),
            # ... and the same collision in the opposite order.
            ("LOAD", "4", "@price", "AS", "qty", "@qty"),
            # A rename onto the key field when the key is also loaded.
            ("LOAD", "4", "@__key", "@price", "AS", "__key"),
            # The same collisions split across clauses.
            ("LOAD", "3", "@price", "AS", "x", "LOAD", "3", "@qty", "AS", "x"),
            ("LOAD", "1", "@qty", "LOAD", "3", "@price", "AS", "qty"),
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

    def test_rename_onto_the_key_while_the_key_is_loaded_errors(self):
        client = self._client()
        self._make_hash(client)
        with pytest.raises(valkey.exceptions.ResponseError,
                           match="`__key` is also loaded"):
            client.execute_command(
                "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
                "LOAD", "6", "@__key", "AS", "k", "@price", "AS", "__key")

    def test_load_of_an_earlier_output_name_errors(self):
        """`@x` after `@price AS x` could mean the rename or a stored `x`."""
        for make, idx in ((self._make_hash, "idx_hash"), (self._make_json, "idx_json")):
            client = self._client()
            client.execute_command("FLUSHALL", "SYNC")
            make(client)
            for load in [
                ("LOAD", "3", "@price", "AS", "x", "LOAD", "3", "@x", "AS", "y"),
                ("LOAD", "6", "@price", "AS", "x", "@x", "AS", "y"),
            ]:
                with pytest.raises(valkey.exceptions.ResponseError,
                                   match="output name of an earlier LOAD entry"):
                    client.execute_command(
                        "FT.AGGREGATE", idx, "@price:[-inf inf]", *load)

    def test_rename_onto_or_of_the_knn_score_errors(self):
        """A score the query emits could also be a stored field of that name;
        a rename that keeps the name is a plain load."""
        client = self._client()
        client.execute_command(
            "FT.CREATE", "idx_vec", "ON", "HASH", "PREFIX", "1", "v:",
            "SCHEMA", "price", "NUMERIC",
            "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
            "DISTANCE_METRIC", "L2")
        client.execute_command(
            "HSET", "v:1", "price", "10", "vec", struct.pack("<2f", 1.0, 0.0))
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx_vec")

        def knn(*load):
            return client.execute_command(
                "FT.AGGREGATE", "idx_vec", "*=>[KNN 1 @vec $q AS dist]", *load,
                "PARAMS", "2", "q", struct.pack("<2f", 0.0, 0.0), "DIALECT", "2")

        for load in [("LOAD", "3", "@price", "AS", "dist"),
                     ("LOAD", "3", "@dist", "AS", "d")]:
            with pytest.raises(valkey.exceptions.ResponseError,
                               match="score name `dist`"):
                knn(*load)
        assert rows(knn("LOAD", "3", "@dist", "AS", "dist")) == [{"dist": b"1"}]
        for load in [("LOAD", "3", "@price", "AS", "__score"),
                     ("LOAD", "3", "@__score", "AS", "s")]:
            with pytest.raises(valkey.exceptions.ResponseError,
                               match="score name `__score`"):
                client.execute_command(
                    "FT.AGGREGATE", "idx_vec", "*", "ADDSCORES", *load)
        with pytest.raises(valkey.exceptions.ResponseError,
                           match="score name `d`"):
            client.execute_command(
                "FT.AGGREGATE", "idx_vec",
                "@vec:[VECTOR_RANGE 5 $q]=>{$YIELD_DISTANCE_AS: d}",
                "LOAD", "3", "@price", "AS", "d",
                "PARAMS", "2", "q", struct.pack("<2f", 0.0, 0.0), "DIALECT", "2")
        # FT.HYBRID: a rename onto `__score` while the fused score is loaded.
        with pytest.raises(valkey.exceptions.ResponseError,
                           match="`__score` is also loaded"):
            client.execute_command(
                "FT.HYBRID", "idx_vec", "SEARCH", "*",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "1",
                "LOAD", "6", "@__score", "AS", "s", "@price", "AS", "__score",
                "PARAMS", "2", "q", struct.pack("<2f", 0.0, 0.0))
        # Without ADDSCORES no score is emitted, so the name is free.
        assert rows(client.execute_command(
            "FT.AGGREGATE", "idx_vec", "*",
            "LOAD", "3", "@price", "AS", "__score")) == [{"__score": b"10"}]

    def test_same_field_under_the_same_name_is_a_repeat(self):
        client = self._client()
        self._make_hash(client)
        for load in [("LOAD", "6", "@price", "AS", "x", "@price", "AS", "x"),
                     ("LOAD", "3", "@price", "AS", "x", "LOAD", "3", "@price", "AS", "x")]:
            assert pairs(client.execute_command(
                "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]", *load)) == [
                    [("x", b"10")]], load
        assert pairs(client.execute_command(
            "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
            "LOAD", "1", "@price", "LOAD", "3", "@price", "AS", "price")) == [
                [("price", b"10")]]

    def test_rename_onto_a_name_an_earlier_stage_uses_errors(self):
        client = self._client()
        self._make_hash(client)
        with pytest.raises(valkey.exceptions.ResponseError,
                           match="an earlier stage already uses that name"):
            client.execute_command(
                "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
                "SORTBY", "2", "@qty", "ASC", "LOAD", "3", "@price", "AS", "qty")

    def test_rename_of_an_earlier_stage_output_errors(self):
        client = self._client()
        self._make_hash(client)
        with pytest.raises(valkey.exceptions.ResponseError,
                           match="output name of an earlier stage"):
            client.execute_command(
                "FT.AGGREGATE", "idx_hash", "@price:[-inf inf]",
                "APPLY", "@price", "AS", "w", "LOAD", "3", "@w", "AS", "z")

    def test_rename_of_an_earlier_stage_output_errors_on_json(self):
        client = self._client()
        self._make_json(client)
        with pytest.raises(valkey.exceptions.ResponseError,
                           match="output name of an earlier stage"):
            client.execute_command(
                "FT.AGGREGATE", "idx_json", "@price:[-inf inf]",
                "APPLY", "@qty*2", "AS", "qty", "LOAD", "3", "$.qty", "AS", "q")

    def test_json_path_after_a_stage_under_its_attribute_name(self):
        client = self._client()
        self._make_json(client)
        assert pairs(client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]",
            "SORTBY", "2", "@qty", "ASC", "LOAD", "3", "$.qty", "AS", "qty")) == [
                [("qty", b"5")]]

    def test_loadall_hides_a_stored_field_only_behind_an_emitted_column(self):
        client = self._client()
        client.execute_command(
            "FT.CREATE", "idx_all", "ON", "HASH", "PREFIX", "1", "a:",
            "SCHEMA", "price", "NUMERIC")
        client.execute_command("HSET", "a:1", "price", "10", "e", "stored")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx_all")

        def reply(*args):
            return sorted(pairs(client.execute_command(
                "FT.AGGREGATE", "idx_all", "*", *args))[0])

        # A rename with no value leaves the stored field visible.
        assert reply("LOAD", "*", "LOAD", "3", "@nosuch", "AS", "e") == [
            ("e", b"stored"), ("price", b"10")]
        # A column filled by a later stage hides it: one `e`, not two.
        assert reply("LOAD", "*", "LOAD", "3", "@nosuch", "AS", "e",
                     "APPLY", "1", "AS", "e") == [("e", b"1"), ("price", b"10")]

    def test_json_loadall_keeps_the_root_beside_a_renamed_root(self):
        client = self._client()
        self._make_json(client)
        assert sorted(pairs(client.execute_command(
            "FT.AGGREGATE", "idx_json", "@price:[-inf inf]",
            "LOAD", "*", "LOAD", "3", "$", "AS", "doc"))[0]) == [
                ("$", b'{"price":10,"qty":5}'), ("doc", b'{"price":10,"qty":5}')]

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
