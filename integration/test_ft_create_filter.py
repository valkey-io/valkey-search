from valkey.client import Valkey
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase, ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper
from ft_info_parser import FTInfoParser
import pytest


class TestFTCreateFilter(ValkeySearchTestCaseBase):
    """Integration tests for the FT.CREATE FILTER option."""

    def test_filter_selective_indexing(self):
        """Test that FILTER selectively indexes only matching documents and that
        documents failing the filter don't disrupt other indexed keys."""
        client: Valkey = self.server.get_new_client()

        # Create an index with a FILTER that only includes active documents.
        assert client.execute_command(
            "FT.CREATE", "filtered_idx",
            "ON", "HASH",
            "PREFIX", "1", "item:",
            "FILTER", "@status=='active'",
            "SCHEMA", "status", "TAG", "price", "NUMERIC"
        ) == b"OK"

        # Verify filter appears in FT.INFO
        info = FTInfoParser(client.execute_command("FT.INFO", "filtered_idx"))
        assert info.index_definition.get("filter") == "@status=='active'"

        # Insert documents: some pass the filter, some don't.
        client.execute_command("HSET", "item:1", "status", "active", "price", "100")
        client.execute_command("HSET", "item:2", "status", "inactive", "price", "200")
        client.execute_command("HSET", "item:3", "status", "active", "price", "300")
        client.execute_command("HSET", "item:4", "status", "pending", "price", "400")
        client.execute_command("HSET", "item:5", "status", "active", "price", "500")

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "filtered_idx")

        # Only active documents (item:1, item:3, item:5) should be in the index.
        result = client.execute_command("FT.SEARCH", "filtered_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 indexed docs, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"item:1", b"item:3", b"item:5"}

        # Verify inactive/pending docs are not found via tag search either.
        result = client.execute_command("FT.SEARCH", "filtered_idx", "@status:{inactive}")
        assert result[0] == 0

        # Insert another failing document after initial indexing — should not
        # disrupt the existing indexed keys.
        client.execute_command("HSET", "item:6", "status", "inactive", "price", "600")

        # The three active documents should still be searchable and unaffected.
        result = client.execute_command("FT.SEARCH", "filtered_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 indexed docs after adding filtered-out key, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"item:1", b"item:3", b"item:5"}

    def test_filter_mutation_removes_from_index(self):
        """Test that mutating an already-indexed document so it no longer
        satisfies the filter effectively removes it from the index."""
        client: Valkey = self.server.get_new_client()

        # Create an index that only includes documents with price > 50.
        assert client.execute_command(
            "FT.CREATE", "price_idx",
            "ON", "HASH",
            "PREFIX", "1", "prod:",
            "FILTER", "@price > 50",
            "SCHEMA", "price", "NUMERIC", "name", "TAG"
        ) == b"OK"

        # Insert documents that all pass the filter.
        client.execute_command("HSET", "prod:1", "price", "100", "name", "alpha")
        client.execute_command("HSET", "prod:2", "price", "200", "name", "beta")
        client.execute_command("HSET", "prod:3", "price", "300", "name", "gamma")

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "price_idx")

        def num_docs():
            return int(FTInfoParser(
                client.execute_command("FT.INFO", "price_idx")).num_docs)

        # All three should be indexed.
        result = client.execute_command("FT.SEARCH", "price_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 docs initially, got {result[0]}"
        assert num_docs() == 3, f"num_docs={num_docs()} before mutation"

        # Mutate prod:2 so its price drops below the filter threshold.
        client.execute_command("HSET", "prod:2", "price", "10", "name", "beta")

        # prod:2 should now be removed from the index.
        result = client.execute_command("FT.SEARCH", "price_idx", "@price:[0 +inf]")
        assert result[0] == 2, f"Expected 2 docs after mutation, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"prod:1", b"prod:3"}
        # An overwrite that fails the filter must un-track the key, not merely
        # empty its attributes: a key counted as a document but absent from
        # every index is what inflates num_docs and puts the key into the RDB.
        assert num_docs() == 2, (
            f"num_docs={num_docs()} after an overwrite failed the filter; "
            "the rejected key is still tracked as a document"
        )

        # Verify prod:2 is specifically not found.
        result = client.execute_command("FT.SEARCH", "price_idx", "@name:{beta}")
        assert result[0] == 0, "prod:2 should not be in the index after failing the filter"

        # Mutate prod:2 back above the threshold — it should re-enter the index.
        client.execute_command("HSET", "prod:2", "price", "250", "name", "beta")

        result = client.execute_command("FT.SEARCH", "price_idx", "@price:[0 +inf]")
        assert result[0] == 3, f"Expected 3 docs after re-qualifying mutation, got {result[0]}"
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"prod:1", b"prod:2", b"prod:3"}
        assert num_docs() == 3, f"num_docs={num_docs()} after re-qualifying"

    def test_filter_with_string_comparison(self):
        """Test FILTER with string-based expressions."""
        client: Valkey = self.server.get_new_client()

        # Only index documents where category is 'electronics'.
        assert client.execute_command(
            "FT.CREATE", "cat_idx",
            "ON", "HASH",
            "PREFIX", "1", "doc:",
            "FILTER", "@category=='electronics'",
            "SCHEMA", "category", "TAG", "rating", "NUMERIC"
        ) == b"OK"

        client.execute_command("HSET", "doc:1", "category", "electronics", "rating", "5")
        client.execute_command("HSET", "doc:2", "category", "books", "rating", "4")
        client.execute_command("HSET", "doc:3", "category", "electronics", "rating", "3")
        client.execute_command("HSET", "doc:4", "category", "clothing", "rating", "5")

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "cat_idx")

        result = client.execute_command("FT.SEARCH", "cat_idx", "@rating:[0 +inf]")
        assert result[0] == 2
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"doc:1", b"doc:3"}

        # Mutate doc:1 to a non-electronics category — should be removed.
        client.execute_command("HSET", "doc:1", "category", "books", "rating", "5")

        result = client.execute_command("FT.SEARCH", "cat_idx", "@rating:[0 +inf]")
        assert result[0] == 1
        assert result[1] == b"doc:3"

    def test_filter_preexisting_data(self):
        """Test that FILTER is applied during backfill of pre-existing data."""
        client: Valkey = self.server.get_new_client()

        # Insert data BEFORE creating the index.
        client.execute_command("HSET", "pre:1", "status", "active", "val", "10")
        client.execute_command("HSET", "pre:2", "status", "disabled", "val", "20")
        client.execute_command("HSET", "pre:3", "status", "active", "val", "30")

        # Now create an index with a filter on that data.
        assert client.execute_command(
            "FT.CREATE", "backfill_idx",
            "ON", "HASH",
            "PREFIX", "1", "pre:",
            "FILTER", "@status=='active'",
            "SCHEMA", "status", "TAG", "val", "NUMERIC"
        ) == b"OK"

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "backfill_idx")

        # Only the active docs should have been indexed during backfill.
        result = client.execute_command("FT.SEARCH", "backfill_idx", "@val:[0 +inf]")
        assert result[0] == 2
        returned_keys = {result[i] for i in range(1, len(result), 2)}
        assert returned_keys == {b"pre:1", b"pre:3"}

    def test_filter_non_numeric_value_against_numeric_literal(self):
        """A value that is not a number, compared against a numeric literal,
        is unordered: != is true and every other comparison is false.

        This replaces a test of filter_numeric_conversion_failures, a counter
        removed when FILTER stopped parsing NUMERIC fields to doubles. The
        field here is undeclared so the value reaches the filter as raw bytes
        and the key is not dropped by the invalid-data rule first, which is
        what makes the comparison observable at all.
        """
        client: Valkey = self.server.get_new_client()

        def keys_for(index, flt):
            assert client.execute_command(
                "FT.CREATE", index, "ON", "HASH", "PREFIX", "1", "conv:",
                "FILTER", flt, "SCHEMA", "price", "NUMERIC"
            ) == b"OK"
            IndexingTestHelper.wait_for_backfill_complete_on_node(client, index)
            res = client.execute_command(
                "FT.SEARCH", index, "@price:[-inf +inf]", "NOCONTENT")
            return {res[i] for i in range(1, len(res))}

        client.execute_command("HSET", "conv:num", "price", "1", "memo", "12")
        client.execute_command("HSET", "conv:bad", "price", "2", "memo", "abc")

        # "abc" is not a number, so it is excluded from every ordered
        # comparison and from equality...
        for flt in ["@memo > 5", "@memo < 5", "@memo >= 5",
                    "@memo <= 5", "@memo == 5"]:
            idx = "c" + flt.replace("@memo", "").replace(" ", "").replace(
                ">", "gt").replace("<", "lt").replace("=", "e")
            keys = keys_for(idx, flt)
            assert b"conv:bad" not in keys, f"{flt!r} admitted the non-numeric value: {keys}"

        # ... but != is true for it, which is what distinguishes this from a
        # plain false and from a byte-order comparison.
        keys = keys_for("c_ne", "@memo != 5")
        assert keys == {b"conv:num", b"conv:bad"}, f"got {keys}"

    def test_filter_undeclared_hash_field(self):
        """For a HASH index, a FILTER may reference a field that is not declared
        in the schema; its value is read directly off the key."""
        client: Valkey = self.server.get_new_client()

        # Schema declares only `price`; the FILTER references undeclared `secret`.
        assert client.execute_command(
            "FT.CREATE", "u_idx",
            "ON", "HASH",
            "PREFIX", "1", "u:",
            "FILTER", "@secret == 'yes'",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        client.execute_command("HSET", "u:1", "price", "100", "secret", "yes")
        client.execute_command("HSET", "u:2", "price", "200", "secret", "no")
        client.execute_command("HSET", "u:3", "price", "300")  # no secret -> false
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "u_idx")

        # u:3 has no `secret` at all, so the comparison is false and the key is
        # not admitted -- a missing field excludes rather than being unknown.
        result = client.execute_command("FT.SEARCH", "u_idx", "@price:[0 +inf]", "NOCONTENT")
        keys = {result[i] for i in range(1, len(result))}
        assert keys == {b"u:1"}, f"got {keys}"

    def test_filter_undeclared_hash_field_numeric_promotion(self):
        """An undeclared HASH field compared against a numeric literal is
        promoted to a number (not compared lexically)."""
        client: Valkey = self.server.get_new_client()

        assert client.execute_command(
            "FT.CREATE", "p_idx",
            "ON", "HASH",
            "PREFIX", "1", "p:",
            "FILTER", "@rank > 50",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        # 100 and 9 discriminate numeric ({100,60}) from string ({9,60}).
        client.execute_command("HSET", "p:100", "price", "1", "rank", "100")
        client.execute_command("HSET", "p:9", "price", "2", "rank", "9")
        client.execute_command("HSET", "p:60", "price", "3", "rank", "60")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "p_idx")

        result = client.execute_command("FT.SEARCH", "p_idx", "@price:[0 +inf]", "NOCONTENT")
        keys = {result[i] for i in range(1, len(result))}
        assert keys == {b"p:100", b"p:60"}, f"numeric promotion expected, got {keys}"

    def test_filter_inequality_rejects_key_without_the_field(self):
        """An inequality over a missing field REJECTS the key: the comparison
        is false, not unknown. A bare negation of that comparison admits it,
        which is the only way a key lacking the field gets in."""
        client: Valkey = self.server.get_new_client()

        assert client.execute_command(
            "FT.CREATE", "neg_idx",
            "ON", "HASH",
            "PREFIX", "1", "neg:",
            "FILTER", "@status != 'active'",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        client.execute_command("HSET", "neg:1", "price", "10", "status", "active")  # excluded
        client.execute_command("HSET", "neg:2", "price", "20", "status", "idle")    # admitted
        client.execute_command("HSET", "neg:3", "price", "30")                       # no status -> rejected
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "neg_idx")

        result = client.execute_command("FT.SEARCH", "neg_idx", "@price:[0 +inf]", "NOCONTENT")
        keys = {result[i] for i in range(1, len(result))}
        assert keys == {b"neg:2"}, f"got {keys}"

        # The same predicate wrapped in a negation admits the key instead,
        # because `@status == 'active'` is false for a key with no status.
        assert client.execute_command(
            "FT.CREATE", "neg2_idx",
            "ON", "HASH",
            "PREFIX", "1", "neg:",
            "FILTER", "!(@status == 'active')",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "neg2_idx")
        result = client.execute_command("FT.SEARCH", "neg2_idx", "@price:[0 +inf]", "NOCONTENT")
        keys = {result[i] for i in range(1, len(result))}
        assert keys == {b"neg:2", b"neg:3"}, f"got {keys}"

    def test_filter_json_undeclared_field_is_rejected(self):
        """For a JSON index, a FILTER referencing an undeclared field is
        rejected at FT.CREATE time (no path to resolve it)."""
        client: Valkey = self.server.get_new_client()

        with pytest.raises(ResponseError, match="not found in index schema"):
            client.execute_command(
                "FT.CREATE", "j_idx",
                "ON", "JSON",
                "PREFIX", "1", "j:",
                "FILTER", "@secret == 'yes'",
                "SCHEMA", "$.price", "AS", "price", "NUMERIC"
            )

    def test_filter_rejected_keys_counter(self):
        """FT.INFO filter_rejected_keys counts keys excluded by the FILTER and
        is the primary signal for a misspelled HASH field name."""
        client: Valkey = self.server.get_new_client()

        assert client.execute_command(
            "FT.CREATE", "rej_idx",
            "ON", "HASH",
            "PREFIX", "1", "rej:",
            "FILTER", "@price > 100",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"

        info = FTInfoParser(client.execute_command("FT.INFO", "rej_idx"))
        assert int(info.filter_rejected_keys) == 0

        client.execute_command("HSET", "rej:pass", "price", "500")   # passes
        client.execute_command("HSET", "rej:fail1", "price", "10")   # rejected
        client.execute_command("HSET", "rej:fail2", "price", "20")   # rejected
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "rej_idx")

        # Only the passing key is indexed.
        result = client.execute_command("FT.SEARCH", "rej_idx", "@price:[0 +inf]", "NOCONTENT")
        keys = {result[i] for i in range(1, len(result))}
        assert keys == {b"rej:pass"}, f"got {keys}"

        # The two rejected keys are counted (>= 2; the counter tracks evaluation
        # events, which may exceed the distinct-key count).
        info = FTInfoParser(client.execute_command("FT.INFO", "rej_idx"))
        assert int(info.filter_rejected_keys) >= 2, (
            f"expected >=2 rejected keys, got {info.filter_rejected_keys}"
        )

    def test_filter_accepted_in_any_pre_schema_position(self):
        """FILTER is a pre-SCHEMA option like SCORE, LANGUAGE and
        SKIPINITIALSCAN, so it must be accepted in any order relative to them.

        It used to be parsed once before the flexible pre-SCHEMA ordering loop,
        so it was only accepted immediately after PREFIX; any other position
        failed with "Unexpected parameter `FILTER`, expecting `SCHEMA`".
        """
        client: Valkey = self.server.get_new_client()

        orderings = {
            "after_prefix": [
                "PREFIX", "1", "ord:", "FILTER", "@price > 100",
                "SCORE", "0.5",
            ],
            "after_score": [
                "PREFIX", "1", "ord:", "SCORE", "0.5",
                "FILTER", "@price > 100",
            ],
            "after_language": [
                "PREFIX", "1", "ord:", "LANGUAGE", "english",
                "FILTER", "@price > 100",
            ],
            "between_options": [
                "PREFIX", "1", "ord:", "SKIPINITIALSCAN",
                "FILTER", "@price > 100", "SCORE", "0.5",
            ],
            # NOTE: no "before PREFIX" case. PREFIX is parsed before the
            # flexible loop, so no pre-SCHEMA option may precede it --
            # `SCORE 0.5 PREFIX ...` and `LANGUAGE english PREFIX ...` fail
            # the same way. That is a pre-existing PREFIX-position
            # limitation shared by every option, not specific to FILTER.
        }

        for name, options in orderings.items():
            index = f"ord_idx_{name}"
            assert client.execute_command(
                "FT.CREATE", index, "ON", "HASH", *options,
                "SCHEMA", "price", "NUMERIC"
            ) == b"OK", f"FT.CREATE rejected FILTER in position '{name}'"

            info = FTInfoParser(client.execute_command("FT.INFO", index))
            assert info.index_definition.get("filter") == "@price > 100", (
                f"filter not stored for ordering '{name}': "
                f"{info.index_definition.get('filter')!r}"
            )
            client.execute_command("FT.DROPINDEX", index)

    def test_filter_empty_expression_rejected_in_any_position(self):
        """An empty FILTER expression is rejected wherever FILTER appears, not
        just in the position the old pre-loop parse handled."""
        client: Valkey = self.server.get_new_client()

        for name, options in {
            "after_prefix": ["PREFIX", "1", "e:", "FILTER", ""],
            "after_score": ["PREFIX", "1", "e:", "SCORE", "0.5", "FILTER", ""],
        }.items():
            with pytest.raises(ResponseError, match="FILTER expression cannot be empty"):
                client.execute_command(
                    "FT.CREATE", f"empty_idx_{name}", "ON", "HASH", *options,
                    "SCHEMA", "price", "NUMERIC"
                )


class TestFTCreateFilterSaveRestore(ValkeySearchTestCaseDebugMode):
    """The FILTER expression must survive an RDB round trip.

    The expression lives only in the index schema, not in any indexed key, so
    if it were dropped from the RDB the index would come back looking healthy
    while silently admitting everything it used to reject.
    """

    def test_filter_survives_rdb_reload(self):
        client: Valkey = self.server.get_new_client()

        assert client.execute_command(
            "FT.CREATE", "rdb_idx",
            "ON", "HASH",
            "PREFIX", "1", "rdb:",
            "FILTER", "@price > 100",
            "SCHEMA", "price", "NUMERIC", "name", "TAG"
        ) == b"OK"

        client.execute_command("HSET", "rdb:keep1", "price", "500", "name", "a")
        client.execute_command("HSET", "rdb:keep2", "price", "300", "name", "b")
        client.execute_command("HSET", "rdb:drop1", "price", "10",  "name", "c")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "rdb_idx")

        def indexed_keys():
            res = client.execute_command(
                "FT.SEARCH", "rdb_idx", "@price:[-inf +inf]", "NOCONTENT")
            return {res[i] for i in range(1, len(res))}

        before = indexed_keys()
        assert before == {b"rdb:keep1", b"rdb:keep2"}, f"before reload: {before}"

        info = FTInfoParser(client.execute_command("FT.INFO", "rdb_idx"))
        assert info.index_definition.get("filter") == "@price > 100"

        # Round trip through the RDB.
        client.execute_command("SAVE")
        client.execute_command("DEBUG", "RELOAD")

        # The index is still there, still carrying its expression.
        assert b"rdb_idx" in client.execute_command("FT._LIST")
        info = FTInfoParser(client.execute_command("FT.INFO", "rdb_idx"))
        assert info.index_definition.get("filter") == "@price > 100", (
            f"filter lost across reload: {info.index_definition.get('filter')!r}"
        )

        # The same documents are indexed, and the rejected one did not sneak
        # back in as part of the reload's own backfill.
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "rdb_idx")
        after = indexed_keys()
        assert after == before, f"after reload: {after}, before: {before}"

        # The restored filter is live, not just recorded: it must still reject
        # and still admit on writes made after the reload.
        client.execute_command("HSET", "rdb:drop2", "price", "20",  "name", "d")
        client.execute_command("HSET", "rdb:keep3", "price", "900", "name", "e")
        assert indexed_keys() == {b"rdb:keep1", b"rdb:keep2", b"rdb:keep3"}

    def test_filter_marks_rdb_minimum_version_1_3_0(self):
        """An RDB holding a FILTER index must record a 1.3.0 minimum.

        FT.CREATE FILTER is new in 1.3.0. A 1.2 module does not know the proto
        field, so without this it would load the index, silently ignore the
        filter, and index every key the filter exists to exclude -- a wrong
        index rather than a refused load. RDBSave writes
        max(GetMinVersion(section)) into the header and the loader refuses when
        that exceeds its own kModuleVersion (rdb_serialization.cc), so
        recording 1.3.0 is what makes the older module reject the file.

        The recorded value is what this asserts. It cannot be demonstrated by
        setting search.emulate-release to 1.2.0: the load gate compares against
        the compile-time kModuleVersion, which emulate-release does not move.
        Refusal itself is exercised by test_versioning.py, which forces a
        too-new minimum through the override_min_version debug variable.
        """
        client: Valkey = self.server.get_new_client()
        primary = self.rg.primary

        # Control first: without a FILTER this schema needs nothing past 1.0.0.
        # Asserted before the filtered index exists, because the log line
        # reports the maximum over every section in that save.
        assert client.execute_command(
            "FT.CREATE", "ver_plain",
            "ON", "HASH", "PREFIX", "1", "ver:",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        client.execute_command("SAVE")
        assert primary.does_logfile_contains(
            "ValkeySearch RDB sections with minimum version 1.0.0"), (
            "an unfiltered NUMERIC-only index should not require 1.3.0"
        )
        assert not primary.does_logfile_contains(
            "ValkeySearch RDB sections with minimum version 1.3.0"), (
            "1.3.0 was recorded before any FILTER index existed"
        )

        client.execute_command("FT.DROPINDEX", "ver_plain")

        # Same schema, now with a FILTER: the RDB must demand 1.3.0.
        assert client.execute_command(
            "FT.CREATE", "ver_filtered",
            "ON", "HASH", "PREFIX", "1", "ver:",
            "FILTER", "@price > 100",
            "SCHEMA", "price", "NUMERIC"
        ) == b"OK"
        client.execute_command("SAVE")
        assert primary.does_logfile_contains(
            "ValkeySearch RDB sections with minimum version 1.3.0"), (
            "a FILTER index did not raise the RDB minimum version to 1.3.0"
        )

    def test_filter_survives_rdb_reload_hash_aliased(self):
        """Same round trip for a HASH index whose filter references an alias.

        Separate from the non-aliased case above because only this one can
        catch a filter that reloads as an undeclared-field read: with
        `status AS st`, resolving `@st` against the key instead of the schema
        looks for a hash member literally named "st", finds nothing, and the
        filter silently stops rejecting anything.
        """
        client: Valkey = self.server.get_new_client()

        assert client.execute_command(
            "FT.CREATE", "rdb_alias_idx",
            "ON", "HASH",
            "PREFIX", "1", "ra:",
            "FILTER", "@st == 'active'",
            "SCHEMA", "status", "AS", "st", "TAG", "price", "AS", "pr", "NUMERIC"
        ) == b"OK"

        client.execute_command("HSET", "ra:keep", "status", "active",   "price", "500")
        client.execute_command("HSET", "ra:drop", "status", "inactive", "price", "300")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "rdb_alias_idx")

        def indexed_keys():
            res = client.execute_command(
                "FT.SEARCH", "rdb_alias_idx", "@pr:[-inf +inf]", "NOCONTENT")
            return {res[i] for i in range(1, len(res))}

        assert indexed_keys() == {b"ra:keep"}

        client.execute_command("SAVE")
        client.execute_command("DEBUG", "RELOAD")

        info = FTInfoParser(client.execute_command("FT.INFO", "rdb_alias_idx"))
        assert info.index_definition.get("filter") == "@st == 'active'"
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "rdb_alias_idx")
        assert indexed_keys() == {b"ra:keep"}, (
            "filter stopped rejecting after reload -- the alias no longer "
            "resolves to the schema attribute"
        )

    def test_filter_survives_rdb_reload_json(self):
        """Same round trip for a JSON index, whose filter references an alias."""
        client: Valkey = self.server.get_new_client()

        assert client.execute_command(
            "FT.CREATE", "rdb_json_idx",
            "ON", "JSON",
            "PREFIX", "1", "rj:",
            "FILTER", "@pr > 100",
            "SCHEMA", "$.price", "AS", "pr", "NUMERIC"
        ) == b"OK"

        client.execute_command("JSON.SET", "rj:keep", "$", '{"price":500}')
        client.execute_command("JSON.SET", "rj:drop", "$", '{"price":10}')
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "rdb_json_idx")

        def indexed_keys():
            res = client.execute_command(
                "FT.SEARCH", "rdb_json_idx", "@pr:[-inf +inf]", "NOCONTENT")
            return {res[i] for i in range(1, len(res))}

        assert indexed_keys() == {b"rj:keep"}

        client.execute_command("SAVE")
        client.execute_command("DEBUG", "RELOAD")

        info = FTInfoParser(client.execute_command("FT.INFO", "rdb_json_idx"))
        assert info.index_definition.get("filter") == "@pr > 100"
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "rdb_json_idx")
        assert indexed_keys() == {b"rj:keep"}

        client.execute_command("JSON.SET", "rj:drop2", "$", '{"price":20}')
        client.execute_command("JSON.SET", "rj:keep2", "$", '{"price":900}')
        assert indexed_keys() == {b"rj:keep", b"rj:keep2"}
