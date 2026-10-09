"""
Integration tests for FT.SEARCH INFIELDS.

Happy-path coverage lives in `TestInfieldsCompatibility` (compatibility suite,
parametrized over hash + json).

Kept here:
- Pure-KNN interaction (INFIELDS is inert for vector-only queries)
- Error-path tests (invalid count, non-existent fields, non-TEXT fields,
  explicit @field not in INFIELDS)
"""

import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from utils import IndexingTestHelper
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFTSearchInfields(ValkeySearchTestCaseBase):
    # ---- module-specific: INFIELDS + pure-KNN ----

    def test_infields_with_pure_vector_query(self):
        """INFIELDS parsed but inert for pure KNN vector queries."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "vec_idx",
            "ON", "HASH",
            "PREFIX", "1", "vec:",
            "SCHEMA",
            "title", "TEXT",
            "emb", "VECTOR", "FLAT", "6",
            "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
        )
        client.execute_command("HSET", "vec:1", "title", "hello",
                               "emb", struct.pack("2f", 1.0, 0.0))
        client.execute_command("HSET", "vec:2", "title", "world",
                               "emb", struct.pack("2f", 0.0, 1.0))

        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "vec_idx")

        query_vec = struct.pack("2f", 1.0, 0.0)
        result = client.execute_command(
            "FT.SEARCH", "vec_idx",
            "*=>[KNN 2 @emb $v]",
            "PARAMS", "2", "v", query_vec,
            "INFIELDS", "1", "title",
            "NOCONTENT", "DIALECT", "2",
        )
        assert result[0] == 2

    # ---- INFIELDS error handling ----

    def test_infields_error_bad_count(self):
        """INFIELDS with non-integer count raises an error."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "err_idx",
            "ON", "HASH",
            "PREFIX", "1", "err:",
            "SCHEMA",
            "title", "TEXT",
        )
        client.execute_command("HSET", "err:1", "title", "apple banana")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "err_idx")

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "err_idx", "apple",
                "INFIELDS", "abc",
                "DIALECT", "2",
            )

    def test_infields_error_negative_count(self):
        """INFIELDS -1 raises an error."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "err_idx2",
            "ON", "HASH",
            "PREFIX", "1", "err2:",
            "SCHEMA",
            "title", "TEXT",
        )
        client.execute_command("HSET", "err2:1", "title", "apple banana")
        IndexingTestHelper.wait_for_backfill_complete_on_node(
            client, "err_idx2")

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "err_idx2", "apple",
                "INFIELDS", "-1",
                "DIALECT", "2",
            )

    def test_infields_error_count_mismatch(self):
        """INFIELDS count > actual field args raises an error."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "err_idx3",
            "ON", "HASH",
            "PREFIX", "1", "err3:",
            "SCHEMA",
            "title", "TEXT",
        )
        client.execute_command("HSET", "err3:1", "title", "apple banana")
        IndexingTestHelper.wait_for_backfill_complete_on_node(
            client, "err_idx3")

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "err_idx3", "apple",
                "INFIELDS", "5", "f1", "f2",
                "DIALECT", "2",
            )

    # ---- INFIELDS field validation errors (diverges from Redis Stack) ----

    def test_infields_nonexistent_field_errors(self):
        """INFIELDS with a non-existent field name raises an error."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "val_idx",
            "ON", "HASH",
            "PREFIX", "1", "val:",
            "SCHEMA",
            "title", "TEXT",
        )
        client.execute_command("HSET", "val:1", "title", "apple banana")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "val_idx")

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "val_idx", "apple",
                "INFIELDS", "1", "nonexistent_field",
                "DIALECT", "2",
            )

    def test_infields_non_text_field_errors(self):
        """INFIELDS with a non-TEXT field (e.g., NUMERIC) raises an error."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "val_idx2",
            "ON", "HASH",
            "PREFIX", "1", "val2:",
            "SCHEMA",
            "title", "TEXT",
            "price", "NUMERIC",
        )
        client.execute_command("HSET", "val2:1", "title", "apple", "price", "5")
        IndexingTestHelper.wait_for_backfill_complete_on_node(
            client, "val_idx2")

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "val_idx2", "apple",
                "INFIELDS", "1", "price",
                "DIALECT", "2",
            )

    def test_infields_explicit_field_not_in_infields_errors(self):
        """@field:term where field is not in INFIELDS list raises an error."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "val_idx3",
            "ON", "HASH",
            "PREFIX", "1", "val3:",
            "SCHEMA",
            "title", "TEXT",
            "body", "TEXT",
        )
        client.execute_command("HSET", "val3:1", "title", "apple",
                               "body", "banana")
        IndexingTestHelper.wait_for_backfill_complete_on_node(
            client, "val_idx3")

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "val_idx3", "@title:apple",
                "INFIELDS", "1", "body",
                "DIALECT", "2",
            )

    # ---- INFIELDS with other query-language forms (checklist items 6, 9) ----

    def _make_two_field_index(self, client: Valkey, name="scope_idx",
                              prefix="scope:"):
        client.execute_command(
            "FT.CREATE", name,
            "ON", "HASH",
            "PREFIX", "1", prefix,
            "SCHEMA",
            "title", "TEXT", "WITHSUFFIXTRIE",
            "body", "TEXT", "WITHSUFFIXTRIE",
        )
        return name

    def test_infields_suffix_query(self):
        """INFIELDS scopes a suffix (*suffix) query to the named field only."""
        client: Valkey = self.server.get_new_client()
        idx = self._make_two_field_index(client)
        client.execute_command("HSET", "scope:1", "title", "running",
                               "body", "other")
        client.execute_command("HSET", "scope:2", "title", "other",
                               "body", "running")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, idx)

        # Unscoped: both docs match (one via title, one via body).
        result = client.execute_command(
            "FT.SEARCH", idx, "*ning", "DIALECT", "2")
        assert result[0] == 2

        # INFIELDS title: only scope:1 (title="running") matches.
        result = client.execute_command(
            "FT.SEARCH", idx, "*ning",
            "INFIELDS", "1", "title", "NOCONTENT", "DIALECT", "2")
        assert result[0] == 1
        assert result[1] == b"scope:1"

    def test_infields_fuzzy_query(self):
        """INFIELDS scopes a fuzzy (%term%) query to the named field only."""
        client: Valkey = self.server.get_new_client()
        idx = self._make_two_field_index(client)
        client.execute_command("HSET", "scope:1", "title", "great",
                               "body", "other")
        client.execute_command("HSET", "scope:2", "title", "other",
                               "body", "great")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, idx)

        # Unscoped fuzzy match on "greet" (ED=1 from "great"): both docs.
        result = client.execute_command(
            "FT.SEARCH", idx, "%greet%", "DIALECT", "2")
        assert result[0] == 2

        # INFIELDS body: only scope:2 (body="great") matches.
        result = client.execute_command(
            "FT.SEARCH", idx, "%greet%",
            "INFIELDS", "1", "body", "NOCONTENT", "DIALECT", "2")
        assert result[0] == 1
        assert result[1] == b"scope:2"

    def test_infields_exact_phrase_query(self):
        """INFIELDS scopes an exact-phrase ("...") query to the named field."""
        client: Valkey = self.server.get_new_client()
        idx = self._make_two_field_index(client)
        client.execute_command("HSET", "scope:1", "title", "hello world",
                               "body", "other content")
        client.execute_command("HSET", "scope:2", "title", "other content",
                               "body", "hello world")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, idx)

        # Unscoped: both docs contain the exact phrase "hello world".
        result = client.execute_command(
            "FT.SEARCH", idx, '"hello world"', "DIALECT", "2")
        assert result[0] == 2

        # INFIELDS title: only scope:1 (title contains the phrase) matches.
        result = client.execute_command(
            "FT.SEARCH", idx, '"hello world"',
            "INFIELDS", "1", "title", "NOCONTENT", "DIALECT", "2")
        assert result[0] == 1
        assert result[1] == b"scope:1"

    def test_infields_field_scoped_group_syntax(self):
        """INFIELDS combined with a field-scoped group @title:(a|b): the
        explicit @title: scope must itself be inside the INFIELDS set, and
        matching still occurs only within that field."""
        client: Valkey = self.server.get_new_client()
        idx = self._make_two_field_index(client)
        client.execute_command("HSET", "scope:1", "title", "alpha",
                               "body", "unrelated")
        client.execute_command("HSET", "scope:2", "title", "beta",
                               "body", "unrelated")
        client.execute_command("HSET", "scope:3", "title", "gamma",
                               "body", "alpha")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, idx)

        # @title:(alpha|beta) with INFIELDS naming title (the same field the
        # group already scopes to): matches scope:1 and scope:2 only. scope:3
        # has "alpha" in body, not title, so it is excluded either way.
        result = client.execute_command(
            "FT.SEARCH", idx, "@title:(alpha|beta)",
            "INFIELDS", "1", "title", "NOCONTENT", "DIALECT", "2")
        assert result[0] == 2
        assert set(result[1:]) == {b"scope:1", b"scope:2"}

        # @title:(alpha|beta) with INFIELDS naming a *different* field (body,
        # which is not referenced by the group) is a mismatch between the
        # explicit scope and the INFIELDS set and must error.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", idx, "@title:(alpha|beta)",
                "INFIELDS", "1", "body", "DIALECT", "2")

    def test_infields_does_not_change_scores(self):
        """INFIELDS narrows which fields are searched but must not change the
        relevance score of documents that still match (checklist item 8)."""
        client: Valkey = self.server.get_new_client()
        idx = self._make_two_field_index(client)
        client.execute_command("HSET", "scope:1", "title", "apple apple",
                               "body", "unrelated text here")
        client.execute_command("HSET", "scope:2", "title", "apple",
                               "body", "banana")
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, idx)

        def scores(*extra_args):
            result = client.execute_command(
                "FT.SEARCH", idx, "@title:apple", *extra_args,
                "WITHSCORES", "NOCONTENT", "DIALECT", "2")
            count, pairs = result[0], {}
            # WITHSCORES + NOCONTENT layout: [count, key, score, key, score, ...]
            for i in range(1, len(result), 2):
                key = result[i].decode()
                pairs[key] = float(result[i + 1])
            assert len(pairs) == count
            return pairs

        without_infields = scores()
        with_infields = scores("INFIELDS", "1", "title")

        assert without_infields.keys() == with_infields.keys()
        for key in without_infields:
            assert without_infields[key] == pytest.approx(with_infields[key]), (
                f"INFIELDS changed the score for {key}: "
                f"{without_infields[key]} -> {with_infields[key]}"
            )


