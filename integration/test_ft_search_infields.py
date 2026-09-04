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
