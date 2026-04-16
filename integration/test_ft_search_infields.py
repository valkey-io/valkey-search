"""
Integration tests for FT.SEARCH INFIELDS option.

INFIELDS restricts full-text query term matching to a caller-specified set of
indexed TEXT fields.
"""

import json as _json
import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


def _setup_index_and_docs(client):
    """Create an index with multiple TEXT fields and populate test documents."""
    client.execute_command(
        "FT.CREATE", "infields_idx",
        "ON", "HASH",
        "PREFIX", "1", "doc:",
        "SCHEMA",
        "title", "TEXT",
        "body", "TEXT",
        "category", "TAG",
        "price", "NUMERIC",
    )
    client.execute_command("HSET", "doc:1",
                           "title", "python programming",
                           "body", "learn to code",
                           "category", "tech",
                           "price", "29")
    client.execute_command("HSET", "doc:2",
                           "title", "advanced coding",
                           "body", "python is great",
                           "category", "tech",
                           "price", "49")
    client.execute_command("HSET", "doc:3",
                           "title", "python tutorial",
                           "body", "python examples",
                           "category", "education",
                           "price", "19")


class TestFTSearchInfields(ValkeySearchTestCaseBase):

    
    # Core scoping behaviour
    

    def test_infields_single_field_scoping(self):
        """INFIELDS restricts term matching to the named field."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        # "programming" only in title
        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "programming",
            "INFIELDS", "1", "title", "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 1
        assert b"doc:1" in result

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "programming",
            "INFIELDS", "1", "body", "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 0

    def test_infields_multiple_fields(self):
        """INFIELDS 2 title body — union of both fields."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "2", "title", "body", "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 3
        assert b"doc:1" in result
        assert b"doc:2" in result
        assert b"doc:3" in result

    def test_infields_nonexistent_field_returns_empty(self):
        """Non-existent / non-TEXT INFIELDS fields silently produce no matches."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "1", "nonexistent_field", "DIALECT", "2"
        )
        assert result[0] == 0

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "1", "category", "DIALECT", "2"
        )
        assert result[0] == 0

    
    # Option composition (LIMIT, NOCONTENT, SORTBY, RETURN)
    

    def test_infields_with_limit_and_nocontent(self):
        """INFIELDS combined with LIMIT and NOCONTENT."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "2", "title", "body",
            "LIMIT", "0", "2", "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 3   # total count
        assert len(result) == 3  # count + 2 keys

    def test_infields_with_sortby_and_return(self):
        """INFIELDS combined with SORTBY and RETURN."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "1", "title",
            "SORTBY", "price", "ASC",
            "RETURN", "1", "title", "DIALECT", "2"
        )
        assert result[0] >= 1
        for i in range(1, len(result), 2):
            fields = result[i + 1]
            field_dict = dict(zip(fields[::2], fields[1::2]))
            assert b"title" in field_dict
            assert b"body" not in field_dict

    
    # Interaction with filter predicates
    

    def test_infields_with_tag_and_numeric_filter(self):
        """INFIELDS combined with tag + numeric filter expression."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx",
            "python @category:{tech} @price:[20 50]",
            "INFIELDS", "1", "body", "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 1
        assert b"doc:2" in result

    def test_infields_non_text_predicate_only(self):
        """INFIELDS does not affect non-text predicates — numeric/tag still match."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx",
            "@price:[20 50]",
            "INFIELDS", "1", "title", "NOCONTENT", "DIALECT", "2"
        )
        # INFIELDS only scopes text terms; pure numeric predicates are unaffected.
        assert result[0] == 2  # doc:1 (price=29) and doc:2 (price=49)

    def test_infields_zero_count_noop(self):
        """INFIELDS 0 is a no-op — searches all text fields."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        # INFIELDS 0 should behave identically to no INFIELDS at all.
        result_with_infields_0 = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "0", "NOCONTENT", "DIALECT", "2"
        )
        result_without_infields = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "NOCONTENT", "DIALECT", "2"
        )
        assert result_with_infields_0[0] == result_without_infields[0]
        assert set(result_with_infields_0[1:]) == set(result_without_infields[1:])

    def test_infields_count_mismatch_error(self):
        """INFIELDS count > actual field args → error."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "infields_idx", "python",
                "INFIELDS", "5", "f1", "f2", "DIALECT", "2"
            )

    def test_infields_non_integer_count_error(self):
        """INFIELDS abc → error."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "infields_idx", "python",
                "INFIELDS", "abc", "DIALECT", "2"
            )

    def test_infields_with_pure_vector_query(self):
        """INFIELDS parsed but silently ignored for pure KNN vector queries."""
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

        query_vec = struct.pack("2f", 1.0, 0.0)
        result = client.execute_command(
            "FT.SEARCH", "vec_idx",
            "*=>[KNN 2 @emb $v]",
            "PARAMS", "2", "v", query_vec,
            "INFIELDS", "1", "title",
            "NOCONTENT", "DIALECT", "2",
        )
        assert result[0] == 2

    def test_infields_json_index(self):
        """INFIELDS scoping on JSON data type."""
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "json_idx",
            "ON", "JSON",
            "PREFIX", "1", "jdoc:",
            "SCHEMA",
            "$.title", "AS", "title", "TEXT",
            "$.body", "AS", "body", "TEXT",
        )
        client.execute_command(
            "JSON.SET", "jdoc:1", "$",
            _json.dumps({"title": "valkey search", "body": "learn databases"})
        )
        client.execute_command(
            "JSON.SET", "jdoc:2", "$",
            _json.dumps({"title": "learn python", "body": "valkey is fast"})
        )

        result = client.execute_command(
            "FT.SEARCH", "json_idx", "valkey",
            "INFIELDS", "1", "title", "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 1
        assert b"jdoc:1" in result

        result = client.execute_command(
            "FT.SEARCH", "json_idx", "valkey",
            "INFIELDS", "1", "body", "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 1
        assert b"jdoc:2" in result

    def test_infields_negative_count_error(self):
        """INFIELDS -1 error."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.SEARCH", "infields_idx", "python",
                "INFIELDS", "-1", "DIALECT", "2"
            )

    def test_infields_duplicate_fields(self):
        """Duplicate field names are deduped — result count is not doubled."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result_dedup = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "3", "title", "title", "title",
            "NOCONTENT", "DIALECT", "2"
        )
        result_single = client.execute_command(
            "FT.SEARCH", "infields_idx", "python",
            "INFIELDS", "1", "title",
            "NOCONTENT", "DIALECT", "2"
        )
        assert result_dedup[0] == result_single[0]
        assert set(result_dedup[1:]) == set(result_single[1:])

    def test_infields_with_verbatim(self):
        """INFIELDS combined with VERBATIM — no stemming applied."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "programming",
            "INFIELDS", "1", "title", "VERBATIM",
            "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 1
        assert b"doc:1" in result

    def test_infields_with_slop_inorder(self):
        """INFIELDS combined with SLOP and INORDER."""
        client: Valkey = self.server.get_new_client()
        _setup_index_and_docs(client)

        result = client.execute_command(
            "FT.SEARCH", "infields_idx", "python tutorial",
            "INFIELDS", "1", "title",
            "SLOP", "0", "INORDER",
            "NOCONTENT", "DIALECT", "2"
        )
        assert result[0] == 1
        assert b"doc:3" in result
