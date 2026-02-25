import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestQueryParser(ValkeySearchTestCaseBase):

    def test_query_string_bytes_limit(self):
        """
            Test the query string bytes depth limit in Valkey Search using Vector based queries.
        """
        client: Valkey = self.server.get_new_client()
        # Test that the default query string limit is 10240
        assert client.execute_command("CONFIG GET search.query-string-bytes") == [b"search.query-string-bytes", b"10240"]
        assert client.execute_command("FT.CREATE my_index ON HASH PREFIX 1 doc: SCHEMA price NUMERIC category TAG SEPARATOR | doc_embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE") == b"OK"
        query = "@price:[10 20] =>[KNN 10 @doc_embedding $BLOB]"
        command_args = [
            "FT.SEARCH", "my_index",
            query,
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ]
        # Validate the query strings above the limit are rejected.
        assert client.execute_command(f"CONFIG SET search.query-string-bytes {len(query) - 1}") == b"OK"
        with pytest.raises(Exception, match="Query string is too long, max length is 45 bytes"):
            client.execute_command(*command_args)
        # Validate the query strings within the limit are rejected.
        assert client.execute_command(f"CONFIG SET search.query-string-bytes {len(query)}") == b"OK"
        assert client.execute_command(*command_args) == [0]

    def test_query_string_depth_limit(self):
        """
            Test the query string recursive depth limit in Valkey Search using Vector based queries.
        """
        client: Valkey = self.server.get_new_client()
        # Test that the default query string limit is 1000
        default_limit = b"1000"
        assert client.execute_command("CONFIG GET search.query-string-depth") == [b"search.query-string-depth", b"1000"]
        # Test that we can set the query string limit to 1
        assert client.execute_command("CONFIG SET search.query-string-depth 1") == b"OK"
        assert client.execute_command("FT.CREATE my_index ON HASH PREFIX 1 doc: SCHEMA price NUMERIC category TAG SEPARATOR | doc_embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE") == b"OK"
        # Validate the success case with a query of depth 1 (no nested parentheses).
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "@price:[10 20] =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]

        assert client.execute_command("CONFIG SET search.query-string-depth 2") == b"OK"
        # Validate another success case at depth 1 (OR at same level, no nesting).
        # With n-ary structure, OR operations at the same level don't increase depth.
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "@price:[10 20] | @category:{electronics|books} =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]
        # Set depth limit to 1 to test that depth-2 query fails
        assert client.execute_command("CONFIG SET search.query-string-depth 1") == b"OK"
        # Validate the failure case with a query of depth 2 (nested parentheses).
        # Parentheses cause a recursive ParseExpression call, increasing depth.
        try:
            client.execute_command(
                "FT.SEARCH", "my_index",
                "(@price:[10 20]) =>[KNN 10 @doc_embedding $BLOB]",
                "PARAMS", 2, "BLOB", "<your_vector_blob>",
                "RETURN", 1, "doc_embedding"
            )
            assert False
        except ResponseError as e:
            assert "Query string is too complex" in str(e)
        # Validate the success case with a query depth of 10.
        assert client.execute_command("CONFIG SET search.query-string-depth 10") == b"OK"
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "(((((((((@price:[10 20]))))))))) =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "((((((((@price:[10 20] | @category:{electronics|books})))))))) =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]
        # Validate the failure case with a query of depth 11.
        try:
            client.execute_command(
                "FT.SEARCH", "my_index",
                "((((((((((@price:[10 20])))))))))) =>[KNN 10 @doc_embedding $BLOB]",
                "PARAMS", 2, "BLOB", "<your_vector_blob>",
                "RETURN", 1, "doc_embedding"
            )
            assert False
        except ResponseError as e:
            assert str(e) == "Invalid filter expression: `((((((((((@price:[10 20]))))))))))`. Query string is too complex"
        # Test that the config ranges from 1 to 4294967295
        try:
            client.execute_command("CONFIG SET search.query-string-depth 0")
            assert False
        except ResponseError as e:
            assert "argument must be between 1 and 4294967295 inclusive" in str(e)
        try:
            client.execute_command("CONFIG SET search.query-string-depth 4294967296")
            assert False
        except ResponseError as e:
            assert "argument must be between 1 and 4294967295 inclusive" in str(e)
            
    def test_query_string_terms_count_limit(self):
        """
            Test the query string terms count limit in Valkey Search using Vector based queries.
        """
        client: Valkey = self.server.get_new_client()
        default_limit = b"1000"
        max_limit = b"10000"
        # Test that the default query string terms count limit is expected default_limit
        assert client.execute_command("CONFIG GET search.query-string-terms-count") == [b"search.query-string-terms-count", default_limit]
        # Create an index for testing
        assert client.execute_command("FT.CREATE my_index ON HASH PREFIX 1 doc: SCHEMA price NUMERIC category TAG SEPARATOR | doc_embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE") == b"OK"
        
        # Test that we can set the query string terms count limit to 5
        assert client.execute_command("CONFIG SET search.query-string-terms-count 5") == b"OK"
        
        # Validate the success case with a query having 4 terms
        # Query: @price:[10 20] @price:[30 40] @price:[50 60]
        # This creates: 3 predicates + 1 AND composed predicate = 4 terms total (within limit of 5)
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "@price:[10 20] @price:[30 40] @price:[50 60] =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]
        
        # Validate the failure case with a query having 7 terms (exceeds limit of 5)
        # Query: @price:[10 20] | @price:[30 40] | @price:[50 60] | @category:{books} | @category:{electronics} | @price:[70 80]
        # N-ary structure flattens all ORs into one node: OR(pred1, pred2, pred3, pred4, pred5, pred6)
        # This creates: 6 predicates + 1 OR composed predicate = 7 terms total (exceeds limit)
        try:
            client.execute_command(
                "FT.SEARCH", "my_index",
                "@price:[10 20] | @price:[30 40] | @price:[50 60] | @category:{books} | @category:{electronics} | @price:[70 80] =>[KNN 10 @doc_embedding $BLOB]",
                "PARAMS", 2, "BLOB", "<your_vector_blob>",
                "RETURN", 1, "doc_embedding"
            )
            assert False
        except ResponseError as e:
            assert "Query string is too complex: max number of terms can't exceed 5" in str(e)
        
        # Test with a higher limit (10 terms)
        assert client.execute_command("CONFIG SET search.query-string-terms-count 10") == b"OK"
        
        # Query: @price:[10 20] @price:[30 40] @price:[50 60] @price:[70 80]
        # This creates: 4 predicates + 1 AND composed predicate = 5 terms total (within limit of 10)
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "@price:[10 20] @price:[30 40] @price:[50 60] @price:[70 80] =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]
        
        # Query: @price:[10 20] | @price:[30 40] | @price:[50 60]
        # N-ary structure flattens: OR(pred1, pred2, pred3)
        # This creates: 3 predicates + 1 OR composed predicate = 4 terms total (within limit of 10)
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "@price:[10 20] | @price:[30 40] | @price:[50 60] =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]
        
        # Test that the config ranges from 1 to max_limit
        try:
            client.execute_command("CONFIG SET search.query-string-terms-count 0")
            assert False
        except ResponseError as e:
            assert f"argument must be between 1 and {max_limit.decode()} inclusive" in str(e)
        try:
            client.execute_command(f"CONFIG SET search.query-string-terms-count {int(max_limit)+1}")
            assert False
        except ResponseError as e:
            assert f"argument must be between 1 and {max_limit.decode()} inclusive" in str(e)

    def test_query_ignore_punctuations(self):
        """
        Test that trailing punctuation (not reserved) after unquoted text tokens is consumed correctly.
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE", "idx1", "ON", "HASH", "SCHEMA", "content", "TEXT", "NOSTEM", "WITHSUFFIXTRIE", "content2", "TEXT", "NOSTEM")
        # Insert test documents
        client.execute_command("HSET", "doc:1", "content", "I am going to a concert", "content2", "racing car")
        client.execute_command("HSET", "doc:2", "content", "word1 word2 word3")
        # 1. Basic prefix wildcard with punctuation
        result = client.execute_command("FT.SEARCH", "idx1", 'con*+')
        assert result[0] == 1
        result = client.execute_command("FT.SEARCH", "idx1", 'con*++')
        assert result[0] == 1
        result = client.execute_command("FT.SEARCH", "idx1", 'con*!!!')
        assert result[0] == 1
        result = client.execute_command("FT.SEARCH", "idx1", 'con*+!,.')
        assert result[0] == 1
        result = client.execute_command("FT.SEARCH", "idx1", '@content:con*+!!')
        assert result[0] == 1
        # 2. Suffix wildcard with punctuation
        result = client.execute_command("FT.SEARCH", "idx1", '*cert+')
        assert result[0] == 1
        result = client.execute_command("FT.SEARCH", "idx1", '*cert!!!')
        assert result[0] == 1
        # 3. Reserved characters should STOP punctuation consumption
        # Parentheses
        result = client.execute_command("FT.SEARCH", "idx1", 'con*+ (racing)')
        assert result[0] == 1
        # with Field (@)
        result = client.execute_command("FT.SEARCH", "idx1", 'con*+ @content2:racing=')
        assert result[0] == 1
        # OR operator
        result = client.execute_command("FT.SEARCH", "idx1", 'con*+ | word1!!')
        assert result[0] == 2

    def test_query_with_identifier_alias(self):
        """
        Test that query works with original identifier as well as alias
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE", "idx1", "ON", "HASH", "SCHEMA", "content", "AS", "con", "TEXT")
        # Insert test documents
        client.execute_command("HSET", "doc:1", "content", "I am going to a concert", "content2", "racing car")
        client.execute_command("HSET", "doc:2", "content", "word1 word2 word3")
        # Query with alias
        result = client.execute_command("FT.SEARCH", "idx1", "@con:concert")
        assert result[0] == 1
        # Query with original identifier
        result = client.execute_command("FT.SEARCH", "idx1", "@content:concert")
        assert result[0] == 1

    def test_simple_json_vector_query(self):
        """
        Test simple JSON vector field query to understand the behavior
        """
        client: Valkey = self.server.get_new_client()
        
        # Create a simple JSON index with one vector field WITH an alias
        client.execute_command(
            "FT.CREATE", "vec_idx", "ON", "JSON", 
            "SCHEMA", "$.embedding", "AS", "embedalias", "VECTOR", "HNSW", "6", 
            "TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE"
        )
        
        # Insert a test document
        import json
        import struct
        client.execute_command(
            "JSON.SET", "doc:1", "$", 
            json.dumps({"embedding": [1.0, 2.0, 3.0], "name": "test"})
        )
        
        # Create proper 3-float vector blob (12 bytes)
        vec_blob = struct.pack('fff', 1.0, 2.0, 3.0)
        
        # Try to search using the ALIAS (vector)
        result = client.execute_command(
            "FT.SEARCH", "vec_idx", "*=>[KNN 1 @embedalias $vec]",
            "PARAMS", "2", "vec", vec_blob,
            "DIALECT", "2"
        )
        print(f"Query with alias @vector: {result}")
        assert result[0] == 1  # Should find 1 document
        
        # Try to search using the IDENTIFIER ($.embedding)
        result = client.execute_command(
            "FT.SEARCH", "vec_idx", "*=>[KNN 1 @$.embedding $vec]",
            "PARAMS", "2", "vec", vec_blob,
            "DIALECT", "2"
        )
        print(f"Query with identifier @$.embedding: {result}")
        assert result[0] == 1  # Should also find 1 document

