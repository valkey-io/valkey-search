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
        assert client.execute_command("CONFIG GET search.query-string-depth") == [b"search.query-string-depth", b"1000"]
        # Test that we can set the query string limit to 1
        assert client.execute_command("CONFIG SET search.query-string-depth 1") == b"OK"
        assert client.execute_command("FT.CREATE my_index ON HASH PREFIX 1 doc: SCHEMA price NUMERIC category TAG SEPARATOR | doc_embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 128 DISTANCE_METRIC COSINE") == b"OK"
        # Validate the success case with a query of depth 1.
        assert client.execute_command(
            "FT.SEARCH", "my_index",
            "@price:[10 20] =>[KNN 10 @doc_embedding $BLOB]",
            "PARAMS", 2, "BLOB", "<your_vector_blob>",
            "RETURN", 1, "doc_embedding"
        ) == [0]
        # Validate the failure case with a query of depth 2.
        try:
            client.execute_command(
                "FT.SEARCH", "my_index",
                "@price:[10 20] | @category:{electronics|books} =>[KNN 10 @doc_embedding $BLOB]",
                "PARAMS", 2, "BLOB", "<your_vector_blob>",
                "RETURN", 1, "doc_embedding"
            )
            assert False
        except ResponseError as e:
            assert str(e) == "Invalid filter expression: `@price:[10 20] | @category:{electronics|books}`. Query string is too complex"
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
