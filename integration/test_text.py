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
        assert client.execute_command("FT.CREATE my_index ON HASH PREFIX 1 doc: PUNCTUATION \",.;\" WITHOFFSETS NOSTEM STOPWORDS 3 the and or SCHEMA text_field TEXT price NUMERIC category TAG SEPARATOR |") == b"OK"
        query = "@price:[10 20]"
        command_args = [
            "FT.SEARCH", "my_index",
            query,
            "RETURN", 1, "text_field"
        ]
        # Validate the query strings above the limit are rejected.
        assert client.execute_command(f"CONFIG SET search.query-string-bytes {len(query) - 1}") == b"OK"
        with pytest.raises(Exception, match="Query string is too long, max length is 45 bytes"):
            client.execute_command(*command_args)
        # Validate the query strings within the limit are rejected.
        assert client.execute_command(f"CONFIG SET search.query-string-bytes {len(query)}") == b"OK"
        assert client.execute_command(*command_args) == [0]
