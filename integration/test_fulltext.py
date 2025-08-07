import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFullText(ValkeySearchTestCaseBase):

    def test_create_function(self):
        """
        Test basic text search for FT.CREATE with multiple cases.
        Validates that the command parsing works correctly even though TEXT indexing is not yet implemented.
        There are some test cases that should pass correctly and some that should not parse correctly
        """
        client: Valkey = self.server.get_new_client()
        
        # Define the FT.CREATE command with punctuation, stopwords, and text field
        command_args = [
            "FT.CREATE", "idx1",
            "ON", "HASH", 
            "PUNCTUATION", ",.;", 
            "WITHOFFSETS", 
            "NOSTEM", 
            "STOPWORDS", "3", "the", "and", "or",
            "SCHEMA", "text_field", "TEXT"
        ]
        
        # Validate that TEXT indexing is not yet implemented (similar to vector test pattern)
        with pytest.raises(ResponseError, match="Text Index is yet to be defined"):
            client.execute_command(*command_args)
        
        # When TEXT indexing is implemented, the test should expect:
        # assert client.execute_command(*command_args) == b"OK"
        # assert b"idx1" in client.execute_command("FT._LIST")
        
        # Invalid command - missing stopwords count
        command_args = [
            "FT.CREATE", "idx2",
            "ON", "HASH",
            "STOPWORDS", "the", "and",  # Missing count before stopwords
            "SCHEMA", "text_field", "TEXT"
        ]
        
        # Should get parsing error before reaching TEXT implementation check
        with pytest.raises(ResponseError):
            client.execute_command(*command_args)

        # Invalid command - PUNCTUATION without value
        command_args = [
            "FT.CREATE", "idx3",
            "ON", "HASH",
            "PUNCTUATION",  # Missing punctuation characters
            "SCHEMA", "text_field", "TEXT"
        ]
        
        # Should get parsing error
        with pytest.raises(ResponseError):
            client.execute_command(*command_args)

        # Command with different parameter combinations
        command_args = [
            "FT.CREATE", "idx4",
            "ON", "HASH",
            "PUNCTUATION", "!@#$%",
            "STOPWORDS", "5", "a", "an", "the", "is", "are", 
            "SCHEMA", "title", "TEXT", "content", "TEXT"
        ]
        
        # Validate that TEXT indexing is not yet implemented
        with pytest.raises(ResponseError, match="Text Index is yet to be defined"):
            client.execute_command(*command_args)

        command_args = [
            "FT.CREATE", "idx5",
            "ON", "HASH",
            "NOSTEM",
            "WITHOFFSETS", 
            "SCHEMA", "description", "TEXT"
        ]
        
        # Validate that TEXT indexing is not yet implemented
        with pytest.raises(ResponseError, match="Text Index is yet to be defined"):
            client.execute_command(*command_args)
   
        # Invalid command - TEXT without field name
        command_args = [
            "FT.CREATE", "idx6",
            "ON", "HASH",
            "SCHEMA", "TEXT"  # Missing field name before TEXT
        ]
        
        # Should get parsing error
        with pytest.raises(ResponseError):
            client.execute_command(*command_args)
