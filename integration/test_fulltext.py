import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestFullText(ValkeySearchTestCaseBase):

    def test_basic_text_create_with_stopwords_and_punctuation(self):
        """
        Test basic text search with stopwords and punctuation using FT.CREATE.
        Validates that the command parsing works correctly even though TEXT indexing is not yet implemented.
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

    def test_text_create_with_invalid_stopwords(self):
        """
        Test FT.CREATE with invalid stopwords parameter.
        Tests error handling for malformed stopwords syntax.
        """
        client: Valkey = self.server.get_new_client()
        
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

    def test_text_create_with_invalid_punctuation(self):
        """
        Test FT.CREATE with invalid punctuation parameter.
        Tests error handling for malformed punctuation syntax.
        """
        client: Valkey = self.server.get_new_client()
        
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

    def test_text_create_with_different_params(self):
        """
        Test FT.CREATE with different text parameters.
        Tests various combinations of text field options.
        """
        client: Valkey = self.server.get_new_client()
        
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

    def test_text_create_with_nostem_and_withoffsets(self):
        """
        Test FT.CREATE with NOSTEM and WITHOFFSETS flags.
        Tests text field flags parsing.
        """
        client: Valkey = self.server.get_new_client()
        
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

    def test_text_create_invalid_schema_syntax(self):
        """
        Test FT.CREATE with invalid schema syntax.
        Tests error handling for malformed schema definitions.
        """
        client: Valkey = self.server.get_new_client()
        
        # Invalid command - TEXT without field name
        command_args = [
            "FT.CREATE", "idx6",
            "ON", "HASH",
            "SCHEMA", "TEXT"  # Missing field name before TEXT
        ]
        
        # Should get parsing error
        with pytest.raises(ResponseError):
            client.execute_command(*command_args)
