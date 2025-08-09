import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

"""
This file contains tests for full text search.
"""
# Constants for text queries on Hash documents.
text_index_on_hash = "FT.CREATE products ON HASH PREFIX 1 product: SCHEMA desc TEXT"
hash_docs = [
    ["HSET", "product:1", "category", "electronics", "name", "Laptop", "price", "999.99", "rating", "4.5", "desc", "Great"],
    ["HSET", "product:2", "category", "electronics", "name", "Tablet", "price", "499.00", "rating", "4.0", "desc", "Good"],
    ["HSET", "product:3", "category", "electronics", "name", "Phone", "price", "299.00", "rating", "3.8", "desc", "Ok"],
    ["HSET", "product:4", "category", "books", "name", "Book", "price", "19.99", "rating", "4.8", "desc", "Wonderful"]
]
text_query = ["FT.SEARCH", "products", '@desc:"Wonderful"']
expected_hash_key = b'product:4'
expected_hash_value = {
    b'name': b"Book",
    b'price': b'19.99',
    b'rating': b'4.8',
    b'desc': b"Wonderful",
    b'category': b"books"
}

class TestFullText(ValkeySearchTestCaseBase):

    def test_text_search(self):
        """
        Test FT.SEARCH command with a text index.
        """
        client: Valkey = self.server.get_new_client()
        # Create the text index on Hash documents
        assert client.execute_command(text_index_on_hash) == b"OK"
        # Insert documents into the index
        for doc in hash_docs:
            assert client.execute_command(*doc) == 5
        # Perform the text search query
        result = client.execute_command(*text_query)
        assert len(result) == 3
        assert result[0] == 1  # Number of documents found
        assert result[1] == expected_hash_key
        document = result[2]
        doc_fields = dict(zip(document[::2], document[1::2]))
        assert doc_fields == expected_hash_value

    def test_ft_create(self):
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
        
        # Create the index
        assert client.execute_command(*command_args) == b"OK"
        assert b"idx1" in client.execute_command("FT._LIST")
        
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
        
        assert client.execute_command(*command_args) == b"OK"
        assert b"idx4" in client.execute_command("FT._LIST")

        command_args = [
            "FT.CREATE", "idx5",
            "ON", "HASH",
            "NOSTEM",
            "WITHOFFSETS", 
            "SCHEMA", "description", "TEXT"
        ]
        
        assert client.execute_command(*command_args) == b"OK"
        assert b"idx5" in client.execute_command("FT._LIST")
   
        # Invalid command - TEXT without field name
        command_args = [
            "FT.CREATE", "idx6",
            "ON", "HASH",
            "SCHEMA", "TEXT"  # Missing field name before TEXT
        ]
        
        # Should get parsing error
        with pytest.raises(ResponseError):
            client.execute_command(*command_args)
