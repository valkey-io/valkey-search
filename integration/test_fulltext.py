import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker

"""
This file contains tests for full text search.
"""

# NOTE: Test data uses lowercase/non-stemmed terms to avoid unpredictable stemming behavior.
# Previous version used "Wonderful" which could stem to "wonder", making tests unreliable.
# TODO: Add exact term match support for words that can be stemmed to allow testing both behaviors.

# Constants for text queries on Hash documents.
text_index_on_hash = "FT.CREATE products ON HASH PREFIX 1 product: SCHEMA desc TEXT"
hash_docs = [
    ["HSET", "product:1", "category", "electronics", "name", "Laptop", "price", "999.99", "rating", "4.5", "desc", "great"],
    ["HSET", "product:2", "category", "electronics", "name", "Tablet", "price", "499.00", "rating", "4.0", "desc", "good"],
    ["HSET", "product:3", "category", "electronics", "name", "Phone", "price", "299.00", "rating", "3.8", "desc", "Ok"],
    ["HSET", "product:4", "category", "books", "name", "Book", "price", "19.99", "rating", "4.8", "desc", "wonder"],
    ["HSET", "product:5", "category", "books", "name", "Book2", "price", "19.99", "rating", "1.0", "desc", "greased"]
]
text_query_term = ["FT.SEARCH", "products", '@desc:"wonder"']
text_query_term_nomatch = ["FT.SEARCH", "products", '@desc:"nomatch"']
text_query_prefix = ["FT.SEARCH", "products", '@desc:"wond*"']
text_query_prefix_nomatch = ["FT.SEARCH", "products", '@desc:"nomatch*"']
text_query_prefix_multimatch = ["FT.SEARCH", "products", '@desc:"grea*"']

expected_hash_key = b'product:4'
expected_hash_value = {
    b'name': b"Book",
    b'price': b'19.99',
    b'rating': b'4.8',
    b'desc': b"wonder",
    b'category': b"books"
}

# Constants for per-field text search test
text_index_on_hash_two_fields = "FT.CREATE products2 ON HASH PREFIX 1 product: SCHEMA desc TEXT desc2 TEXT"
hash_docs_with_desc2 = [
    ["HSET", "product:1", "category", "electronics", "name", "Laptop", "price", "999.99", "rating", "4.5", "desc", "Great", "desc2", "wonder experience here"],
    ["HSET", "product:2", "category", "electronics", "name", "Tablet", "price", "499.00", "rating", "4.0", "desc", "Good", "desc2", "Hello, where are you here ?"],
    ["HSET", "product:3", "category", "electronics", "name", "Phone", "price", "299.00", "rating", "3.8", "desc", "Ok", "desc2", "Hello, how are you doing?"],
    ["HSET", "product:4", "category", "books", "name", "Book", "price", "19.99", "rating", "4.8", "desc", "wonder", "desc2", "Hello, what are you doing Great?"]
]

# Search queries for specific fields
text_query_desc_field = ["FT.SEARCH", "products2", '@desc:"wonder"']
text_query_desc_prefix = ["FT.SEARCH", "products2", '@desc:"wonde*"']
text_query_desc2_field = ["FT.SEARCH", "products2", '@desc2:"wonder"']
text_query_desc2_prefix = ["FT.SEARCH", "products2", '@desc2:"wonde*"']

# Expected results for desc field search
expected_desc_hash_key = b'product:4'
expected_desc_hash_value = {
    b'name': b"Book",
    b'price': b'19.99', 
    b'rating': b'4.8',
    b'desc': b"wonder",
    b'desc2': b"Hello, what are you doing Great?",
    b'category': b"books"
}

# Expected results for desc2 field search  
expected_desc2_hash_key = b'product:1'
expected_desc2_hash_value = {
    b'name': b"Laptop",
    b'price': b'999.99',
    b'rating': b'4.5', 
    b'desc': b"Great",
    b'desc2': b"wonder experience here",
    b'category': b"electronics"
}

# Index and document setup
ingestion_pipeline_index = "FT.CREATE idx ON HASH SCHEMA content TEXT"
ingestion_pipeline_doc = ["HSET", "doc:1", "content", "The quick-running searches are finding effective results! But slow searches aren't working..."]

# Common expected results
ingestion_match_expected = 1
ingestion_nomatch_expected = 0
ingestion_expected_key = b'doc:1'
ingestion_expected_fields = [b'content', b"The quick-running searches are finding effective results! But slow searches aren't working..."]

# Query arrays for ingestion pipeline test
ingestion_punctuation_queries = [
    ["FT.SEARCH", "idx", '@content:"quick*"'],
    ["FT.SEARCH", "idx", '@content:"run*"'],
    ["FT.SEARCH", "idx", '@content:"result*"'],
    ["FT.SEARCH", "idx", '@content:"work*"']
]

ingestion_stopword_queries = [
    ["FT.SEARCH", "idx", '@content:"the"'],
    ["FT.SEARCH", "idx", '@content:"are"'],
    ["FT.SEARCH", "idx", '@content:"but"']
]

ingestion_case_queries = [
    ["FT.SEARCH", "idx", '@content:"quick*"'],
    ["FT.SEARCH", "idx", '@content:"effect*"']
]

ingestion_wildcard_queries = [
    ["FT.SEARCH", "idx", '@content:"find*"'],
    ["FT.SEARCH", "idx", '@content:"effect*"'],
    ["FT.SEARCH", "idx", '@content:"work*"'],
    ["FT.SEARCH", "idx", '@content:"search*"']
]

ingestion_nomatch_queries = [
    ["FT.SEARCH", "idx", '@content:"nonexistent"'],
    ["FT.SEARCH", "idx", '@content:"missing*"'],
    ["FT.SEARCH", "idx", '@content:"xyz*"']
]

# Constants for punctuation test
punctuation_test_index = "FT.CREATE idx ON HASH PUNCTUATION . SCHEMA content TEXT"
punctuation_test_doc = ["HSET", "doc:1", "content", "hello.world"]
punctuation_test_query = ["FT.SEARCH", "idx", '@content:"world"']
punctuation_expected_fields = [b'content', b"hello.world"]

# Constants for multi-field test
multi_field_index = "FT.CREATE idx ON HASH SCHEMA title TEXT content TEXT NOSTEM"
multi_field_doc = ["HSET", "doc:1", "title", "running fast", "content", "running quickly"]
multi_field_query = ["FT.SEARCH", "idx", '@content:"running"']
multi_field_expected_value = {
    b'title': b'running fast',
    b'content': b'running quickly'
}

# Constants for stopwords test
stopwords_test_index = "FT.CREATE idx ON HASH STOPWORDS 2 the and SCHEMA content TEXT"
stopwords_test_doc = ["HSET", "doc:1", "content", "the cat and dog"]
stopwords_test_queries_filtered = [
    ["FT.SEARCH", "idx", '@content:"the"'],
    ["FT.SEARCH", "idx", '@content:"and"']
]
stopwords_test_query_indexed = ["FT.SEARCH", "idx", '@content:"cat"']
stopwords_expected_fields = [b'content', b"the cat and dog"]

# Constants for nostem test
nostem_test_index = "FT.CREATE idx ON HASH NOSTEM SCHEMA content TEXT"
nostem_test_doc = ["HSET", "doc:1", "content", "running quickly"]
nostem_test_queries = [
    ["FT.SEARCH", "idx", '@content:"running"'],
    ["FT.SEARCH", "idx", '@content:"quickly"']
]
nostem_expected_fields = [b'content', b"running quickly"]

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
        # Perform the text search query with term and prefix operations that return a match.
        match = [text_query_term, text_query_prefix]
        for query in match:
            result = client.execute_command(*query)
            assert len(result) == 3
            assert result[0] == 1  # Number of documents found
            assert result[1] == expected_hash_key
            document = result[2]
            doc_fields = dict(zip(document[::2], document[1::2]))
            assert doc_fields == expected_hash_value
        # Perform the text search query with term and prefix operations that return no match.
        nomatch = [text_query_term_nomatch, text_query_prefix_nomatch]
        for query in nomatch:
            result = client.execute_command(*query)
            assert len(result) == 1
            assert result[0] == 0  # Number of documents found
        # Perform a wild card prefix operation with multiple matches
        result = client.execute_command(*text_query_prefix_multimatch)
        assert len(result) == 5
        assert result[0] == 2  # Number of documents found. Both docs below start with Grea* => Great and Greased
        assert result[1] == b"product:1" and result[3] == b"product:5" or result[1] == b"product:5" and result[3] == b"product:1"

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

        # Test multiple text fields
        command_args = [
            "FT.CREATE", "idx4",
            "ON", "HASH",
            "SCHEMA", "desc", "TEXT", "desc2", "TEXT"
        ]
        
        assert client.execute_command(*command_args) == b"OK"
        assert b"idx4" in client.execute_command("FT._LIST")

    def test_text_search_per_field(self):
        """
        Test FT.SEARCH command with field-specific text searches.
        Return only documents where the term appears in the specified field.
        """
        client: Valkey = self.server.get_new_client()
        # Create the text index on Hash documents with two text fields
        assert client.execute_command(text_index_on_hash_two_fields) == b"OK"
        
        # Insert documents into the index - each doc has 6 fields now (including desc2)
        for doc in hash_docs_with_desc2:
            assert client.execute_command(*doc) == 6
        
        # 1) Perform a term search on desc field for "wonder"
        # 2) Perform a prefix search on desc field for "Wonder*"
        desc_queries = [text_query_desc_field, text_query_desc_prefix]
        for query in desc_queries:
            result_desc = client.execute_command(*query)
            assert len(result_desc) == 3
            assert result_desc[0] == 1  # Number of documents found
            assert result_desc[1] == expected_desc_hash_key
            document_desc = result_desc[2]
            doc_fields_desc = dict(zip(document_desc[::2], document_desc[1::2]))
            assert doc_fields_desc == expected_desc_hash_value
        
        # 1) Perform a term search on desc2 field for "wonder"
        # 2) Perform a prefix search on desc2 field for "Wonder*"
        desc2_queries = [text_query_desc2_field, text_query_desc2_prefix]
        for query in desc2_queries:
            result_desc2 = client.execute_command(*query)
            assert len(result_desc2) == 3
            assert result_desc2[0] == 1  # Number of documents found
            assert result_desc2[1] == expected_desc2_hash_key
            document_desc2 = result_desc2[2]
            doc_fields_desc2 = dict(zip(document_desc2[::2], document_desc2[1::2]))
            assert doc_fields_desc2 == expected_desc2_hash_value

    def test_default_ingestion_pipeline(self):
        """
        Test comprehensive ingestion pipeline: FT.CREATE → HSET → FT.SEARCH with full tokenization
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(ingestion_pipeline_index)
        client.execute_command(*ingestion_pipeline_doc)
        
        # Punctuation tokenization (using prefix to handle stemming)
        for query in ingestion_punctuation_queries:
            result = client.execute_command(*query)
            assert result[0] == ingestion_match_expected
            assert result[1] == ingestion_expected_key
            assert result[2] == ingestion_expected_fields
        
        # Stop word filtering
        for query in ingestion_stopword_queries:
            result = client.execute_command(*query)
            assert result[0] == ingestion_nomatch_expected
        
        # Case insensitivity (using prefix to handle stemming)
        for query in ingestion_case_queries:
            result = client.execute_command(*query)
            assert result[0] == ingestion_match_expected
            assert result[1] == ingestion_expected_key
            assert result[2] == ingestion_expected_fields
        
        # Wildcard matching (prefix only, no suffix tree by default)
        for query in ingestion_wildcard_queries:
            result = client.execute_command(*query)
            assert result[0] == ingestion_match_expected
            assert result[1] == ingestion_expected_key
            assert result[2] == ingestion_expected_fields
        
        # Non-existent terms
        for query in ingestion_nomatch_queries:
            result = client.execute_command(*query)
            assert result[0] == ingestion_nomatch_expected

    def test_punctuation(self):
        """
        Test that per-index PUNCTUATION actually affects search results
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(punctuation_test_index)
        client.execute_command(*punctuation_test_doc)
        result = client.execute_command(*punctuation_test_query)
        assert result[0] == ingestion_match_expected  # Dot separator worked
        assert result[1] == ingestion_expected_key
        assert result[2] == punctuation_expected_fields

    def test_multi_text_field(self):
        """
        Test different TEXT field configs in same index
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(multi_field_index)
        client.execute_command(*multi_field_doc)
        result = client.execute_command(*multi_field_query)
        assert result[0] == ingestion_match_expected  # Document found in content field
        assert result[1] == ingestion_expected_key

        actual_fields = dict(zip(result[2][::2], result[2][1::2]))
        assert actual_fields == multi_field_expected_value

    def test_stopwords(self):
        """
        End-to-end test: FT.CREATE STOPWORDS config actually filters stop words in search
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(stopwords_test_index)
        client.execute_command(*stopwords_test_doc)
        
        # Stop words should not be findable
        for query in stopwords_test_queries_filtered:
            result = client.execute_command(*query)
            assert result[0] == ingestion_nomatch_expected  # Stop word filtered out
        
        # Regular words should be findable
        result = client.execute_command(*stopwords_test_query_indexed)
        assert result[0] == ingestion_match_expected  # Regular word indexed
        assert result[1] == ingestion_expected_key
        assert result[2] == stopwords_expected_fields

    def test_nostem(self):
        """
        End-to-end test: FT.CREATE NOSTEM config actually affects stemming in search
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command(nostem_test_index)
        client.execute_command(*nostem_test_doc)
        
        # With NOSTEM, exact forms should be findable
        for query in nostem_test_queries:
            result = client.execute_command(*query)
            assert result[0] == ingestion_match_expected  # Exact form found
            assert result[1] == ingestion_expected_key
            assert result[2] == nostem_expected_fields
