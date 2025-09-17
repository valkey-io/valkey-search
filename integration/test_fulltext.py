import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from ft_info_parser import FTInfoParser

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

    def test_ft_create_and_info(self):
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

        command_args = [
            "FT.CREATE", "idx4", "ON", "HASH",
            "PREFIX", "2", "product:", "item:",
            "STOPWORDS", "2", "the", "and",
            "PUNCTUATION", ".,!",
            "WITHOFFSETS",
            "SCHEMA",
            "title", "TEXT", "NOSTEM",
            "description", "TEXT",
            "price", "NUMERIC",
            "category", "TAG", "SEPARATOR", "|",
            "subcategory", "TAG", "CASESENSITIVE"
        ]
        
        assert client.execute_command(*command_args) == b"OK"
        assert b"idx4" in client.execute_command("FT._LIST")
        
        info_response = client.execute_command("FT.INFO", "idx4")
        parser = FTInfoParser(info_response)
        
        # Validate top-level structure
        required_top_level_fields = [
            "index_name", "index_options", "index_definition", "attributes",
            "num_docs", "num_terms", "num_records", "hash_indexing_failures",
            "gc_stats", "cursor_stats", "dialect_stats", "Index Errors",
            "backfill_in_progress", "backfill_complete_percent", 
            "mutation_queue_size", "recent_mutations_queue_delay",
            "state", "punctuation", "stop_words", "with_offsets", "language"
        ]
        
        for field in required_top_level_fields:
            assert field in parser.parsed_data, f"Missing required field: {field}"
        
        # Validate index definition structure
        index_def = parser.index_definition
        assert "key_type" in index_def
        assert "prefixes" in index_def
        assert "default_score" in index_def

        text_attr = parser.get_attribute_by_name("title")
        assert text_attr["type"] == "TEXT"
        assert text_attr.get("NO_STEM") == 1
        
        numeric_attr = parser.get_attribute_by_name("price")
        assert numeric_attr["type"] == "NUMERIC"
        
        tag_attr = parser.get_attribute_by_name("category")
        assert tag_attr["type"] == "TAG"
        assert tag_attr.get("SEPARATOR") == "|"
        
        # Add validation checks for specific fields
        assert parser.num_docs >= 0, f"num_docs should be non-negative, got: {parser.num_docs}"
        assert parser.num_records >= 0, f"num_records should be non-negative, got: {parser.num_records}"
        assert parser.hash_indexing_failures >= 0, f"hash_indexing_failures should be non-negative, got: {parser.hash_indexing_failures}"
        
        # Validate backfill fields
        assert isinstance(parser.backfill_in_progress, bool), f"backfill_in_progress should be boolean, got: {type(parser.backfill_in_progress)}"
        assert isinstance(parser.backfill_complete_percent, (int, float)), f"backfill_complete_percent should be numeric, got: {type(parser.backfill_complete_percent)}"
        assert 0.0 <= parser.backfill_complete_percent <= 1.0, f"backfill_complete_percent should be between 0.0 and 1.0, got: {parser.backfill_complete_percent}"
        
        # Validate queue and delay fields
        assert parser.mutation_queue_size >= 0, f"mutation_queue_size should be non-negative, got: {parser.mutation_queue_size}"
        assert isinstance(parser.recent_mutations_queue_delay, str), f"recent_mutations_queue_delay should be string, got: {type(parser.recent_mutations_queue_delay)}"
        
        # Validate state field
        assert isinstance(parser.state, str), f"state should be string, got: {type(parser.state)}"
        assert parser.state in ["ready", "backfill_in_progress"], f"state should be 'ready' or 'backfill_in_progress', got: {parser.state}"
        
        # Validate punctuation setting
        punctuation = parser.parsed_data.get("punctuation", "")
        assert punctuation == ".,!", f"Expected punctuation '.,!', got: '{punctuation}'"
        
        # Validate stop_words setting
        stop_words = parser.parsed_data.get("stop_words", [])
        assert isinstance(stop_words, list), f"stop_words should be list, got: {type(stop_words)}"
        assert set(stop_words) == {"the", "and"}, f"Expected stop_words ['the', 'and'], got: {stop_words}"
        
        # Validate with_offsets setting
        with_offsets = parser.parsed_data.get("with_offsets")
        assert with_offsets in [0, 1, "0", "1", True, False], f"with_offsets should be boolean-like, got: {with_offsets}"
        
        # Validate language setting
        language = parser.parsed_data.get("language", "")
        assert language == "english", f"Expected language 'english', got: '{language}'"

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
        client.execute_command("FT.CREATE idx ON HASH SCHEMA content TEXT")
        client.execute_command("HSET", "doc:1", "content", "The quick-running searches are finding EFFECTIVE results!")
        client.execute_command("HSET", "doc:2", "content", "But slow searches aren't working...")
        
        # List of queries with pass/fail expectations
        test_cases = [
            ("quick*", True, "Punctuation tokenization - hyphen creates word boundaries"),
            ("effect*", True, "Case insensitivity - lowercase matches uppercase"),
            ("the", False, "Stop word filtering - common words filtered out"),
            ("find*", True, "Prefix wildcard - matches 'finding'"),
            ("nonexistent", False, "Non-existent terms return no results")
        ]
        
        expected_key = b'doc:1'
        expected_fields = [b'content', b"The quick-running searches are finding EFFECTIVE results!"]
        
        for query_term, should_match, description in test_cases:
            result = client.execute_command("FT.SEARCH", "idx", f'@content:"{query_term}"')
            if should_match:
                assert result[0] == 1 and result[1] == expected_key and result[2] == expected_fields, f"Failed: {description}"
            else:
                assert result[0] == 0, f"Failed: {description}"

    def test_multi_text_field(self):
        """
        Test different TEXT field configs in same index
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE idx ON HASH SCHEMA title TEXT content TEXT NOSTEM")
        client.execute_command("HSET", "doc:1", "title", "running fast", "content", "running quickly")

        expected_value = {
            b'title': b'running fast',
            b'content': b'running quickly'
        }

        result = client.execute_command("FT.SEARCH", "idx", '@title:"run"')
        actual_fields = dict(zip(result[2][::2], result[2][1::2]))
        assert actual_fields == expected_value

        result = client.execute_command("FT.SEARCH", "idx", '@content:"run"')
        assert result[0] == 0  # Should not find (NOSTEM)

    def test_custom_stopwords(self):
        """
        End-to-end test: FT.CREATE STOPWORDS config actually filters custom stop words in search
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE idx ON HASH STOPWORDS 2 the and SCHEMA content TEXT")
        client.execute_command("HSET", "doc:1", "content", "the cat and dog are good")
        
        # Stop words should not be findable
        
        result = client.execute_command("FT.SEARCH", "idx", '@content:"and"')
        assert result[0] == 0  # Stop word "and" filtered out
        
        # non stop words should be findable
        result = client.execute_command("FT.SEARCH", "idx", '@content:"are"')
        assert result[0] == 1  # Regular word indexed
        assert result[1] == b'doc:1'
        assert result[2] == [b'content', b"the cat and dog are good"]

    def test_nostem(self):
        """
        End-to-end test: FT.CREATE NOSTEM config actually affects stemming in search
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE idx ON HASH NOSTEM SCHEMA content TEXT")
        client.execute_command("HSET", "doc:1", "content", "running quickly")
        
        # With NOSTEM, exact forms should be findable
        result = client.execute_command("FT.SEARCH", "idx", '@content:"running"')
        assert result[0] == 1  # Exact form "running" found
        assert result[1] == b'doc:1'
        assert result[2] == [b'content', b"running quickly"]

    def test_custom_punctuation(self):
        """
        Test PUNCTUATION directive configures custom tokenization separators
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE idx ON HASH PUNCTUATION . SCHEMA content TEXT")
        client.execute_command("HSET", "doc:1", "content", "hello.world test@email")
        
        # Dot configured as separator - should find split words
        result = client.execute_command("FT.SEARCH", "idx", '@content:"hello"')
        assert result[0] == 1  # Found "hello" as separate token
        assert result[1] == b'doc:1'
        assert result[2] == [b'content', b"hello.world test@email"]
        
        # @ NOT configured as separator - should not be able with split words
        result = client.execute_command("FT.SEARCH", "idx", '@content:"test"')
        assert result[0] == 0