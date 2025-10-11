import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from ft_info_parser import FTInfoParser
from valkeytestframework.util import waiters
import threading
import time
from utils import IndexingTestHelper

"""
This file contains tests for full text search.
"""

# NOTE: Test data uses lowercase/non-stemmed terms to avoid unpredictable stemming behavior.
# Previous version used "Wonderful" which could stem to "wonder", making tests unreliable.
# TODO: Add exact term match support for words that can be stemmed to allow testing both behaviors.

# Constants for text queries on Hash documents.
text_index_on_hash = "FT.CREATE products ON HASH PREFIX 1 product: SCHEMA desc TEXT"
hash_docs = [
    ["HSET", "product:4", "category", "books", "name", "Book", "price", "19.99", "rating", "4.8", "desc", "Order Opposite. Random Words. Random Words. wonder of wonders. Uncommon random words. Random Words."],
    ["HSET", "product:1", "category", "electronics", "name", "Laptop", "price", "999.99", "rating", "4.5", "desc", "1 2 3 4 5 6 7 8 9 0. Great oaks. Random Words. Random Words. Great oaks from little grey acorns grow. Impressive oak."],
    ["HSET", "product:3", "category", "electronics", "name", "Phone", "price", "299.00", "rating", "3.8", "desc", "Random Words. Experience. Random Words. Ok, this document uses some more common words from other docs. Interesting desc, impressive tablet. Random Words."],
    ["HSET", "product:5", "category", "books", "name", "Book2", "price", "19.99", "rating", "1.0", "desc", "Unique slop word. Random Words. Random Words. greased the inspector's palm"],
    ["HSET", "product:2", "category", "electronics", "name", "Tablet", "price", "499.00", "rating", "4.0", "desc", "Random Words. Random Words. Interesting. Good beginning makes a good ending. Interesting desc"]
]
text_query_term = ["FT.SEARCH", "products", '@desc:"wonder"']
text_query_term_nomatch = ["FT.SEARCH", "products", '@desc:"nomatch"']
text_query_prefix = ["FT.SEARCH", "products", '@desc:"wond*"']
text_query_prefix2 = ["FT.SEARCH", "products", '@desc:"wond*"']
text_query_prefix_nomatch = ["FT.SEARCH", "products", '@desc:"nomatch*"']
text_query_prefix_multimatch = ["FT.SEARCH", "products", '@desc:"grea*"']
text_query_exact_phrase1 = ["FT.SEARCH", "products", '@desc:"word wonder"']
text_query_exact_phrase2 = ["FT.SEARCH", "products", '@desc:"random word wonder"']

expected_hash_key = b'product:4'
expected_hash_value = {
    b'name': b"Book",
    b'price': b'19.99',
    b'rating': b'4.8',
    b'desc': b"Order Opposite. Random Words. Random Words. wonder of wonders. Uncommon random words. Random Words.",
    b'category': b"books"
}

# Constants for per-field text search test
text_index_on_hash_two_fields = "FT.CREATE products2 ON HASH PREFIX 1 product: SCHEMA desc TEXT desc2 TEXT"
hash_docs_with_desc2 = [
    ["HSET", "product:2", "category", "electronics", "name", "Tablet", "price", "499.00", "rating", "4.0", "desc", "Good", "desc2", "Hello, where are you here ?"],
    ["HSET", "product:4", "category", "books", "name", "Book", "price", "19.99", "rating", "4.8", "desc", "wonder", "desc2", "Hello, what are you doing Great?"],
    ["HSET", "product:1", "category", "electronics", "name", "Laptop", "price", "999.99", "rating", "4.5", "desc", "Great. 1 2 3 4 5 6 7 8 9 10.", "desc2", "wonder experience here. 2 4 6 8 10."],
    ["HSET", "product:3", "category", "electronics", "name", "Phone", "price", "299.00", "rating", "3.8", "desc", "Ok", "desc2", "Hello, how are you doing?"]
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
    b'desc': b"Great. 1 2 3 4 5 6 7 8 9 10.",
    b'desc2': b"wonder experience here. 2 4 6 8 10.",
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
        # text_query_exact_phrase1 is crashing.
        match = [text_query_term, text_query_prefix, text_query_prefix2, text_query_exact_phrase1, text_query_exact_phrase2]
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
        assert (result[1] == b"product:1" and result[3] == b"product:5") or (
            result[1] == b"product:5" and result[3] == b"product:1"
        )
        # Test Prefix wildcard searches with a single match. Also, check that when the starting of the word
        # is missing, no matches are found.
        result1 = client.execute_command("FT.SEARCH", "products", '@desc:experi*')
        result2 = client.execute_command("FT.SEARCH", "products", '@desc:expe*')
        result3 = client.execute_command("FT.SEARCH", "products", '@desc:xpe*')
        assert result1[0] == 1 and result2[0] == 1 and result3[0] == 0
        assert result1[1] == b"product:3" and result2[1] == b"product:3"
        # TODO: Update these queries to non stemmed versions after queries are stemmed.
        # Perform an exact phrase search operation on a unique phrase (exists in one doc).
        result1 = client.execute_command("FT.SEARCH", "products", '@desc:"great oak from littl"')
        result2 = client.execute_command("FT.SEARCH", "products", '@desc:"great oak from littl grey acorn grow"')
        assert result1[0] == 1 and result2[0] == 1
        assert result1[1] == b"product:1" and result2[1] == b"product:1"
        result3 = client.execute_command("FT.SEARCH", "products", '@desc:great @desc:oa* @desc:from @desc:lit* @desc:gr* @desc:acorn @desc:gr*')
        assert result3[0] == 1
        assert result3[1] == b"product:1"
        result3 = client.execute_command("FT.SEARCH", "products", '@desc:great @desc:oa* @desc:from @desc:lit* @desc:gr* @desc:acorn @desc:grea*')
        assert result3[0] == 0
        result3 = client.execute_command("FT.SEARCH", "products", '@desc:great @desc:oa* @desc:from @desc:lit* @desc:gr* @desc:acorn @desc:great')
        assert result3[0] == 0
        # Perform an exact phrase search operation on a phrase existing in 2 documents.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"interest desc"')
        assert result[0] == 2
        assert set(result[1::2]) == {b"product:3", b"product:2"}
        # Perform an exact phrase search operation on a phrase existing in 5 documents.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"random word"')
        assert result[0] == 5
        assert set(result[1::2]) == {b"product:1", b"product:2", b"product:3", b"product:4", b"product:5"}
        # Perform an exact phrase search operation on a phrase existing in 1 document.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"uncommon random word"')
        assert result[0] == 1
        assert result[1] == b"product:4"
        # Test for searches on tokens that have common keys, but in-order does not match.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"opposit order"')
        assert result[0] == 0
        # Test for searches on tokens that have common keys, but slop does not match.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"word uniqu"')
        assert result[0] == 0
        # Test for searches on tokens that have common keys and inorder matches but slop does not match.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"uniqu word"')
        assert result[0] == 0
        # Test for searches on tokens that have common keys and slop matches but inorder does not match.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"uniqu word slop"')
        assert result[0] == 0
        # Now, with the inorder, with no slop, it should match.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"uniqu slop word"')
        assert result[0] == 1
        assert result[1] == b"product:5"
        # Validating the inorder and slop checks for a query with multiple tokens.
        result = client.execute_command("FT.SEARCH", "products", '@desc:"1 2 3 4 5 6 7 9 8 0"')
        assert result[0] == 0
        result = client.execute_command("FT.SEARCH", "products", '@desc:"1 2 3 4 5 6 7 9"')
        assert result[0] == 0
        result = client.execute_command("FT.SEARCH", "products", '@desc:"1 2 3 4 5 6 7 8 9 0"')
        assert result[0] == 1
        assert result[1] == b"product:1"

        # TODO: We can test this once the queries are tokenized with punctuation applied.
        # result = client.execute_command("FT.SEARCH", "products", '@desc:"inspector\'s palm"')
        # TODO: We can test this once the queries are tokenized with punctuation and stopword removal applied.
        # result = client.execute_command("FT.SEARCH", "products", '@desc:"random words, these are not"')

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
        
        parser = IndexingTestHelper.get_ft_info(client, "idx4")
        assert parser is not None
        
        # Validate top-level structure
        required_top_level_fields = [
            "index_name", "index_definition", "attributes",
            "num_docs", "num_records", "hash_indexing_failures",
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

        # Validate backfill fields
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, "idx4")        
        
        # Add validation checks for specific fields
        assert parser.num_docs == 0, f"num_docs should be zero"
        assert parser.num_records == 0, f"num_records should be zero"
        assert parser.hash_indexing_failures == 0, f"hash_indexing_failures should be zero"
        
        # Validate queue and delay fields
        assert parser.mutation_queue_size == 0, f"mutation_queue_size should be non-negative, got: {parser.mutation_queue_size}"
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
        assert with_offsets == 1, f"with_offsets is set to true any other value is wrong"
        
        # Validate language setting
        language = parser.parsed_data.get("language", "")
        assert language == "english", f"Expected language 'english', got: '{language}'"

    def test_text_per_field_search(self):
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
        # When searching for tokens in the same doc and same field of what is queried, results are returned.
        result = client.execute_command("FT.SEARCH", "products2", '@desc:"1 2 3 4 5 6 7 8 9 10"')
        assert result[0] == 1
        assert result[1] == b"product:1"
        result = client.execute_command("FT.SEARCH", "products2", '@desc2:"2 4 6 8 10"')
        assert result[0] == 1
        assert result[1] == b"product:1"
        # When searching for tokens in the same doc, but in a different field than what is queried, no results are returned.
        result = client.execute_command("FT.SEARCH", "products2", '@desc:"2 4 6 8 10"')
        assert result[0] == 0
        result = client.execute_command("FT.SEARCH", "products2", '@desc2:"1 2 3 4 5 6 7 8 9 10"')
        assert result[0] == 0

    def test_default_tokenization(self):
        """
        Test FT.CREATE → HSET → FT.SEARCH with full tokenization
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE idx ON HASH SCHEMA content TEXT")
        client.execute_command("HSET", "doc:1", "content", "The quick-running searches are finding EFFECTIVE results!")
        
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

    @pytest.mark.skip(reason="TODO: ingest original words when stemming enabled")
    def test_stemming(self):
        """
        Test text index NOSTEM option
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE idx ON HASH SCHEMA title TEXT content TEXT NOSTEM")
        client.execute_command("HSET", "doc:1", "title", "running fast", "content", "running quickly")

        expected = [1, b'doc:1', [b'content', b'running quickly', b'title', b'running fast']]

        # We can find stems on 'title'
        assert client.execute_command("FT.SEARCH", "idx", '@title:"run"') == expected

        # We cannot find stems on 'content' with NOSTEM
        assert client.execute_command("FT.SEARCH", "idx", '@content:"run"') == [0]

        # We can find original words in both cases
        assert client.execute_command("FT.SEARCH", "idx", '@title:"running"') == expected # TODO: fails here
        assert client.execute_command("FT.SEARCH", "idx", '@content:"running"') == expected

    def test_custom_stopwords(self):
        """
        Test FT.CREATE STOPWORDS option filters out custom stop words
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

    def test_custom_punctuation(self):
        """
        Test FT.CREATE PUNCTUATION directive configures custom tokenization separators
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

    def test_add_update_delete_documents_single_client(self):
        """
        Tests we properly ingest added, updated, and deleted documents from a single client
        """
        client: Valkey = self.server.get_new_client()
        client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "content", "TEXT")
        num_docs = 5

        # Add
        for i in range(num_docs):
            client.execute_command("HSET", f"doc:{i}", "content", f"What a cool document{i}")
        result = client.execute_command("FT.SEARCH", "idx", "@content:document*")
        assert result[0] == num_docs

        # Update
        for i in range(num_docs):
            client.execute_command("HSET", f"doc:{i}", "content", f"What a cool doc{i}")
        result = client.execute_command("FT.SEARCH", "idx", "@content:document*")
        assert result[0] == 0
        result = client.execute_command("FT.SEARCH", "idx", "@content:doc*")
        assert result[0] == num_docs
        
        # Delete
        for i in range(num_docs):
            client.execute_command("DEL", f"doc:{i}")
        result = client.execute_command("FT.SEARCH", "idx", "@content:doc*")
        assert result[0] == 0

    def test_add_update_delete_documents_multi_client(self):
        """
        Tests we properly ingest added, updated, and deleted documents from multiple clients

        TODO: To ensure concurrent ingestion, add a debug config to pause updates from the
              waiting room and then let them index in batches. Otherwise, we're dependent on
              this Python test sending them faster than the server is cutting indexing batches.
        """
        
        def perform_concurrent_searches(clients, num_clients, searches, phase_name):
            """
            Helper function to perform concurrent searches across multiple clients and validate consistency
            
            Args:
                clients: List of client connections
                num_clients: Number of clients to use
                searches: List of (query, description) tuples to execute
                phase_name: Name of the phase for error reporting (ADD/UPDATE/DELETE)
            """
            search_results = {}
            def concurrent_search(client_id):
                client = clients[client_id]
                client_results = []
                for query, desc in searches:
                    result = client.execute_command("FT.SEARCH", "idx", query)
                    client_results.append((desc, result[0]))  # Store description and count
                search_results[client_id] = client_results
            
            threads = []
            for client_id in range(num_clients):
                thread = threading.Thread(target=concurrent_search, args=(client_id,))
                threads.append(thread)
                thread.start()
            
            for thread in threads:
                thread.join()
            
            # Validate concurrent search results are consistent
            expected_results = search_results[0]  # Use first client as reference
            for client_id in range(1, num_clients):
                assert search_results[client_id] == expected_results, f"{phase_name}: Search results inconsistent between clients 0 and {client_id}"
        
        # Setup
        num_clients = 50
        docs_per_client = 50
        clients = [self.server.get_new_client() for _ in range(num_clients)]
        
        # Create the index
        clients[0].execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "content", "TEXT")
        IndexingTestHelper.wait_for_backfill_complete_on_node(clients[0], "idx")        
        
        # Phase 1: Concurrent ADD
        def add_documents(client_id):
            client = clients[client_id]
            for i in range(docs_per_client):
                # Longer content to increase ingestion processing time
                content = f"client{client_id} document doc{i} original content with many additional words to process during indexing. " \
                         f"This extended text includes various terms like analysis, processing, indexing, searching, and retrieval. " \
                         f"The purpose is to create substantial content that requires more computational effort during text analysis. " \
                         f"Additional keywords include: database, storage, performance, optimization, concurrent, threading, synchronization. " \
                         f"More descriptive text about document {i} from client {client_id} with original data and content."
                client.execute_command("HSET", f"doc:{client_id}_{i}", "content", content)
        
        threads = []
        for client_id in range(num_clients):
            thread = threading.Thread(target=add_documents, args=(client_id,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Validate ADD phase with concurrent searching
        client = clients[0]
        total_docs = num_clients * docs_per_client
        
        result = client.execute_command("FT.SEARCH", "idx", "@content:document")
        assert result[0] == total_docs, f"ADD: Expected {total_docs} documents with 'document', got {result[0]}"
        
        result = client.execute_command("FT.SEARCH", "idx", "@content:origin")  # "original" stems to "origin"
        assert result[0] == total_docs, f"ADD: Expected {total_docs} documents with 'origin', got {result[0]}"
        
        # Concurrent search phase after ADD
        add_searches = [
            ("@content:document", "document"),
            ("@content:origin", "origin"),
            ("@content:analysis", "analysis"),
            ("@content:process*", "process*"),
            ("@content:database", "database"),
            ("@content:concurrent", "concurrent")
        ]
        perform_concurrent_searches(clients, num_clients, add_searches, "ADD")
        
        # Phase 2: Concurrent UPDATE
        def update_documents(client_id):
            client = clients[client_id]
            for i in range(docs_per_client):
                # Longer updated content to increase ingestion processing time
                content = f"client{client_id} document doc{i} updated content with comprehensive text for thorough indexing analysis. " \
                         f"This modified version contains different terminology including: revision, modification, alteration, enhancement. " \
                         f"The updated document now features expanded vocabulary for testing concurrent update operations effectively. " \
                         f"Technical terms added: algorithm, computation, execution, validation, verification, testing, debugging. " \
                         f"Enhanced description of document {i} from client {client_id} with updated information and revised content."
                client.execute_command("HSET", f"doc:{client_id}_{i}", "content", content)
        
        threads = []
        for client_id in range(num_clients):
            thread = threading.Thread(target=update_documents, args=(client_id,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Validate UPDATE phase with concurrent searching
        result = client.execute_command("FT.SEARCH", "idx", "@content:origin")  # "original" stems to "origin"
        assert result[0] == 0, f"UPDATE: Expected 0 documents with 'origin', got {result[0]}"
        
        result = client.execute_command("FT.SEARCH", "idx", "@content:updat")  # "updated" stems to "updat"
        assert result[0] == total_docs, f"UPDATE: Expected {total_docs} documents with 'updat', got {result[0]}"
        
        # Concurrent search phase after UPDATE
        update_searches = [
            ("@content:document", "document"),
            ("@content:updat", "updat"),
            ("@content:revision", "revision"),
            ("@content:modif*", "modif*"),
            ("@content:algorithm", "algorithm"),
            ("@content:validation", "validation")
        ]
        perform_concurrent_searches(clients, num_clients, update_searches, "UPDATE")
        
        # Phase 3: Concurrent DELETE
        def delete_documents(client_id):
            client = clients[client_id]
            for i in range(docs_per_client // 2):  # Delete half the documents
                client.execute_command("DEL", f"doc:{client_id}_{i}")
        
        threads = []
        for client_id in range(num_clients):
            thread = threading.Thread(target=delete_documents, args=(client_id,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Validate DELETE phase with concurrent searching
        remaining_docs = total_docs // 2
        
        result = client.execute_command("FT.SEARCH", "idx", "@content:updat")  # "updated" stems to "updat"
        assert result[0] == remaining_docs, f"DELETE: Expected {remaining_docs} documents with 'updat', got {result[0]}"
        
        result = client.execute_command("FT.SEARCH", "idx", "@content:document")
        assert result[0] == remaining_docs, f"DELETE: Expected {remaining_docs} documents with 'document', got {result[0]}"
        
        # Concurrent search phase after DELETE
        delete_searches = [
            ("@content:document", "document"),
            ("@content:updat", "updat"),
            ("@content:revision", "revision"),
            ("@content:algorithm", "algorithm"),
            ("@content:validation", "validation"),
            ("@content:enhanced", "enhanced")
        ]
        perform_concurrent_searches(clients, num_clients, delete_searches, "DELETE")

    def test_suffix_search(self):
        # TODO
        pass
