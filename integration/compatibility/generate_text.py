import pytest, traceback, valkey, time, struct
import sys, os
import pickle
import gzip
import random
from . import data_sets
from .data_sets import *
from valkey.exceptions import ConnectionError
from .generate import ClientRSystem, SYSTEM_R_ADDRESS, BaseCompatibilityTest
from .text_query_builder import TextQueryBuilder

@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestTextSearchCompatibility(BaseCompatibilityTest):
    TEXT_QUERY_TEST_SEED = 111
    MAX_QUERIES=300
    ANSWER_FILE_NAME = "text-search-answers.pickle.gz"

    def setup_data(self, data_set_name, key_type):
        """Override to specify text data source."""
        self.data_set_name = data_set_name
        self.key_type = key_type
        load_data(self.client, data_set_name, key_type, data_source='text')

    def setup_method(self):
        super().setup_method()
        random.seed(self.TEXT_QUERY_TEST_SEED)

    def generate_and_execute_queries(self, data_set_name, key_type, dialect, max_queries,
        max_terms, max_phrase_words, max_depth):
        """Common query generation and execution logic.
        
        Args:
            data_set_name: Name of the dataset to use
            key_type: "hash" or "json"
            dialect: Query dialect version
            max_queries: Maximum number of unique queries to generate
            max_terms: Max terms in query
            max_phrase_words: Max words in a phrase
            max_depth: Max nesting depth
        """
        self.setup_data(data_set_name, key_type)

        # Extract vocab and field values from the dataset
        vocab = data_sets.extract_vocab_from_text_data(data_set_name, key_type)
        tag_values = data_sets.extract_tag_values_from_text_data(data_set_name, key_type)
        numeric_ranges = data_sets.extract_numeric_ranges_from_text_data(data_set_name, key_type)

        # Initialize QueryBuilder
        builder = TextQueryBuilder(
            vocab=vocab,
            text_fields=data_sets.TEXT_SCHEMA['text'],
            tag_values=tag_values,
            numeric_ranges=numeric_ranges
        )

        # Generate and execute queries
        seen = set()
        query_count = 0
        attempts = 0
        max_attempts = max_queries * 20
        
        while query_count < max_queries and attempts < max_attempts:
            attempts += 1
            try:
                q = builder.generate(
                    max_terms=max_terms,
                    max_phrase_words=max_phrase_words,
                    max_depth=max_depth,
                    seed=random.randint(1, 1_000_000)
                )
                q_str = builder.render(q)
                
                if q_str not in seen:
                    seen.add(q_str)
                    self.check("FT.SEARCH", f"{key_type}_idx1", q_str, "DIALECT", str(dialect))
                    query_count += 1
            except Exception:
                continue
        
        print(f"Generated {query_count} unique queries from {attempts} attempts")

    def test_text_search_single_words(self, key_type, dialect):
        self.generate_and_execute_queries(
            data_set_name="single words",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            max_terms=2,
            max_phrase_words=2,
            max_depth=1
        )
    
    def test_text_search_phrase_pairs(self, key_type, dialect):
        self.generate_and_execute_queries(
            data_set_name="phrase pairs",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            max_terms=2,
            max_phrase_words=2,
            max_depth=1
        )

    def test_text_search_triple_phrases(self, key_type, dialect):
        self.generate_and_execute_queries(
            data_set_name="triple phrases",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            max_terms=2,
            max_phrase_words=2,
            max_depth=1
        )

    def test_text_search_mixed_easy(self, key_type, dialect):
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            max_terms=1,
            max_phrase_words=1,
            max_depth=1
        )
    
    def test_text_search_mixed_medium(self, key_type, dialect):
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            max_terms=2,
            max_phrase_words=2,
            max_depth=2
        )
    
    def test_text_search_mixed_hard(self, key_type, dialect):
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            max_terms=3,
            max_phrase_words=3,
            max_depth=3
        )