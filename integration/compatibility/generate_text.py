import pytest, traceback, valkey, time, struct
import sys, os
import pickle
import gzip
import random
from . import data_sets
from .data_sets import *
from valkey.exceptions import ConnectionError
from .generate import ClientRSystem, SYSTEM_R_ADDRESS, BaseCompatibilityTest
from .text_query_builder import TextQueryBuilder, ControlledTextQueryBuilder, QueryGenerationConfig

@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestTextSearchCompatibility(BaseCompatibilityTest):
    TEXT_QUERY_TEST_SEED = 111
    MAX_QUERIES=100
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
        config: QueryGenerationConfig):
        """Common query generation and execution logic.
        """
        self.setup_data(data_set_name, key_type)

        # Extract vocab and field values from the dataset
        vocab = data_sets.extract_vocab_from_text_data(data_set_name, key_type)
        tag_values = data_sets.extract_tag_values_from_text_data(data_set_name, key_type)
        numeric_ranges = data_sets.extract_numeric_ranges_from_text_data(data_set_name, key_type)

        # Initialize QueryBuilder
        builder = ControlledTextQueryBuilder(
            vocab=vocab,
            text_fields=data_sets.TEXT_SCHEMA['text'],
            tag_values=tag_values,
            numeric_ranges=numeric_ranges,
            config=config
        )

        # Generate and execute queries
        seen = set()
        query_count = 0
        attempts = 0
        max_attempts = max_queries * 20
        
        while query_count < max_queries and attempts < max_attempts:
            attempts += 1
            try:
                q = builder.generate(seed=random.randint(1, 1_000_000))
                q_str = builder.render(q)
                
                if q_str not in seen:
                    seen.add(q_str)
                    self.check("FT.SEARCH", f"{key_type}_idx1", q_str, "DIALECT", str(dialect))
                    query_count += 1
            except Exception:
                continue
        
        print(f"Generated {query_count} unique queries from {attempts} attempts")
    
    # base term types

    def test_text_search_exact_match(self, key_type, dialect):
        config_exact_word = QueryGenerationConfig(
            allow_exact_match=True,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=False,
            allow_or=False,
            allow_field_match=False,
            max_terms=1,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="single words",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_exact_word
        )

    def test_text_search_prefix(self, key_type, dialect):
        config_prefix = QueryGenerationConfig(
            allow_exact_match=False,
            allow_prefix=True,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=False,
            allow_or=False,
            allow_field_match=False,
            max_terms=1,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="single words",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_prefix
        )

    def test_text_search_suffix(self, key_type, dialect):
        config_suffix = QueryGenerationConfig(
            allow_exact_match=False,
            allow_prefix=False,
            allow_suffix=True,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=False,
            allow_or=False,
            allow_field_match=False,
            max_terms=1,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="single words",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_suffix
        )
    
    def test_text_phrase(self, key_type, dialect):
        config_phrase = QueryGenerationConfig(
            allow_exact_match=False,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=False,
            allow_or=False,
            allow_field_match=True,
            max_terms=3,
            max_depth=0,
            max_phrase_words=3,
            prob_use_field=1,
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_phrase
        )

    def test_tag(self, key_type, dialect):
        config_tag = QueryGenerationConfig(
            allow_exact_match=False,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=True,
            allow_numeric=False,
            allow_and=True,
            allow_or=False,
            allow_field_match=False,
            max_terms=1,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_tag
        )

    def test_numeric(self, key_type, dialect):
        config_numeric = QueryGenerationConfig(
            allow_exact_match=False,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=True,
            allow_and=True,
            allow_or=False,
            allow_field_match=False,
            max_terms=1,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_numeric
        )
    
    # operators
    
    def test_text_search_exact_and(self, key_type, dialect):
        config_exact_and = QueryGenerationConfig(
            allow_exact_match=True,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=True,
            allow_or=False,
            allow_field_match=False,
            max_terms=3,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_exact_and
        )

    def test_text_search_exact_or(self, key_type, dialect):
        config_exact_or = QueryGenerationConfig(
            allow_exact_match=True,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=False,
            allow_or=True,
            allow_field_match=False,
            max_terms=3,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_exact_or
        )

    def test_text_search_exact_and_or(self, key_type, dialect):
        config_exact_and_or = QueryGenerationConfig(
            allow_exact_match=True,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=True,
            allow_or=True,
            allow_field_match=False,
            max_terms=3,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_exact_and_or
        )
    
    # field matching
    def test_text_search_field_match(self, key_type, dialect):
        config_field_match = QueryGenerationConfig(
            allow_exact_match=True,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=False,
            allow_or=False,
            allow_field_match=True,
            max_terms=3,
            max_depth=0
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_field_match
        )
    
    # groups
    def test_text_search_group_depth_1(self, key_type, dialect):
        config_group_depth_1 = QueryGenerationConfig(
            allow_exact_match=True,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=True,
            allow_or=True,
            allow_field_match=False,
            max_terms=4,
            min_terms=2,
            max_depth=1,
            prob_use_group=0.5
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_group_depth_1
        )
    
    def test_text_search_group_depth_2(self, key_type, dialect):
        config_group_depth_2 = QueryGenerationConfig(
            allow_exact_match=True,
            allow_prefix=False,
            allow_suffix=False,
            allow_phrase=False,
            allow_tag=False,
            allow_numeric=False,
            allow_and=True,
            allow_or=True,
            allow_field_match=False,
            max_terms=5,
            min_terms=3,
            max_depth=2,
            prob_use_group=0.6
        )
        self.generate_and_execute_queries(
            data_set_name="mixed content",
            key_type=key_type,
            dialect=dialect,
            max_queries=self.MAX_QUERIES,
            config=config_group_depth_2
        )

    # # mixed
    # def test_text_search_mixed_terms(self, key_type, dialect):
    #     config_mixed_terms = QueryGenerationConfig(
    #         allow_exact_match=True,
    #         allow_prefix=True,
    #         allow_suffix=True,
    #         allow_phrase=False,
    #         allow_tag=False,
    #         allow_numeric=False,
    #         allow_and=True,
    #         allow_or=True,
    #         allow_field_match=True,
    #         max_depth=1,
    #         max_terms=3,
    #         min_terms=2,
    #         prob_use_field=0.3
    #     )
    #     self.generate_and_execute_queries(
    #         data_set_name="mixed content",
    #         key_type=key_type,
    #         dialect=dialect,
    #         max_queries=self.MAX_QUERIES,
    #         config=config_mixed_terms
    #     )