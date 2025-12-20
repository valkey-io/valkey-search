import pytest
import random
from . import data_sets
from .data_sets import load_data
from .generate import BaseCompatibilityTest
from .text_query_builder import *


@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["json", "hash"])
class TestTextSearchCompatibility(BaseCompatibilityTest):
    TEXT_QUERY_TEST_SEED = 3948
    MAX_QUERIES = 100
    ANSWER_FILE_NAME = "text-search-answers.pickle.gz"

    def setup_data(self, data_set_name, key_type):
        """Override to specify text data source."""
        self.data_set_name = data_set_name
        self.key_type = key_type
        load_data(self.client, data_set_name, key_type, data_source='text')

    def _run_test(self, builder_fn, data_set_name, key_type=None, dialect=None):
        """Helper to run a test with given term builder function."""
        self.setup_data(data_set_name, key_type)
        rng = random.Random(self.TEXT_QUERY_TEST_SEED)

        vocab_by_field = data_sets.extract_vocab_by_field_from_text_data(data_set_name, key_type)
        renderer = TermRenderer()

        seen = set()
        query_count = 0
        attempts = 0
        max_attempts = self.MAX_QUERIES * 20
        
        while query_count < self.MAX_QUERIES and attempts < max_attempts:
            attempts += 1
            try:
                # Pick a random field and its vocab
                field = rng.choice(list(vocab_by_field.keys()))
                vocab = vocab_by_field[field]
                
                # Use the passed-in builder function to generate a term
                term = builder_fn(vocab, rng)
                q_str = term if isinstance(term, str) else renderer.render(term)
                
                if q_str not in seen:
                    seen.add(q_str)
                    self.check("FT.SEARCH", f"{key_type}_idx1", q_str, "DIALECT", str(dialect))
                    query_count += 1
            except Exception:
                continue
        
        print(f"Generated {query_count} unique queries from {attempts} attempts")


    # Base term types
    def test_text_search_exact_match(self, key_type, dialect):
        self._run_test(lambda vocab, rng: gen_word(vocab), "pure text", key_type, dialect)

    def test_text_search_prefix(self, key_type, dialect):
        self._run_test(lambda vocab, rng: gen_prefix(vocab), "pure text", key_type, dialect)

    def test_text_search_suffix(self, key_type, dialect):
        self._run_test(lambda vocab, rng: gen_suffix(vocab), "pure text", key_type, dialect)

    # Groups
    def test_text_search_group_depth2(self, key_type, dialect):
        self._run_test(
            lambda vocab,
            rng: gen_depth2(vocab, rng),
            "pure text",
            key_type,
            dialect,
        )

    def test_text_search_group_depth3(self, key_type, dialect):
        self._run_test(
            lambda vocab,
            rng: gen_depth3(vocab, rng),
            "pure text",
            key_type,
            dialect,
        )