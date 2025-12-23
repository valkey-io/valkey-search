import pytest
import random
from . import data_sets
from .data_sets import load_data
from .generate import BaseCompatibilityTest
from .text_query_builder import *


@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["hash"])
class TestTextSearchCompatibility(BaseCompatibilityTest):
    TEXT_QUERY_TEST_SEED = 3948
    MAX_QUERIES = 20
    ANSWER_FILE_NAME = "text-search-answers.pickle.gz"

    def setup_data(self, data_set_name, key_type):
        """Override to specify text data source."""
        self.data_set_name = data_set_name
        self.key_type = key_type
        load_data(self.client, data_set_name, key_type, data_source='text')

    def _run_test(self, builder_fn, data_set_name, key_type, dialect, inorder=False, slop=False):
        """Helper to run a test with given term builder function.
        
        Args:
            builder_fn: Function that takes (vocab, rng) and returns term(s) or query string
            data_set_name: Name of the data set to test against
            key_type: Type of key ("json" or "hash")
            dialect: Query dialect version
        """
        self.setup_data(data_set_name, key_type)
        rng = random.Random(self.TEXT_QUERY_TEST_SEED)
        renderer = TermRenderer()

        vocab_by_field = data_sets.extract_vocab_by_field_from_text_data(
            data_set_name, key_type
        )

        seen = set()
        query_count = 0
        attempts = 0
        max_attempts = self.MAX_QUERIES * 20
        while query_count < self.MAX_QUERIES and attempts < max_attempts:
            attempts += 1
            
            # Pick a random field and its vocab
            field = rng.choice(list(vocab_by_field.keys()))
            vocab = vocab_by_field[field]

            try:
                # Generate term(s) or query string using the builder function
                result = builder_fn(vocab, rng)
                
                # Convert to query string if needed
                query_str = result if isinstance(result, str) else renderer.render(result)

                # Only test unique queries
                if query_str not in seen:
                    seen.add(query_str)
                    args = [
                        "FT.SEARCH",
                        f"{key_type}_idx1",
                        query_str,
                    ]
                    if inorder:
                        args.append("INORDER")
                    if slop:
                        slop_value = str(rng.randint(1, 2))
                        args.append("SLOP")
                        args.append(slop_value)
                    args.extend([
                        "DIALECT",
                        str(dialect)
                    ])
                    self.check(*args)
                    query_count += 1
                    
            except Exception:
                # Skip invalid queries and continue
                continue

        print(f"Generated {query_count} unique queries from {attempts} attempts")

    # ========================================================================
    # Base term types
    # ========================================================================

    def test_text_search_exact_match(self, key_type, dialect):
        """Test exact word matching queries."""
        self._run_test(gen_word, "pure text", key_type, dialect)

    def test_text_search_prefix(self, key_type, dialect):
        """Test prefix wildcard queries."""
        self._run_test(gen_prefix, "pure text", key_type, dialect)

    def test_text_search_suffix(self, key_type, dialect):
        """Test suffix wildcard queries."""
        self._run_test(gen_suffix, "pure text", key_type, dialect)

    # ========================================================================
    # Complex grouped queries
    # ========================================================================

    def test_text_search_group_depth2(self, key_type, dialect):
        """Test grouped queries with depth 2."""
        self._run_test(gen_depth2, "pure text", key_type, dialect)

    def test_text_search_group_depth3(self, key_type, dialect):
        """Test grouped queries with depth 3."""
        self._run_test(gen_depth3, "pure text", key_type, dialect)
    
    # def test_text_search_group_depth2_inorder(self, key_type, dialect):
    #     """Test grouped queries with depth 2."""
    #     self._run_test(gen_depth2, "pure text", key_type, dialect, inorder=True)

    # def test_text_search_group_depth3_inorder(self, key_type, dialect):
    #     """Test grouped queries with depth 3."""
    #     self._run_test(gen_depth3, "pure text", key_type, dialect, inorder=True)
    
    # def test_text_search_group_depth2_slop(self, key_type, dialect):
    #     """Test grouped queries with depth 2."""
    #     self._run_test(gen_depth2, "pure text", key_type, dialect, slop=True)

    # def test_text_search_group_depth3_slop(self, key_type, dialect):
    #     """Test grouped queries with depth 3."""
    #     self._run_test(gen_depth3, "pure text", key_type, dialect, slop=True)

    # ========================================================================
    # numeric text datasets
    # ========================================================================

    # def test_text_search_exact_match(self, key_type, dialect):
    #     """Test exact word matching queries."""
    #     self._run_test(gen_word, "numeric text", key_type, dialect)

    # def test_text_search_prefix(self, key_type, dialect):
    #     """Test prefix wildcard queries."""
    #     self._run_test(gen_prefix, "numeric text", key_type, dialect)

    # def test_text_search_suffix(self, key_type, dialect):
    #     """Test suffix wildcard queries."""
    #     self._run_test(gen_suffix, "numeric text", key_type, dialect)

    # def test_text_search_group_depth2(self, key_type, dialect):
    #     """Test grouped queries with depth 2."""
    #     self._run_test(gen_depth2, "numeric text", key_type, dialect)

    # def test_text_search_group_depth3(self, key_type, dialect):
    #     """Test grouped queries with depth 3."""
    #     self._run_test(gen_depth3, "numeric text", key_type, dialect)


    # TODO: fuzzy testing, lexical parsing(backslash, characters)
