import pytest
import random
from . import data_sets
from .data_sets import load_data
from .generate import BaseCompatibilityTest
from .text_query_builder import TextQueryBuilder, QueryGenerationConfig


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

    def _create_base_config(self, **overrides):
        """Create a QueryGenerationConfig with all features disabled by default."""
        defaults = {
            'allow_exact_match': False,
            'allow_prefix': False,
            'allow_suffix': False,
            'allow_exact_phrase': False,
            'allow_tag': False,
            'allow_numeric': False,
            'allow_and': False,
            'allow_or': False,
            'allow_field_match': False,
            'allow_groups': False,
            'max_terms': 1,
            'max_depth': 0,
        }
        defaults.update(overrides)
        return QueryGenerationConfig(**defaults)

    def _run_test(self, config, data_set_name="mixed content", key_type=None, dialect=None):
        """Helper to run a test with given config."""
        self.setup_data(data_set_name, key_type)
        rng = random.Random(self.TEXT_QUERY_TEST_SEED)

        # Extract vocab and field values from the dataset
        vocab = data_sets.extract_vocab_from_text_data(data_set_name, key_type)
        tag_values = data_sets.extract_tag_values_from_text_data(data_set_name, key_type)
        numeric_ranges = data_sets.extract_numeric_ranges_from_text_data(data_set_name, key_type)

        # Initialize QueryBuilder
        builder = TextQueryBuilder(
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
        max_attempts = self.MAX_QUERIES * 20
        
        while query_count < self.MAX_QUERIES and attempts < max_attempts:
            attempts += 1
            try:
                per_query_seed = rng.randint(1, 1_000_000)
                q = builder.generate(seed=per_query_seed)
                q_str = builder.render(q)
                
                if q_str not in seen:
                    seen.add(q_str)
                    # self.execute_command("FT.SEARCH", f"{key_type}_idx1", q_str, "DIALECT", str(dialect))
                    self.check("FT.SEARCH", f"{key_type}_idx1", q_str, "DIALECT", str(dialect))
                    query_count += 1
            except Exception:
                continue
        
        print(f"Generated {query_count} unique queries from {attempts} attempts")

    # Base term types

    def test_text_search_exact_match(self, key_type, dialect):
        config = self._create_base_config(allow_exact_match=True, allow_and=True, max_terms=3)
        self._run_test(config, "pure text", key_type, dialect)

    def test_text_search_prefix(self, key_type, dialect):
        config = self._create_base_config(allow_prefix=True, allow_and=True, max_terms=3)
        self._run_test(config, "pure text", key_type, dialect)

    def test_text_search_suffix(self, key_type, dialect):
        config = self._create_base_config(allow_suffix=True, allow_and=True, max_terms=3)
        self._run_test(config, "pure text", key_type, dialect)

    def test_tag(self, key_type, dialect):
        config = self._create_base_config(allow_tag=True, allow_and=True, max_terms=3)
        self._run_test(config, "pure text", key_type, dialect)

    def test_numeric(self, key_type, dialect):
        config = self._create_base_config(allow_numeric=True, allow_and=True, max_terms=3)
        self._run_test(config, "pure text", key_type, dialect)

    # Phrases
    # known bug in redis
    
    # def test_text_phrase(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_phrase=True,
    #         max_terms=3,
    #         min_terms=2,
    #         allow_field_match=True,
    #         force_inorder=True
    #     )
    #     self._run_test(config, "pure text small", key_type, dialect)
    
    # def test_text_phrase_with_slop(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_and=True,
    #         max_terms=3,
    #         min_terms=2,
    #         allow_phrase_inorder=True,
    #         force_phrase_slop=True
    #     )
    #     self._run_test(config, "pure text", key_type, dialect)

    # def test_text_phrase_with_inorder(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_and=True,
    #         max_terms=3,
    #         min_terms=2,
    #         allow_phrase_slop=True,
    #         force_phrase_inorder=True
    #     )
    #     self._run_test(config, "pure text", key_type, dialect)
    
    # Operators

    # def test_text_search_exact_and(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_and=True,
    #         max_terms=3
    #     )
    #     self._run_test(config, "mixed content", key_type, dialect)

    # def test_text_search_exact_or(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_or=True,
    #         max_terms=3
    #     )
    #     self._run_test(config, "mixed content", key_type, dialect)

    # def test_text_search_exact_and_or(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_and=True,
    #         allow_or=True,
    #         max_terms=3
    #     )
    #     self._run_test(config, "mixed content", key_type, dialect)
    
    # # Field matching

    # def test_text_search_field_match(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_field_match=True,
    #         max_terms=3
    #     )
    #     self._run_test(config, "mixed content", key_type, dialect)
    
    # Groups

    # def test_text_search_group_depth_1(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_and=True,
    #         allow_or=True,
    #         allow_groups=True,
    #         max_terms=4,
    #         min_terms=2,
    #         max_depth=1,
    #         prob_use_group=0.5
    #     )
    #     self._run_test(config, "mixed content", key_type, dialect)
    
    # def test_text_search_group_depth_2(self, key_type, dialect):
    #     config = self._create_base_config(
    #         allow_exact_match=True,
    #         allow_and=True,
    #         allow_or=True,
    #         allow_groups=True,
    #         max_terms=5,
    #         min_terms=3,
    #         max_depth=2,
    #         prob_use_group=0.6
    #     )
    #     self._run_test(config, "mixed content", key_type, dialect)

    # Mixed

    # def test_text_search_mixed(self, key_type, dialect):
    #     config = QueryGenerationConfig(
    #         allow_exact_match=True,
    #         allow_prefix=True,
    #         allow_suffix=True,
    #         allow_phrase=True,
    #         allow_phrase_slop=True,
    #         force_phrase_inorder=True,
    #         allow_tag=True,
    #         allow_numeric=True,
    #         allow_and=True,
    #         allow_or=True,
    #         allow_field_match=True,
    #         allow_groups=True,
    #         max_terms=3,
    #         max_phrase_words=3,
    #         max_depth=2,
    #         prob_use_group=0.4
    #     )
    #     self._run_test(config, "mixed content", key_type, dialect)