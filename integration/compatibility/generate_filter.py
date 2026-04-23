import pytest
from .generate import BaseCompatibilityTest
from .data_sets import load_data


@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["hash", "json"])
class TestFilterCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "filter-answers.pickle.gz"

    def setup_data(self, data_set_name, key_type):
        self.data_set_name = data_set_name
        self.key_type = key_type
        load_data(self.client, data_set_name, key_type, data_source='filter')

    def _run_filter_queries(self, key_type, dialect):
        """Run standard queries against the current filter index."""
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@price:[0 +inf]",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@status:{active}",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@price:[0 +inf]",
            "load", "5", "@__key", "@status", "@price", "@category", "@rating",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@status:{active}",
            "load", "5", "@__key", "@status", "@price", "@category", "@rating",
            "DIALECT", str(dialect),
        )

    def test_filter_base(self, key_type, dialect):
        self.setup_data("filter base", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_tag_eq(self, key_type, dialect):
        self.setup_data("filter tag eq", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_tag_neq(self, key_type, dialect):
        self.setup_data("filter tag neq", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_numeric_gt(self, key_type, dialect):
        self.setup_data("filter numeric gt", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_numeric_range(self, key_type, dialect):
        self.setup_data("filter numeric range", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_exists_rating(self, key_type, dialect):
        self.setup_data("filter exists rating", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_not_exists_category(self, key_type, dialect):
        self.setup_data("filter not exists category", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_combined(self, key_type, dialect):
        self.setup_data("filter combined", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_strlen_numeric(self, key_type, dialect):
        self.setup_data("filter strlen numeric", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_startswith_numeric(self, key_type, dialect):
        self.setup_data("filter startswith numeric", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_contains_text(self, key_type, dialect):
        self.setup_data("filter contains text", key_type)
        self._run_filter_queries(key_type, dialect)
