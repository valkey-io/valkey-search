import pytest
from .generate import BaseCompatibilityTest
from .data_sets import load_data, HARD_NUM_FILTER_EXPRS, HARD_STR_FILTER_EXPRS

@pytest.mark.parametrize("dialect", [2])
@pytest.mark.parametrize("key_type", ["hash", "json"])
class TestFilterCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "filter-answers.pickle.gz"

    def setup_data(self, data_set_name, key_type):
        self.data_set_name = data_set_name
        self.key_type = key_type
        load_data(self.client, data_set_name, key_type, data_source='filter')

    # FT.AGGREGATE in these helpers only loads @__key. The point of a FILTER
    # compatibility test is to verify *which* documents the FILTER admits;
    # loading other fields exposes formatting differences (e.g. hash "-0" vs
    # "0", JSON aliases vs "$.n1" paths) that have nothing to do with FILTER.

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
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@status:{active}",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )

    def _run_hard_numbers_queries(self, key_type, dialect):
        """Run standard queries against an index built over the hard-numbers data."""
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@n1:[-inf +inf]",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@n1:[-inf +inf]",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )

    def _run_hard_strings_queries(self, key_type, dialect):
        """Run standard queries against an index built over the hard-strings data.

        Avoids the bare '*' query — valkey-search does not support it. Two
        complementary queries (@s1 tag + s2 text) cover the whole document set
        across the FILTER variants.
        """
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@s1:{alpha}",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@s2:bravo",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@s1:{alpha}",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@s2:bravo",
            "load", "1", "@__key",
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

    # Table-driven tests over edge-case numeric inputs.
    # Each entry in HARD_NUM_FILTER_EXPRS exercises one numeric operator or
    # function against the hard-numbers data set (zero, +/-0.5, +/-1, large
    # magnitudes, +/- inf and NaN for hash).
    @pytest.mark.parametrize(
        "dataset", sorted(HARD_NUM_FILTER_EXPRS.keys()),
        ids=lambda d: d.replace(" ", "_"),
    )
    def test_filter_hard_numbers(self, key_type, dialect, dataset):
        self.setup_data(dataset, key_type)
        self._run_hard_numbers_queries(key_type, dialect)

    # Table-driven tests over edge-case string inputs.
    # Each entry in HARD_STR_FILTER_EXPRS exercises one string operator or
    # function against the hard-strings data set (empty, single char, mixed
    # case, multi-token, punctuation).
    @pytest.mark.parametrize(
        "dataset", sorted(HARD_STR_FILTER_EXPRS.keys()),
        ids=lambda d: d.replace(" ", "_"),
    )
    def test_filter_hard_strings(self, key_type, dialect, dataset):
        self.setup_data(dataset, key_type)
        self._run_hard_strings_queries(key_type, dialect)
