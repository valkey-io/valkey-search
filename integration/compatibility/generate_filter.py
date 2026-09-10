import pytest
from .generate import BaseCompatibilityTest
from .data_sets import (
    load_data,
    ALIAS_FILTER_EXPRS,
    HARD_NUM_FILTER_EXPRS,
    HARD_STR_FILTER_EXPRS,
    MISSING_FIELD_FILTER_EXPRS,
)

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
        """Run standard queries against an index built over the hard-numbers data.

        Two probes, on n1 and on n2, because neither alone can see every row.

        A NUMERIC range query cannot return a document whose value for the
        queried field is NaN, so the @n1 probe is blind to the hash-only
        (n1=NaN, n2=0.0, n3=0.0) row: whether the FILTER admitted or rejected
        that row is invisible, and every comparison operator in
        HARD_NUM_FILTER_EXPRS would pass vacuously on the one input that
        produces an unordered comparison. n2 is finite in every row (n3 is
        not -- the +inf row carries n3=+inf), so @n2:[-inf +inf] reaches the
        whole corpus and pins the engines' NaN behavior.
        """
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@n1:[-inf +inf]",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@n1:[-inf +inf]",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@n2:[-inf +inf]",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@n2:[-inf +inf]",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )

    def _run_hard_strings_queries(self, key_type, dialect):
        """Run standard queries against an index built over the hard-strings data.

        Avoids the bare '*' query -- valkey-search does not support it, so the
        corpus has to be reached with real predicates. Three queries are
        needed to span all seven _HARD_STRINGS rows; with fewer, a FILTER bug
        confined to an unreached row changes no answer and passes silently:

          @s1:{alpha}              -> "alpha", and "Alpha" (TAG is
                                      case-insensitive without CASESENSITIVE)
          @s2:bravo                -> the rows whose TEXT contains "bravo",
                                      including "alpha-bravo", whose hyphen
                                      would need escaping in a TAG query
          @s1:{a|abc|abc123|zulu}  -> the four rows neither of the above
                                      reaches
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
            "FT.SEARCH", f"{key_type}_idx1", "@s1:{a|abc|abc123|zulu}",
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
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@s1:{a|abc|abc123|zulu}",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )

    def _run_alias_queries(self, key_type, dialect):
        """Run standard queries against an index built over the alias schema.

        Every field in _alias_schema is declared `<identifier> AS <alias>`
        with the two differing, so both the FILTER and these probes have to
        resolve the alias. Probes mirror _run_filter_queries but by alias:
        @pr for price, @st for status.

        Only the alias form is covered. Redis resolves the alias and nothing
        else -- with `status AS st` declared, a FILTER on `@status` matches no
        document at all, on HASH and JSON alike -- so an identifier-form case
        would pin a behaviour that is arguably a Redis wart rather than an
        intended contract.
        """
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@pr:[0 +inf]",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.SEARCH", f"{key_type}_idx1", "@st:{active}",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@pr:[0 +inf]",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )
        self.check(
            "FT.AGGREGATE", f"{key_type}_idx1", "@st:{active}",
            "load", "1", "@__key",
            "DIALECT", str(dialect),
        )

    # Table-driven tests over filters that reference fields by SCHEMA alias.
    # Covers HASH and JSON through the key_type parametrization.
    @pytest.mark.parametrize(
        "dataset", sorted(ALIAS_FILTER_EXPRS.keys()),
        ids=lambda d: d.replace(" ", "_"),
    )
    def test_filter_alias(self, key_type, dialect, dataset):
        self.setup_data(dataset, key_type)
        self._run_alias_queries(key_type, dialect)

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

    # Hash-only: applying a string function (strlen / startswith) to a NUMERIC
    # attribute is well-defined for hash (the original string bytes are stored
    # verbatim) but diverges on JSON. JSON parses the value to a double, losing
    # the original textual form. valkey-search re-stringifies via %.11g and
    # applies the function (e.g. strlen(100)==3); Redis Stack on JSON appears
    # not to coerce a numeric to a string at all and admits every document
    # unconditionally. Coverage of strlen/startswith on JSON is provided by
    # HARD_STR_FILTER_EXPRS, which operates on TEXT/TAG fields where both
    # engines agree.
    def test_filter_strlen_numeric(self, key_type, dialect):
        if key_type == "json":
            pytest.skip("strlen() on JSON NUMERIC behaves inconsistently in Redis Stack")
        self.setup_data("filter strlen numeric", key_type)
        self._run_filter_queries(key_type, dialect)

    def test_filter_startswith_numeric(self, key_type, dialect):
        if key_type == "json":
            pytest.skip("startswith() on JSON NUMERIC behaves inconsistently in Redis Stack")
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

    # Table-driven tests exercising missing-field behavior inside boolean
    # compositions (&&, ||, negation, relational). Uses the FILTER_DOCS
    # data set, which has rows with missing `status`, `category`, and
    # `rating` fields, so each expression's nil branch actually fires on
    # at least one document.
    @pytest.mark.parametrize(
        "dataset", sorted(MISSING_FIELD_FILTER_EXPRS.keys()),
        ids=lambda d: d.replace(" ", "_"),
    )
    def test_filter_missing_field_composition(self, key_type, dialect, dataset):
        self.setup_data(dataset, key_type)
        self._run_filter_queries(key_type, dialect)
