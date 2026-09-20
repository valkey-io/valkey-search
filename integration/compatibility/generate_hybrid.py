"""Capture FT.HYBRID reference answers from the Redis query engine.

FT.HYBRID does not exist in the `redis/redis-stack-server` image the other
generators once used (its RediSearch is 2.x); the command was added in the
Redis 8.4 query engine. This generator needed its own `redis:8` override until
the shared reference image moved to `redis:latest` (#1366). The override is
gone: it now inherits DOCKER_IMAGE from BaseCompatibilityTest like every other
generator.

The LOAD clause is swept separately, in test_load_clause. One form is still
recorded `xfail` -- loading a field the index does not have; see
TODO(load-unknown-field) and integration/compatibility/unsupported_tests.md.

The corpus is `hybrid text` (see data_sets.py): a text corpus with a spread of
term frequency, document frequency and document length, plus a vector field
whose KNN ordering deliberately disagrees with the text ordering -- so a fused
result is distinguishable from either arm on its own.

Two restrictions come from the state of the FT.HYBRID scoring path, and every
command generated here stays inside them:

  1. Text fields are indexed NOSTEM (the scoring path does not stem).
  2. Query terms are plain words only -- no prefix (`alpha*`), suffix,
     wildcard or fuzzy (`%alpha%`) forms.

Boolean structure over plain terms (intersection, union, negation, field
scoping) is fair game and is exercised below.

BM25STD is the only scorer swept; it is the default on both engines, and is
named explicitly so the reference answers do not depend on that default.
"""

import struct

import pytest

from .data_sets import HYBRID_VECTOR_DIM
from .generate import BaseCompatibilityTest

# Query vectors, chosen against the `hybrid text` ramp (doc i sits at
# (1 + i/4, i/2, 0, 0)): NEAR sits on top of doc 0, MID lands among the middle
# documents, and FAR sits past the end of the ramp, so the three produce three
# different KNN orderings over the same corpus.
#
# MID is deliberately *off* the ramp's lattice. Squared L2 distance along the
# ramp is a parabola in the document index, so a query point whose nearest
# position is an integer or half-integer index puts two documents at exactly
# equal distance -- and a tie in the vector arm leaves the fused ranking to
# each engine's tie-break, which this suite should not be pinning down.
QUERY_VECTORS = {
    "near": [1.0, 0.0, 0.0, 0.0],
    "mid": [4.0, 6.4, 0.0, 0.0],
    "far": [9.0, 15.0, 0.0, 0.0],
}

# Plain-word text queries. Restriction 2 above: terms only, no wildcards.
#
#   * Single terms span the IDF range, from `stone` (every document) through
#     `alpha` (18 of 24) down to `epsilon` (exactly one).
#   * `omega` matches nothing, which leaves the SEARCH arm empty and reduces
#     the fused result to the vector arm alone.
#   * The boolean forms mix the title and body distributions, which were
#     offset against each other precisely so these are not degenerate.
SEARCH_QUERIES = [
    # Single terms, decreasing document frequency.
    "@title:alpha",
    "@title:beta",
    "@title:gamma",
    "@title:delta",
    "@title:epsilon",
    "@body:stone",
    "@body:river",
    "@body:canyon",
    # No field prefix: `alpha` occurs only in `title`, so this also covers the
    # all-fields search path.
    "alpha",
    # Empty arm.
    "@title:omega",
    # Intersection across the two text fields.
    "@title:alpha @body:mountain",
    "@title:gamma @body:river",
    # Union. Spelled with the field repeated on each side rather than as
    # `@title:(alpha|gamma)`: Redis accepts both, the Valkey query parser only
    # the distributed form, and the two spellings select the same documents --
    # so this keeps the sweep about hybrid scoring rather than re-testing a
    # known text-parser gap.
    "(@title:alpha|@title:gamma)",
    "(@body:forest|@body:canyon)",
    # Union intersected with a term.
    "(@title:alpha|@title:gamma) @body:river",
    # Negation.
    "@body:stone -@title:alpha",
]

# COMBINE clauses as (method, options). hybrid() supplies the argument count
# and appends the score alias.
#
# The alias is counted *inside* the method block -- `COMBINE RRF 4 CONSTANT 60
# YIELD_SCORE_AS x`, not `COMBINE RRF 2 CONSTANT 60 YIELD_SCORE_AS x`. Redis
# accepts either placement; the Valkey parser only accepts the alias inside the
# count, so the counted form is the one spelling that reaches both engines.
RRF_COMBINES = [
    ("RRF", []),
    ("RRF", ["CONSTANT", "1"]),
    ("RRF", ["CONSTANT", "20"]),
    ("RRF", ["CONSTANT", "300"]),
]

RRF_WINDOW_COMBINES = [
    ("RRF", ["WINDOW", "3"]),
    ("RRF", ["WINDOW", "10"]),
    ("RRF", ["CONSTANT", "20", "WINDOW", "5"]),
]

# Weights that sum to one cannot tell an engine that normalizes them from one
# that applies them raw -- both give the same answer. The pairs below that sum
# to something else are what separates the two readings, and `ALPHA 1 BETA 1`
# distinguishes a third: an engine that averages rather than adds.
LINEAR_COMBINES = [
    ("LINEAR", ["ALPHA", "0.5", "BETA", "0.5"]),
    ("LINEAR", ["ALPHA", "1", "BETA", "0"]),
    ("LINEAR", ["ALPHA", "0", "BETA", "1"]),
    ("LINEAR", ["ALPHA", "0.2", "BETA", "0.8"]),
    ("LINEAR", ["ALPHA", "1", "BETA", "1"]),
    ("LINEAR", ["ALPHA", "2", "BETA", "2"]),
    ("LINEAR", ["ALPHA", "3", "BETA", "1"]),
    ("LINEAR", ["ALPHA", "0.7", "BETA", "0.7"]),
]

# Every sweep that is about *scoring* pins WINDOW and LIMIT rather than taking
# the defaults, so that a disagreement about either changes the size of exactly
# the sweeps written to look for it and not of every other answer. Both engines
# default LIMIT to 10 rows; the per-arm WINDOW default is what
# test_default_arm_sizes is for.
NON_BINDING_WINDOW = "100"   # > corpus size, so WINDOW never binds
PINNED_LIMIT = ("0", "10")

# The LOAD clause every other sweep pins, so that those sweeps are about
# scoring rather than about which columns come back. test_load_clause varies it.
LOAD_ALL = ("LOAD", "*")
NO_LOAD = ()

# (knn_count, knn_args) for the VSIM clause, or None for no KNN block at all.
# The count is the number of arguments in the block, and YIELD_SCORE_AS is
# never one of them -- it names the arm and sits after the block, which is
# the only placement Redis accepts.
#
# SHARD_K_RATIO tunes how much of K each shard returns during a fanout. This
# implementation parses and discards it; the value does not change what a
# query returns, only how much work the shards do, so the two engines still
# have to agree on the answer.
#
# Only forms both engines accept are swept. Valkey additionally tolerates an
# empty block (`KNN 0`) and a block that omits K, defaulting K in each case;
# Redis rejects both. That leniency is an extension rather than a divergence
# in an answer, so it is pinned by testing/ft_hybrid_parser_test.cc instead of
# being recorded here as a permanent mismatch.
KNN_CLAUSES = [
    None,                                        # no block: K defaults to 10
    ("2", ["K", "5"]),
    ("2", ["K", "10"]),
    ("2", ["K", "24"]),
    ("4", ["K", "10", "EF_RUNTIME", "50"]),
    ("4", ["K", "10", "SHARD_K_RATIO", "0.5"]),
    ("6", ["K", "10", "EF_RUNTIME", "50", "SHARD_K_RATIO", "1.0"]),
]


# HASH only. A JSON index under `LOAD *` returns the document as a single
# `$` column, which no `@field` reference in a pipeline stage resolves
# against -- an FT.AGGREGATE limitation being fixed on its own branch. That
# has nothing to do with FT.HYBRID, and sweeping JSON here would only pin
# that gap. FT.HYBRID has no key-type-specific code of its own.
@pytest.mark.parametrize("key_type", ["hash"])
class TestHybridCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "hybrid-answers.pickle.gz"
    DATA_SET = "hybrid text"

    def setup_data(self, key_type):
        super().setup_data(self.DATA_SET, key_type)

    def hybrid(
        self,
        key_type,
        search_query,
        *,
        combine=("RRF", ["CONSTANT", "60"]),
        knn=("2", ["K", "10"]),
        vector_field="@vec",
        vsim_filter=None,
        vector="near",
        search_score_as="text_score",
        vector_score_as=None,
        fused_score_as="hybrid_score",
        window=NON_BINDING_WINDOW,
        limit=PINNED_LIMIT,
        load=LOAD_ALL,
        tail=(),
        xfail=False,
    ):
        """Issue one FT.HYBRID command and record the reference answer.

        `LOAD *` is the default so that every other sweep sees the whole
        document and stays about scoring; test_load_clause is where the clause
        itself is varied. Without any LOAD, both engines reply with just the
        key and the score aliases.

        Every score alias ends in `score`, which is what makes
        compatibility_test.compare_row() compare it as a float rather than
        byte-for-byte -- the two engines format the same score to different
        precision.
        """
        cmd = [
            "FT.HYBRID", f"{key_type}_idx1",
            "SEARCH", search_query, "SCORER", "BM25STD",
        ]
        if search_score_as:
            cmd += ["YIELD_SCORE_AS", search_score_as]
        cmd += ["VSIM", vector_field, "$q"]
        if knn is not None:
            knn_count, knn_args = knn
            cmd += ["KNN", knn_count, *knn_args]
        # A FILTER inside the VSIM clause pre-filters the vector search, and
        # goes after the KNN block and before YIELD_SCORE_AS -- put after the
        # alias it ends the clause and becomes an aggregate stage instead.
        if vsim_filter is not None:
            cmd += ["FILTER", *([vsim_filter] if isinstance(vsim_filter, str)
                                else list(vsim_filter))]
        if vector_score_as:
            cmd += ["YIELD_SCORE_AS", vector_score_as]
        method, options = combine
        if window is not None:
            options = [*options, "WINDOW", window]
        if fused_score_as:
            options = [*options, "YIELD_SCORE_AS", fused_score_as]
        cmd += ["COMBINE", method, str(len(options)), *options]
        cmd += list(load)
        # Pipeline stages first, then LIMIT. An explicit LIMIT is a positional
        # stage on both engines, so writing it last is what makes `GROUPBY` see
        # the whole fused set rather than only the first page.
        cmd += list(tail)
        if limit is not None:
            cmd += ["LIMIT", *limit]
        cmd += [
            "PARAMS", "2", "q",
            struct.pack(f"<{HYBRID_VECTOR_DIM}f", *QUERY_VECTORS[vector]),
        ]
        self.execute_command(cmd)
        if xfail:
            self.answers[-1]["xfail"] = True

    # -----------------------------------------------------------------
    # The SEARCH arm: does a BM25STD score computed over this corpus reach
    # the fused reply, and does it rank the same way?
    # -----------------------------------------------------------------

    def test_search_arm_text_queries(self, key_type):
        self.setup_data(key_type)
        for query in SEARCH_QUERIES:
            self.hybrid(key_type, query)

    def test_search_arm_score_alias_only(self, key_type):
        """Same sweep without a per-arm alias: the fused score must not depend
        on whether the text arm's own score was named."""
        self.setup_data(key_type)
        for query in SEARCH_QUERIES:
            self.hybrid(key_type, query, search_score_as=None)

    # -----------------------------------------------------------------
    # Fusion methods.
    # -----------------------------------------------------------------

    def test_rrf_constants(self, key_type):
        self.setup_data(key_type)
        for combine in RRF_COMBINES:
            for query in ["@title:alpha", "(@title:alpha|@title:gamma)", "@body:canyon"]:
                self.hybrid(key_type, query, combine=combine)

    def test_rrf_fractional_constants(self, key_type):
        """The RRF constant is a real number, not an integer.

        Fusion divides by `constant + rank`, so a fractional constant has to
        land strictly between the integers around it. Sweeping the halves
        beside the whole numbers is what separates a real constant from one
        truncated on the way in -- a sweep of integers alone would pass either
        way.
        """
        self.setup_data(key_type)
        for constant in ["0", "0.5", "1", "1.25", "1.5", "1.75", "2", "2.5",
                         "59.5", "60", "60.5", "1000.5"]:
            self.hybrid(key_type, "@title:alpha",
                        combine=("RRF", ["CONSTANT", constant]))

    def test_linear_weight_range(self, key_type):
        """Weights outside [0, 1] are meaningful and both engines take them.

        A negative weight subtracts an arm and one above 1 amplifies it, so
        neither is an input to reject. The zero cases pin that an arm can be
        switched off entirely from the COMBINE clause.
        """
        self.setup_data(key_type)
        weights = [("0", "1"), ("1", "0"), ("0", "0"), ("-0.5", "1.5"),
                   ("2.5", "0.5"), ("1000", "1"), ("0.001", "0.002")]
        for alpha, beta in weights:
            self.hybrid(key_type, "@title:alpha",
                        combine=("LINEAR", ["ALPHA", alpha, "BETA", beta]))

    def test_rrf_window(self, key_type):
        """WINDOW carries its own value, so no default is appended."""
        self.setup_data(key_type)
        for combine in RRF_WINDOW_COMBINES:
            for query in ["@title:alpha", "@body:stone", "@title:omega"]:
                self.hybrid(key_type, query, combine=combine, window=None)

    def test_linear_weights(self, key_type):
        self.setup_data(key_type)
        for combine in LINEAR_COMBINES:
            for query in ["@title:alpha", "(@title:alpha|@title:gamma)", "@body:canyon"]:
                self.hybrid(key_type, query, combine=combine)

    def test_linear_default_weights(self, key_type):
        """LINEAR with neither ALPHA nor BETA written is not an error: the
        reference fuses with ALPHA 0.3 and BETA 0.7.

        Three spellings, because the weights are defaulted by what was *not*
        written and the other sub-arguments must not disturb that: the bare
        block, the bare block carrying a binding WINDOW, and the block carrying
        only YIELD_SCORE_AS. The last is the one that puts the fused score in
        the reply, so it pins the weights themselves rather than only the
        order they produce; the first two pin the order alone, which is what a
        caller who never named the score sees.

        The explicit `ALPHA 0.3 BETA 0.7` row beside them is the control: it is
        what makes a future drift in the defaults show up as two answers
        diverging from each other rather than as one answer simply changing.

        Writing exactly *one* weight is an error on both engines. That is a
        parse outcome rather than an answer, so it is pinned in
        testing/ft_hybrid_parser_test.cc instead of here.
        """
        self.setup_data(key_type)
        for query in ["@title:alpha", "(@title:alpha|@title:gamma)", "@body:canyon"]:
            self.hybrid(key_type, query, combine=("LINEAR", []),
                        window=None, fused_score_as=None)
            self.hybrid(key_type, query, combine=("LINEAR", ["WINDOW", "5"]),
                        window=None, fused_score_as=None)
            self.hybrid(key_type, query, combine=("LINEAR", []), window=None)
            self.hybrid(key_type, query,
                        combine=("LINEAR", ["ALPHA", "0.3", "BETA", "0.7"]),
                        window=None)

    def test_linear_with_a_binding_window(self, key_type):
        """LINEAR was only ever swept with a window too wide to bind, so
        whether WINDOW bounds a LINEAR arm the way it bounds an RRF one was
        never compared."""
        self.setup_data(key_type)
        for window in ["3", "5", "10"]:
            for query in ["@title:alpha", "@body:stone"]:
                self.hybrid(key_type, query,
                            combine=("LINEAR", ["ALPHA", "0.5", "BETA", "0.5",
                                                "WINDOW", window]),
                            window=None)

    # -----------------------------------------------------------------
    # The VSIM arm.
    # -----------------------------------------------------------------

    def test_knn_variations(self, key_type):
        self.setup_data(key_type)
        for knn in KNN_CLAUSES:
            for vector in QUERY_VECTORS:
                self.hybrid(key_type, "@title:alpha", knn=knn, vector=vector)

    def test_distance_metrics(self, key_type):
        """The same vectors under each distance metric.

        A vector arm reports a similarity derived from its distance, and the
        derivation differs per metric -- `1/(1+d)` for L2, `(1+d)/2` for inner
        product, `1-d/2` for cosine. Sweeping one metric left the other two
        formulas uncompared, which is how an inner-product arm came to report
        the same similarity for each of its three best matches.

        Inner product is the interesting one: its distance is `1 - dot` and so
        unbounded below, which is the only way a negative distance reaches
        fusion at all.
        """
        self.setup_data(key_type)
        for field in ["@vec", "@vec_ip", "@vec_cos"]:
            for vector in QUERY_VECTORS:
                for query in ["@title:alpha", "@body:stone", "@title:omega"]:
                    self.hybrid(key_type, query, vector_field=field,
                                vector=vector,
                                vector_score_as="vector_score")

    def test_distance_metrics_under_linear(self, key_type):
        """And through LINEAR, which sums the arm scores rather than their
        ranks, so a wrong similarity moves the fused score directly instead of
        being absorbed by the ranking."""
        self.setup_data(key_type)
        for field in ["@vec", "@vec_ip", "@vec_cos"]:
            for weights in [["ALPHA", "0.5", "BETA", "0.5"],
                            ["ALPHA", "0", "BETA", "1"]]:
                self.hybrid(key_type, "@title:alpha", vector_field=field,
                            combine=("LINEAR", weights),
                            vector_score_as="vector_score")

    # Expressions in the FT.SEARCH query language, which is what a VSIM FILTER
    # takes -- not the aggregate FILTER's expression language, which is what the
    # same token means after COMBINE.
    VSIM_FILTERS = [
        "@price:[0 20]",
        "@price:[21 52]",
        "@color:{red}",
        "@color:{red|blue}",
        "@title:alpha",
        "@body:stone",
        "-@color:{red}",
    ]

    def test_vsim_filter_pre_filters_the_vector_arm(self, key_type):
        """A FILTER before COMBINE narrows what the vector search considers.

        The SEARCH arm is given a term that matches nothing, so the vector arm
        is the only contributor and the filter is visible in the rows. With a
        matching SEARCH arm the union hides it: the text arm returns the
        documents the filter excluded anyway.
        """
        self.setup_data(key_type)
        for expr in self.VSIM_FILTERS:
            self.hybrid(key_type, "@title:omega", vsim_filter=expr,
                        vector_score_as="vector_score")

    def test_vsim_filter_alongside_a_matching_search_arm(self, key_type):
        """The same filters with a SEARCH arm that does match, where what is
        being compared is the union of a filtered vector arm and an unfiltered
        text one."""
        self.setup_data(key_type)
        for expr in ["@price:[0 20]", "@color:{red}", "@title:alpha"]:
            self.hybrid(key_type, "@title:alpha", vsim_filter=expr,
                        vector_score_as="vector_score")

    def test_vsim_filter_does_not_touch_the_score(self, key_type):
        """A filter decides membership, never the score. A text predicate is
        the case that matters: for a SEARCH arm written as a vector query it
        trades the distance for text relevance, and here it must not."""
        self.setup_data(key_type)
        # Written the way the rest of this generator writes text queries: one
        # term per field reference, an intersection across fields, and a union
        # distributed over the field. The compact forms (`@body:stone river`,
        # `@title:alpha|beta`) and two terms on one field are text-parser gaps
        # that would be tested here instead of the filter.
        for expr in ["@title:alpha", "(@title:alpha|@title:beta)"]:
            self.hybrid(key_type, "@title:omega", vsim_filter=expr,
                        vector_score_as="vector_score")

    # Skipped, not xfail: a conjunction containing a text predicate loses that
    # conjunct when used as a KNN pre-filter, and the defect is in the shared
    # pre-filter path rather than in FT.HYBRID. FT.SEARCH and FT.AGGREGATE
    # produce identical counts and identical wrong rows for the same
    # expressions, so this suite is the wrong place to track it -- see
    # unsupported_tests.md 5.10 for the measurements.
    #
    # Re-enable when the pre-filter honours a conjunction. The expressions are
    # kept here so that re-enabling is deleting one line.
    @pytest.mark.skip(reason="see unsupported_tests.md 5.10 -- a text conjunct "
                             "is dropped from a KNN pre-filter, in the shared "
                             "path rather than in FT.HYBRID")
    def test_vsim_filter_conjunction(self, key_type):
        self.setup_data(key_type)
        for expr in ["@title:alpha @body:river",
                     "@price:[0 30] @title:alpha",
                     "@color:{red} @title:alpha"]:
            self.hybrid(key_type, "@title:omega", vsim_filter=expr,
                        vector_score_as="vector_score")

    def test_vsim_filter_with_a_count(self, key_type):
        """The count is optional, and when given it counts the tokens that
        follow."""
        self.setup_data(key_type)
        for expr in ["@price:[0 20]", "@color:{red}"]:
            self.hybrid(key_type, "@title:omega", vsim_filter=["1", expr],
                        vector_score_as="vector_score")

    def test_vsim_filter_interacts_with_k(self, key_type):
        """K bounds the filtered set rather than the set before filtering --
        or the other way round, which is the thing being pinned."""
        self.setup_data(key_type)
        for k in ["1", "3", "10", "24"]:
            self.hybrid(key_type, "@title:omega", knn=("2", ["K", k]),
                        vsim_filter="@color:{red}", limit=("0", "100"),
                        vector_score_as="vector_score")

    def test_vector_score_alias(self, key_type):
        """The VSIM arm's own score, surfaced through YIELD_SCORE_AS."""
        self.setup_data(key_type)
        for vector in QUERY_VECTORS:
            self.hybrid(
                key_type, "@title:alpha", vector=vector, vector_score_as="vector_score"
            )

    def test_score_alias_on_both_arms(self, key_type):
        """Both arms named at once. Each alias has to carry its own arm's
        score, and naming them must not disturb the fused score."""
        self.setup_data(key_type)
        for query in ["@title:alpha", "@body:canyon", "@title:omega"]:
            self.hybrid(
                key_type,
                query,
                search_score_as="text_score",
                vector_score_as="vector_score",
            )

    def test_k_bounds_the_vector_arm_only(self, key_type):
        """K caps how many neighbors the vector arm contributes; the text arm
        keeps returning everything it matched. `@body:stone` matches the whole
        corpus, so a K below the corpus size is visible in the fused set only
        as the arm each row came from, never as a smaller total."""
        self.setup_data(key_type)
        for k in ["1", "3", "12", "24"]:
            self.hybrid(
                key_type,
                "@body:stone",
                knn=("2", ["K", k]),
                limit=("0", "100"),
                vector_score_as="vector_score",
            )

    # -----------------------------------------------------------------
    # The LOAD clause.
    #
    # FT.HYBRID honors LOAD the way FT.AGGREGATE does: it names the columns the
    # reply carries, `AS` renames them, and the rename is visible to the stages
    # that follow. These are compared normally.
    #
    # The one form still recorded `xfail` is a LOAD naming a field the index
    # does not have; see TODO(load-unknown-field) below.
    # -----------------------------------------------------------------

    def _load_cases(self, key_type):
        """(load-clause tokens, trailing stages) for the LOAD sweep."""
        cases = [
            # No LOAD at all: the document key and the score aliases.
            (NO_LOAD, []),
            # A single field, and a subset of fields.
            (["LOAD", "1", "@price"], []),
            (["LOAD", "2", "@price", "@color"], []),
            # The document key is loadable by name, beside a field in either
            # order, renamed, and reachable from a following stage. It is a
            # reserved column rather than a document field, so none of that is
            # implied by the field cases above -- and the score column, the
            # other reserved one, diverges on exactly the rename (5.5b).
            (["LOAD", "1", "@__key"], []),
            (["LOAD", "2", "@__key", "@price"], []),
            (["LOAD", "2", "@price", "@__key"], []),
            (["LOAD", "3", "@__key", "AS", "id"], []),
            (["LOAD", "4", "@__key", "AS", "id", "@price"], []),
            (["LOAD", "1", "@__key"], ["SORTBY", "2", "@__key", "ASC"]),
            (["LOAD", "1", "@__key"], ["APPLY", "upper(@__key)", "AS",
                                       "shout"]),
            # The key column crossed with a real field name in both
            # directions: a field renamed onto `__key`, and `__key` renamed
            # onto a field's name. Neither engine reserves the name against
            # either, so the rename is what reaches the reply -- unlike
            # `__score`, which Redis does protect (5.5b).
            (["LOAD", "3", "@price", "AS", "__key"], []),
            (["LOAD", "4", "@price", "AS", "__key", "@color"], []),
            (["LOAD", "3", "@__key", "AS", "price"], []),
            (["LOAD", "4", "@__key", "AS", "price", "@color"], []),
            # Naming it twice without a rename: one column, not two.
            (["LOAD", "2", "@__key", "@__key"], []),
            # A rename has to be reachable from the stages that follow it,
            # the same as a renamed document field.
            (["LOAD", "3", "@__key", "AS", "id"],
             ["SORTBY", "2", "@id", "ASC"]),
            (["LOAD", "3", "@__key", "AS", "id"],
             ["APPLY", "upper(@id)", "AS", "shout"]),
            # Renaming an existing field. The LOAD count covers the `AS` and
            # the alias too.
            (["LOAD", "3", "@price", "AS", "cost"], []),
            (["LOAD", "3", "@title", "AS", "heading"], []),
            (["LOAD", "4", "@price", "AS", "cost", "@color"], []),
            # Renaming to a name that is already another field of the index:
            # the alias has to resolve to the renamed column, not the field it
            # collides with.
            (["LOAD", "3", "@price", "AS", "color"], []),
            # Renaming two fields at once.
            (["LOAD", "6", "@price", "AS", "a", "@color", "AS", "b"], []),
            # (Renaming onto an output name the same clause already claims --
            # `LOAD 4 @color @price AS color` -- is rejected by valkey-search
            # as stricter input validation, a documented divergence covered by
            # the FT.AGGREGATE suite. Nothing FT.HYBRID-specific, so not swept
            # again here.)
            # A rename has to be visible to the stages that follow it.
            (["LOAD", "3", "@price", "AS", "cost"],
             ["SORTBY", "2", "@cost", "ASC"]),
            (["LOAD", "3", "@price", "AS", "cost"],
             ["APPLY", "@cost * 2", "AS", "doubled"]),
            (["LOAD", "6", "@price", "AS", "a", "@color", "AS", "b"],
             ["GROUPBY", "1", "@b", "REDUCE", "SUM", "1", "@a", "AS", "total"]),
            # A loaded field has to be visible to a FILTER.
            (["LOAD", "1", "@price"], ["FILTER", "@price > 20"]),
        ]
        return cases

    def test_load_clause(self, key_type):
        self.setup_data(key_type)
        for load, tail in self._load_cases(key_type):
            self.hybrid(key_type, "@title:alpha", load=load, tail=tail)

    # Skipped until PR 1381 ("Ask for content with `all_content`, not by
    # leaving the list empty") merges, plus the three-line follow-up it needs
    # in FT.HYBRID.
    #
    # What these sweep is `LOAD *` feeding a pipeline stage. `LOAD *` today
    # means "return_attributes is empty", which is the same encoding as "fetch
    # nothing", so a stage that names a field cannot ask for it alongside the
    # whole record. 1381 splits the two with an explicit `all_content` flag.
    # FT.HYBRID does not get the fix for free: its `FusedResolver` copies
    # `return_attributes` and `no_content` but not `all_content`, and
    # `WantsNoDatabaseContent` still tests `loadall_`, so landing 1381 without
    # touching `src/commands/ft_hybrid.cc` silently empties `LOAD *` replies
    # instead of fixing them.
    #
    # Measured on a merged build (hybrid + 1381 + that follow-up) against a
    # redis:8 reference, over the 15 cases below and on both key types:
    #
    #                        hash                json
    #   hybrid alone         10 pass / 1 fail    4 pass / 7 fail
    #   merged + follow-up   11 pass / 0 fail    11 pass / 0 fail
    #
    # (Four of the 15 record a reference error rather than an answer -- Redis
    # auto-loads for SORTBY and GROUPBY but not for APPLY or FILTER -- so they
    # pass unconditionally and are kept only to pin that behaviour, the same
    # way 1381 does for FT.AGGREGATE.)
    #
    # The seven JSON failures are exactly unsupported_tests.md 5.2, the reason
    # this suite is HASH-only. When 1381 lands: drop the skip, and 5.2 can
    # come off with the class parametrized over both key types -- but
    # test_load_unknown_field's `LOAD 1 $.price` case has to be gated to HASH
    # first, because that path resolves on a JSON index and would XPASS for
    # the wrong reason.
    @pytest.mark.skip(reason="needs PR 1381 plus the all_content follow-up in "
                             "ft_hybrid.cc; enable once both have merged")
    def test_loadall_feeding_a_stage(self, key_type):
        self.setup_data(key_type)
        cases = [
            # `LOAD *` plus one stage, one per stage kind.
            (LOAD_ALL, ["SORTBY", "2", "@price", "ASC"]),
            (LOAD_ALL, ["SORTBY", "2", "@title", "ASC"]),
            # A tag sort key takes only four values over the corpus, so the
            # page boundary lands inside a tie that the two engines break
            # differently. The unique second key is what makes it comparable.
            (LOAD_ALL, ["SORTBY", "4", "@color", "ASC", "@price", "ASC"]),
            # A column fusion produced rather than one the document carries.
            (LOAD_ALL, ["SORTBY", "2", "@hybrid_score", "DESC"]),
            (LOAD_ALL, ["GROUPBY", "1", "@color",
                        "REDUCE", "COUNT", "0", "AS", "cnt"]),
            (LOAD_ALL, ["GROUPBY", "1", "@color",
                        "REDUCE", "SUM", "1", "@price", "AS", "total"]),
            # APPLY and FILTER over a field only `LOAD *` brings in. The
            # reference refuses both, so these record an error.
            (LOAD_ALL, ["APPLY", "@price * 2", "AS", "doubled"]),
            (LOAD_ALL, ["FILTER", "@price < 25"]),
            # Two stages: the field has to survive one hop further down.
            (LOAD_ALL, ["SORTBY", "2", "@price", "ASC",
                        "APPLY", "@price * 2", "AS", "doubled"]),
            (LOAD_ALL, ["GROUPBY", "1", "@color",
                        "REDUCE", "SUM", "1", "@price", "AS", "total",
                        "APPLY", "@total * 2", "AS", "bumped"]),
            (LOAD_ALL, ["APPLY", "@price * 2", "AS", "doubled",
                        "SORTBY", "2", "@doubled", "ASC"]),
            (LOAD_ALL, ["FILTER", "@price < 25",
                        "SORTBY", "2", "@price", "ASC"]),
            # Controls, which separate "the stage works" from "`LOAD *` fed
            # the stage": the same stage behind an explicit LOAD, and behind
            # no LOAD at all.
            (["LOAD", "1", "@price"], ["SORTBY", "2", "@price", "ASC"]),
            (NO_LOAD, ["SORTBY", "2", "@price", "ASC"]),
            (NO_LOAD, ["GROUPBY", "1", "@color",
                       "REDUCE", "COUNT", "0", "AS", "cnt"]),
        ]
        for load, tail in cases:
            self.hybrid(key_type, "@title:alpha", load=load, tail=tail)

    # TODO(load-unknown-field): Redis lets a LOAD name a field the index does
    # not have and simply returns no column for it; Valkey rejects the command
    # with "Index field `x` does not exist". FT.AGGREGATE does the same thing
    # on the same input, so this is not an FT.HYBRID gap -- when it is fixed
    # for the aggregate pipeline these should start matching, the run will
    # report XPASS, and both the `xfail=True` here and the entry in
    # unsupported_tests.md should come off.
    def test_load_unknown_field(self, key_type):
        self.setup_data(key_type)
        cases = [
            ["LOAD", "1", "@nosuchfield"],
            ["LOAD", "3", "@nosuchfield", "AS", "mystery"],
            ["LOAD", "2", "@price", "@nosuchfield"],
        ]
        # A JSON path against a HASH index names nothing, the same way an
        # unknown attribute does.
        cases += [["LOAD", "1", "$.price"]]
        # The reserved column names are matched case-sensitively by both
        # engines, so an upper-case spelling names nothing either. Included
        # because the lower-case spellings are real columns, which makes this
        # the one place a caller can reach the unknown-field gap by writing a
        # name that exists.
        cases += [["LOAD", "1", "@__KEY"], ["LOAD", "1", "@__SCORE"]]
        for load in cases:
            self.hybrid(key_type, "@title:alpha", load=load, xfail=True)

    # -----------------------------------------------------------------
    # Defaults. These are the cases the sweeps above deliberately pin, so
    # that a disagreement about a default shows up here and only here.
    # -----------------------------------------------------------------

    def test_default_limit(self, key_type):
        """No LIMIT clause: how many rows come back by default?"""
        self.setup_data(key_type)
        for query in ["@title:alpha", "@body:stone", "@body:canyon"]:
            self.hybrid(key_type, query, limit=None)

    def test_default_arm_sizes(self, key_type):
        """How many documents each arm contributes when nothing says.

        LIMIT is pinned wide enough not to bind, which is what makes the count
        itself visible: with the usual `LIMIT 0 10` a disagreement about the
        per-arm default is only detectable if it happens to disturb the first
        page. `@body:stone` matches the whole corpus, so the text arm is as
        large as it can be.
        """
        self.setup_data(key_type)
        for query in ["@body:stone", "@title:alpha", "@title:omega"]:
            self.hybrid(key_type, query, window=None, limit=("0", "100"),
                        vector_score_as="vector_score")

    def test_default_arm_sizes_track_k(self, key_type):
        """And whether the vector arm's default contribution follows K.

        If the per-arm default is a fixed window the vector arm stops growing
        once K passes it; if it follows K it keeps growing. Either is a
        defensible design, so what matters is that both engines do the same
        thing -- and the row counts here say which.
        """
        self.setup_data(key_type)
        for k in ["1", "5", "10", "11", "20", "24"]:
            self.hybrid(key_type, "@body:stone", knn=("2", ["K", k]),
                        window=None, limit=("0", "100"),
                        vector_score_as="vector_score")

    def test_default_window(self, key_type):
        """No WINDOW: how much of each arm reaches the fusion, and is the
        fused list itself capped?"""
        self.setup_data(key_type)
        for query in ["@title:alpha", "@body:stone", "@body:canyon"]:
            self.hybrid(key_type, query, window=None)

    def test_no_combine_clause(self, key_type):
        """FT.HYBRID with no COMBINE at all falls back to the default fusion."""
        self.setup_data(key_type)
        for query in ["@title:alpha", "@body:canyon"]:
            cmd = [
                "FT.HYBRID", f"{key_type}_idx1",
                "SEARCH", query, "SCORER", "BM25STD",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
                "LOAD", "*", "LIMIT", "0", "10",
                "PARAMS", "2", "q",
                struct.pack(f"<{HYBRID_VECTOR_DIM}f", *QUERY_VECTORS["near"]),
            ]
            self.execute_command(cmd)

    # -----------------------------------------------------------------
    # The aggregate post-pipeline over the fused record set.
    # -----------------------------------------------------------------

    def test_limit(self, key_type):
        self.setup_data(key_type)
        for limit in [("0", "1"), ("0", "5"), ("2", "5"), ("0", "100")]:
            self.hybrid(key_type, "@title:alpha", limit=limit)

    # NOTE on what a SORTBY sweep can and cannot pin down here.
    # compatibility_test.py re-sorts both replies on a key it derives from the
    # command before comparing them, so what these cases compare is which rows
    # come back and what each column holds -- not the order the engine emitted
    # them in. Emitted order is asserted in integration/test_ft_hybrid.py,
    # which compares replies as they arrive.

    def test_sortby(self, key_type):
        """The page is wide enough to hold every fused row, deliberately.

        K also covers the corpus: a wide page shows the whole fused set, and
        while the vector field is HNSW its tail is approximate.

        Two documents here share a fused score, and the usual page of ten cuts
        between them. Which of two equally-scored rows survives that cut is
        not specified by either engine -- they order the pair differently in
        the fused list to begin with -- so paging through the tie compares
        something neither engine promises. Comparing the whole ordering
        compares the sort.
        """
        self.setup_data(key_type)
        for sort in [
            ["SORTBY", "2", "@price", "ASC"],
            ["SORTBY", "2", "@price", "DESC"],
            ["SORTBY", "2", "@hybrid_score", "DESC"],
        ]:
            self.hybrid(key_type, "@title:alpha", tail=sort,
                        limit=("0", "100"), knn=("2", ["K", "40"]))

    def test_sortby_every_kind_of_column(self, key_type):
        """SORTBY over each kind of column a fused record carries, alone and
        in combination: the fused score, a per-arm score, a plain document
        field, the document key, and the name the fused score falls back to
        when COMBINE does not alias it.

        Both engines refuse a per-arm score here, and for the same reason: a
        YIELD_SCORE_AS alias is not a field of the index and, under `LOAD *`,
        not a loaded column either. The cases are swept so that the agreement
        is on the record, and so that one engine starting to accept them shows
        up as a difference.
        """
        self.setup_data(key_type)
        cases = [
            # The fused score, both directions.
            ["SORTBY", "2", "@hybrid_score", "DESC"],
            ["SORTBY", "2", "@hybrid_score", "ASC"],
            # Per-arm scores, each named by its own arm's YIELD_SCORE_AS.
            ["SORTBY", "2", "@text_score", "DESC"],
            ["SORTBY", "2", "@vector_score", "DESC"],
            # A plain field, and the document key.
            ["SORTBY", "2", "@price", "ASC"],
            ["SORTBY", "2", "@__key", "ASC"],
            # The fused score's default name, which a reply carries but no
            # stage resolves.
            ["SORTBY", "2", "@__score", "DESC"],
            # Combinations. A score first with a field to break its ties, the
            # same pair the other way round, and the two arms against each
            # other.
            ["SORTBY", "4", "@hybrid_score", "DESC", "@price", "ASC"],
            ["SORTBY", "4", "@price", "ASC", "@hybrid_score", "DESC"],
            ["SORTBY", "4", "@text_score", "DESC", "@vector_score", "DESC"],
            ["SORTBY", "4", "@vector_score", "DESC", "@price", "ASC"],
        ]
        for sort in cases:
            # Wide page, for the reason test_sortby gives: the default page
            # cuts between two rows that share a fused score. And K covering
            # the corpus, because a wide page shows the whole fused set,
            # whose tail is approximate while the vector field is HNSW.
            self.hybrid(key_type, "@title:alpha", tail=sort,
                        limit=("0", "100"), knn=("2", ["K", "40"]),
                        vector_score_as="vector_score")

    def test_sortby_per_arm_score_is_reachable(self, key_type):
        """Sorting by an arm's own score, under each LOAD clause that allows
        it.

        Most documents are found by one arm only, so most rows carry one
        arm's alias and not the other's. That makes these cases turn on two
        things at once: the alias resolving at all, and a row that has no
        value for the sort key going last rather than tying with everything.
        """
        self.setup_data(key_type)
        for load in [NO_LOAD, ["LOAD", "1", "@price"],
                     ["LOAD", "2", "@price", "@color"]]:
            for sort in [
                ["SORTBY", "2", "@vector_score", "DESC"],
                ["SORTBY", "2", "@text_score", "DESC"],
                ["SORTBY", "2", "@vector_score", "ASC"],
                ["SORTBY", "2", "@text_score", "ASC"],
                ["SORTBY", "4", "@text_score", "DESC", "@vector_score", "ASC"],
                # A per-arm key first, then a field to break its ties, and the
                # same pair the other way round.
                ["SORTBY", "4", "@vector_score", "DESC", "@price", "ASC"],
                ["SORTBY", "4", "@price", "ASC", "@vector_score", "DESC"],
                # The fused score against a per-arm one: every row has the
                # former, only some have the latter.
                ["SORTBY", "4", "@hybrid_score", "DESC", "@text_score", "ASC"],
            ]:
                self.hybrid(key_type, "@title:alpha", load=load, tail=sort,
                            vector_score_as="vector_score")

    # Grouping by a per-arm alias works, and both engines agree on it, but it
    # is not swept: in this harness the comparison is not reliable enough to
    # keep. Measured directly against the reference, on a freshly loaded index
    # and repeated runs, the two engines return byte-identical groups every
    # time -- 21 groups for the two-key form, 19 for the one-key form, same
    # counts, same keys. Replayed inside the suite it fails about one run in
    # three, always the same way: one group short, because one document is
    # missing from the fused set.
    #
    #   redis 19 groups, valkey 18      GROUPBY 1 @text_score
    #   redis 21 groups, valkey 20      GROUPBY 2 @text_score @vector_score
    #
    # The text arm is exact and K covers the corpus, so a missing document is
    # not approximation; it is the index not being fully caught up when the
    # case runs. Every other sweep pages at ten and never notices a marginal
    # document, which is why this one is the first to show it. Until the
    # harness's data-load wait is tightened, sweeping this would put a test
    # that fails a third of the time into the suite.
    #
    # test_ft_hybrid.py::test_groupby_per_arm_score covers that the grouping
    # works.

    # `SORTBY ... MAX` is deliberately not swept: the reference refuses it on
    # this command, so there is no answer to compare against, and what it
    # means on the reference's FT.AGGREGATE is an open question -- see
    # unsupported_tests.md 5.4d. test_ft_hybrid.py::
    # test_sortby_max_bounds_the_rows_the_sort_emits pins what it does here.

    def test_pipeline_stages_over_scores(self, key_type):
        """The other stages, over the same columns. The fused score is
        reachable to all of them on both engines; a per-arm score is reachable
        to neither under `LOAD *`."""
        self.setup_data(key_type)
        cases = [
            ["APPLY", "@hybrid_score * 100", "AS", "scaled_score"],
            ["APPLY", "@hybrid_score + @price", "AS", "mixed_score"],
            ["APPLY", "@text_score + @vector_score", "AS", "arm_sum_score"],
            ["APPLY", "@vector_score", "AS", "copied_score"],
            ["FILTER", "@hybrid_score > 0.01"],
            ["FILTER", "@vector_score > 0.2"],
            ["FILTER", "@text_score > 0"],
            ["GROUPBY", "1", "@color",
             "REDUCE", "MAX", "1", "@hybrid_score", "AS", "max_score"],
            ["GROUPBY", "1", "@color",
             "REDUCE", "MAX", "1", "@vector_score", "AS", "max_vec_score"],
            ["GROUPBY", "1", "@color",
             "REDUCE", "AVG", "1", "@text_score", "AS", "avg_text_score"],
            ["GROUPBY", "1", "@color",
             "REDUCE", "COUNT", "0", "AS", "cnt"],
        ]
        for tail in cases:
            self.hybrid(key_type, "@title:alpha", tail=tail,
                        vector_score_as="vector_score")

    def test_unaliased_fused_score_without_load(self, key_type):
        """No LOAD and no COMBINE alias: the fused score is named `__score`.

        The narrowest shape in which the default score name is observable at
        all -- any LOAD clause replaces the default projection, and any
        COMBINE alias renames the column -- so this is the case that pins the
        name itself.
        """
        self.setup_data(key_type)
        for query in ["@title:alpha", "@body:canyon"]:
            self.hybrid(key_type, query, fused_score_as=None, load=NO_LOAD,
                        search_score_as=None)

    def test_output_score_across_combine_and_load(self, key_type):
        """Every COMBINE method against every LOAD shape, unaliased.

        The default score column is emitted when the caller gave no LOAD
        clause and dropped when they gave one, whichever way the scores were
        fused. Sweeping the methods together is what separates the emission
        rule from any one fusion method: an RRF-only sweep would leave open
        whether LINEAR carries its score differently.
        """
        self.setup_data(key_type)
        for combine in [("RRF", ["CONSTANT", "60"]),
                        ("RRF", ["CONSTANT", "1"]),
                        ("LINEAR", ["ALPHA", "0.5", "BETA", "0.5"]),
                        ("LINEAR", ["ALPHA", "0.9", "BETA", "0.1"])]:
            for load in [NO_LOAD,
                         LOAD_ALL,
                         ["LOAD", "1", "@price"],
                         ["LOAD", "2", "@price", "@color"],
                         ["LOAD", "1", "@__key"]]:
                self.hybrid(key_type, "@title:alpha", combine=combine,
                            load=load, fused_score_as=None,
                            search_score_as=None)

    def test_output_score_alias_across_combine_and_load(self, key_type):
        """The same sweep with COMBINE ... YIELD_SCORE_AS.

        A named score is an explicit request, so unlike the default it
        survives every LOAD shape. Pairing this with the sweep above is what
        makes the reply's score column a function of two inputs rather than
        one.
        """
        self.setup_data(key_type)
        for combine in [("RRF", ["CONSTANT", "60"]),
                        ("LINEAR", ["ALPHA", "0.5", "BETA", "0.5"])]:
            for load in [NO_LOAD,
                         LOAD_ALL,
                         ["LOAD", "1", "@price"],
                         ["LOAD", "2", "@price", "@color"]]:
                self.hybrid(key_type, "@title:alpha", combine=combine,
                            load=load, fused_score_as="hybrid_score",
                            search_score_as=None)

    def test_per_arm_score_alias_combinations(self, key_type):
        """Each arm named, neither, and both, against every LOAD shape.

        A per-arm alias is an explicit request like the COMBINE alias, so it
        reaches the reply under `LOAD *` as well -- including for JSON, where
        `LOAD *` returns the document as a single `$` column and the aliases
        sit beside it. A document only one arm found carries only that arm's
        alias, which the narrow SEARCH query below is chosen to produce.
        """
        self.setup_data(key_type)
        aliases = [(None, None),
                   ("text_score", None),
                   (None, "vector_score"),
                   ("text_score", "vector_score")]
        for search_as, vector_as in aliases:
            for load in [NO_LOAD, LOAD_ALL, ["LOAD", "1", "@price"]]:
                # A wide query, where most rows are in both arms.
                self.hybrid(key_type, "@title:alpha", load=load,
                            search_score_as=search_as,
                            vector_score_as=vector_as,
                            fused_score_as=None)
                # A narrow one, where most rows are in the vector arm only
                # and so carry no text-arm alias.
                self.hybrid(key_type, "@title:epsilon", load=load,
                            search_score_as=search_as,
                            vector_score_as=vector_as,
                            fused_score_as=None)

    def test_score_columns_json(self, key_type):
        """The score-column rules again, on a JSON index.

        The class sweeps HASH only, because a pipeline stage naming an
        indexed field does not resolve against a JSON document under `LOAD *`
        (see the note on the class, and unsupported_tests.md 5.2). None of the
        score columns depend on that: a score alias is written by fusion, not
        read out of the document, so it is reachable on JSON where an
        `@color` reference is not. This sweep therefore uses no pipeline
        stage, and pins that the emission rules are the same for both key
        types on both engines.

        `key_type` is ignored: this case names its own.
        """
        self.setup_data("json")
        for load in [NO_LOAD, LOAD_ALL, ["LOAD", "1", "@price"]]:
            # Unaliased, so the default `__score` rule is what is under test.
            self.hybrid("json", "@title:alpha", load=load,
                        fused_score_as=None, search_score_as=None)
            # Each arm named, then both, then all three.
            self.hybrid("json", "@title:alpha", load=load,
                        fused_score_as=None, search_score_as="text_score")
            self.hybrid("json", "@title:alpha", load=load,
                        fused_score_as=None, search_score_as=None,
                        vector_score_as="vector_score")
            self.hybrid("json", "@title:alpha", load=load,
                        fused_score_as=None, search_score_as="text_score",
                        vector_score_as="vector_score")
            self.hybrid("json", "@title:alpha", load=load,
                        fused_score_as="hybrid_score",
                        search_score_as="text_score",
                        vector_score_as="vector_score")
            # A narrow query, so most rows are in the vector arm only and
            # carry no text-arm alias.
            self.hybrid("json", "@title:epsilon", load=load,
                        fused_score_as=None, search_score_as="text_score",
                        vector_score_as="vector_score")
        # A LOAD that names the score column back into the projection.
        for load in [["LOAD", "1", "@__score"],
                     ["LOAD", "2", "@price", "@__score"]]:
            self.hybrid("json", "@title:alpha", load=load,
                        fused_score_as=None, search_score_as=None)

    def test_load_naming_the_score_column(self, key_type):
        """A LOAD clause that names `@__score` brings it back.

        A LOAD replaces the default projection, which is where the fused score
        normally comes from -- so naming the score in the LOAD clause is the
        one way to get it alongside a chosen set of fields. It is as explicit
        a request as COMBINE ... YIELD_SCORE_AS, and has to be honoured the
        same way.
        """
        self.setup_data(key_type)
        cases = [
            ["LOAD", "1", "@__score"],
            ["LOAD", "2", "@price", "@__score"],
            ["LOAD", "2", "@__score", "@price"],
            ["LOAD", "2", "@__score", "@__key"],
            ["LOAD", "3", "@price", "@__score", "@color"],
            # Named twice without a rename: one column, not two.
            ["LOAD", "2", "@__score", "@__score"],
        ]
        for load in cases:
            self.hybrid(key_type, "@title:alpha", load=load,
                        fused_score_as=None, search_score_as=None)
        # And with no COMBINE clause at all, so the score is the default
        # fusion's rather than a named method's.
        self.hybrid(key_type, "@title:alpha", load=["LOAD", "1", "@__score"],
                    fused_score_as=None, search_score_as=None, window=None)
        # A loaded score column has to be reachable from a later stage, the
        # same as any other loaded field.
        self.hybrid(key_type, "@title:alpha",
                    load=["LOAD", "1", "@__score"],
                    tail=["SORTBY", "2", "@__score", "DESC"],
                    fused_score_as=None, search_score_as=None)
        # The alias ends in `score` so that compare_row() compares it as a
        # float: the two engines format the same double to different
        # precision, and a derived column is otherwise compared byte for byte.
        self.hybrid(key_type, "@title:alpha",
                    load=["LOAD", "1", "@__score"],
                    tail=["APPLY", "@__score * 2", "AS", "doubled_score"],
                    fused_score_as=None, search_score_as=None)
        # Loading the score beside the per-arm aliases: three score columns,
        # one asked for by LOAD and two by the arms.
        self.hybrid(key_type, "@title:alpha",
                    load=["LOAD", "2", "@__score", "@price"],
                    fused_score_as=None, search_score_as="text_score",
                    vector_score_as="vector_score")

    # Skipped, not xfail: Redis is broken here, so there is no answer worth
    # recording. `LOAD 3 @__score AS s` renames the fused score and Redis then
    # returns rows carrying no columns at all --
    #
    #   redis:  [b'total_results', 20, b'results', [[], [], []], ...]
    #   valkey: [3, [b's', b'0.0320184417069'], [b's', b'0.0317460335791'], ...]
    #
    # -- and with the renamed column feeding a stage it drops every *row*,
    # reporting the reason in the reply's warnings:
    #
    #   [b'total_results', 20, b'results', [],
    #    b'warnings', [b'SEARCH_VALUE_NOT_FOUND Could not find the value ...']]
    #
    # It accepts the clause, loses the data, and says so. Every other LOAD
    # rename in this suite emits the renamed column, and so does a LOAD of
    # `@__score` without the rename, so this is a defect rather than a rule
    # this engine should converge on. `xfail` would say we intend to match it
    # one day; we do not, because matching it means discarding a column the
    # caller named.
    #
    # The cases are kept so that re-enabling is deleting one line, for
    # whenever Redis fixes the rename. See unsupported_tests.md 5.5b.
    @pytest.mark.skip(reason="Redis is broken for a renamed `@__score`: it "
                             "returns rows with no columns, or no rows at "
                             "all -- see unsupported_tests.md 5.5b")
    def test_load_renaming_the_score_column(self, key_type):
        self.setup_data(key_type)
        for load in [["LOAD", "3", "@__score", "AS", "s"],
                     ["LOAD", "4", "@price", "@__score", "AS", "s"],
                     # Renamed onto the other reserved column's name.
                     ["LOAD", "3", "@__score", "AS", "__key"],
                     # Renamed onto a real field's name.
                     ["LOAD", "3", "@__score", "AS", "price"],
                     # Both reserved columns renamed at once, either order,
                     # and with a document field alongside. Redis emits the
                     # renamed key and drops the renamed score.
                     ["LOAD", "6", "@__key", "AS", "a", "@__score", "AS", "b"],
                     ["LOAD", "6", "@__score", "AS", "b", "@__key", "AS", "a"],
                     ["LOAD", "7", "@__key", "AS", "a", "@__score", "AS", "b",
                      "@price"]]:
            self.hybrid(key_type, "@title:alpha", load=load,
                        fused_score_as=None, search_score_as=None)
        # The renamed score feeding a later stage, which is where Redis loses
        # the rows rather than only the column.
        self.hybrid(key_type, "@title:alpha",
                    load=["LOAD", "3", "@__score", "AS", "s"],
                    tail=["SORTBY", "2", "@s", "DESC"],
                    fused_score_as=None, search_score_as=None)
        self.hybrid(key_type, "@title:alpha",
                    load=["LOAD", "3", "@__score", "AS", "s"],
                    tail=["APPLY", "@s * 2", "AS", "doubled_score"],
                    fused_score_as=None, search_score_as=None)

    # TODO(load-rename-onto-score): `LOAD 3 @price AS __score` renames a
    # different field onto the name the default projection uses for the fused
    # score. The two engines resolve the clash the opposite way:
    #
    #   redis:  [('__score', <the fused score>)]   -- the rename is discarded
    #   valkey: [('__score', b'21')]               -- the price, as asked for
    #
    # Ours emits the column the caller named; Redis silently drops the field
    # and keeps the score under it. Neither reply is malformed, and reversing
    # ours would mean discarding a field the LOAD clause asked for by name, so
    # this is recorded rather than matched. Once COMBINE has renamed the score
    # away there is no clash and both engines agree, which the third case
    # below pins. See unsupported_tests.md 5.5b.
    def test_load_renaming_another_field_onto_the_score_name(self, key_type):
        self.setup_data(key_type)
        for load in [["LOAD", "3", "@price", "AS", "__score"],
                     ["LOAD", "4", "@price", "AS", "__score", "@color"],
                     # The key column renamed onto the score's name: Redis
                     # keeps the score and drops the rename, as it does for a
                     # document field.
                     ["LOAD", "3", "@__key", "AS", "__score"]]:
            self.hybrid(key_type, "@title:alpha", load=load,
                        fused_score_as=None, search_score_as=None,
                        xfail=True)
        # With the fused score renamed by COMBINE, `__score` is free and the
        # clash does not arise. Compared normally.
        self.hybrid(key_type, "@title:alpha",
                    load=["LOAD", "3", "@price", "AS", "__score"],
                    fused_score_as="hybrid_score", search_score_as=None)

    # TODO(load-unknown-field): the same gap as test_load_unknown_field and
    # unsupported_tests.md 5.1, reached through the score column: COMBINE ...
    # YIELD_SCORE_AS renames the fused score, so `@__score` no longer names
    # anything, and Redis ignores the unknown LOAD entry while valkey-search
    # rejects the command. Kept separate from that sweep because this is the
    # one way a LOAD entry becomes unknown without the caller misspelling a
    # field.
    def test_load_score_column_that_combine_renamed_away(self, key_type):
        self.setup_data(key_type)
        self.hybrid(key_type, "@title:alpha", load=["LOAD", "1", "@__score"],
                    fused_score_as="hybrid_score", search_score_as=None,
                    xfail=True)

    def test_per_arm_and_fused_score_aliases_together(self, key_type):
        """All three aliases at once, and the fused one renamed away from the
        default, so that three independently named score columns have to coexist
        in one reply."""
        self.setup_data(key_type)
        for load in [NO_LOAD, LOAD_ALL, ["LOAD", "1", "@price"],
                     ["LOAD", "2", "@price", "@title"]]:
            self.hybrid(key_type, "@title:alpha", load=load,
                        search_score_as="text_score",
                        vector_score_as="vector_score",
                        fused_score_as="hybrid_score")

    def test_noncolliding_score_aliases_under_load_all(self, key_type):
        """`LOAD *` with a yielded score whose name collides with nothing.

        The corpus's only columns are the schema's -- title, body, color,
        price, vec, vec_ip, vec_cos -- and `LOAD *` brings every one of them
        into the reply. The aliases below match none of them, none of each
        other, and neither reserved name (`__key`, `__score`), so there is
        nothing for the alias to land on top of: the score column is simply
        added beside the document.

        That is the shape where the two engines have no room to disagree, and
        it is pinned here on its own because collision handling is moving from
        a parse-time rejection to a runtime resolution. Whatever that does to a
        name that *does* collide, these four shapes have to keep answering
        exactly as they do now, on each of the three YIELD_SCORE_AS positions
        separately and on all three at once.

        Both queries are swept for each shape: `@title:alpha` is wide, so most
        rows are in both arms and carry both per-arm aliases, while
        `@title:epsilon` matches one document, so nearly every row comes from
        the vector arm alone and carries no SEARCH-arm alias at all. A rule
        that only added the column when the arm found the row would look
        correct under the first query and not the second.

        Every alias ends in `score` for the reason hybrid() gives: that is what
        makes compatibility_test.compare_row() compare it as a float.
        """
        self.setup_data(key_type)
        # (search arm, vsim arm, combine)
        alias_sets = [
            ("uniq_search_score", None, None),
            (None, "uniq_vsim_score", None),
            (None, None, "uniq_fused_score"),
            ("arm_text_score", "arm_vector_score", "fusion_total_score"),
        ]
        for search_as, vector_as, fused_as in alias_sets:
            for query in ["@title:alpha", "@title:epsilon"]:
                self.hybrid(key_type, query, load=LOAD_ALL,
                            search_score_as=search_as,
                            vector_score_as=vector_as,
                            fused_score_as=fused_as)

    def test_unaliased_fused_score_with_explicit_load(self, key_type):
        """No score named anywhere, and a LOAD clause that names its columns.

        The control for test_unaliased_fused_score_without_load above: an
        explicit LOAD suppresses the default score column on both engines, the
        same way `LOAD *` does, so these are compared normally. That is what
        confines the `__score` divergence to the LOAD-less shape rather than
        leaving it a property of score-aliasing in general.
        """
        self.setup_data(key_type)
        for load in [["LOAD", "1", "@price"],
                     ["LOAD", "2", "@price", "@color"],
                     ["LOAD", "1", "@__key"],
                     ["LOAD", "2", "@__key", "@price"],
                     ["LOAD", "3", "@price", "@color", "@title"]]:
            self.hybrid(key_type, "@title:alpha", load=load,
                        fused_score_as=None, search_score_as=None)

    def test_unaliased_fused_score(self, key_type):
        """COMBINE without YIELD_SCORE_AS, under `LOAD *`. Neither engine emits
        a fused-score column in that shape, so what this compares is the
        document columns and the top-10 membership."""
        self.setup_data(key_type)
        for query in ["@title:alpha", "@body:canyon"]:
            self.hybrid(key_type, query, fused_score_as=None)
        # Naming one arm but not the fusion, and the reverse.
        self.hybrid(key_type, "@title:alpha", fused_score_as=None,
                    vector_score_as="vector_score")
        self.hybrid(key_type, "@title:alpha", fused_score_as=None,
                    search_score_as=None, vector_score_as=None)

    def test_groupby_reduce(self, key_type):
        self.setup_data(key_type)
        for reduce in [
            ["REDUCE", "COUNT", "0", "AS", "cnt"],
            ["REDUCE", "SUM", "1", "@price", "AS", "total"],
            ["REDUCE", "MAX", "1", "@hybrid_score", "AS", "max_score"],
            ["REDUCE", "AVG", "1", "@hybrid_score", "AS", "avg_score"],
        ]:
            self.hybrid(
                key_type,
                "@title:alpha",
                tail=["GROUPBY", "1", "@color", *reduce],
            )

    def test_apply(self, key_type):
        self.setup_data(key_type)
        for expr, alias in [
            ("@price * 2", "dbl"),
            ("upper(@color)", "shout"),
            ("@hybrid_score + @price", "sum_score"),
        ]:
            self.hybrid(
                key_type, "@title:alpha", tail=["APPLY", expr, "AS", alias]
            )

    def test_filter(self, key_type):
        self.setup_data(key_type)
        for expr in ["@price > 20", "@price <= 20", "@color == 'red'"]:
            self.hybrid(key_type, "@title:alpha", tail=["FILTER", expr])
