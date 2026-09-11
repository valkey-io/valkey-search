"""Capture FT.HYBRID reference answers from the Redis query engine.

FT.HYBRID does not exist in the `redis/redis-stack-server` image the other
generators use (its RediSearch is 2.x); the command was added in the Redis 8.4
query engine. This generator therefore runs against the `redis:8` image, which
carries a query engine new enough to answer FT.HYBRID. That override is
temporary -- see TODO(reference-image) on the class below.

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

LINEAR_COMBINES = [
    ("LINEAR", ["ALPHA", "0.5", "BETA", "0.5"]),
    ("LINEAR", ["ALPHA", "1", "BETA", "0"]),
    ("LINEAR", ["ALPHA", "0", "BETA", "1"]),
    ("LINEAR", ["ALPHA", "0.2", "BETA", "0.8"]),
    ("LINEAR", ["ALPHA", "0.5", "BETA", "0.5"]),
]

# Every sweep that is about *scoring* pins WINDOW and LIMIT rather than taking
# the defaults, because the two engines' defaults do not agree: Redis defaults
# to returning 10 rows and applies WINDOW only as a per-arm cap, while Valkey
# returns everything the window allows and truncates the fused list to WINDOW
# as well. Left implicit, those two disagreements would change the *size* of
# every result and bury the score comparison the sweeps exist to make. The
# defaults get their own dedicated tests below instead.
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
    # TODO(reference-image): temporary. FT.HYBRID needs the Redis 8.4+ query
    # engine, which redis/redis-stack-server does not have. A separate PR moves
    # BaseCompatibilityTest.DOCKER_IMAGE to redis:latest for every generator;
    # once that lands this override can be dropped and this class can inherit
    # the shared image again.
    DOCKER_IMAGE = "redis:8"

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
        cmd += ["VSIM", "@vec", "$q"]
        if knn is not None:
            knn_count, knn_args = knn
            cmd += ["KNN", knn_count, *knn_args]
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

    # -----------------------------------------------------------------
    # The VSIM arm.
    # -----------------------------------------------------------------

    def test_knn_variations(self, key_type):
        self.setup_data(key_type)
        for knn in KNN_CLAUSES:
            for vector in QUERY_VECTORS:
                self.hybrid(key_type, "@title:alpha", knn=knn, vector=vector)

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
            # The document key is loadable by name.
            (["LOAD", "1", "@__key"], []),
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
        self.setup_data(key_type)
        for sort in [
            ["SORTBY", "2", "@price", "ASC"],
            ["SORTBY", "2", "@price", "DESC"],
            ["SORTBY", "2", "@hybrid_score", "DESC"],
        ]:
            self.hybrid(key_type, "@title:alpha", tail=sort)

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
            self.hybrid(key_type, "@title:alpha", tail=sort,
                        vector_score_as="vector_score")

    # TODO(stage-refs-per-arm-score): Redis resolves a per-arm YIELD_SCORE_AS
    # alias in a SORTBY whenever the LOAD clause is anything other than
    # `LOAD *`, and sorts by it. valkey-search rejects the reference outright,
    # at parse time, under every LOAD clause: "Index field `vector_score` does
    # not exist". The fused COMBINE alias is reachable in stages on both
    # engines; only the per-arm ones are not, here.
    #
    # When the per-arm aliases become reachable these will start matching, the
    # run will print XPASS, and both the `xfail=True` and the entry in
    # unsupported_tests.md should be removed.
    def test_sortby_per_arm_score_is_reachable(self, key_type):
        self.setup_data(key_type)
        for load in [NO_LOAD, ["LOAD", "1", "@price"],
                     ["LOAD", "2", "@price", "@color"]]:
            for sort in [
                ["SORTBY", "2", "@vector_score", "DESC"],
                ["SORTBY", "2", "@text_score", "DESC"],
                ["SORTBY", "4", "@text_score", "DESC", "@vector_score", "ASC"],
            ]:
                self.hybrid(key_type, "@title:alpha", load=load, tail=sort,
                            vector_score_as="vector_score", xfail=True)

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

    def test_unaliased_fused_score(self, key_type):
        """COMBINE without YIELD_SCORE_AS. The fused score still reaches the
        reply, under whatever name the engine falls back to, and the sweep
        records what that name is."""
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
