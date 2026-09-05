"""Capture FT.HYBRID reference answers from the Redis query engine.

FT.HYBRID does not exist in the `redis/redis-stack-server` image the other
generators use (its RediSearch is 2.x); the command was added in the Redis 8.4
query engine. This generator therefore runs against the `redis:8` image, which
carries a query engine new enough to answer FT.HYBRID. That override is
temporary -- see TODO(reference-image) on the class below.

The LOAD clause is swept separately and every one of those answers is recorded
`xfail`: Valkey does not implement LOAD for FT.HYBRID yet. See TODO(load) on
test_load_clause, and integration/compatibility/unsupported_tests.md.

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

import os
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

# The LOAD clause every other sweep pins. `LOAD *` is the only form Valkey
# currently honors; see test_load_clause and unsupported_tests.md.
LOAD_ALL = ("LOAD", "*")
NO_LOAD = ()

# (knn_count, knn_args) for the VSIM clause. The count is the number of
# arguments in the KNN block. An explicit KNN block is always emitted: the
# Valkey parser requires one, while Redis merely defaults it, so spelling it
# out keeps a single command string valid on both engines.
KNN_CLAUSES = [
    ("2", ["K", "5"]),
    ("2", ["K", "10"]),
    ("2", ["K", "24"]),
    ("4", ["K", "10", "EF_RUNTIME", "50"]),
]


@pytest.mark.parametrize("key_type", ["hash", "json"])
class TestHybridCompatibility(BaseCompatibilityTest):
    ANSWER_FILE_NAME = "hybrid-answers.pickle.gz"
    # TODO(reference-image): temporary. FT.HYBRID needs the Redis 8.4+ query
    # engine, which redis/redis-stack-server does not have. A separate PR moves
    # BaseCompatibilityTest.DOCKER_IMAGE to redis:latest for every generator;
    # once that lands this override and CONTAINER_NAME can both be dropped and
    # this class can inherit the shared image again.
    DOCKER_IMAGE = "redis:8"
    CONTAINER_NAME = "Generate-hybrid"

    DATA_SET = "hybrid text"

    def setup_data(self, key_type):
        super().setup_data(self.DATA_SET, key_type)

    def record_excluded(self, cmd):
        """Record a command that is run against Valkey for a no-crash check
        only, with no answer to compare against.

        Used where a known Valkey limitation outside FT.HYBRID would make the
        comparison a test of that limitation instead. Each caller says which.
        """
        self.answers.append({
            "cmd": cmd,
            "key_type": self.key_type,
            "data_set_name": self.data_set_name,
            "testname": os.environ.get("PYTEST_CURRENT_TEST")
            .split(":")[-1]
            .split(" ")[0],
            "excluded": True,
        })

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
        window=NON_BINDING_WINDOW,
        limit=PINNED_LIMIT,
        load=LOAD_ALL,
        tail=(),
        excluded=False,
        xfail=False,
    ):
        """Issue one FT.HYBRID command and record the reference answer.

        `LOAD *` is the default because it is the one LOAD form Valkey handles
        the same way Redis does; every other form is swept in test_load_clause
        below, marked xfail. Without any LOAD, Redis returns only the key and
        the score aliases, so `LOAD *` is what makes the two engines' record
        shapes comparable at all.

        Every score alias ends in `score`, which is what makes
        compatibility_test.compare_row() compare it as a float rather than
        byte-for-byte -- the two engines format the same score to different
        precision.
        """
        knn_count, knn_args = knn
        cmd = [
            "FT.HYBRID", f"{key_type}_idx1",
            "SEARCH", search_query, "SCORER", "BM25STD",
        ]
        if search_score_as:
            cmd += ["YIELD_SCORE_AS", search_score_as]
        cmd += ["VSIM", "@vec", "$q", "KNN", knn_count, *knn_args]
        if vector_score_as:
            cmd += ["YIELD_SCORE_AS", vector_score_as]
        method, options = combine
        if window is not None:
            options = [*options, "WINDOW", window]
        cmd += [
            "COMBINE", method, str(len(options) + 2),
            *options, "YIELD_SCORE_AS", "hybrid_score",
        ]
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
        if excluded:
            self.record_excluded(cmd)
            return
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

    # -----------------------------------------------------------------
    # The LOAD clause.
    #
    # TODO(load): every answer below is recorded `xfail`. Valkey does not yet
    # implement LOAD for FT.HYBRID -- it ignores the clause and returns every
    # schema field (for a JSON index, the whole document under `$`) whatever
    # the caller asked for. Redis honors it. A separate PR revises LOAD
    # handling across the aggregate pipeline; once that lands and FT.HYBRID
    # picks it up, these should start matching, the run will report XPASS, and
    # the `xfail=True` here plus the FT.HYBRID section of
    # unsupported_tests.md should both come off.
    #
    # The forms are swept now, rather than after the fix, so that the shape of
    # the gap is recorded against a real Redis answer and the fix has something
    # to be measured against.
    # -----------------------------------------------------------------

    def _load_cases(self, key_type):
        """(label, load-clause tokens, trailing stages) for the LOAD sweep."""
        cases = [
            # No LOAD at all: Redis replies with the key and the score
            # aliases only.
            ("none", NO_LOAD, []),
            # Single field, and a subset of fields.
            ("one-field", ["LOAD", "1", "@price"], []),
            ("two-fields", ["LOAD", "2", "@price", "@color"], []),
            # The document key is loadable by name.
            ("key", ["LOAD", "1", "@__key"], []),
            # AS renames. The count covers the AS and the alias too.
            ("rename", ["LOAD", "3", "@price", "AS", "cost"], []),
            ("rename-text", ["LOAD", "3", "@title", "AS", "heading"], []),
            ("rename-plus-field",
             ["LOAD", "4", "@price", "AS", "cost", "@color"], []),
            # A rename has to be visible to the stages that follow it.
            ("rename-then-sortby", ["LOAD", "3", "@price", "AS", "cost"],
             ["SORTBY", "2", "@cost", "ASC"]),
            ("rename-then-apply", ["LOAD", "3", "@price", "AS", "cost"],
             ["APPLY", "@cost * 2", "AS", "doubled"]),
            # A loaded field has to be visible to a FILTER. Valkey currently
            # returns an empty result here rather than an error, which is the
            # worst shape this gap takes.
            ("load-then-filter", ["LOAD", "1", "@price"],
             ["FILTER", "@price > 20"]),
        ]
        if key_type == "json":
            # JSON paths are a second spelling of the same clause; Redis names
            # the loaded column by the path unless AS renames it.
            cases += [
                ("json-path", ["LOAD", "1", "$.price"], []),
                ("json-path-rename",
                 ["LOAD", "3", "$.price", "AS", "cost"], []),
            ]
        return cases

    def test_load_clause(self, key_type):
        self.setup_data(key_type)
        for _label, load, tail in self._load_cases(key_type):
            self.hybrid(
                key_type, "@title:alpha", load=load, tail=tail, xfail=True,
            )

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

    # A pipeline stage that names an indexed field -- SORTBY @price, GROUPBY
    # @color -- cannot resolve it on a JSON index under `LOAD *`: Valkey
    # returns the document as a single `$` column and never materializes the
    # individual fields. That is an FT.AGGREGATE limitation (the same
    # FT.AGGREGATE query has the same problem), not an FT.HYBRID one, so the
    # JSON variants are recorded for a no-crash check only rather than turning
    # this suite into a test of that gap.
    _FIELD_REF_UNRESOLVED_ON_JSON = "json"

    def test_sortby(self, key_type):
        self.setup_data(key_type)
        excluded = key_type == self._FIELD_REF_UNRESOLVED_ON_JSON
        for sort in [
            ["SORTBY", "2", "@price", "ASC"],
            ["SORTBY", "2", "@price", "DESC"],
            ["SORTBY", "2", "@hybrid_score", "DESC"],
        ]:
            # Sorting on the fused score needs no field resolution, so it is
            # compared on both key types.
            is_score_sort = "@hybrid_score" in sort
            self.hybrid(key_type, "@title:alpha", tail=sort,
                        excluded=excluded and not is_score_sort)

    def test_groupby_reduce(self, key_type):
        self.setup_data(key_type)
        # GROUPBY names @color; see _FIELD_REF_UNRESOLVED_ON_JSON above.
        excluded = key_type == self._FIELD_REF_UNRESOLVED_ON_JSON
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
                excluded=excluded,
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
