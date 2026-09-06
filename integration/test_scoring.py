"""
End-to-end tests for BM25STD scoring through FT.SEARCH ... WITHSCORES.

Scores are verified against the 8.6 reference implementation. Most TEXT fields are
NOSTEM so query terms match raw tokens and the verified tables apply directly;
idxStem (Group 15) enables stemming to cover stem-family scoring.

WITHSCORES reply layout: [count, key, score_str, attrs, key, score_str, ...]
where score_str is formatted "%.12g".
"""

import struct

import pytest
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper
from valkeytestframework.util import waiters

SCORE_ABS_TOL = 1e-5


def _vec(*floats):
    """Packs floats into a FLOAT32 little-endian blob for a VECTOR field."""
    return struct.pack(f"{len(floats)}f", *floats)


# =====================================================================
# Indexes
# =====================================================================

# General-purpose index: two TEXT fields + NUMERIC + TAG + VECTOR.
IDX_MAIN = [
    "FT.CREATE", "idxMain", "ON", "HASH", "PREFIX", "1", "doc:",
    "SCHEMA", "body", "TEXT", "NOSTEM", "title", "TEXT", "NOSTEM",
    "rank", "NUMERIC", "cat", "TAG",
    "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
    "DISTANCE_METRIC", "L2",
]

# Same text schema as idxMain, so base scores match, plus the document_score
# multiplier: index-level SCORE is the fallback, per-doc `boost` the override.
IDX_DOC_SCORE = [
    "FT.CREATE", "idxDocScore", "ON", "HASH", "PREFIX", "1", "doc:",
    "SCORE", "0.5", "SCORE_FIELD", "boost",
    "SCHEMA", "body", "TEXT", "NOSTEM",
]

# No TEXT field, so avg_doc_len is 0 and neither numeric nor tag can rank.
IDX_NO_TEXT_FIELD = [
    "FT.CREATE", "idxNoTextField", "ON", "HASH", "PREFIX", "1", "doc:",
    "SCHEMA", "rank", "NUMERIC", "cat", "TAG",
]


# idxStem: one stemming index (no NOSTEM) shared by the stem-family tests. Query
# "running" (stem root "run") exercises all three BM25 leaves:
#   - exact word "running"    -> s:1, s:5, s:6
#   - stem root literal "run" -> s:4, s:5   (its own leaf, own dt)
#   - stem inflections        -> s:1/s:2/s:3/s:5/s:6 (distinct-doc dt)
# s:5 "run running" hits all three leaves; s:6 "running runs" holds two distinct
# inflections so the stem leaf's dt must count it once; s:7 ("swim") doesn't
# match. N=7, avg_doc_len=1.571. Leaf dts: exact 3, root 2, stem 5 (distinct).
_DOCS_STEM = {
    "s:1": "running",         # exact + stem
    "s:2": "runs",            # stem only
    "s:3": "runs runs runs",  # stem only, TF 3
    "s:4": "run",             # stem root literal -> its own leaf only
    "s:5": "run running",     # root + exact + stem -> all three leaves summed
    "s:6": "running runs",    # two inflections -> stem dt counts once
    "s:7": "swimming",        # filler, does not match
}
IDX_STEM = [
    "FT.CREATE", "idxStem", "ON", "HASH", "PREFIX", "1", "s:",
    "SCHEMA", "body", "TEXT",
]
STEM_DOCS = {key: {"body": body} for key, body in _DOCS_STEM.items()}
# Each matched doc scored on the leaves it hits, summed. Verified against the
# industry-standard reference.
STEM_RUNNING_SCORES = {
    "s:5": 2.127192, "s:1": 1.411321, "s:4": 1.366420,
    "s:6": 1.222204, "s:3": 0.492803, "s:2": 0.440174,
}

# idxStemMix: the SAME 7 stemming text docs as idxStem (same keys, so N=7,
# avg_doc_len, and every stem-leaf dt are identical, and the stem scores match
# STEM_RUNNING_SCORES) PLUS a numeric `rank` and tag `cat` field. A query that
# adds a numeric or tag clause has a non-text predicate, so its scoring takes the
# EXTRA-STEP path (ResolveLeaves/ScoreNode) rather than the pure-text in-iterator
# path -- Group 15 pins the stem split on that path against idxStem.
_STEMMIX_RANK = {"s:1": 1, "s:2": 2, "s:3": 3, "s:4": 4,
                 "s:5": 5, "s:6": 6, "s:7": 7}
_STEMMIX_CAT = {"s:1": "a", "s:2": "a", "s:3": "b", "s:4": "a",
                "s:5": "b", "s:6": "a", "s:7": "b"}
IDX_STEM_MIX = [
    "FT.CREATE", "idxStemMix", "ON", "HASH", "PREFIX", "1", "s:",
    "SCHEMA", "body", "TEXT", "rank", "NUMERIC", "cat", "TAG",
]
STEM_MIX_DOCS = {
    key: {"body": body, "rank": str(_STEMMIX_RANK[key]),
          "cat": _STEMMIX_CAT[key]}
    for key, body in _DOCS_STEM.items()
}

# =====================================================================
# Documents
# =====================================================================

# Text corpus: N=8, dt hello=6 world=6 rare=2 unique=1, avg_doc_len=5.25.
# `boost` is read only by idxDocScore (doc:2 omits it to exercise the SCORE
# fallback); `vec` only by the hybrid KNN query, which prefilters to doc:6/doc:8.
TEXT_DOCS = {
    "doc:1": {"body": "hello world one two three",
              "rank": "10", "boost": "2.0"},
    "doc:2": {"body": "hello world one two three hello",
              "rank": "20"},
    "doc:3": {"body": "hello world one two three hello hello",
              "rank": "30", "boost": "0.25"},
    "doc:4": {"body": "hello world one two three hello hello hello hello",
              "rank": "40", "boost": "-1.0"},
    "doc:5": {"body": "hello hello hello hello",
              "rank": "50", "boost": "inf"},
    "doc:6": {"body": "world rare unique document",
              "rank": "55", "vec": _vec(1.0, 1.0)},
    "doc:7": {"body": "hello world one two three hello",
              "rank": "60", "boost": "-inf"},
    "doc:8": {"body": "rare",
              "rank": "70", "vec": _vec(5.0, 5.0)},
}

# Equal doc_len, but per-field TF favors doc:2 (body TF 2 vs 1) while doc-wide TF
# favors doc:1 (3 vs 2), so only document-wide counting ranks doc:1 first.
MULTI_FIELD_DOCS = {
    "doc:1": {"body": "alpha", "title": "alpha alpha"},
    "doc:2": {"body": "alpha alpha", "title": "beta"},
}

# 4 text docs + 6 text-less ones, so N must be 10 rather than 4.
# avg_doc_len=0.70; cat dt: a=2, b=2, c=1, x=6.
PARTIAL_TEXT_DOCS = {
    "doc:1": {"body": "hello world", "cat": "a,b", "rank": "1"},
    "doc:2": {"body": "hello there friend", "cat": "b", "rank": "2"},
    "doc:3": {"body": "hello", "cat": "a", "rank": "3"},
    "doc:4": {"body": "world only", "cat": "c", "rank": "4"},
    **{f"doc:{i}": {"cat": "x", "rank": str(i)} for i in range(5, 11)},
}

# Ten non-stopword tokens: doc_len is 10 in an indexed TEXT field, 0 anywhere
# else, so the same value tells the two apart.
TEN_WORDS = "one two three four five six seven eight nine ten"


# =====================================================================
# Helpers
# =====================================================================

def load(client, index, docs):
    """Creates the index, writes the docs, waits for backfill to complete."""
    client.execute_command(*index)
    for key, mapping in docs.items():
        client.hset(key, mapping=mapping)
    IndexingTestHelper.wait_for_backfill_complete_on_node(client, index[1])


def wait_indexed(client, index, num_docs):
    """Waits for queued mutations to drain and the index to hold num_docs."""
    def ready():
        info = IndexingTestHelper.get_ft_info(client, index[1])
        return info.mutation_queue_size == 0 and info.num_docs == num_docs
    waiters.wait_for_true(ready)


def search(client, index, *query):
    """Runs FT.SEARCH <query> WITHSCORES; returns (ordered keys, {key: score})."""
    result = client.execute_command("FT.SEARCH", index[1], *query, "WITHSCORES")
    count, pairs = result[0], []
    # After the count, each result is a triple: key, score, attrs.
    for i in range(1, len(result), 3):
        key = result[i].decode() if isinstance(result[i], bytes) else result[i]
        pairs.append((key, float(result[i + 1])))
    assert len(pairs) == count
    return [k for k, _ in pairs], dict(pairs)


# =====================================================================
# Tests
# =====================================================================

class TestScoring(ValkeySearchTestCaseBase):

    # Group 1: single-term ranking.
    def test_single_term(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)

        # Exact scores, order, and the doc:2/doc:7 tie-break by key.
        keys, hello = search(client, IDX_MAIN, "hello")
        assert keys == ["doc:5", "doc:4", "doc:3", "doc:2", "doc:7", "doc:1"]
        assert hello == pytest.approx({
            "doc:5": 0.574385, "doc:4": 0.523122, "doc:3": 0.477286,
            "doc:2": 0.430172, "doc:7": 0.430172, "doc:1": 0.331888,
        }, abs=SCORE_ABS_TOL)
        # Doc-length normalization.
        assert hello["doc:5"] > hello["doc:4"]
        # Sub-linear TF saturation.
        assert hello["doc:4"] / hello["doc:1"] < 5.0

        # Rarer term (dt=2).
        keys, rare = search(client, IDX_MAIN, "rare")
        assert keys == ["doc:8", "doc:6"]
        assert rare == pytest.approx({"doc:8": 1.915183, "doc:6": 1.419164},
                                     abs=SCORE_ABS_TOL)
        # IDF outweighs term frequency at equal doc_len.
        assert rare["doc:6"] > hello["doc:5"]

        # No match.
        assert search(client, IDX_MAIN, "nonexistent") == ([], {})

    # Group 2: AND / OR admission and score accumulation.
    def test_and_or(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)
        # Single-term baseline the AND/OR totals build on.
        _, hello = search(client, IDX_MAIN, "hello")

        # AND admits only docs holding both terms, scored on both leaves.
        keys, hello_world = search(client, IDX_MAIN, "hello world")
        assert keys == ["doc:4", "doc:3", "doc:2", "doc:7", "doc:1"]
        assert hello_world == pytest.approx({
            "doc:4": 0.774956, "doc:3": 0.763658, "doc:2": 0.737626,
            "doc:7": 0.737626, "doc:1": 0.663776,
        }, abs=SCORE_ABS_TOL)

        # OR admits either term, scoring each doc only on the terms it has.
        keys, either = search(client, IDX_MAIN, "hello | world")
        assert keys == ["doc:4", "doc:3", "doc:2", "doc:7", "doc:1",
                        "doc:5", "doc:6"]
        assert either == pytest.approx(
            {**hello_world, "doc:5": hello["doc:5"], "doc:6": 0.360540},
            abs=SCORE_ABS_TOL)

        # An OR branch must not leak its AND sibling's leaf: doc:6 is admitted by
        # rare, so its "world" token stays unscored.
        keys, or_leaf = search(client, IDX_MAIN, "(hello world) | rare")
        assert keys == ["doc:8", "doc:6", "doc:4", "doc:3", "doc:2",
                        "doc:7", "doc:1"]
        assert or_leaf == pytest.approx(
            {**hello_world, "doc:8": 1.915183, "doc:6": 1.419164},
            abs=SCORE_ABS_TOL)

        # OR of two AND groups: doc:6 satisfies only (rare unique) and is scored
        # on those two leaves alone.
        keys, or_groups = search(client, IDX_MAIN, "(hello world) | (rare unique)")
        assert keys == ["doc:6", "doc:4", "doc:3", "doc:2", "doc:7", "doc:1"]
        assert or_groups == pytest.approx({**hello_world, "doc:6": 3.404279},
                                          abs=SCORE_ABS_TOL)

        # Three-leaf AND accumulates every leaf.
        keys, three_leaf = search(client, IDX_MAIN, "hello world one")
        assert keys == ["doc:2", "doc:7", "doc:3", "doc:1", "doc:4"]
        assert three_leaf == pytest.approx({
            "doc:2": 1.202911, "doc:7": 1.202911, "doc:3": 1.197037,
            "doc:1": 1.166036, "doc:4": 1.156069,
        }, abs=SCORE_ABS_TOL)

        # A repeated term is scored once per predicate position.
        _, repeated = search(client, IDX_MAIN, "hello hello")
        assert repeated == pytest.approx({k: 2 * v for k, v in hello.items()},
                                        abs=SCORE_ABS_TOL)

    # Group 3: query weights scale leaves and compound through nesting.
    def test_query_weights(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)
        _, hello = search(client, IDX_MAIN, "hello")
        _, rare = search(client, IDX_MAIN, "rare")

        # A leaf weight scales linearly and leaves the order untouched.
        keys, weighted = search(client, IDX_MAIN, "(hello)=>{$weight:5}")
        assert keys == ["doc:5", "doc:4", "doc:3", "doc:2", "doc:7", "doc:1"]
        assert weighted == pytest.approx({k: 5 * v for k, v in hello.items()},
                                         abs=SCORE_ABS_TOL)

        # Inner leaf weights (4 on hello, 3 on world) and an outer group weight
        # of 2 compound: each doc scores 2 * (4 * hello_leaf + 3 * world_leaf).
        keys, nested = search(
            client, IDX_MAIN,
            "((hello)=>{$weight:4} (world)=>{$weight:3})=>{$weight:2}")
        assert keys == ["doc:4", "doc:3", "doc:2", "doc:7", "doc:1"]
        assert nested == pytest.approx({
            "doc:4": 5.695980, "doc:3": 5.536520, "doc:2": 5.286103,
            "doc:7": 5.286103, "doc:1": 4.646429,
        }, abs=SCORE_ABS_TOL)

        # Inside an OR each leaf keeps its own weight times the group weight, so
        # the rare-only docs (2 * 3) overtake the hello docs (4 * 3).
        keys, or_weighted = search(
            client, IDX_MAIN,
            "((hello)=>{$weight:4} | (rare)=>{$weight:2})=>{$weight:3}")
        assert keys == ["doc:8", "doc:6", "doc:5", "doc:4", "doc:3",
                        "doc:2", "doc:7", "doc:1"]
        assert or_weighted == pytest.approx(
            {**{k: 12 * v for k, v in hello.items()},
             **{k: 6 * v for k, v in rare.items()}},
            abs=SCORE_ABS_TOL)

    # Group 4: SCORE / SCORE_FIELD document_score multiplier.
    def test_document_score(self):
        client = self.server.get_new_client()

        # SCORE is rejected at create time unless it is within [0.0, 1.0].
        for bad in ("2.0", "-0.5"):
            with pytest.raises(ResponseError,
                               match=r"must be between 0\.0 and 1\.0"):
                client.execute_command(
                    "FT.CREATE", f"idxBad{bad}", "ON", "HASH",
                    "SCORE", bad, "SCHEMA", "body", "TEXT", "NOSTEM")

        load(client, IDX_DOC_SCORE, TEXT_DOCS)

        # Score is the BM25 total times the doc's boost, and doc:2 has no boost
        # field so it falls back to the index-level SCORE of 0.5. A negative boost
        # is not floored; -inf is kept and ranked last, where the reference instead
        # drops the doc from the results.
        keys, scores = search(client, IDX_DOC_SCORE, "hello")
        assert keys == ["doc:5", "doc:1", "doc:2", "doc:3", "doc:4", "doc:7"]
        assert scores == pytest.approx({
            "doc:5": float("inf"), "doc:1": 0.663776, "doc:2": 0.215086,
            "doc:3": 0.119322, "doc:4": -0.523122, "doc:7": float("-inf"),
        }, abs=SCORE_ABS_TOL)

    # Group 5: term frequency is counted document-wide, not per field.
    def test_doc_wide_term_frequency(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, MULTI_FIELD_DOCS)

        keys, scoped = search(client, IDX_MAIN, "@body:alpha")
        assert keys == ["doc:1", "doc:2"]
        assert scoped == pytest.approx({"doc:1": 0.286505, "doc:2": 0.250692},
                                       abs=SCORE_ABS_TOL)

        # Field scoping narrows admission but leaves the scores untouched.
        _, unscoped = search(client, IDX_MAIN, "alpha")
        assert unscoped == pytest.approx(scoped, abs=SCORE_ABS_TOL)
        _, title = search(client, IDX_MAIN, "@title:alpha")
        assert title == pytest.approx({"doc:1": scoped["doc:1"]},
                                      abs=SCORE_ABS_TOL)

    # Group 6: an exact phrase narrows admission by adjacency without changing scores.
    def test_exact_phrase(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)

        # Adjacent terms: the exact phrase admits and scores exactly like the AND.
        and_keys, and_scores = search(client, IDX_MAIN, "hello world")
        keys, scores = search(client, IDX_MAIN, '@body:"hello world"')
        assert keys == and_keys
        assert scores == pytest.approx(and_scores, abs=SCORE_ABS_TOL)

        # Reversing the exact phrase admits nothing, while the AND still matches.
        assert search(client, IDX_MAIN, '@body:"one world"') == ([], {})
        keys, _ = search(client, IDX_MAIN, "one world")
        assert set(keys) == {"doc:1", "doc:2", "doc:3", "doc:4", "doc:7"}

    # Group 7: SORTBY replaces the result order, leaving the scores untouched.
    def test_sortby(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)
        _, hello = search(client, IDX_MAIN, "hello")

        # Rank ascending, not the score order (doc:5, 4, 3, 2, 7, 1).
        keys, scores = search(client, IDX_MAIN, "hello", "SORTBY", "rank", "ASC")
        assert keys == ["doc:1", "doc:2", "doc:3", "doc:4", "doc:5", "doc:7"]
        assert scores == pytest.approx(hello, abs=SCORE_ABS_TOL)

        keys, scores = search(client, IDX_MAIN, "hello", "SORTBY", "rank", "DESC")
        assert keys == ["doc:7", "doc:5", "doc:4", "doc:3", "doc:2", "doc:1"]
        assert scores == pytest.approx(hello, abs=SCORE_ABS_TOL)

    # Group 8: BM25STD is the default scorer; unknown scorers are rejected.
    def test_scorer_selection(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)

        # An explicit SCORER BM25STD matches the default exactly.
        default_keys, default_scores = search(client, IDX_MAIN, "hello")
        keys, scores = search(client, IDX_MAIN, "hello", "SCORER", "BM25STD")
        assert keys == default_keys
        assert scores == pytest.approx(default_scores, abs=SCORE_ABS_TOL)

        # BM25STD is the only registered scorer, so anything else fails to parse.
        with pytest.raises(ResponseError, match=r"Unknown argument `TFIDF`"):
            client.execute_command("FT.SEARCH", IDX_MAIN[1], "hello",
                                   "SCORER", "TFIDF")

    # Group 9: N counts every keyspace match, but only indexed TEXT adds doc_len.
    def test_index_size_counts_all_docs(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, PARTIAL_TEXT_DOCS)
        assert IndexingTestHelper.get_ft_info(client, IDX_MAIN[1]).num_docs == 10

        # Three docs match, but IDF and avg_doc_len are computed over all ten.
        keys, scores = search(client, IDX_MAIN, "hello")
        assert keys == ["doc:3", "doc:1", "doc:2"]
        assert scores == pytest.approx({
            "doc:3": 0.974311, "doc:1": 0.650739, "doc:2": 0.650739,
        }, abs=SCORE_ABS_TOL)

        # A doc matching only the keyspace still joins the corpus: N goes 10 -> 11
        # so IDF rises, while its ten words sit outside the schema and leave
        # total_doc_len at 7, dropping avg_doc_len from 0.70 to 7/11.
        client.hset("doc:11", mapping={"filler": TEN_WORDS})
        wait_indexed(client, IDX_MAIN, 11)
        keys, unindexed_words = search(client, IDX_MAIN, "hello")
        assert keys == ["doc:3", "doc:1", "doc:2"]
        assert unindexed_words == pytest.approx({
            "doc:3": 0.998685, "doc:1": 0.656575, "doc:2": 0.656575,
        }, abs=SCORE_ABS_TOL)

        # The same ten words in the indexed TEXT field: N goes to 12 and
        # total_doc_len to 17, so avg_doc_len jumps to 17/12 and lifts every
        # score, even though the new doc carries no query term.
        client.hset("doc:12", mapping={"body": TEN_WORDS})
        wait_indexed(client, IDX_MAIN, 12)
        keys, indexed_words = search(client, IDX_MAIN, "hello")
        assert keys == ["doc:3", "doc:1", "doc:2"]
        assert indexed_words == pytest.approx({
            "doc:3": 1.491665, "doc:1": 1.123015, "doc:2": 1.123015,
        }, abs=SCORE_ABS_TOL)

    # Group 10: a numeric predicate filters but never contributes to the score.
    def test_numeric_never_ranks(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, PARTIAL_TEXT_DOCS)
        _, hello = search(client, IDX_MAIN, "hello")

        # ORing a numeric branch admits a text-less doc and changes no score.
        keys, mixed = search(client, IDX_MAIN, "hello | @rank:[5 5]")
        assert keys == ["doc:3", "doc:1", "doc:2", "doc:5"]
        assert mixed == pytest.approx({**hello, "doc:5": 0.0},
                                      abs=SCORE_ABS_TOL)

        # A pure numeric range admits everything and scores it all 0.
        _, scores = search(client, IDX_MAIN, "@rank:[0 100]")
        assert scores == pytest.approx({f"doc:{i}": 0.0 for i in range(1, 11)},
                                       abs=SCORE_ABS_TOL)

    # Group 11: tag values score as BM25 terms with per-value IDF.
    # With a TEXT field in the schema: tags score exactly like text, except TF=1.
    # Without one: avg_doc_len is 0, so every leaf degenerates to a score of 0.
    def test_tag_scoring(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, PARTIAL_TEXT_DOCS)
        _, hello = search(client, IDX_MAIN, "hello")

        # IDF over the per-value doc count, normalized by the doc's text length.
        keys, cat_a = search(client, IDX_MAIN, "@cat:{a}")
        assert keys == ["doc:3", "doc:1"]
        assert cat_a == pytest.approx({"doc:3": 1.260592, "doc:1": 0.841945},
                                      abs=SCORE_ABS_TOL)

        # A rarer value scores higher: dt=1 for c against dt=6 for x.
        _, cat_c = search(client, IDX_MAIN, "@cat:{c}")
        _, cat_x = search(client, IDX_MAIN, "@cat:{x}")
        assert cat_c == pytest.approx({"doc:4": 1.132230}, abs=SCORE_ABS_TOL)
        assert cat_x == pytest.approx(
            {f"doc:{i}": 0.890311 for i in range(5, 11)}, abs=SCORE_ABS_TOL)
        assert cat_c["doc:4"] > cat_x["doc:5"]

        # A union sums every matched value a doc carries.
        keys, cat_a_or_b = search(client, IDX_MAIN, "@cat:{a|b}")
        assert keys == ["doc:1", "doc:3", "doc:2"]
        assert cat_a_or_b["doc:1"] == pytest.approx(
            cat_a["doc:1"] + cat_a_or_b["doc:2"], abs=SCORE_ABS_TOL)

        # A weight scales a tag term like a text leaf.
        _, weighted = search(client, IDX_MAIN, "(@cat:{a})=>{$weight:3}")
        assert weighted == pytest.approx({k: 3 * v for k, v in cat_a.items()},
                                         abs=SCORE_ABS_TOL)

        # Text and tag leaves sum, and a numeric clause adds nothing.
        keys, text_and_tag = search(client, IDX_MAIN, "hello @cat:{a}")
        assert keys == ["doc:3", "doc:1"]
        assert text_and_tag == pytest.approx(
            {k: hello[k] + cat_a[k] for k in cat_a}, abs=SCORE_ABS_TOL)
        _, with_numeric = search(client, IDX_MAIN, "hello @cat:{a} @rank:[0 100]")
        assert with_numeric == pytest.approx(text_and_tag, abs=SCORE_ABS_TOL)

        # ORing them admits either branch and sums only the branches a doc
        # matches, so doc:2 (no `a` tag) keeps its text score alone.
        keys, text_or_tag = search(client, IDX_MAIN, "hello | @cat:{a}")
        assert keys == ["doc:3", "doc:1", "doc:2"]
        assert text_or_tag == pytest.approx({
            "doc:3": 2.234903, "doc:1": 1.492684, "doc:2": 0.650739,
        }, abs=SCORE_ABS_TOL)
        assert text_or_tag == pytest.approx(
            {k: hello[k] + cat_a.get(k, 0.0) for k in hello},
            abs=SCORE_ABS_TOL)

        # Weights compound across a mixed OR just as they do for text-only
        # branches: each doc scores 3 * (4 * text_leaf + 2 * tag_leaf).
        keys, weighted_or = search(
            client, IDX_MAIN,
            "((hello)=>{$weight:4} | (@cat:{a})=>{$weight:2})=>{$weight:3}")
        assert keys == ["doc:3", "doc:1", "doc:2"]
        assert weighted_or == pytest.approx({
            "doc:3": 19.255288, "doc:1": 12.860543, "doc:2": 7.808872,
        }, abs=SCORE_ABS_TOL)
        assert weighted_or == pytest.approx(
            {k: 3 * (4 * hello[k] + 2 * cat_a.get(k, 0.0)) for k in hello},
            abs=SCORE_ABS_TOL)

        # Same docs, same tag query, but a schema without a TEXT field: the leaf
        # that scored 1.260592 / 0.841945 above degenerates to a well-defined 0,
        # where the reference returns nan instead. Numerics stay 0 either way.
        load(client, IDX_NO_TEXT_FIELD, PARTIAL_TEXT_DOCS)
        _, no_text_cat_a = search(client, IDX_NO_TEXT_FIELD, "@cat:{a}")
        assert no_text_cat_a == pytest.approx({"doc:1": 0.0, "doc:3": 0.0},
                                              abs=SCORE_ABS_TOL)
        _, no_text_ranks = search(client, IDX_NO_TEXT_FIELD, "@rank:[1 10]")
        assert no_text_ranks == pytest.approx(
            {f"doc:{i}": 0.0 for i in range(1, 11)}, abs=SCORE_ABS_TOL)

    # Group 12: a wildcard scores every doc on doc_len alone.
    def test_match_all(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)

        # IDF and TF are constant here, so the order is shortest-first and the
        # equal-length doc:5 and doc:6 tie despite holding different terms.
        keys, scores = search(client, IDX_MAIN, "*")
        assert keys == ["doc:8", "doc:5", "doc:6", "doc:1", "doc:2",
                        "doc:7", "doc:3", "doc:4"]
        assert scores == pytest.approx({
            "doc:8": 1.495146, "doc:5": 1.107914, "doc:6": 1.107914,
            "doc:1": 1.019868, "doc:2": 0.944785, "doc:7": 0.944785,
            "doc:3": 0.880000, "doc:4": 0.773869,
        }, abs=SCORE_ABS_TOL)

    # Group 13: a hybrid text=>[KNN] query ranks by text score, not by distance.
    def test_hybrid_text_vector(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)
        _, rare = search(client, IDX_MAIN, "rare")
        params = ("PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")

        # doc:8 leads on text score while sitting furthest away (distance 32 vs 0),
        # and the hybrid scores are exactly the text-only scores.
        keys, scores = search(client, IDX_MAIN, "rare=>[KNN 2 @vec $q]", *params)
        assert keys == ["doc:8", "doc:6"]
        assert scores == pytest.approx(rare, abs=SCORE_ABS_TOL)

        # SORTBY __vec_score flips the order to nearest-first, scores unchanged.
        keys, scores = search(client, IDX_MAIN, "rare=>[KNN 2 @vec $q]",
                              "SORTBY", "__vec_score", *params)
        assert keys == ["doc:6", "doc:8"]
        assert scores == pytest.approx(rare, abs=SCORE_ABS_TOL)

        # __vec_score is synthesized rather than stored, so WITHSORTKEYS emits the
        # distance with a '#' prefix: [count, key, sortkey, attrs, ...].
        res = client.execute_command(
            "FT.SEARCH", IDX_MAIN[1], "rare=>[KNN 2 @vec $q]",
            "SORTBY", "__vec_score", "WITHSORTKEYS", *params)
        assert res[0] == 2
        assert res[1] == b"doc:6" and res[2] == b"#0"
        assert res[4] == b"doc:8" and res[5] == b"#32"

    # Group 14: corpus statistics track mutations immediately, so scores change as
    # soon as a doc is added or removed. The reference defers this until GC runs.
    def test_scores_follow_mutations(self):
        client = self.server.get_new_client()
        load(client, IDX_MAIN, TEXT_DOCS)
        _, before = search(client, IDX_MAIN, "hello")

        # Deleting a non-matching doc changes N and avg_doc_len only.
        client.delete("doc:6")
        wait_indexed(client, IDX_MAIN, 7)
        keys, smaller_n = search(client, IDX_MAIN, "hello")
        assert keys == ["doc:5", "doc:4", "doc:3", "doc:2", "doc:7", "doc:1"]
        assert smaller_n == pytest.approx({
            "doc:5": 0.368158, "doc:4": 0.336278, "doc:3": 0.307233,
            "doc:2": 0.277295, "doc:7": 0.277295, "doc:1": 0.214569,
        }, abs=SCORE_ABS_TOL)

        # Deleting a matching doc also drops dt, raising IDF for the rest.
        client.delete("doc:5")
        wait_indexed(client, IDX_MAIN, 6)
        _, smaller_dt = search(client, IDX_MAIN, "hello")
        assert set(smaller_dt) == {"doc:4", "doc:3", "doc:2", "doc:7", "doc:1"}
        assert all(smaller_dt[k] > smaller_n[k] for k in smaller_dt)

        # Re-adding both docs restores the original statistics exactly.
        for key in ("doc:5", "doc:6"):
            client.hset(key, mapping=TEXT_DOCS[key])
        wait_indexed(client, IDX_MAIN, 8)
        _, restored = search(client, IDX_MAIN, "hello")
        assert restored == pytest.approx(before, abs=SCORE_ABS_TOL)

    # Group 15: a stemmed term is a UNION of independent BM25 leaves whose scores
    # are SUMMED -- the exact word, the stem root literal, and the stem
    # inflections -- each with its own dt, unlike prefix/fuzzy which pick one.
    # A pure-text query scores in the term iterator; adding a numeric or tag
    # clause routes the same split through the extra-step path, yield same result.
    def test_stemming(self):
        client = self.server.get_new_client()
        load(client, IDX_STEM, STEM_DOCS)

        # Each doc scores on the leaves it hits: s:5 "run running" all three,
        # s:4 "run" the root leaf alone, s:6 "running runs" counted once in the
        # stem leaf's distinct dt. s:7 ("swimming") does not match at all.
        keys, stem = search(client, IDX_STEM, "running")
        assert keys == ["s:5", "s:1", "s:4", "s:6", "s:3", "s:2"]
        assert stem == pytest.approx(STEM_RUNNING_SCORES, abs=SCORE_ABS_TOL)

        # The exact query form earns both the exact and the stem leaf, so s:1
        # ("running", TF 1) outranks s:3 ("runs runs runs", TF 3) which carries
        # the stem leaf alone: the exact-match boost beats the higher inflection TF.
        assert stem["s:1"] > stem["s:3"]

        # Same docs and same query, but a schema with a numeric and a tag field:
        # any non-text clause makes scoring take the extra-step path instead of
        # the term iterator. idxStemMix indexes the same 7 bodies, so every corpus
        # statistic matches and the scores above are the oracle for that path.
        load(client, IDX_STEM_MIX, STEM_MIX_DOCS)

        # A numeric clause admits every doc and contributes 0, so the run-family
        # scores must come out identical to the in-iterator run above.
        keys, with_numeric = search(client, IDX_STEM_MIX, "running @rank:[1 100]")
        assert keys == ["s:5", "s:1", "s:4", "s:6", "s:3", "s:2"]
        assert with_numeric == pytest.approx(stem, abs=SCORE_ABS_TOL)

        # ORing the numeric branch admits s:7 too, which holds no "run" term and
        # so scores 0 while the rest keep their stem scores.
        _, or_numeric = search(client, IDX_STEM_MIX, "running | @rank:[7 7]")
        assert or_numeric == pytest.approx({**stem, "s:7": 0.0},
                                           abs=SCORE_ABS_TOL)

        # The stem expansion and a tag leaf sum, so each admitted doc scores its
        # stem total plus its tag-only score.
        keys, with_tag = search(client, IDX_STEM_MIX, "running @cat:{a}")
        _, tag_only = search(client, IDX_STEM_MIX, "@cat:{a}")
        assert set(keys) == {"s:1", "s:2", "s:4", "s:6"}
        assert with_tag == pytest.approx(
            {k: stem[k] + tag_only[k] for k in keys}, abs=SCORE_ABS_TOL)
