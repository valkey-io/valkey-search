import struct

import pytest
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper


def _vec(*floats):
    """Packs floats into a FLOAT32 little-endian blob for a VECTOR field."""
    return struct.pack(f"{len(floats)}f", *floats)

"""
End-to-end tests for BM25STD scoring through FT.SEARCH ... WITHSCORES.

Scores are verified against the Redis 8.6 docker baseline. All indexes use NOSTEM
so query terms match raw tokens and the verified score table applies directly.

WITHSCORES reply layout: [count, key, score_str, attrs, key, score_str, ...]
where score_str is formatted "%.12g".
"""

SCORE_ABS_TOL = 1e-5


def parse_withscores(result):
    """Parses a WITHSCORES reply into an ordered list of (key, score) tuples,
    preserving result order (score desc, key asc)."""
    count = result[0]
    pairs = []
    # After the count, each result is a triple: key, score, attrs.
    for i in range(1, len(result), 3):
        key = result[i].decode() if isinstance(result[i], bytes) else result[i]
        pairs.append((key, float(result[i + 1])))
    assert len(pairs) == count
    return pairs


class ScoringIndex:
    """An FT.CREATE command plus its documents ({key: {field: value}}), with
    helpers to load it and run scored searches against it."""

    def __init__(self, index, create_cmd, docs):
        self.index = index
        self.create_cmd = create_cmd
        self.docs = docs

    def load(self, client: Valkey):
        client.execute_command(*self.create_cmd)
        for key, mapping in self.docs.items():
            client.hset(key, mapping=mapping)
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, self.index)

    def search(self, client: Valkey, *query):
        """Runs FT.SEARCH <query> WITHSCORES; returns (ordered keys, {key: score})."""
        pairs = parse_withscores(
            client.execute_command("FT.SEARCH", self.index, *query, "WITHSCORES")
        )
        return [k for k, _ in pairs], dict(pairs)


# =====================================================================
# Test indexes
# =====================================================================

# idxA: single text field, NOSTEM, all document_score = 1.0. Doc lengths and
# term frequencies feed the verified expected-score tables below.
#   N = 8, dt(hello) = 6, dt(world) = 6, dt(rare) = 2, dt(unique) = 1,
#   total_doc_len = 42, avg_doc_len = 5.25.
_DOCS_A = {
    "docA:1": "hello world one two three",
    "docA:2": "hello world one two three hello",
    "docA:3": "hello world one two three hello hello",
    "docA:4": "hello world one two three hello hello hello hello",
    "docA:5": "hello hello hello hello",
    "docA:6": "world rare unique document",
    "docA:7": "hello world one two three hello",
    "docA:8": "rare",
}
INDEX_A = ScoringIndex(
    "idxA",
    ["FT.CREATE", "idxA", "ON", "HASH", "PREFIX", "1", "docA:",
     "SCHEMA", "body", "TEXT", "NOSTEM"],
    {key: {"body": body} for key, body in _DOCS_A.items()},
)

# idxA7: the same docs as idxA (so N / avg_doc_len / dt are identical) plus a
# numeric `rank` field assigned in key order, so SORTBY rank yields an order
# clearly distinct from score order. The numeric field is not a text term, so
# per-doc scores match idxA. docA:6 and docA:8 have no "hello" and stay out of
# hello results but still count toward the index stats.
_RANKS_A7 = {"docA:1": 10, "docA:2": 20, "docA:3": 30, "docA:4": 40,
             "docA:5": 50, "docA:6": 55, "docA:7": 60, "docA:8": 70}
INDEX_A7 = ScoringIndex(
    "idxA7",
    ["FT.CREATE", "idxA7", "ON", "HASH", "PREFIX", "1", "docA:",
     "SCHEMA", "body", "TEXT", "NOSTEM", "rank", "NUMERIC"],
    {key: {"body": _DOCS_A[key], "rank": str(rank)}
     for key, rank in _RANKS_A7.items()},
)

# idxB: a numeric `boost` field declared as SCORE_FIELD. Every doc has the
# identical body "hello world", so the unweighted BM25 total (the "base") is the
# same for all; only document_score (from boost) differs => final = base * boost.
_BOOSTS_B = {
    "docB:1": "2.0", "docB:2": "1.0", "docB:3": "0.5",
    "docB:4": "-1.0", "docB:5": "inf", "docB:6": "-inf",
}
INDEX_B = ScoringIndex(
    "idxB",
    ["FT.CREATE", "idxB", "ON", "HASH", "PREFIX", "1", "docB:", "SCORE_FIELD",
     "boost", "SCHEMA", "body", "TEXT", "NOSTEM", "boost", "NUMERIC"],
    {key: {"body": "hello world", "boost": boost}
     for key, boost in _BOOSTS_B.items()},
)

# idxBdef: SCORE default multiplier, no SCORE_FIELD. Six identical docs so its
# index stats (N, avg_doc_len, dt) match idxB exactly; each doc's unweighted
# base equals BASE_SCORE and the final score is 0.5 * base.
INDEX_BDEF = ScoringIndex(
    "idxBdef",
    ["FT.CREATE", "idxBdef", "ON", "HASH", "PREFIX", "1", "docBd:",
     "SCORE", "0.5", "SCHEMA", "body", "TEXT", "NOSTEM"],
    {f"docBd:{i}": {"body": "hello world"} for i in range(1, 7)},
)

# idxC: three text fields. TF is counted document-wide, not per-field, so
# docC:1 ("redis" in all three fields, TF=3) must outscore docC:2 (TF=1) on the
# term "redis" -- whether the query is field-scoped (@f1:redis) or unscoped.
INDEX_C = ScoringIndex(
    "idxC",
    ["FT.CREATE", "idxC", "ON", "HASH", "PREFIX", "1", "docC:",
     "SCHEMA", "f1", "TEXT", "NOSTEM", "f2", "TEXT", "NOSTEM",
     "f3", "TEXT", "NOSTEM"],
    {
        "docC:1": {"f1": "redis", "f2": "redis stack", "f3": "redis valkey"},
        "docC:2": {"f1": "redis", "f2": "other", "f3": "words"},
    },
)

# idxMix: text `body` + numeric `rank` + tag `cat`, a general-purpose combined
# index for text + numeric/tag query tests. 4 docs have a text body; 6 have only
# rank+cat (no text), so text-doc count (4) != total-doc count (10).
#
# BM25's index size N must be ALL indexed docs, not just text-bearing ones. N
# feeds both IDF and avg_doc_len, so scores computed with the wrong N=4 diverge
# sharply from the correct N=10. Verified against Redis 8.6: FT.INFO
# num_docs=10 and EXPLAINSCORE uses N=10.
_DMIX_TEXT = {
    "d:1": {"body": "hello world", "cat": "a,b"},
    "d:2": {"body": "hello there friend", "cat": "b"},
    "d:3": {"body": "hello", "cat": "a"},
    "d:4": {"body": "world only", "cat": "c"},
}
INDEX_MIX = ScoringIndex(
    "idxMix",
    # WITHSUFFIXTRIE enables suffix queries; it is a lookup structure only and
    # does not change tokenization or scores, so the verified constants below
    # (and the existing idxMix tests) are unaffected.
    ["FT.CREATE", "idxMix", "ON", "HASH", "PREFIX", "1", "d:",
     "SCHEMA", "body", "TEXT", "NOSTEM", "WITHSUFFIXTRIE", "rank", "NUMERIC",
     "cat", "TAG"],
    {
        **{k: {**v, "rank": str(i + 1)}
           for i, (k, v) in enumerate(_DMIX_TEXT.items())},
        **{f"d:{i}": {"rank": str(i), "cat": "x"} for i in range(5, 11)},
    },
)
# Expected "hello" scores with the correct N=10 (all docs). With the buggy
# N=4 (text docs only) these would be ~0.303 / ~0.203 instead.
MIX_HELLO_SCORES = {"d:3": 0.974311, "d:1": 0.650739, "d:2": 0.650739}

# --- idxMix tag / numeric scores (verified against Redis 8.6) ---------------
# A tag value is a BM25 term with F == 1, IDF over the per-tag-value document
# count (dt), normalized by the document's TEXT length. N=10, avg_doc_len=0.70.
#   cat dt: a=2 (d:1,d:3), b=2 (d:1,d:2), c=1 (d:4), x=6 (d:5..10).
# @cat:{a}: rarer-per-shorter-text d:3 (text len 1) outranks d:1 (text len 2).
MIX_CAT_A_SCORES = {"d:3": 1.260592, "d:1": 0.841945}
# @cat:{c}: dt=1 (highest tag IDF here); single match d:4.
MIX_CAT_C_SCORE = 1.132230
# @cat:{x}: dt=6, six text-less docs -> doc_len 0, all identical.
MIX_CAT_X_SCORE = 0.890311
# @cat:{a|b}: union sums matched values; d:1 carries both (a+b), d:3 only a,
# d:2 only b.
MIX_CAT_A_OR_B_SCORES = {"d:1": 1.683890, "d:3": 1.260592, "d:2": 0.841945}
# hello @cat:{a}: text term + tag term summed (d:3 = 0.974311 + 1.260592).
MIX_HELLO_AND_CAT_A_SCORES = {"d:3": 2.234903, "d:1": 1.492684}

# idxNumOnly / idxTagOnly: indexes with NO text field. BM25 doc-length
# normalization needs a TEXT field (avg_doc_len derives from it); without one
# avg_doc_len is 0. Numeric never ranks, and a tag term with avg_doc_len 0
# scores a well-defined 0 (our implementation returns 0 here rather than Redis's
# nan). So every matching doc scores 0 on either index.
INDEX_NUM_ONLY = ScoringIndex(
    "idxNumOnly",
    ["FT.CREATE", "idxNumOnly", "ON", "HASH", "PREFIX", "1", "n:",
     "SCHEMA", "rank", "NUMERIC"],
    {f"n:{i}": {"rank": str(i)} for i in range(1, 5)},
)
INDEX_TAG_ONLY = ScoringIndex(
    "idxTagOnly",
    ["FT.CREATE", "idxTagOnly", "ON", "HASH", "PREFIX", "1", "t:",
     "SCHEMA", "cat", "TAG"],
    {
        "t:1": {"cat": "red"},
        "t:2": {"cat": "red,blue"},
        "t:3": {"cat": "green"},
    },
)
# index with numeric and tag field (no text field)
INDEX_NUM_TAG = ScoringIndex(
    "idxNumTag",
    ["FT.CREATE", "idxNumTag", "ON", "HASH", "PREFIX", "1", "nt:",
     "SCHEMA", "rank", "NUMERIC", "cat", "TAG"],
    {
        "nt:1": {"rank": 1, "cat": "red"},
        "nt:2": {"rank": 2, "cat": "red,blue"},
        "nt:3": {"rank": 3, "cat": "green"},
        "nt:4": {"rank": 4, "cat": "red,green"},
        "nt:5": {"rank": 5, "cat": "blue"},
    },
)

# idxHV: text `body` + a 2-D FLOAT32 vector `vec` (L2), for hybrid text=>[KNN]
# scoring. Both docs contain "cat"; "hv:short" is a one-word body (high BM25) far
# from the query vector, while "hv:long" buries "cat" in a long body (low BM25)
# but sits exactly on the query vector [1,1] (distance 0). Ranked by text score
# "hv:short" wins; ranked by vector distance "hv:long" would win.
INDEX_HV = ScoringIndex(
    "idxHV",
    ["FT.CREATE", "idxHV", "ON", "HASH", "PREFIX", "1", "hv:",
     "SCHEMA", "body", "TEXT", "NOSTEM",
     "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
     "DISTANCE_METRIC", "L2"],
    {
        "hv:short": {"body": "cat", "vec": _vec(5.0, 5.0)},
        "hv:long": {"body": "cat dog bird fish tree stone river cloud",
                    "vec": _vec(1.0, 1.0)},
    },
)
# Hybrid text scores (verified against Redis 8.6). __vec_score (distance): 32 for
# hv:short, 0 for hv:long.
HV_HYBRID_SCORES = {"hv:short": 0.267405, "hv:long": 0.138313}

# idxPSF: NOSTEM + suffix trie, for prefix/suffix/fuzzy EXPANSION scoring.
#
# NOTE — unlike every other index in this file, the prefix/suffix/fuzzy tests do
# NOT pin Redis EXPLAINSCORE values. An expansion is scored on a SINGLE matched
# term (its own IDF+F, never the sum over matched terms), but WHICH term is an
# unspecified, corpus-dependent union-iterator artifact
# (docs/redis_prefix_suffix_fuzzy_scoring.md §2). On a doc that matches via
# several expansion terms we deliberately pick a different representative than
# Redis, so pinning Redis's numbers would be spuriously incompatible. Instead
# these tests assert self-consistent relationships against our OWN exact-term
# scores on the same index:
#   - single-match doc  == the exact-term query (this DOES agree with Redis), and
#   - multi-match doc    == ONE matched term's score AND < the sum of both
#     (the invariant, independent of which term wins).
#
# Terms: cat* -> {cat, category(dt=3), catalog}; *ing -> {running, jogging};
# %cat% neighbourhood here is just {cat} (dog/others are >1 edit away).
INDEX_PSF = ScoringIndex(
    "idxPSF",
    ["FT.CREATE", "idxPSF", "ON", "HASH", "PREFIX", "1", "psf:",
     "SCHEMA", "body", "TEXT", "NOSTEM", "WITHSUFFIXTRIE"],
    {
        "psf:cat": {"body": "cat"},
        "psf:multi": {"body": "category catalog"},
        "psf:cat2": {"body": "category"},
        "psf:cat3": {"body": "category"},
        "psf:run": {"body": "running"},
        "psf:jog": {"body": "jogging"},
        "psf:dog": {"body": "dog"},
    },
)

# =====================================================================
# Expected scores (verified against Redis 8.6; idxA unless noted)
# =====================================================================

# --- Group 1: single-term ---
HELLO_SCORES = {
    "docA:5": 0.574385, "docA:4": 0.523122, "docA:3": 0.477286,
    "docA:2": 0.430172, "docA:7": 0.430172, "docA:1": 0.331888,
}
RARE_SCORES = {"docA:8": 1.915183, "docA:6": 1.419164}
# NOSTEM: no stem-root inflation. Redis's stemmed value would be 3.970230.
UNIQUE_SCORE = 1.985115

# --- Group 2: AND / OR ---
# "hello world": docs with BOTH terms, scored on hello+world.
HELLO_WORLD_SCORES = {
    "docA:4": 0.774956, "docA:3": 0.763658, "docA:2": 0.737626,
    "docA:7": 0.737626, "docA:1": 0.663776,
}
# OR "hello | world" partial-match score for a doc with only one term.
WORLD_ONLY_SCORE = 0.360540  # docA:6 (world only)
# "hello world one": 3-leaf AND, each admitted doc scored on all three leaves.
HELLO_WORLD_ONE_SCORES = {
    "docA:2": 1.202911, "docA:7": 1.202911, "docA:3": 1.197037,
    "docA:1": 1.166036, "docA:4": 1.156069,
}
# "(hello world) | (rare unique)": docA:6 fully satisfies only the rare+unique
# group, so its world token is not scored; the rest satisfy hello+world.
HELLO_WORLD_OR_RARE_UNIQUE_SCORES = {
    "docA:6": 3.404279, **HELLO_WORLD_SCORES,
}
# "hello hello": the repeated term is scored once per predicate position, so
# every score is exactly twice the single-term "hello" value.
HELLO_HELLO_SCORES = {k: 2 * v for k, v in HELLO_SCORES.items()}

# --- Group 3: query weights ---
# "(hello)=>{$weight:5}": a leaf weight scales the leaf score linearly.
HELLO_WEIGHT5_SCORES = {k: 5 * v for k, v in HELLO_SCORES.items()}
# "((hello)=>{$weight:4} (world)=>{$weight:3})=>{$weight:2}": nested layered
# weights multiply -- doc = 2 * (4*hello_leaf + 3*world_leaf).
HELLO4_WORLD3_OUTER2_SCORES = {
    "docA:4": 5.695980, "docA:3": 5.536520, "docA:2": 5.286103,
    "docA:7": 5.286103, "docA:1": 4.646429,
}
# "((hello)=>{$weight:4} | (rare)=>{$weight:2})=>{$weight:3}": per-leaf weight
# inside an OR, times the outer group weight. hello docs scored 12*hello_leaf,
# rare docs 6*rare_leaf.
HELLO4_OR_RARE2_OUTER3_SCORES = {
    "docA:8": 11.491096, "docA:6": 8.514985, "docA:5": 6.892615,
    "docA:4": 6.277460, "docA:3": 5.727435, "docA:2": 5.162066,
    "docA:7": 5.162066, "docA:1": 3.982653,
}

# --- Group 4: document_score multiplier (idxB) ---
# Unweighted BM25 total for body "hello world" (the boost=1.0 case).
BASE_SCORE = 0.148215949535

# --- Group 13: match-all (idxA) ---
# A wildcard `*` scores every doc as a single BM25 leaf with a constant IDF (1.0)
# and term frequency (1), normalized by the doc's text length (N=8,
# avg_doc_len=5.25). Only doc_len varies, so the shortest docs score highest.
MATCH_ALL_A_SCORES = {
    "docA:8": 1.495146, "docA:5": 1.107914, "docA:6": 1.107914,
    "docA:1": 1.019868, "docA:2": 0.944785, "docA:7": 0.944785,
    "docA:3": 0.880000, "docA:4": 0.773869,
}


class TestTextScoring(ValkeySearchTestCaseBase):

    # Group 1: single-term ranking ----------------------------------------

    # 1.1-1.5: single-term "hello" ranking.
    def test_single_term_hello_ranking(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "hello")

        # 1.1 only the 6 docs containing "hello"; docA:6, docA:8 absent.
        assert keys == ["docA:5", "docA:4", "docA:3", "docA:2", "docA:7", "docA:1"]

        # 1.2 exact scores.
        for key, expected in HELLO_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

        # 1.3 length normalization: shorter docA:5 (F=4,len=4) outranks the
        # higher-TF longer docA:4 (F=5,len=9).
        assert scores["docA:5"] > scores["docA:4"]

        # 1.4 TF saturation is sub-linear: 5x the TF yields well under 5x score.
        assert scores["docA:4"] / scores["docA:1"] < 5.0

        # 1.5 tie-break by key ascending: identical scores, docA:2 before docA:7.
        assert scores["docA:2"] == pytest.approx(scores["docA:7"], abs=SCORE_ABS_TOL)
        assert keys.index("docA:2") < keys.index("docA:7")

    # 1.6: "rare" (dt=2) two matches.
    def test_single_term_rare(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "rare")

        assert keys == ["docA:8", "docA:6"]
        for key, expected in RARE_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 1.7: "unique" (dt=1, highest IDF) single match; NOSTEM avoids stem inflation.
    def test_single_term_unique(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "unique")

        assert keys == ["docA:6"]
        assert scores["docA:6"] == pytest.approx(UNIQUE_SCORE, abs=SCORE_ABS_TOL)

    # 1.8: nonexistent term yields empty result.
    def test_single_term_nonexistent(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, _ = INDEX_A.search(client, "nonexistent")
        assert keys == []

    # 1.9: a single rare occurrence (dt=2) outscores a higher-TF common term
    # (dt=6) at equal doc_len, purely on IDF. docA:6 "rare" (F=1,len=4) vs
    # docA:5 "hello" (F=4,len=4).
    def test_idf_differentiation_across_terms(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        _, rare = INDEX_A.search(client, "rare")
        _, hello = INDEX_A.search(client, "hello")
        assert rare["docA:6"] > hello["docA:5"]

    # Group 2: AND / OR admission + accumulation --------------------------

    # 2.1 / 2.2: implicit AND admits only docs with BOTH terms; each is scored
    # on the hello and world leaves summed.
    def test_and_admits_both_terms_and_sums(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "hello world")

        # docA:5 (no world), docA:6 (no hello), docA:8 (neither) excluded.
        assert keys == ["docA:4", "docA:3", "docA:2", "docA:7", "docA:1"]
        for key, expected in HELLO_WORLD_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 2.3 / 2.4: OR admits any doc with either term; docs with both are scored
    # on both, single-term docs on the one they have.
    def test_or_admits_either_term_partial_scoring(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "hello | world")

        # Every doc except docA:8 (neither term) is admitted, score desc:
        # both-term docs, then hello-only docA:5, then world-only docA:6.
        assert keys == ["docA:4", "docA:3", "docA:2", "docA:7", "docA:1",
                        "docA:5", "docA:6"]
        # docs with both terms keep their AND totals.
        for key, expected in HELLO_WORLD_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
        # single-term docs are scored on the one term they contain.
        assert scores["docA:5"] == pytest.approx(HELLO_SCORES["docA:5"],
                                                 abs=SCORE_ABS_TOL)
        assert scores["docA:6"] == pytest.approx(WORLD_ONLY_SCORE,
                                                 abs=SCORE_ABS_TOL)

    # 2.5: mixed admission paths. docA:6 ("world rare unique document") is
    # admitted via the rare branch and must be scored on rare ONLY -- its world
    # token is inside the (hello world) AND branch it never satisfied (no
    # hello), so it must NOT leak into the score. This is the regression test
    # for the AND-group scoring bug.
    def test_or_branch_does_not_leak_and_sibling_leaf(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "(hello world) | rare")

        # docA:5 (no rare, no world) excluded; everything else admitted, score
        # desc: the high-IDF rare docs first, then the hello+world AND docs.
        assert keys == ["docA:8", "docA:6", "docA:4", "docA:3", "docA:2",
                        "docA:7", "docA:1"]
        # rare-branch docs scored on rare only (NOT rare + world for docA:6).
        assert scores["docA:8"] == pytest.approx(RARE_SCORES["docA:8"],
                                                 abs=SCORE_ABS_TOL)
        assert scores["docA:6"] == pytest.approx(RARE_SCORES["docA:6"],
                                                 abs=SCORE_ABS_TOL)
        # AND-branch docs keep their hello+world totals.
        for key, expected in HELLO_WORLD_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 2.6: AND of a leaf and an OR group. None of the admitted docs contain
    # rare, so (world | rare) contributes only world -- equivalent to the plain
    # "hello world" AND query for this corpus.
    def test_and_of_leaf_and_or_group(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "hello (world | rare)")

        assert keys == ["docA:4", "docA:3", "docA:2", "docA:7", "docA:1"]
        for key, expected in HELLO_WORLD_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 2.7: three-leaf AND accumulates all three leaves into each doc's score,
    # with tie-break by key ascending (docA:2 before docA:7).
    def test_three_leaf_and_accumulates(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "hello world one")

        assert keys == ["docA:2", "docA:7", "docA:3", "docA:1", "docA:4"]
        for key, expected in HELLO_WORLD_ONE_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 2.8: OR of two AND-groups. docA:6 ("world rare unique document") fully
    # satisfies only the (rare unique) group, so it is scored on rare+unique
    # ONLY -- its world token belongs to the (hello world) group it never
    # satisfied and must NOT leak in. docA:5 (hello only) and docA:8 (rare only)
    # complete neither group and are excluded.
    def test_or_of_two_and_groups(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "(hello world) | (rare unique)")

        # score desc: docA:6 (rare+unique, high IDF) first, then hello+world.
        assert keys == ["docA:6", "docA:4", "docA:3", "docA:2", "docA:7",
                        "docA:1"]
        for key, expected in HELLO_WORLD_OR_RARE_UNIQUE_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 2.9: a repeated term is scored once per predicate position, so every
    # doc's score is exactly twice its single-term "hello" score.
    def test_repeated_term_double_counts(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "hello hello")

        assert keys == ["docA:5", "docA:4", "docA:3", "docA:2", "docA:7", "docA:1"]
        for key, expected in HELLO_HELLO_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # Group 3: query weights ----------------------------------------------

    # 3.1 / 3.2: a leaf weight scales the score linearly (5x) and leaves the
    # ordering identical to the unweighted query.
    def test_leaf_weight_scales_linearly(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "(hello)=>{$weight:5}")

        # 3.2 order unchanged vs unweighted (query 1.1).
        assert keys == ["docA:5", "docA:4", "docA:3", "docA:2", "docA:7", "docA:1"]
        # 3.1 each score is 5x the unweighted value.
        for key, expected in HELLO_WEIGHT5_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 3.3: nested layered weights multiply -- inner per-leaf weights (4 on
    # hello, 3 on world) and an outer group weight of 2 compound.
    def test_nested_layered_weights_multiply(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(
            client, "((hello)=>{$weight:4} (world)=>{$weight:3})=>{$weight:2}")

        assert keys == ["docA:4", "docA:3", "docA:2", "docA:7", "docA:1"]
        for key, expected in HELLO4_WORLD3_OUTER2_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 3.4 / 3.5: per-leaf weights inside an OR, times the outer group weight.
    # The rare-only docA:8 (high IDF x weight) outranks the hello-only docA:5.
    def test_per_leaf_weight_inside_or_times_group_weight(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        _, scores = INDEX_A.search(
            client, "((hello)=>{$weight:4} | (rare)=>{$weight:2})=>{$weight:3}")

        for key, expected in HELLO4_OR_RARE2_OUTER3_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
        # 3.5 rare-only doc outranks hello-only doc via IDF x weight.
        assert scores["docA:8"] > scores["docA:5"]

    # Group 4: SCORE / SCORE_FIELD document_score multiplier ---------------

    # 4.1: SCORE must be within [0.0, 1.0]; out-of-range rejected at create.
    def test_score_param_range_rejected(self):
        client = self.server.get_new_client()
        for bad in ("2.0", "-0.5"):
            with pytest.raises(ResponseError, match="must be between 0.0 and 1.0"):
                client.execute_command(
                    "FT.CREATE", f"idxBad{bad}", "ON", "HASH",
                    "SCORE", bad, "SCHEMA", "body", "TEXT", "NOSTEM"
                )

    # 4.2: in-range SCORE applied as a multiplier on the BM25 total.
    def test_score_param_applied(self):
        client = self.server.get_new_client()
        INDEX_BDEF.load(client)
        _, scores = INDEX_BDEF.search(client, "hello world")
        for score in scores.values():
            assert score == pytest.approx(0.5 * BASE_SCORE, abs=SCORE_ABS_TOL)

    # 4.3 / 4.5: SCORE_FIELD scales the final score (overriding the SCORE
    # default), including negatives which take the score below zero.
    def test_score_field_scales_and_overrides(self):
        client = self.server.get_new_client()
        INDEX_B.load(client)
        _, scores = INDEX_B.search(client, "hello world")

        assert scores["docB:1"] == pytest.approx(2.0 * BASE_SCORE, abs=SCORE_ABS_TOL)
        assert scores["docB:2"] == pytest.approx(1.0 * BASE_SCORE, abs=SCORE_ABS_TOL)
        assert scores["docB:3"] == pytest.approx(0.5 * BASE_SCORE, abs=SCORE_ABS_TOL)
        # negative boost -> negative score (BM25STD is not floored at 0).
        assert scores["docB:4"] == pytest.approx(-1.0 * BASE_SCORE, abs=SCORE_ABS_TOL)
        assert scores["docB:4"] < 0

    # 4.4: results ordered by final score; identical body so order follows boost.
    def test_score_field_orders_by_boost(self):
        client = self.server.get_new_client()
        INDEX_B.load(client)
        keys, _ = INDEX_B.search(client, "hello world")
        assert keys == ["docB:5", "docB:1", "docB:2", "docB:3", "docB:4", "docB:6"]

    # 4.6 / 4.7: infinite SCORE_FIELD propagates to an infinite final score.
    # +inf ranks first; -inf is accepted (not excluded) and ranks last.
    def test_score_field_infinity(self):
        client = self.server.get_new_client()
        INDEX_B.load(client)
        keys, scores = INDEX_B.search(client, "hello world")

        assert scores["docB:5"] == float("inf")
        assert keys[0] == "docB:5"
        # -inf is accepted and returned (diverges from Redis, which excludes it).
        assert scores["docB:6"] == float("-inf")
        assert keys[-1] == "docB:6"

    # Group 5: document-wide TF across fields ------------------------------

    # 5.1: a field-scoped query (@f1:redis) still uses the document-wide term
    # frequency. docC:1 has "redis" in all three fields (TF=3) and outscores
    # docC:2 (TF=1), even though both match @f1:redis with one f1 occurrence.
    def test_field_scoped_query_uses_doc_wide_tf(self):
        client = self.server.get_new_client()
        INDEX_C.load(client)
        keys, scores = INDEX_C.search(client, "@f1:redis")

        assert keys == ["docC:1", "docC:2"]
        assert scores["docC:1"] > scores["docC:2"]

    # 5.2: an unscoped query yields the same doc-wide TF and ordering.
    def test_unscoped_query_uses_doc_wide_tf(self):
        client = self.server.get_new_client()
        INDEX_C.load(client)
        keys, scores = INDEX_C.search(client, "redis")

        assert keys == ["docC:1", "docC:2"]
        assert scores["docC:1"] > scores["docC:2"]

    # Group 6: phrase scoring == AND scoring -------------------------------

    # 6.1: an exact phrase narrows admission (adjacency) but does not change
    # scoring -- it scores the same leaves as the proximity AND of the same
    # terms. In Corpus A "hello world" is always adjacent, so the phrase and
    # the AND query return the same docs with identical scores.
    def test_phrase_scores_equal_to_and(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        phrase_keys, phrase_scores = INDEX_A.search(client, '@body:"hello world"')
        and_keys, and_scores = INDEX_A.search(client, "hello world")

        assert phrase_keys == and_keys
        for key in and_keys:
            assert phrase_scores[key] == pytest.approx(and_scores[key],
                                                       abs=SCORE_ABS_TOL)
        # and both match the verified AND totals.
        for key, expected in HELLO_WORLD_SCORES.items():
            assert phrase_scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # Group 7: SORTBY interaction ------------------------------------------

    # 7.1 / 7.2: SORTBY rank ASC overrides the score-desc order, but WITHSCORES
    # is still populated with the same per-doc scores as the unsorted query.
    def test_sortby_overrides_order_scores_intact(self):
        client = self.server.get_new_client()
        INDEX_A7.load(client)
        keys, scores = INDEX_A7.search(client, "hello", "SORTBY", "rank", "ASC")

        # 7.1 order is by rank ascending, NOT by score (which would be 5,4,3,2,7,1).
        assert keys == ["docA:1", "docA:2", "docA:3", "docA:4", "docA:5", "docA:7"]
        # 7.2 scores still present and equal to the no-SORTBY values.
        for key, expected in HELLO_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 7.3: SORTBY rank DESC reverses the order; scores are unchanged.
    def test_sortby_desc_reverses_order(self):
        client = self.server.get_new_client()
        INDEX_A7.load(client)
        keys, scores = INDEX_A7.search(client, "hello", "SORTBY", "rank", "DESC")

        assert keys == ["docA:7", "docA:5", "docA:4", "docA:3", "docA:2", "docA:1"]
        for key, expected in HELLO_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # Group 8: scorer selection -------------------------------------------

    # 8.1 / 8.2: explicit SCORER BM25STD yields the same scores and order as the
    # default (no SCORER), confirming BM25STD is the default scorer.
    def test_explicit_bm25std_equals_default(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        default_keys, default_scores = INDEX_A.search(client, "hello")
        explicit_keys, explicit_scores = INDEX_A.search(
            client, "hello", "SCORER", "BM25STD")

        assert explicit_keys == default_keys
        for key in default_keys:
            assert explicit_scores[key] == pytest.approx(default_scores[key],
                                                         abs=SCORE_ABS_TOL)
        # and both match the verified query 1.2 values.
        for key, expected in HELLO_SCORES.items():
            assert explicit_scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 8.3: an unimplemented/unknown SCORER is rejected at parse time
    def test_invalid_scorer_get_rejected(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        with pytest.raises(ResponseError, match=r"Unknown argument `TFIDF`"):
            client.execute_command(
                "FT.SEARCH", INDEX_A.index, "hello", "SCORER", "TFIDF")

    # Group 9: index size N = all docs, not just text docs -----------------

    # 9.1: BM25's N must count every indexed doc, not only text-bearing ones.
    # idxMix has 4 text docs + 6 numeric/tag-only docs (N=10). The verified
    # scores match Redis's N=10; scoring with N=4 (text docs only) would produce
    # ~0.303/~0.203 and fail here.
    def test_index_size_counts_all_docs_not_just_text(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(client, "hello")

        assert keys == ["d:3", "d:1", "d:2"]
        for key, expected in MIX_HELLO_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # Group 10: numeric scoring — a filter, never a ranker ------------------

    # 10.1: a numeric predicate contributes 0 to the score. Adding a numeric
    # clause to a text query changes neither scores nor order: the four "hello"
    # docs keep their verified text scores, and the numeric-only docs it also
    # admits carry no text term (score 0).
    def test_numeric_clause_contributes_zero(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        # Text-only baseline.
        _, base = INDEX_MIX.search(client, "hello")
        # OR a numeric branch matching a text-less doc (d:5, rank=5, no body).
        keys, scores = INDEX_MIX.search(client, "hello | @rank:[5 5]")

        assert set(keys) == {"d:1", "d:2", "d:3", "d:5"}
        # text docs keep their exact text scores; numeric adds nothing.
        for key, expected in MIX_HELLO_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
            assert scores[key] == pytest.approx(base[key], abs=SCORE_ABS_TOL)
        # numeric-only match carries no text term, so score 0.
        assert scores["d:5"] == pytest.approx(0.0, abs=SCORE_ABS_TOL)

    # 10.2: a pure numeric range scores every matching doc 0, so it only filters
    # (all ten docs match rank in [0, 100]).
    def test_numeric_only_scores_zero(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(client, "@rank:[0 100]")

        assert len(keys) == 10
        for score in scores.values():
            assert score == pytest.approx(0.0, abs=SCORE_ABS_TOL)

    # Group 11: tag scoring — BM25 term with F == 1 -------------------------

    # 11.1: a tag value is scored with per-tag-value IDF and TEXT-length
    # normalization. @cat:{a} (dt=2) matches d:1, d:3; the shorter-text d:3
    # (len 1) outranks d:1 (len 2).
    def test_tag_single_value_scores(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(client, "@cat:{a}")

        assert keys == ["d:3", "d:1"]
        for key, expected in MIX_CAT_A_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 11.2: a rarer tag value scores higher (IDF). @cat:{c} (dt=1) on d:4
    # outscores @cat:{x} (dt=6) on the text-less d:5..10, purely on IDF.
    def test_tag_rarer_value_scores_higher(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        _, c = INDEX_MIX.search(client, "@cat:{c}")
        keys_x, x = INDEX_MIX.search(client, "@cat:{x}")

        assert c["d:4"] == pytest.approx(MIX_CAT_C_SCORE, abs=SCORE_ABS_TOL)
        # dt=6, all six text-less docs identical (doc_len 0).
        assert len(keys_x) == 6
        for score in x.values():
            assert score == pytest.approx(MIX_CAT_X_SCORE, abs=SCORE_ABS_TOL)
        assert c["d:4"] > x["d:5"]

    # 11.3: a union {a|b} sums the terms for every matched value a doc carries.
    # d:1 has both (a+b), d:3 only a, d:2 only b.
    def test_tag_union_sums_matched_values(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(client, "@cat:{a|b}")

        assert keys == ["d:1", "d:3", "d:2"]
        for key, expected in MIX_CAT_A_OR_B_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
        # d:1's a+b total equals the sum of its single-value contributions.
        assert scores["d:1"] == pytest.approx(
            MIX_CAT_A_SCORES["d:1"] + MIX_CAT_A_OR_B_SCORES["d:2"],
            abs=SCORE_ABS_TOL)

    # 11.4: tag term frequency is NOT counted (F == 1). A $weight scales the
    # tag term linearly, exactly like a text leaf.
    def test_tag_weight_scales_linearly(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        _, base = INDEX_MIX.search(client, "@cat:{a}")
        _, weighted = INDEX_MIX.search(client, "(@cat:{a})=>{$weight:3}")

        for key, b in MIX_CAT_A_SCORES.items():
            assert base[key] == pytest.approx(b, abs=SCORE_ABS_TOL)
            assert weighted[key] == pytest.approx(3 * b, abs=SCORE_ABS_TOL)

    # 11.5: text + tag AND sums the text term and the tag term into each doc's
    # score; a numeric clause added to the same query contributes 0 (unchanged).
    def test_text_and_tag_terms_sum_numeric_adds_zero(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(client, "hello @cat:{a}")

        assert keys == ["d:3", "d:1"]
        for key, expected in MIX_HELLO_AND_CAT_A_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
        # d:3's total is the text term + the tag term.
        assert scores["d:3"] == pytest.approx(
            MIX_HELLO_SCORES["d:3"] + MIX_CAT_A_SCORES["d:3"], abs=SCORE_ABS_TOL)
        # adding a numeric clause changes nothing (numeric contributes 0).
        _, with_num = INDEX_MIX.search(client, "hello @cat:{a} @rank:[0 100]")
        for key, expected in MIX_HELLO_AND_CAT_A_SCORES.items():
            assert with_num[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # Group 12: text-less indexes score 0 (no doc-length normalization) -----

    # 12.1: on a numeric-only index a numeric predicate matches (filters) but
    # every matching doc scores 0 -- numeric never ranks.
    def test_numeric_only_index_scores_zero(self):
        client = self.server.get_new_client()
        INDEX_NUM_ONLY.load(client)
        keys, scores = INDEX_NUM_ONLY.search(client, "@rank:[1 4]")

        assert len(keys) == 4
        for score in scores.values():
            assert score == pytest.approx(0.0, abs=SCORE_ABS_TOL)

    # 12.2: on a tag-only index (no TEXT field) a tag query matches but scores a
    # well-defined 0 -- avg_doc_len is 0 without a text field, so the BM25 tag
    # term degenerates to 0 (our implementation returns 0, not Redis's nan).
    def test_tag_only_index_scores_zero(self):
        client = self.server.get_new_client()
        INDEX_TAG_ONLY.load(client)
        keys, scores = INDEX_TAG_ONLY.search(client, "@cat:{red}")

        assert set(keys) == {"t:1", "t:2"}
        for score in scores.values():
            assert score == pytest.approx(0.0, abs=SCORE_ABS_TOL)

    # 12.3: on a numeric and tag index (no text field), the scores are all 0
    def test_num_tag_index_scores_zero(self):
        client = self.server.get_new_client()
        INDEX_NUM_TAG.load(client)
        # search by tag red, get 3 docs with score 0
        keys, scores = INDEX_NUM_TAG.search(client, "@cat:{red}")
        assert len(keys) == 3
        for score in scores.values():
            assert score == pytest.approx(0.0, abs=SCORE_ABS_TOL)

        # search by numeric range 1-5, get 5 docs with score 0
        keys, scores = INDEX_NUM_TAG.search(client, "@rank:[1 5]")
        assert len(keys) == 5
        for score in scores.values():
            assert score == pytest.approx(0.0, abs=SCORE_ABS_TOL)

    # Group 13: match-all (wildcard) scoring --------------------------------

    # 13.1: `*` admits every doc and scores it as a constant BM25 leaf (IDF 1.0,
    # F 1) normalized by text length, so the ranking is purely shortest-first.
    def test_match_all_scores_by_doc_length(self):
        client = self.server.get_new_client()
        INDEX_A.load(client)
        keys, scores = INDEX_A.search(client, "*")

        # all 8 docs, score desc then key asc (docA:5 before docA:6; docA:2
        # before docA:7 on ties).
        assert keys == ["docA:8", "docA:5", "docA:6", "docA:1", "docA:2",
                        "docA:7", "docA:3", "docA:4"]
        for key, expected in MATCH_ALL_A_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # Group 14: hybrid text + vector ---------------------------------------

    # 14.1: a hybrid `text=>[KNN]` query ranks by the text (BM25) score, not by
    # vector distance. hv:short (short body, high BM25) outranks hv:long (long
    # body, low BM25) even though hv:short is FARther in vector space
    # (__vec_score 32 vs 0). Scores match the Redis 8.6 oracle.
    def test_hybrid_ranks_by_text_score_not_vector_distance(self):
        client = self.server.get_new_client()
        INDEX_HV.load(client)
        keys, scores = INDEX_HV.search(
            client, "cat=>[KNN 2 @vec $q]",
            "PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")

        # ranked by text score (short before long), NOT by distance.
        assert keys == ["hv:short", "hv:long"]
        for key, expected in HV_HYBRID_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
        assert scores["hv:short"] > scores["hv:long"]

    # 14.2: SORTBY __vec_score orders a hybrid query by vector distance (nearest
    # first), overriding the default text-score ranking. hv:long (distance 0)
    # now precedes hv:short (distance 32). WITHSCORES still reports the text
    # scores, unchanged by SORTBY.
    def test_hybrid_sortby_vector_distance(self):
        client = self.server.get_new_client()
        INDEX_HV.load(client)
        keys, scores = INDEX_HV.search(
            client, "cat=>[KNN 2 @vec $q]",
            "SORTBY", "__vec_score",
            "PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")

        # ranked by vector distance ascending (nearest first), NOT text score.
        assert keys == ["hv:long", "hv:short"]
        # scores are still the text (BM25) scores, unchanged by SORTBY.
        for key, expected in HV_HYBRID_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)

    # 14.3: WITHSORTKEYS on a hybrid `text=>[KNN]` sorted by __vec_score emits
    # each result's sort key (the vector distance) prefixed with '#', since
    # __vec_score is a synthesized field, not a stored attribute. Reply layout
    # (no WITHSCORES) is [count, key, sortkey, attrs, ...].
    def test_hybrid_sortby_vector_distance_withsortkeys(self):
        client = self.server.get_new_client()
        INDEX_HV.load(client)
        res = client.execute_command(
            "FT.SEARCH", "idxHV", "cat=>[KNN 2 @vec $q]",
            "SORTBY", "__vec_score", "WITHSORTKEYS",
            "PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")

        assert res[0] == 2
        # nearest first: hv:long (distance 0) then hv:short (distance 32).
        assert res[1] == b"hv:long" and res[2] == b"#0"
        assert res[4] == b"hv:short" and res[5] == b"#32"

    # Group 15: prefix / suffix / fuzzy expansion scoring -------------------
    #
    # An expansion contributes ONE matched term's BM25, never the sum. We assert
    # against our own exact-term scores rather than pinned Redis values, because
    # our representative-term pick diverges from Redis on multi-match docs (see
    # the INDEX_PSF note above and docs/redis_prefix_suffix_fuzzy_scoring.md §2).

    # 15.1: a doc matching the prefix via a single term scores exactly like the
    # exact-term query for that term (this case DOES agree with Redis).
    def test_prefix_single_match_equals_exact_term(self):
        client = self.server.get_new_client()
        INDEX_PSF.load(client)
        _, prefix = INDEX_PSF.search(client, "cat*")
        _, exact = INDEX_PSF.search(client, "cat")
        assert prefix["psf:cat"] > 0.0
        assert prefix["psf:cat"] == pytest.approx(exact["psf:cat"],
                                                  abs=SCORE_ABS_TOL)

    # 15.2: a doc matching the prefix via SEVERAL terms is scored on exactly ONE
    # of them (never their sum). Which term wins is unspecified/corpus-dependent,
    # so assert only the invariant: == one exact-term score, and < their sum.
    def test_prefix_multi_match_scores_one_term_not_sum(self):
        client = self.server.get_new_client()
        INDEX_PSF.load(client)
        _, prefix = INDEX_PSF.search(client, "cat*")
        _, category = INDEX_PSF.search(client, "category")
        _, catalog = INDEX_PSF.search(client, "catalog")
        got = prefix["psf:multi"]
        cat_s = category["psf:multi"]
        cat_l = catalog["psf:multi"]
        # one matched term, not the sum
        assert got < cat_s + cat_l - SCORE_ABS_TOL
        assert (got == pytest.approx(cat_s, abs=SCORE_ABS_TOL)
                or got == pytest.approx(cat_l, abs=SCORE_ABS_TOL)), (
            f"prefix={got} category={cat_s} catalog={cat_l}")

    # 15.3: suffix single-match scores like the exact term (needs WITHSUFFIXTRIE).
    def test_suffix_single_match_equals_exact_term(self):
        client = self.server.get_new_client()
        INDEX_PSF.load(client)
        _, suffix = INDEX_PSF.search(client, "@body:*ing")
        _, exact = INDEX_PSF.search(client, "running")
        assert suffix["psf:run"] > 0.0
        assert suffix["psf:run"] == pytest.approx(exact["psf:run"],
                                                  abs=SCORE_ABS_TOL)

    # 15.4: fuzzy single-match scores like the exact term.
    def test_fuzzy_single_match_equals_exact_term(self):
        client = self.server.get_new_client()
        INDEX_PSF.load(client)
        _, fuzzy = INDEX_PSF.search(client, "%cat%")
        _, exact = INDEX_PSF.search(client, "cat")
        assert fuzzy["psf:cat"] > 0.0
        assert fuzzy["psf:cat"] == pytest.approx(exact["psf:cat"],
                                                 abs=SCORE_ABS_TOL)

    # 15.5: a prefix combined with tag + numeric (text + numeric + tag) takes
    # the EXTRA-STEP scoring path (numeric/tag/negate force it, unlike a
    # pure-text query). "hel*" single-matches "hello", so the prefix leaf equals
    # the exact "hello" leaf; text + tag sum to the Redis-verified
    # "hello @cat:{a}" values (single-match agrees with Redis) and the numeric
    # adds 0. Proves expansions are scored in combined queries, not dropped to 0.
    def test_prefix_combined_with_tag_and_numeric_scored(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(
            client, "hel* @cat:{a} @rank:[0 100]")
        # cat:{a} restricts to d:1, d:3; ranked by score (d:3 > d:1).
        assert keys == ["d:3", "d:1"]
        for key, expected in MIX_HELLO_AND_CAT_A_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
        # d:3 = prefix(hello) leaf + tag(a) leaf; numeric contributes 0.
        assert scores["d:3"] == pytest.approx(
            MIX_HELLO_SCORES["d:3"] + MIX_CAT_A_SCORES["d:3"],
            abs=SCORE_ABS_TOL)

    # 15.6: a suffix combined with tag + numeric (text + numeric + tag), on the
    # extra-step path. "*llo" single-matches "hello" in every doc here, so the
    # suffix leaf equals the exact "hello" leaf, and text + tag sum to the same
    # Redis-verified values as "hello @cat:{a}"; the numeric adds 0.
    def test_suffix_combined_with_tag_and_numeric_scored(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(
            client, "@body:*llo @cat:{a} @rank:[0 100]")
        # cat:{a} restricts to d:1, d:3; ranked by score (d:3 > d:1).
        assert keys == ["d:3", "d:1"]
        for key, expected in MIX_HELLO_AND_CAT_A_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
        # d:3 = suffix(hello) leaf + tag(a) leaf; numeric contributes 0.
        assert scores["d:3"] == pytest.approx(
            MIX_HELLO_SCORES["d:3"] + MIX_CAT_A_SCORES["d:3"],
            abs=SCORE_ABS_TOL)

    # 15.7: a fuzzy pattern combined with tag + numeric (text + numeric + tag),
    # on the extra-step path. "%helo%" (edit distance 1) single-matches "hello",
    # so the fuzzy leaf equals the exact "hello" leaf and the combined scores
    # match "hello @cat:{a}"; the numeric adds 0.
    def test_fuzzy_combined_with_tag_and_numeric_scored(self):
        client = self.server.get_new_client()
        INDEX_MIX.load(client)
        keys, scores = INDEX_MIX.search(
            client, "@body:%helo% @cat:{a} @rank:[0 100]")
        assert keys == ["d:3", "d:1"]
        for key, expected in MIX_HELLO_AND_CAT_A_SCORES.items():
            assert scores[key] == pytest.approx(expected, abs=SCORE_ABS_TOL)
