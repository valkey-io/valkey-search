import pytest
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper

# Reuse the exact indexes, documents, and queries from the standalone suite.
from test_scoring import (
    INDEX_A, INDEX_A7, INDEX_B, INDEX_BDEF, INDEX_C, ScoringIndex,
    parse_withscores, _vec,
)

"""
Cluster-mode counterpart to test_scoring.py. Same data and queries, but the
expected scores/order differ because BM25 statistics are computed per shard
(each shard only sees the docs whose slot it owns). Baseline captured from a
native Redis 8.6 3-shard cluster; see docs/test_scoring_cluster_reference.md.

Topology: ValkeySearchClusterTestCaseDebugMode default = 3 shards, 0 replicas.
docA docs land as: shard0 {1,4,5,8}, shard1 {3,7}, shard2 {2,6}. docA:2 shares
shard2 only with docA:6 (no "hello"), so its local IDF is inflated and it leads
every "hello" query -- unlike standalone where docA:5 leads.
"""

SCORE_ABS_TOL = 1e-5

# Per-shard cluster scores for the "hello" query on idxA.
CL_HELLO = {
    "docA:2": 0.902322, "docA:5": 0.620562, "docA:4": 0.560068,
    "docA:1": 0.349157, "docA:3": 0.281859, "docA:7": 0.256236,
}
CL_HELLO_ORDER = ["docA:2", "docA:5", "docA:4", "docA:1", "docA:3", "docA:7"]

CL_HELLO_WORLD = {
    "docA:2": 1.070854, "docA:4": 1.067486, "docA:1": 1.027695,
    "docA:3": 0.458619, "docA:7": 0.444481,
}
CL_HELLO_WORLD_ORDER = ["docA:2", "docA:4", "docA:1", "docA:3", "docA:7"]

# idxHVCL: hybrid text `body` + 2-D FLOAT32 vector `vec` (L2) for cluster-mode
# `text=>[KNN]`. Three docs, each alone on its own shard (hv:2 shard0, hv:3
# shard1, hv:1 shard2), so every body is a single-doc corpus. The KNN k=2
# selects the two NEAREST by vector distance; the decoy hv:1 is farthest so it
# is evicted -- even though its text score is the SMALLEST. This exercises the
# cross-shard coordinator eviction: keying that merge on text score instead of
# distance would wrongly keep the decoy. Verified against Redis 8.6 cluster.
INDEX_HV_CL = ScoringIndex(
    "idxHVCL",
    ["FT.CREATE", "idxHVCL", "ON", "HASH", "PREFIX", "1", "hv:",
     "SCHEMA", "body", "TEXT", "NOSTEM",
     "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
     "DISTANCE_METRIC", "L2"],
    {
        "hv:2": {"body": "cat cat", "vec": _vec(1.0, 1.0)},      # dist 0
        "hv:3": {"body": "cat cat cat", "vec": _vec(2.0, 2.0)},  # dist 2
        "hv:1": {"body": "cat", "vec": _vec(5.0, 5.0)},          # dist 32, decoy
    },
)
# Text (BM25) scores of the two kept docs; __vec_score (distance): 0 for hv:2,
# 2 for hv:3, 32 for the evicted hv:1.
CL_HV_SCORES = {"hv:3": 0.452072, "hv:2": 0.395563}

# idxHVK: hybrid index where one shard holds MORE than k matching docs, so both
# shard-local and coordinator eviction run. The {SA} hashtag co-locates three
# docs on one shard; {SB} places the fourth on another. Query vector is (1,1);
# L2 distance is squared-euclidean. KNN k=2:
#   hv:{SA}1 vec(1,1) dist 0,  body "cat"           low text  -> kept
#   hv:{SA}2 vec(2,2) dist 2,  body "cat cat cat"   text 0.19 -> coordinator evicts
#   hv:{SA}3 vec(8,8) dist 98, body "cat cat"       text 0.18 -> shard-local evicts
#   hv:{SB}1 vec(1,2) dist 1,  body "cat cat cat cat" high text -> kept
# The decoy {SA}2 is nearer in text than {SB}1, so keying the merge on text
# (the old bug) would keep {SA}1,{SA}2 and drop {SB}1. Verified vs Redis 8.6.
INDEX_HVK = ScoringIndex(
    "idxHVK",
    ["FT.CREATE", "idxHVK", "ON", "HASH", "PREFIX", "1", "hv:",
     "SCHEMA", "body", "TEXT", "NOSTEM",
     "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
     "DISTANCE_METRIC", "L2"],
    {
        "hv:{SA}1": {"body": "cat", "vec": _vec(1.0, 1.0)},
        "hv:{SA}2": {"body": "cat cat cat", "vec": _vec(2.0, 2.0)},
        "hv:{SA}3": {"body": "cat cat", "vec": _vec(8.0, 8.0)},
        "hv:{SB}1": {"body": "cat cat cat cat", "vec": _vec(1.0, 2.0)},
    },
)
CL_HVK_SCORES = {"hv:{SB}1": 0.486847, "hv:{SA}1": 0.167868}

# idxVN: numeric-filtered vector query (no text predicate), spread across three
# shards via hashtags. Query vector (1,1), filter @n:[0 100], KNN k=2:
#   vn:{SA}1 n=10  vec(1,1) dist 0  -> kept
#   vn:{SC}2 n=20  vec(1,2) dist 1  -> kept
#   vn:{SB}3 n=30  vec(3,3) dist 8  -> in range but evicted by KNN
#   vn:{SA}4 n=500 vec(1,1) dist 0  -> nearest, but filtered out by @n
INDEX_VN = ScoringIndex(
    "idxVN",
    ["FT.CREATE", "idxVN", "ON", "HASH", "PREFIX", "1", "vn:",
     "SCHEMA", "n", "NUMERIC",
     "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
     "DISTANCE_METRIC", "L2"],
    {
        "vn:{SA}1": {"n": 10, "vec": _vec(1.0, 1.0)},
        "vn:{SC}2": {"n": 20, "vec": _vec(1.0, 2.0)},
        "vn:{SB}3": {"n": 30, "vec": _vec(3.0, 3.0)},
        "vn:{SA}4": {"n": 500, "vec": _vec(1.0, 1.0)},
    },
)
CL_VN_DIST = {"vn:{SA}1": 0.0, "vn:{SC}2": 1.0}


class TestTextScoringCluster(ValkeySearchClusterTestCase):

    # --- helpers -----------------------------------------------------------

    def _load(self, index):
        """Create the index on a primary and route document writes across the
        cluster, then wait for backfill on every shard."""
        cluster: ValkeyCluster = self.new_cluster_client()
        cluster.execute_command(*index.create_cmd)
        for key, mapping in index.docs.items():
            cluster.hset(key, mapping=mapping)
        for client in self.get_all_primary_clients():
            IndexingTestHelper.wait_for_backfill_complete_on_node(
                client, index.index)

    def _search(self, index, *query):
        """Run FT.SEARCH ... WITHSCORES via a primary (fans out across shards)."""
        client: Valkey = self.new_client_for_primary(0)
        pairs = parse_withscores(
            client.execute_command("FT.SEARCH", index.index, *query, "WITHSCORES"))
        return [k for k, _ in pairs], dict(pairs)

    def _assert_scores(self, scores, expected):
        for key, exp in expected.items():
            assert key in scores, f"{key} missing from {scores}"
            assert scores[key] == pytest.approx(exp, abs=SCORE_ABS_TOL), \
                f"{key}: {scores[key]} != {exp}"

    def _search_page(self, index, *query):
        """Like _search but tolerates LIMIT: the reply's leading count is the
        total match count, which can exceed the returned page size. Returns
        (total_count, ordered keys, {key: score})."""
        client: Valkey = self.new_client_for_primary(0)
        res = client.execute_command(
            "FT.SEARCH", index.index, *query, "WITHSCORES")
        pairs = [(res[i].decode() if isinstance(res[i], bytes) else res[i],
                  float(res[i + 1])) for i in range(1, len(res), 3)]
        return res[0], [k for k, _ in pairs], dict(pairs)

    def _search_vec(self, index, *query):
        """Run FT.SEARCH (no WITHSCORES) for a non-text vector query, which has
        no top-level score. Returns (ordered keys, {key: __vec_score})."""
        client: Valkey = self.new_client_for_primary(0)
        res = client.execute_command("FT.SEARCH", index.index, *query)
        keys, scores = [], {}
        for i in range(1, len(res), 2):
            key = res[i].decode() if isinstance(res[i], bytes) else res[i]
            attrs = res[i + 1]
            vs = None
            for j in range(0, len(attrs), 2):
                name = attrs[j].decode() if isinstance(attrs[j], bytes) \
                    else attrs[j]
                if name == "__vec_score":
                    vs = float(attrs[j + 1])
            keys.append(key)
            scores[key] = vs
        return keys, scores

    # --- Group 1: single-term ---------------------------------------------

    def test_single_term_hello_ranking(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "hello")
        # docA:2 leads (per-shard IDF), unlike standalone where docA:5 leads.
        assert keys == CL_HELLO_ORDER
        self._assert_scores(scores, CL_HELLO)

    def test_single_term_rare(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "rare")
        assert keys == ["docA:8", "docA:6"]
        self._assert_scores(scores, {"docA:8": 1.778306, "docA:6": 0.754913})

    def test_single_term_unique(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "unique")
        assert keys == ["docA:6"]
        self._assert_scores(scores, {"docA:6": 0.754913})

    def test_single_term_nonexistent(self):
        self._load(INDEX_A)
        keys, _ = self._search(INDEX_A, "nonexistent")
        assert keys == []

    # --- Group 2: AND / OR -------------------------------------------------

    def test_and_admits_both_terms(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "hello world")
        assert keys == CL_HELLO_WORLD_ORDER
        self._assert_scores(scores, CL_HELLO_WORLD)

    def test_or_admits_either_term(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "hello | world")
        assert keys == ["docA:2", "docA:4", "docA:1", "docA:5",
                        "docA:3", "docA:7", "docA:6"]
        self._assert_scores(scores, {**CL_HELLO_WORLD,
                                     "docA:5": 0.620562, "docA:6": 0.198568})

    def test_or_branch_does_not_leak(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "(hello world) | rare")
        assert keys == ["docA:8", "docA:2", "docA:4", "docA:1",
                        "docA:6", "docA:3", "docA:7"]
        self._assert_scores(scores, {"docA:8": 1.778306, "docA:6": 0.754913,
                                     **CL_HELLO_WORLD})

    def test_three_leaf_and(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "hello world one")
        assert keys == ["docA:2", "docA:1", "docA:4", "docA:3", "docA:7"]
        self._assert_scores(scores, {
            "docA:2": 1.711579, "docA:1": 1.706232, "docA:4": 1.574904,
            "docA:3": 0.635378, "docA:7": 0.632727})

    def test_or_of_two_and_groups(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "(hello world) | (rare unique)")
        assert keys == ["docA:6", "docA:2", "docA:4", "docA:1", "docA:3", "docA:7"]
        self._assert_scores(scores, {"docA:6": 1.509826, **CL_HELLO_WORLD})

    def test_repeated_term_double_counts(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "hello hello")
        assert keys == ["docA:2", "docA:5", "docA:4", "docA:1", "docA:3", "docA:7"]
        self._assert_scores(scores, {
            "docA:2": 1.804644, "docA:5": 1.241125, "docA:4": 1.120136,
            "docA:1": 0.698314, "docA:3": 0.563719, "docA:7": 0.512472})

    # --- Group 3: query weights -------------------------------------------

    def test_leaf_weight_scales(self):
        self._load(INDEX_A)
        keys, scores = self._search(INDEX_A, "(hello)=>{$weight:5}")
        assert keys == CL_HELLO_ORDER
        # 5x the unweighted per-shard scores.
        self._assert_scores(scores, {k: 5 * v for k, v in CL_HELLO.items()})

    # --- Group 4: SCORE / SCORE_FIELD -------------------------------------

    def test_score_param_applied(self):
        self._load(INDEX_BDEF)
        _, scores = self._search(INDEX_BDEF, "hello world")
        # Identical docs split into two per-shard base scores (0.5 * base each):
        # docBd:3, docBd:4 are alone on their shards; docBd:1/2/5/6 share shard1.
        self._assert_scores(scores, {
            "docBd:3": 0.287682, "docBd:4": 0.287682,
            "docBd:1": 0.105361, "docBd:2": 0.105361,
            "docBd:5": 0.105361, "docBd:6": 0.105361})

    def test_score_field_infinity_ordering(self):
        self._load(INDEX_B)
        keys, scores = self._search(INDEX_B, "hello world")
        assert scores["docB:5"] == float("inf")
        assert keys[0] == "docB:5"
        # Finite boosts scale each shard-local base.
        self._assert_scores(scores, {
            "docB:1": 0.421442, "docB:3": 0.287682,
            "docB:2": 0.210721, "docB:4": -0.575364})

    # --- Group 5: document-wide TF ----------------------------------------

    def test_doc_wide_tf(self):
        self._load(INDEX_C)
        keys, scores = self._search(INDEX_C, "redis")
        assert keys == ["docC:1", "docC:2"]
        assert scores["docC:1"] > scores["docC:2"]
        self._assert_scores(scores, {"docC:1": 0.452072, "docC:2": 0.287682})

    # --- Group 7: SORTBY (order is score-independent, matches standalone) --

    def test_sortby_overrides_order(self):
        self._load(INDEX_A7)
        keys, scores = self._search(INDEX_A7, "hello", "SORTBY", "rank", "ASC")
        assert keys == ["docA:1", "docA:2", "docA:3", "docA:4", "docA:5", "docA:7"]
        # Scores are the per-shard cluster values, not the standalone ones.
        self._assert_scores(scores, CL_HELLO)

    # --- Group 14: hybrid text + vector (cross-shard) ----------------------

    def test_hybrid_cluster_ranks_by_text_score(self):
        # A hybrid text=>[KNN] query fanned across shards ranks by the text
        # (BM25) score, not by vector distance. KNN k=2 keeps the two nearest
        # (hv:2 dist 0, hv:3 dist 2) and evicts the decoy hv:1 (dist 32); the
        # survivors are then ordered by text score descending.
        self._load(INDEX_HV_CL)
        keys, scores = self._search(
            INDEX_HV_CL, "cat=>[KNN 2 @vec $q]",
            "PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")
        assert keys == ["hv:3", "hv:2"]
        assert "hv:1" not in scores
        self._assert_scores(scores, CL_HV_SCORES)

    def test_hybrid_cluster_sortby_vector_distance(self):
        # SORTBY __vec_score orders the merged fanout by vector distance ascending
        # (hv:2 dist 0 before hv:3 dist 2), overriding the text-score ranking. The
        # decoy hv:1 is still evicted by KNN, and WITHSCORES reports text scores.
        self._load(INDEX_HV_CL)
        keys, scores = self._search(
            INDEX_HV_CL, "cat=>[KNN 2 @vec $q]", "SORTBY", "__vec_score",
            "PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")
        assert keys == ["hv:2", "hv:3"]
        assert "hv:1" not in scores
        self._assert_scores(scores, CL_HV_SCORES)

    # --- Group 15: cross-shard pagination / eviction / non-text ------------

    def test_pagination_limit_offset(self):
        # LIMIT offset+count applies to the GLOBAL cross-shard ranking: the
        # coordinator merges every shard, orders by score, then trims to the
        # requested page. total_count still reports all matches.
        self._load(INDEX_A)
        total, keys, scores = self._search_page(
            INDEX_A, "hello", "LIMIT", "2", "2")
        assert total == 6
        assert keys == ["docA:4", "docA:1"]
        self._assert_scores(scores, {"docA:4": 0.560068, "docA:1": 0.349157})

    def test_hybrid_cluster_shard_local_and_coordinator_eviction(self):
        # A shard holding more than k matches evicts locally (hv:{SA}3, dist 98)
        # AND at the coordinator (hv:{SA}2, dist 2), leaving the two globally
        # nearest re-ranked by text score. Keying the merge on text instead of
        # distance would keep hv:{SA}2 and drop hv:{SB}1.
        self._load(INDEX_HVK)
        keys, scores = self._search(
            INDEX_HVK, "cat=>[KNN 2 @vec $q]",
            "PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")
        assert keys == ["hv:{SB}1", "hv:{SA}1"]
        assert "hv:{SA}2" not in scores and "hv:{SA}3" not in scores
        self._assert_scores(scores, CL_HVK_SCORES)

    def test_numeric_filtered_vector_orders_by_distance(self):
        # A vector query filtered by numeric (no text predicate) fans out, then
        # the coordinator orders by vector distance ascending. The numeric filter
        # excludes vn:{SA}4 even though it is the nearest (dist 0), and vn:{SB}3
        # (in range, dist 8) is evicted by KNN k=2.
        self._load(INDEX_VN)
        keys, scores = self._search_vec(
            INDEX_VN, "@n:[0 100]=>[KNN 2 @vec $q]",
            "PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")
        assert keys == ["vn:{SA}1", "vn:{SC}2"]
        assert "vn:{SA}4" not in scores and "vn:{SB}3" not in scores
        for key, dist in CL_VN_DIST.items():
            assert scores[key] == pytest.approx(dist, abs=SCORE_ABS_TOL)
