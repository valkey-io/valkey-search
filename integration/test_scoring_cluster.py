import pytest
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import ValkeySearchClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper

# Reuse the exact indexes, documents, and queries from the standalone suite.
from test_scoring import (
    INDEX_A, INDEX_A7, INDEX_B, INDEX_BDEF, INDEX_C, parse_withscores,
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
