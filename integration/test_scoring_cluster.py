"""
Cluster-mode counterpart to test_scoring.py: same indexes, documents and queries,
but different expected scores because BM25 statistics are computed per shard —
each shard only sees the docs whose slot it owns.

TEXT_DOCS lands as shard0 {1,4,5,8}, shard1 {2,6}, shard2 {3,7}. Only
co-location matters, so these are the same groupings (and the same verified
scores) as the pre-rename baseline in docs/test_scoring_cluster_reference.md.
doc:2 shares a shard only with doc:6, which has no "hello", so its local IDF is
inflated and it leads every "hello" query — unlike standalone, where doc:5 leads.

Topology: ValkeySearchClusterTestCase default = 3 shards, 0 replicas.
"""

import pytest
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchClusterTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper, run_in_thread

from test_scoring import (
    IDX_MAIN, IDX_DOC_SCORE, TEXT_DOCS, MULTI_FIELD_DOCS, SCORE_ABS_TOL, _vec,
)


# =====================================================================
# Cluster-only fixtures
# =====================================================================

# Hybrid text + vector. The two doc sets below share this index and differ only
# in placement, which is what decides where KNN eviction runs.
IDX_HYBRID = [
    "FT.CREATE", "idxHybrid", "ON", "HASH", "PREFIX", "1", "hv:",
    "SCHEMA", "body", "TEXT", "NOSTEM",
    "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
    "DISTANCE_METRIC", "L2",
]

HYBRID_SPREAD_DOCS = {
    "hv:2": {"body": "cat cat", "vec": _vec(1.0, 1.0)},      # dist 0
    "hv:3": {"body": "cat cat cat", "vec": _vec(2.0, 2.0)},  # dist 2
    "hv:1": {"body": "cat", "vec": _vec(5.0, 5.0)},          # dist 32, decoy
}

# {SA} co-locates three docs so that shard holds more than k, making shard-local
# eviction run as well.
HYBRID_COLOCATED_DOCS = {
    "hv:{SA}1": {"body": "cat", "vec": _vec(1.0, 1.0)},              # dist 0
    "hv:{SA}2": {"body": "cat cat cat", "vec": _vec(2.0, 2.0)},      # dist 2
    "hv:{SA}3": {"body": "cat cat", "vec": _vec(8.0, 8.0)},          # dist 98
    "hv:{SB}1": {"body": "cat cat cat cat", "vec": _vec(1.0, 2.0)},  # dist 1
}

# Numeric-filtered vector query with no text predicate, spread over three shards.
IDX_VECTOR_NUM = [
    "FT.CREATE", "idxVectorNum", "ON", "HASH", "PREFIX", "1", "vn:",
    "SCHEMA", "n", "NUMERIC",
    "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2",
    "DISTANCE_METRIC", "L2",
]
VECTOR_NUM_DOCS = {
    "vn:{SA}1": {"n": 10, "vec": _vec(1.0, 1.0)},   # dist 0, kept
    "vn:{SC}2": {"n": 20, "vec": _vec(1.0, 2.0)},   # dist 1, kept
    "vn:{SB}3": {"n": 30, "vec": _vec(3.0, 3.0)},   # dist 8, evicted by KNN
    "vn:{SA}4": {"n": 500, "vec": _vec(1.0, 1.0)},  # nearest, filtered out by @n
}


def _pairs(res, no_content=False):
    """Parses a WITHSCORES reply into ordered (key, score) tuples.

    After the count, each result is a triple: key, score, attrs. A NOCONTENT
    reply omits the attrs element."""
    out = []
    stride = 2 if no_content else 3
    for i in range(1, len(res), stride):
        key = res[i].decode() if isinstance(res[i], bytes) else res[i]
        out.append((key, float(res[i + 1])))
    return out


def _agg_pairs(res):
    """Parses an ADDSCORES reply into ordered (key, score) tuples. Each row is a
    flat field/value list, so the query has to LOAD __key alongside ADDSCORES."""
    out = []
    for row in res[1:]:
        fields = {row[i]: row[i + 1] for i in range(0, len(row), 2)}
        out.append((fields[b"__key"].decode(), float(fields[b"__score"])))
    return out


def _load(test, index, docs):
    """Creates the index, routes writes across the cluster, waits for backfill
    on every shard."""
    cluster: ValkeyCluster = test.new_cluster_client()
    cluster.execute_command(*index)
    for key, mapping in docs.items():
        cluster.hset(key, mapping=mapping)
    for client in test.get_all_primary_clients():
        IndexingTestHelper.wait_for_backfill_complete_on_node(client, index[1])


class TestScoringCluster(ValkeySearchClusterTestCase):

    # --- helpers -----------------------------------------------------------

    def _search(self, index, *query):
        """Runs FT.SEARCH ... WITHSCORES via a primary, which fans out to every
        shard; returns (ordered keys, {key: score})."""
        client: Valkey = self.new_client_for_primary(0)
        res = client.execute_command("FT.SEARCH", index[1], *query, "WITHSCORES")
        pairs = _pairs(res)
        assert len(pairs) == res[0]
        return [k for k, _ in pairs], dict(pairs)

    def _search_page(self, index, *query):
        """Like _search but tolerates LIMIT, where the leading count is the total
        match count rather than the page size."""
        client: Valkey = self.new_client_for_primary(0)
        res = client.execute_command("FT.SEARCH", index[1], *query, "WITHSCORES")
        pairs = _pairs(res)
        return res[0], [k for k, _ in pairs], dict(pairs)

    def _search_vec(self, index, *query):
        """Runs FT.SEARCH for a non-text vector query, which carries no top-level
        score; returns (ordered keys, {key: __vec_score})."""
        client: Valkey = self.new_client_for_primary(0)
        res = client.execute_command("FT.SEARCH", index[1], *query)
        keys, scores = [], {}
        for i in range(1, len(res), 2):
            key = res[i].decode() if isinstance(res[i], bytes) else res[i]
            attrs = res[i + 1]
            for j in range(0, len(attrs), 2):
                name = attrs[j].decode() if isinstance(attrs[j], bytes) else attrs[j]
                if name == "__vec_score":
                    scores[key] = float(attrs[j + 1])
            keys.append(key)
        return keys, scores

    # --- tests -------------------------------------------------------------

    # Group 1: single-term ranking, driven by each shard's local statistics.
    def test_single_term(self):
        _load(self, IDX_MAIN, TEXT_DOCS)

        # doc:2 leads on per-shard IDF, where standalone puts doc:5 first.
        keys, hello = self._search(IDX_MAIN, "hello")
        assert keys == ["doc:2", "doc:5", "doc:4", "doc:1", "doc:3", "doc:7"]
        assert hello == pytest.approx({
            "doc:2": 0.902322, "doc:5": 0.620562, "doc:4": 0.560068,
            "doc:1": 0.349157, "doc:3": 0.281859, "doc:7": 0.256236,
        }, abs=SCORE_ABS_TOL)

        keys, rare = self._search(IDX_MAIN, "rare")
        assert keys == ["doc:8", "doc:6"]
        assert rare == pytest.approx({"doc:8": 1.778306, "doc:6": 0.754913},
                                     abs=SCORE_ABS_TOL)

        assert self._search(IDX_MAIN, "nonexistent") == ([], {})

    # Group 2: AND / OR admission and accumulation across shards.
    def test_and_or(self):
        _load(self, IDX_MAIN, TEXT_DOCS)
        _, hello = self._search(IDX_MAIN, "hello")

        keys, hello_world = self._search(IDX_MAIN, "hello world")
        assert keys == ["doc:2", "doc:4", "doc:1", "doc:3", "doc:7"]
        assert hello_world == pytest.approx({
            "doc:2": 1.070854, "doc:4": 1.067486, "doc:1": 1.027695,
            "doc:3": 0.458619, "doc:7": 0.444481,
        }, abs=SCORE_ABS_TOL)

        keys, either = self._search(IDX_MAIN, "hello | world")
        assert keys == ["doc:2", "doc:4", "doc:1", "doc:5", "doc:3",
                        "doc:7", "doc:6"]
        assert either == pytest.approx(
            {**hello_world, "doc:5": 0.620562, "doc:6": 0.198568},
            abs=SCORE_ABS_TOL)

        # doc:6 is admitted by rare, so its "world" token stays unscored.
        keys, or_leaf = self._search(IDX_MAIN, "(hello world) | rare")
        assert keys == ["doc:8", "doc:2", "doc:4", "doc:1", "doc:6",
                        "doc:3", "doc:7"]
        assert or_leaf == pytest.approx(
            {**hello_world, "doc:8": 1.778306, "doc:6": 0.754913},
            abs=SCORE_ABS_TOL)

        keys, or_groups = self._search(IDX_MAIN, "(hello world) | (rare unique)")
        assert keys == ["doc:6", "doc:2", "doc:4", "doc:1", "doc:3", "doc:7"]
        assert or_groups == pytest.approx({**hello_world, "doc:6": 1.509826},
                                          abs=SCORE_ABS_TOL)

        keys, three_leaf = self._search(IDX_MAIN, "hello world one")
        assert keys == ["doc:2", "doc:1", "doc:4", "doc:3", "doc:7"]
        assert three_leaf == pytest.approx({
            "doc:2": 1.711579, "doc:1": 1.706232, "doc:4": 1.574904,
            "doc:3": 0.635378, "doc:7": 0.632727,
        }, abs=SCORE_ABS_TOL)

        _, repeated = self._search(IDX_MAIN, "hello hello")
        assert repeated == pytest.approx({k: 2 * v for k, v in hello.items()},
                                         abs=SCORE_ABS_TOL)

    # Group 3: a leaf weight scales each shard-local score linearly.
    def test_query_weights(self):
        _load(self, IDX_MAIN, TEXT_DOCS)
        _, hello = self._search(IDX_MAIN, "hello")

        keys, weighted = self._search(IDX_MAIN, "(hello)=>{$weight:5}")
        assert keys == ["doc:2", "doc:5", "doc:4", "doc:1", "doc:3", "doc:7"]
        assert weighted == pytest.approx({k: 5 * v for k, v in hello.items()},
                                         abs=SCORE_ABS_TOL)

    # Group 4: the document_score multiplier applies to the shard-local base.
    def test_document_score(self):
        _load(self, IDX_DOC_SCORE, TEXT_DOCS)

        # doc:2 has no boost field and falls back to the index SCORE of 0.5.
        keys, scores = self._search(IDX_DOC_SCORE, "hello")
        assert keys == ["doc:5", "doc:1", "doc:2", "doc:3", "doc:4", "doc:7"]
        assert scores == pytest.approx({
            "doc:5": float("inf"), "doc:1": 0.698314, "doc:2": 0.451161,
            "doc:3": 0.070465, "doc:4": -0.560068, "doc:7": float("-inf"),
        }, abs=SCORE_ABS_TOL)

    # Group 5: term frequency is counted document-wide, not per field.
    def test_doc_wide_term_frequency(self):
        _load(self, IDX_MAIN, MULTI_FIELD_DOCS)

        # The two docs land on different shards, so each is a single-doc corpus;
        # per-field TF would still invert this order.
        keys, scores = self._search(IDX_MAIN, "@body:alpha")
        assert keys == ["doc:1", "doc:2"]
        assert scores == pytest.approx({"doc:1": 0.452072, "doc:2": 0.395563},
                                       abs=SCORE_ABS_TOL)

    # Group 7: SORTBY replaces the order; the scores stay shard-local.
    def test_sortby(self):
        _load(self, IDX_MAIN, TEXT_DOCS)
        _, hello = self._search(IDX_MAIN, "hello")

        keys, scores = self._search(IDX_MAIN, "hello", "SORTBY", "rank", "ASC")
        assert keys == ["doc:1", "doc:2", "doc:3", "doc:4", "doc:5", "doc:7"]
        assert scores == pytest.approx(hello, abs=SCORE_ABS_TOL)

        keys, scores = self._search(IDX_MAIN, "hello", "SORTBY", "rank", "DESC")
        assert keys == ["doc:7", "doc:5", "doc:4", "doc:3", "doc:2", "doc:1"]
        assert scores == pytest.approx(hello, abs=SCORE_ABS_TOL)

    # Group 13: a hybrid text=>[KNN] fanout evicts by distance -- first on any
    # shard holding more than k matches, then again at the coordinator -- and
    # ranks the survivors by text score.
    def test_hybrid_text_vector(self):
        _load(self, IDX_HYBRID, HYBRID_SPREAD_DOCS)
        params = ("PARAMS", "2", "q", _vec(1.0, 1.0), "DIALECT", "2")
        expected = {"hv:3": 0.452072, "hv:2": 0.395563}

        # One doc per shard, so only the coordinator evicts: hv:1 (dist 32) goes,
        # and hv:3 still leads on text score even though hv:2 is nearer.
        keys, scores = self._search(IDX_HYBRID, "cat=>[KNN 2 @vec $q]", *params)
        assert keys == ["hv:3", "hv:2"]
        assert scores == pytest.approx(expected, abs=SCORE_ABS_TOL)

        # SORTBY __vec_score reorders the merge by distance, scores unchanged.
        keys, scores = self._search(IDX_HYBRID, "cat=>[KNN 2 @vec $q]",
                                    "SORTBY", "__vec_score", *params)
        assert keys == ["hv:2", "hv:3"]
        assert scores == pytest.approx(expected, abs=SCORE_ABS_TOL)

        # FLUSHALL on all nodes to clear the cluster
        for client in self.get_all_primary_clients():
            client.execute_command("FLUSHALL", "SYNC")
        self.new_cluster_client().execute_command("FT.DROPINDEX", IDX_HYBRID[1])
        _load(self, IDX_HYBRID, HYBRID_COLOCATED_DOCS)

        # {SA} holds three matches, so that shard evicts to its two nearest before
        # the coordinator evicts again: {SA}3 never leaves the shard and {SA}2 loses
        # the merge, though both outscore the surviving {SA}1 on text.
        keys, scores = self._search(IDX_HYBRID, "cat=>[KNN 2 @vec $q]", *params)
        assert keys == ["hv:{SB}1", "hv:{SA}1"]
        assert scores == pytest.approx({"hv:{SB}1": 0.486847,
                                        "hv:{SA}1": 0.167868},
                                       abs=SCORE_ABS_TOL)

    # Group 15: LIMIT trims the merged global ranking, not each shard's page.
    def test_pagination(self):
        _load(self, IDX_MAIN, TEXT_DOCS)

        total, keys, scores = self._search_page(
            IDX_MAIN, "hello", "LIMIT", "2", "2")
        assert total == 6
        assert keys == ["doc:4", "doc:1"]
        assert scores == pytest.approx({"doc:4": 0.560068, "doc:1": 0.349157},
                                       abs=SCORE_ABS_TOL)



# The index is tag so queries skip the contention check. Tag and Numeric data
# revalidated is fetched directly from the key unlike Text which needs to wait
# for the key to be indexed. Just a simplification to ease test writing here.
IDX_NAME = "idxRecompute"
IDX = [
    "FT.CREATE", IDX_NAME, "ON", "HASH", "PREFIX", "1", "r:",
    "SCHEMA",  "t", "TAG", "body", "TEXT", "NOSTEM",
]

DOCS = {f"r:{i}": {"n": str(i), "t": "red", "body": "hello"} for i in range(12)}

BOOSTED_BODY = "hello hello hello"

class TestScoringRecomputeCluster(ValkeySearchClusterTestCaseDebugMode):
    """
    Cluster-mode coverage for the main-thread score recompute path.

    Only content-fetching queries reach the recompute, and each does so on the local
    shard that ran Search() and pre-built the scorer. A no_content FT.AGGREGATE fetches
    nothing and does not re-compute a score.
    """

    def _counter(self, client: Valkey, name: str) -> int:
        return int(client.info("SEARCH")[name])

    def _load_local_key(self, coordinator: Valkey) -> str:
        """Loads the fixture and returns a key the coordinator owns."""
        _load(self, IDX, DOCS)
        local = sorted(k.decode() for k in coordinator.keys("r:*"))
        if not local:
            pytest.fail("coordinator owns none of the documents")
        return local[0]

    def _run_with_mutation_in_flight(self, coordinator: Valkey, key: str, *cmd):
        """Parks a mutation for `key` in the queue so its db sequence number
        diverges from the one carried on the neighbor — what drives VerifyFilter
        into the recompute — then runs `cmd` on the coordinator.

        The mutation touches `n`, not the queried `t`: revalidation must still
        MATCH, otherwise VerifyFilter returns on the non-match branch and never
        reaches the recompute."""

        # Run the command while blocked
        coordinator.execute_command("ft._debug PAUSEPOINT SET block_mutation_queue")
        writer, _, writer_err = run_in_thread(
            lambda: self.new_client_for_primary(0).hset(key, "body", BOOSTED_BODY)
        )
        waiters.wait_for_true(
            lambda: int(
                coordinator.execute_command(
                    "ft._debug PAUSEPOINT test block_mutation_queue"
                )
            )
            > 0
        )
        blocked_baseline = self._counter(
            coordinator, "search_text_query_blocked_count"
        )
        runner, res, err = run_in_thread(
            lambda: self.new_client_for_primary(0).execute_command(*cmd)
        )

        # A no_content query never reaches the contention check, so it just
        # runs to completion; either outcome means it is safe to release.
        waiters.wait_for_true(
            lambda: not runner.is_alive()
            or self._counter(coordinator, "search_text_query_blocked_count")
            > blocked_baseline
        )

        # Unblock and reset the index state
        coordinator.execute_command(
            "ft._debug PAUSEPOINT RESET block_mutation_queue"
        )
        writer.join()
        runner.join()
        assert writer_err[0] is None
        assert err[0] is None
        self.new_client_for_primary(0).hset(key, "body", DOCS[key]["body"])
        IndexingTestHelper.wait_for_indexing_complete_on_node(coordinator, IDX_NAME)
    
        return res[0]

    def test_search_recomputes_on_local_responder(self):
        coordinator: Valkey = self.new_client_for_primary(0)
        key = self._load_local_key(coordinator)
        assert self._counter(coordinator, "search_predicate_revalidation") == 0

        # Basic search without key in flight doesn't revalidate
        res = coordinator.execute_command(
            "FT.SEARCH", IDX_NAME, "@t:{red}", "LIMIT", "0", "100",
            "WITHSCORES",
        )
        assert self._counter(coordinator, "search_predicate_revalidation") == 0
        scores0 = [s for _, s in _pairs(res)]
        assert len(scores0) == len(DOCS)
        assert all(s > 0 for s in scores0)

        # Basic search needs to revalidate predicate on main thread
        res = self._run_with_mutation_in_flight(
            coordinator, key,
            "FT.SEARCH", IDX_NAME, "@t:{red}", "LIMIT", "0", "100",
            "WITHSCORES",
        )
        assert self._counter(coordinator, "search_predicate_revalidation") == 1
        pairs = _pairs(res)
        assert sorted(k for k, _ in pairs) == sorted(DOCS)
        scores1 = [s for _, s in pairs]
        assert all(s > 0 for s in scores1)
        # TODO: Uncomment after https://github.com/valkey-io/valkey-search/issues/1439 is fixed
        # assert scores1 != scores0
        # assert sorted(scores1) != sorted(scores0)

        # LOAD __key doesn't resolve to a record attribute, so no content from
        # updated key is fetched and therefore no revalidation is needed.
        res = self._run_with_mutation_in_flight(
            coordinator, key,
            "FT.AGGREGATE", IDX_NAME, "@t:{red}", "ADDSCORES",
            "LOAD", "1", "__key",
        )
        assert self._counter(coordinator, "search_predicate_revalidation") == 1
        pairs = _agg_pairs(res)
        assert sorted(k for k, _ in pairs) == sorted(DOCS)
        scores2 = [s for _, s in pairs]
        assert scores2 != scores1
        # NOTE: FT.AGGREGATE results aren't implicitly sorted by score
        sorted_scores2 = sorted(scores2, reverse=True)
        # TODO: Uncomment after https://github.com/valkey-io/valkey-search/issues/1439 is fixed
        # assert sorted_scores2 != scores1
        assert sorted_scores2 == scores0

        # A LOAD that actually fetches content triggers revalidation and
        # score recompute
        res = self._run_with_mutation_in_flight(
            coordinator, key,
            "FT.AGGREGATE", IDX_NAME, "@t:{red}", "ADDSCORES",
            "LOAD", "2", "__key", "@t",
        )

        assert self._counter(coordinator, "search_predicate_revalidation") == 2
        pairs = _agg_pairs(res)
        assert sorted(k for k, _ in pairs) == sorted(DOCS)
        scores3 = [s for _, s in pairs]
        assert sorted(scores3, reverse=True) == scores1

        # SORTBY makes FT.SEARCH fetch content even with NOCONTENT, so the score
        # is still recomputed
        res = self._run_with_mutation_in_flight(
            coordinator, key,
            "FT.SEARCH", IDX_NAME, "@t:{red}", "LIMIT", "0", "100",
            "NOCONTENT", "WITHSCORES", "SORTBY", "t",
        )
        assert self._counter(coordinator, "search_predicate_revalidation") == 3
        pairs = _pairs(res, no_content=True)
        assert sorted(k for k, _ in pairs) == sorted(DOCS)
        scores4 = [s for _, s in pairs]
        assert sorted(scores4, reverse=True) == scores1

        # Without SORTBY nothing needs the key's content, so the reply is built on
        # the background thread with the score the search computed
        res = self._run_with_mutation_in_flight(
            coordinator, key,
            "FT.SEARCH", IDX_NAME, "@t:{red}", "LIMIT", "0", "100",
            "NOCONTENT", "WITHSCORES",
        )
        assert self._counter(coordinator, "search_predicate_revalidation") == 3
        pairs = _pairs(res, no_content=True)
        assert sorted(k for k, _ in pairs) == sorted(DOCS)
        scores5 = [s for _, s in pairs]
        assert scores5 == scores0
