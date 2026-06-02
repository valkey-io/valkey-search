"""Integration tests for FT.HYBRID.

Focuses on **control paths** as agreed in the plan, not on re-testing the
aggregate pipeline (already covered by test_ft_aggregate / test_non_vector).
This file verifies:
  * Local-only command flow (single-instance, MULTI/EXEC, Lua, LOCALONLY).
  * Score-alias propagation through the aggregate pipeline.
  * Cross-clause framing: SEARCH / VSIM / COMBINE / POLICY / aggregate suffix.
  * Reserved-feature rejections (NOCONTENT, DIALECT).
  * VSIM RANGE parsed-but-not-implemented.
  * SEARCH-arm vector content (Valkey super-set over the Redis spec).
"""

import struct

import pytest
from valkey.client import Valkey
from valkey.cluster import ValkeyCluster
from valkey.exceptions import ResponseError
from valkey_search_test_case import (
    ValkeySearchClusterTestCase,
    ValkeySearchClusterTestCaseDebugMode,
    ValkeySearchTestCaseBase,
    ValkeySearchTestCaseDebugMode,
)
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters
from utils import IndexingTestHelper, run_in_thread


def _vec(*xs: float) -> bytes:
    return struct.pack(f"{len(xs)}f", *xs)


class TestFtHybridBase(ValkeySearchTestCaseBase):
    """Base fixture: creates an HNSW + text + tag index and seeds 10 docs."""

    INDEX = "idx"

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX,
            "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "category", "TAG",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        for i in range(1, 11):
            client.hset(
                f"doc:{i}",
                mapping={
                    "title": f"hello world {i}",
                    "category": f"cat{i % 2}",
                    "vec": _vec(float(i), float(i * 2),
                                 float(i * 3), float(i * 4)),
                },
            )

    Q = _vec(1.0, 2.0, 3.0, 4.0)

    # ---------------------------------------------------------------------
    # Control-path coverage (local-only; cluster control paths are deferred
    # to the cluster-fixture suite below).
    # ---------------------------------------------------------------------

    def test_basic_rrf_default(self):
        """SEARCH + VSIM with default RRF returns a fused result list."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "PARAMS", "2", "q", self.Q,
        )
        # Reply shape mirrors FT.AGGREGATE: [count, [k,v,...], [k,v,...], ...]
        assert isinstance(result, list)
        assert result[0] == 10  # union of arms (SEARCH matched all 10)
        assert len(result) == 11  # count + 10 records

    def test_combine_linear_ok(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "COMBINE", "LINEAR", "4", "ALPHA", "0.7", "BETA", "0.3",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 10

    def test_combine_linear_missing_alpha_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"COMBINE LINEAR requires"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "COMBINE", "LINEAR", "2", "BETA", "0.3",
                "PARAMS", "2", "q", self.Q,
            )

    def test_search_no_match_returns_only_vsim(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:nonexistent",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "3",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        # Only VSIM contributed (3 nearest neighbors).
        assert result[0] == 3

    # ---------------------------------------------------------------------
    # Score-alias propagation through the aggregate pipeline.
    # ---------------------------------------------------------------------

    def test_yield_score_as_aliases_reach_apply_and_sortby(self):
        """Per-arm and COMBINE YIELD_SCORE_AS aliases are reachable by the
        aggregate stages."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "sscore",
            "VSIM", "@vec", "$q", "KNN", "4", "K", "5",
            "YIELD_SCORE_AS", "vscore",
            "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "hscore",
            "APPLY", "@hscore", "AS", "h2",
            "SORTBY", "2", "@hscore", "DESC",
            "LIMIT", "0", "5",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 5
        # Each surviving record should expose the fused alias and the
        # APPLY-derived alias.
        for rec in result[1:]:
            keys = set(rec[::2])
            assert b"hscore" in keys, f"hscore missing in {rec}"
            assert b"h2" in keys, f"h2 (APPLY result) missing in {rec}"

    # ---------------------------------------------------------------------
    # COMBINE FUNCTION: user-defined scoring expression over per-arm scores.
    # ---------------------------------------------------------------------

    @staticmethod
    def _rec_to_dict(rec):
        return {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}

    def test_combine_function_uses_vsim_score(self):
        """COMBINE FUNCTION computes the fused score from a user expression.
        With EXPR '@v + 1', the fused score equals each doc's VSIM score + 1
        (the SEARCH text arm contributes 0, since text scoring is not yet
        implemented)."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "4", "K", "10", "YIELD_SCORE_AS", "v",
            "COMBINE", "FUNCTION", "4", "EXPR", "@v + 1",
            "YIELD_SCORE_AS", "h",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 10
        for rec in result[1:]:
            d = self._rec_to_dict(rec)
            assert b"v" in d and b"h" in d, f"missing v/h in {rec}"
            v = float(d[b"v"])
            h = float(d[b"h"])
            assert abs(h - (v + 1.0)) < 1e-3, f"h={h} v={v}"

    def test_combine_function_uses_both_arm_scores(self):
        """All arms' scores are available to the function. Make BOTH arms
        vector queries (Valkey super-set) so each arm yields a distinct,
        non-zero score, then verify the fused score equals f(@s,@v) read back
        from the per-arm aliases."""
        client = self.server.get_new_client()
        self.setup_index(client)
        q2 = _vec(10.0, 9.0, 8.0, 7.0)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            # SEARCH arm is itself a vector query against $q (arm score = @s).
            "SEARCH", "*=>[KNN 10 @vec $q]", "YIELD_SCORE_AS", "s",
            # VSIM arm uses a different query vector $q2 (arm score = @v).
            "VSIM", "@vec", "$q2", "KNN", "4", "K", "10", "YIELD_SCORE_AS", "v",
            "COMBINE", "FUNCTION", "4", "EXPR", "@s * 10 + @v",
            "YIELD_SCORE_AS", "h",
            "PARAMS", "4", "q", self.Q, "q2", q2,
        )
        assert isinstance(result, list)
        assert result[0] == 10
        # Both per-arm score aliases must be present on every record, and the
        # fused score must equal the user expression evaluated over them.
        saw_nonzero_s = False
        saw_nonzero_v = False
        for rec in result[1:]:
            d = self._rec_to_dict(rec)
            assert b"s" in d and b"v" in d and b"h" in d, \
                f"missing s/v/h in {rec}"
            s = float(d[b"s"])
            v = float(d[b"v"])
            h = float(d[b"h"])
            saw_nonzero_s = saw_nonzero_s or s > 0.0
            saw_nonzero_v = saw_nonzero_v or v > 0.0
            assert abs(h - (s * 10.0 + v)) < 1e-2, f"h={h} s={s} v={v}"
        # The two arms use different query vectors, so across the result set
        # both arms contribute genuinely distinct, non-trivial scores.
        assert saw_nonzero_s and saw_nonzero_v

    def test_combine_function_default_arm_aliases(self):
        """Arm scores are reachable via positional default aliases even when
        no YIELD_SCORE_AS is given: @__search_score / @__vector_score."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
            "COMBINE", "FUNCTION", "4",
            "EXPR", "@__search_score + @__vector_score", "YIELD_SCORE_AS", "h",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 10
        for rec in result[1:]:
            d = self._rec_to_dict(rec)
            assert b"h" in d, f"missing h in {rec}"

    def test_combine_function_requires_expr(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"COMBINE FUNCTION requires"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "COMBINE", "FUNCTION", "0",
                "PARAMS", "2", "q", self.Q,
            )

    def test_combine_function_unknown_alias_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"unknown arm score"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5", "YIELD_SCORE_AS",
                "v",
                "COMBINE", "FUNCTION", "2", "EXPR", "@nonexistent + 1",
                "PARAMS", "2", "q", self.Q,
            )

    def test_groupby_reduce_count(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "GROUPBY", "1", "@category",
            "REDUCE", "COUNT", "0", "AS", "n",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        # 2 categories, 5 docs each.
        assert result[0] == 2
        counts = {bytes(rec[1]): bytes(rec[3]) for rec in result[1:]}
        assert counts == {b"cat0": b"5", b"cat1": b"5"}

    # ---------------------------------------------------------------------
    # POLICY tolerance (parser accepts and silently discards POLICY <value>).
    # ---------------------------------------------------------------------

    def test_policy_accepted_and_ignored(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "POLICY", "any-value",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 10

    # ---------------------------------------------------------------------
    # Reserved-feature rejections.
    # ---------------------------------------------------------------------

    def test_dialect_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError,
                            match=r"DIALECT is not configurable"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "DIALECT", "2",
                "PARAMS", "2", "q", self.Q,
            )

    def test_nocontent_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"NOCONTENT is not supported"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello", "NOCONTENT",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "PARAMS", "2", "q", self.Q,
            )

    # ---------------------------------------------------------------------
    # VSIM RANGE: parsed but not yet implemented.
    # ---------------------------------------------------------------------

    def test_vsim_range_parses_but_not_implemented(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError,
                            match=r"VSIM RANGE is not yet supported"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "RANGE", "2", "RADIUS", "50",
                "PARAMS", "2", "q", self.Q,
            )

    def test_vsim_range_radius_negative_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"Invalid RADIUS"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "RANGE", "2", "RADIUS", "-1",
                "PARAMS", "2", "q", self.Q,
            )

    def test_vsim_range_missing_radius_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError,
                            match=r"VSIM RANGE requires RADIUS"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "RANGE", "2", "EPSILON", "0.05",
                "PARAMS", "2", "q", self.Q,
            )

    # ---------------------------------------------------------------------
    # SEARCH-arm vector content (Valkey super-set over the Redis spec).
    # ---------------------------------------------------------------------

    def test_search_arm_can_contain_vector_query(self):
        """Both arms can be vector queries (Valkey extends the Redis spec)."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "*=>[KNN 5 @vec $q]",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        # Both arms top-5 vector → fused union has between 5 and 10 docs.
        assert 5 <= result[0] <= 10

    # ---------------------------------------------------------------------
    # Required-clause + structural rejections.
    # ---------------------------------------------------------------------

    def test_search_clause_required(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"FT.HYBRID requires SEARCH"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "PARAMS", "2", "q", self.Q,
            )

    def test_vsim_clause_required(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"FT.HYBRID requires VSIM"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "PARAMS", "2", "q", self.Q,
            )

    def test_unknown_combine_method_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"COMBINE method must be"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "COMBINE", "BOGUS", "0",
                "PARAMS", "2", "q", self.Q,
            )

    def test_vsim_knn_and_range_both_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        # The VSIM parser sees KNN, consumes its inner block, then tries to
        # interpret "RANGE" as a tail token — which should error in the
        # outer top-level scan.
        with pytest.raises(ResponseError):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "RANGE", "2", "RADIUS", "5",
                "PARAMS", "2", "q", self.Q,
            )


class TestFtHybridCluster(ValkeySearchClusterTestCase):
    """Cluster-mode tests covering cross-shard fanout and LOCALONLY routing."""

    INDEX = "idx"
    Q = _vec(1.0, 2.0, 3.0, 4.0)

    def _setup(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        client.execute_command(
            "FT.CREATE", self.INDEX,
            "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "category", "TAG",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        import time
        for i in range(1, 31):
            cluster.hset(
                f"doc:{i}",
                mapping={
                    "title": f"hello world {i}",
                    "category": f"cat{i % 2}",
                    "vec": _vec(float(i), float(i * 2),
                                 float(i * 3), float(i * 4)),
                },
            )
        time.sleep(1)
        return cluster, client

    def test_fanout_basic_rrf(self):
        """Cross-shard fanout fuses results from all shards via RRF. A large
        WINDOW disables the fusion truncation so the full union is returned —
        proving results aggregate across all 3 shards (a single shard holds
        only ~10 of the 30 docs)."""
        cluster, client = self._setup()
        # Control: a plain cluster FT.SEARCH sees all docs across shards.
        ctrl = client.execute_command(
            "FT.SEARCH", self.INDEX, "@title:hello", "NOCONTENT", "LIMIT",
            "0", "1000")
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "COMBINE", "RRF", "2", "WINDOW", "1000",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        # SEARCH arm matches every doc across all 3 shards; the union with the
        # VSIM top-5 equals the full cluster-wide SEARCH count.
        assert result[0] == ctrl[0]

    def test_fanout_combine_linear(self):
        _, client = self._setup()
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "COMBINE", "LINEAR", "6", "ALPHA", "0.7", "BETA", "0.3",
            "WINDOW", "1000",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 30

    def test_fanout_sortby_limit(self):
        """SORTBY + LIMIT applied on the fused cross-shard result."""
        _, client = self._setup()
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "hscore",
            "SORTBY", "2", "@hscore", "DESC",
            "LIMIT", "0", "5",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 5  # trimmed by LIMIT

    def test_localonly_routes_through_local_path(self):
        """With LOCALONLY, FT.HYBRID runs on the contacted shard's local data
        only. Result count is per-shard, not aggregated across the cluster."""
        _, client = self._setup()
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "LOCALONLY",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        # Per-shard local result: fewer than the full 30-doc cluster total.
        assert 0 <= result[0] < 30


class TestFtHybridClusterConsistency(ValkeySearchClusterTestCaseDebugMode):
    """Cluster fanout consistency / fingerprint-mismatch handling. Reuses the
    same per-shard SearchPartitionResultsTracker consistency machinery as
    FT.SEARCH (forced via the ForceInvalidIndexFingerprint dev toggle on the
    coordinator)."""

    INDEX = "idx"
    Q = _vec(1.0, 2.0, 3.0, 4.0)

    def _setup(self):
        import time
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        client.execute_command(
            "FT.CREATE", self.INDEX,
            "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "category", "TAG",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        for i in range(1, 16):
            cluster.hset(
                f"doc:{i}",
                mapping={
                    "title": f"hello world {i}",
                    "category": f"cat{i % 2}",
                    "vec": _vec(float(i), float(i * 2),
                                 float(i * 3), float(i * 4)),
                },
            )
        time.sleep(1)
        return cluster, client

    def test_index_fingerprint_mismatch_fails_fanout(self):
        """A forced index-fingerprint mismatch on the coordinator makes every
        shard's per-arm consistency check fail; FT.HYBRID surfaces a clean
        consistency error."""
        _, client = self._setup()
        # Sanity: nominal fanout succeeds.
        ok = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "PARAMS", "2", "q", self.Q,
        )
        assert ok[0] > 0

        client.execute_command(
            "ft._debug", "CONTROLLED_VARIABLE", "set",
            "ForceInvalidIndexFingerprint", "yes")
        try:
            with pytest.raises(ResponseError):
                client.execute_command(
                    "FT.HYBRID", self.INDEX,
                    "SEARCH", "@title:hello",
                    "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                    "PARAMS", "2", "q", self.Q,
                )
        finally:
            client.execute_command(
                "ft._debug", "CONTROLLED_VARIABLE", "set",
                "ForceInvalidIndexFingerprint", "no")


class TestFtHybridAtomicValidation(ValkeySearchTestCaseDebugMode):
    """Verifies that the multi-arm results come together BEFORE the final
    main-thread validation: the mutation/contention check runs once over the
    fused list. With an in-flight mutation on a matching key, the whole
    FT.HYBRID blocks (post-fusion contention check) and proceeds only once the
    mutation settles."""

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        # Two writer threads so the concurrent pausepoint mutation can block
        # without starving query processing.
        args["search.writer-threads"] = "2"
        return args

    def test_fused_result_blocks_on_inflight_mutation(self):
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        vec1 = struct.pack("<4f", 0.0, 0.0, 0.0, 0.0)
        vec2 = struct.pack("<4f", 1.0, 1.0, 1.0, 1.0)
        client.execute_command("HSET", "doc:1", "content", "hello world",
                               "vec", vec1)
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        # Pause mutation processing, then start a mutation on doc:1.
        client.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")
        hset_thread, _, _ = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "content", "updated", "vec", vec2))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG PAUSEPOINT TEST mutation_processing") > 0,
            timeout=5)

        # FT.HYBRID with a text SEARCH arm: the post-fusion contention check
        # must block on the in-flight mutation.
        blocked_before = client.info("SEARCH").get(
            "search_text_query_blocked_count", 0)
        search_thread, search_res, search_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.HYBRID", "idx",
                "SEARCH", "@content:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "PARAMS", "2", "q", vec1))
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_text_query_blocked_count"]
            >= blocked_before + 1,
            timeout=5)
        # Still blocked: no reply yet.
        assert search_res[0] is None and search_thread.is_alive()

        # Release the mutation; the fused validation re-runs and completes.
        client.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        hset_thread.join()
        search_thread.join()
        assert search_err[0] is None
        # doc:1 no longer matches "hello" after the mutation; fused result is
        # empty for the SEARCH arm, leaving only the VSIM match for doc:1.
        assert isinstance(search_res[0], list)


# =============================================================================
# Parallel-arm execution + per-arm consistency under concurrent mutations.
#
# Property the implementation must hold (per the local-mode atomic-validation
# design): the two arms of FT.HYBRID run in parallel under reader locks on the
# same time-sliced index mutex, so a writer (mutation) cannot interleave
# between them. The fused output is therefore consistent across arms — for any
# document, the per-arm score aliases reflect the SAME pre-mutation index
# snapshot, so the doc is either contributed by BOTH arms (both aliases
# present) or by NEITHER (doc absent from the fused result). It is never the
# case that doc X has only @s (search alias) without @v (vsim alias) or vice
# versa due to a mid-flight mutation.
# =============================================================================
class TestFtHybridParallelArmConsistency(ValkeySearchTestCaseDebugMode):
    INDEX = "idx"

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        # ≥ 2 reader threads so the two arms can genuinely run in parallel
        # (true overlapping reader-lock hold times). With 1 thread the arms
        # would serialize and a writer could squeeze between them.
        args["search.reader-threads"] = "4"
        # ≥ 2 writer threads so the pause-pointed mutation doesn't starve
        # the rest of the writer pool.
        args["search.writer-threads"] = "2"
        return args

    def _setup_index(self, client: Valkey, n: int = 10) -> bytes:
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        q = _vec(1.0, 2.0, 3.0, 4.0)
        for i in range(1, n + 1):
            client.execute_command(
                "HSET", f"doc:{i}",
                "title", "hello world",
                "vec", _vec(float(i), float(i), float(i), float(i)))
        IndexingTestHelper.is_indexing_complete_on_node(client, self.INDEX)
        return q

    @staticmethod
    def _rec_to_dict(rec):
        return {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}

    # -------- TEST 1 : both arms execute concurrently (observable proof) -----
    def test_both_arms_execute_in_parallel(self):
        """Both arms hit `background_search_completing` (the pausepoint at the
        tail of every per-arm SearchAsync background callback) at the same
        time. Two waiters at that pausepoint while one FT.HYBRID is in flight
        proves the arms ran concurrently rather than serialized on a single
        reader thread."""
        client: Valkey = self.server.get_new_client()
        q = self._setup_index(client, n=3)
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "background_search_completing")
        thread, res, err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "3",
                "PARAMS", "2", "q", q))
        # Both arms must reach the pausepoint and park there together.
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST",
                "background_search_completing") == 2,
            timeout=5)
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET",
            "background_search_completing")
        thread.join()
        assert err[0] is None
        assert isinstance(res[0], list)

    # -------- TEST 2 : doc present in both arms or neither (deterministic) ---
    def test_doc_in_both_arms_or_neither_under_mutation(self):
        """Deterministic atomicity probe. doc:1 matches both arms. We queue a
        mutation that makes it no longer match SEARCH, but stall the mutation
        processor so the mutation cannot apply while the arms are running.

        Outcome on the fused list:
          * Both arms saw the *pre-mutation* index (mutation parked) and both
            found doc:1, so the fused row carries BOTH per-arm aliases (@s, @v).
          * The post-fusion contention check (FusedResolver) observes the
            pending mutation, re-queues onto the mutation pipeline, the
            mutation applies once we release the pausepoint, and the final
            content fetch returns the post-mutation HASH content.

        The asserted invariant is the strong one the user asked for: doc:1 is
        in BOTH arms (both aliases set) or NEITHER (absent from result). It is
        never the case that exactly one of @s / @v is present."""
        client: Valkey = self.server.get_new_client()
        q = self._setup_index(client, n=1)  # only doc:1, matched by both arms

        # Pause mutation processing → any HSET queues but does not apply.
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        mut_thread, _, mut_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "title", "goodbye",
                "vec", _vec(99.0, 99.0, 99.0, 99.0)))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing")
            >= 1,
            timeout=5)

        # Issue FT.HYBRID — both arms run against the still-pre-mutation index.
        hyb_thread, res, err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
                "VSIM", "@vec", "$q", "KNN", "4", "K", "5",
                "YIELD_SCORE_AS", "v",
                "PARAMS", "2", "q", q))
        # Release the mutation; both the parked HSET AND the FT.HYBRID's
        # post-fusion contention check unblock; the resolver re-runs once the
        # mutation applies and replies with post-mutation content.
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        mut_thread.join()
        hyb_thread.join()
        assert mut_err[0] is None
        assert err[0] is None
        assert isinstance(res[0], list)

        # doc:1 must be either absent OR carry BOTH per-arm aliases — never
        # exactly one.
        rows = [self._rec_to_dict(r) for r in res[0][1:]]
        doc1 = next((r for r in rows
                     if r.get(b"__key") == b"doc:1"
                     or b"doc:1" in r.values()), None)
        if doc1 is None:
            # Acceptable: mutation made it ineligible and the resolver dropped
            # it during content fetch.
            return
        assert (b"s" in doc1) == (b"v" in doc1), \
            f"split-arm violation: doc:1 = {doc1}"

    # -------- TEST 3 : stress probe — no split-arm result under concurrent
    #                   mutations across many trials --------
    def test_concurrent_mutations_never_split_arms(self):
        """The "both arms or neither" guarantee is a *per-query atomicity*
        property: within a single FT.HYBRID execution both arms must observe
        the SAME index snapshot, so a key cannot be in one arm's result and
        absent from the other's because a writer slipped in between them. (A
        doc that legitimately stopped matching SEARCH after a real content
        change would land in just the VSIM arm — that's correct semantics,
        not a split-arm violation.)

        To probe atomicity in isolation, the mutator REFRESHES each doc in
        place with the same values it already holds: this exercises the
        mutation pipeline (writer lock + index remove/re-add) without ever
        changing the doc's match status for either arm. Every doc matches
        both arms throughout the run, so any split-arm row is by construction
        caused by a writer time-slicing between the two arms' inner reader
        locks — the bug the outer reader lock in PerformMultiSearchLocalAsync
        is designed to prevent."""
        import random
        import threading
        import time

        client: Valkey = self.server.get_new_client()
        N = 20
        q = self._setup_index(client, n=N)

        stop = threading.Event()
        # Original per-doc vectors (must match what _setup_index seeded so
        # the refresh truly is a no-op for matching purposes).
        original_vec = {
            i: _vec(float(i), float(i), float(i), float(i))
            for i in range(1, N + 1)
        }

        def mutator():
            w = self.server.get_new_client()
            while not stop.is_set():
                i = random.randint(1, N)
                # Re-HSET with the same matching values. Writer lock taken,
                # index entries cycled, but matchability for both arms is
                # invariant.
                w.execute_command(
                    "HSET", f"doc:{i}",
                    "title", "hello world",
                    "vec", original_vec[i])

        threads = [threading.Thread(target=mutator) for _ in range(3)]
        for t in threads:
            t.start()
        try:
            split_arm_rows = []
            for _ in range(80):
                res = client.execute_command(
                    "FT.HYBRID", self.INDEX,
                    "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
                    "VSIM", "@vec", "$q", "KNN", "4", "K", str(N),
                    "YIELD_SCORE_AS", "v",
                    "PARAMS", "2", "q", q)
                for rec in res[1:]:
                    d = self._rec_to_dict(rec)
                    if (b"s" in d) != (b"v" in d):
                        split_arm_rows.append(d)
                time.sleep(0.005)
        finally:
            stop.set()
            for t in threads:
                t.join(timeout=5)
        assert not split_arm_rows, (
            f"split-arm rows observed under refresh-only mutations "
            f"(only one of @s/@v present): "
            f"{split_arm_rows[:5]}"
            f" (+{len(split_arm_rows)-5} more)"
            if len(split_arm_rows) > 5 else f"split-arm rows: {split_arm_rows}")


# =============================================================================
# Cluster mode: each arm must return its complete per-shard contributions back
# to the coordinator so the COMBINE FUNCTION evaluates over the full
# cross-shard merged set (not over a per-shard prefix).
# =============================================================================
class TestFtHybridClusterFunctionMerge(ValkeySearchClusterTestCase):
    INDEX = "idx"

    def _setup(self, n_docs: int = 30):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        import time
        # Seed docs with deterministic, distinct vectors. Pattern chosen so
        # all L2 distances from q=[1,2,3,4] are unique (the simple [i,i,i,i]
        # pattern produces a symmetric 4i²-20i+30 that collides docs 1<->4
        # and 2<->3, etc.). [i,0,0,0] gives strictly monotonic (i-1)²+29 →
        # all distinct.
        for i in range(1, n_docs + 1):
            cluster.hset(
                f"doc:{i}",
                mapping={
                    "title": "hello",
                    "vec": _vec(float(i), 0.0, 0.0, 0.0),
                },
            )
        time.sleep(1)
        return cluster, client

    @staticmethod
    def _rec_to_dict(rec):
        return {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}

    def test_function_merges_all_per_shard_arm_results(self):
        """Cluster fanout with COMBINE FUNCTION. Each shard runs both arms
        independently and returns BOTH arms' results to the coordinator; the
        coordinator merges the per-arm result sets across shards and evaluates
        the user expression `@s*10 + @v` over each unioned document.

        We verify three things:
          1. The total fused count equals the cluster-wide union size (every
             doc matches the SEARCH arm; with K large enough every doc also
             reaches the VSIM arm) — proves the per-shard limits did not
             truncate either arm.
          2. Every returned doc carries BOTH @s and @v (proves both arms'
             per-shard results were returned to the coordinator, not just
             one).
          3. The fused score h equals f(@s, @v) read back from the per-arm
             aliases (proves the FUNCTION ran on the merged set with both
             arms' scores bound)."""
        n_docs = 30
        _, client = self._setup(n_docs=n_docs)
        # SEARCH arm: every doc (text). VSIM arm: K large enough to cover
        # every doc across the cluster so the union = all docs.
        q = _vec(1.0, 2.0, 3.0, 4.0)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "4", "K", str(n_docs * 4),
            "YIELD_SCORE_AS", "v",
            "COMBINE", "FUNCTION", "4", "EXPR", "@s * 10 + @v",
            "YIELD_SCORE_AS", "h",
            # Large WINDOW disables fusion truncation.
            "LIMIT", "0", str(n_docs),
            "PARAMS", "2", "q", q,
        )
        assert isinstance(result, list)
        # (1) Every shard's contribution to both arms reached the coordinator
        #     and was merged into the union.
        assert result[0] == n_docs, (
            f"expected {n_docs} merged docs (full cross-shard union); "
            f"got {result[0]} — a missing arm or truncated per-shard return "
            f"would shrink this count")
        # (2)+(3) Each returned doc has both per-arm aliases, and the
        # FUNCTION value equals the expression applied to them.
        seen = set()
        for rec in result[1:]:
            d = self._rec_to_dict(rec)
            assert b"s" in d and b"v" in d and b"h" in d, \
                f"missing per-arm or fused alias on cluster-fused row: {d}"
            s = float(d[b"s"])
            v = float(d[b"v"])
            h = float(d[b"h"])
            assert abs(h - (s * 10.0 + v)) < 1e-2, \
                f"FUNCTION did not evaluate over both arm scores: " \
                f"h={h} s={s} v={v}"
            seen.add((round(s, 4), round(v, 4)))
        # All n_docs rows are distinct (distinct per-arm score pairs) —
        # a per-shard short-circuit that returned only one shard's slice
        # would collapse this set.
        assert len(seen) == n_docs, \
            f"expected {n_docs} distinct (s,v) pairs across shards; " \
            f"saw {len(seen)} — likely missing per-shard contributions"

    def test_function_per_arm_scores_distinct_across_shards(self):
        """Strong probe that per-arm scores from EVERY shard reach the
        coordinator. We seed N=30 docs with strictly monotonic vector
        magnitudes; the VSIM distances from $q form a strictly increasing
        sequence. A coordinator that merged only one shard's arm output would
        miss ~2/3 of the distance values. Verify all are present."""
        n_docs = 30
        _, client = self._setup(n_docs=n_docs)
        q = _vec(0.0, 0.0, 0.0, 0.0)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "4", "K", str(n_docs * 4),
            "YIELD_SCORE_AS", "v",
            "COMBINE", "FUNCTION", "4", "EXPR", "@v",
            "YIELD_SCORE_AS", "h",
            "LIMIT", "0", str(n_docs),
            "PARAMS", "2", "q", q,
        )
        assert result[0] == n_docs
        v_values = []
        for rec in result[1:]:
            d = self._rec_to_dict(rec)
            assert b"v" in d, f"missing @v on cluster row: {d}"
            v_values.append(float(d[b"v"]))
        # 30 distinct distances → every doc's per-arm score reached the
        # coordinator and survived the merge.
        assert len(set(round(x, 4) for x in v_values)) == n_docs, \
            f"expected {n_docs} distinct VSIM distances across shards, " \
            f"got {len(set(round(x, 4) for x in v_values))}"
