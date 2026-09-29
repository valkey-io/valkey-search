"""Integration tests for FT.HYBRID.

Focuses on **control paths** as agreed in the plan, not on re-testing the
aggregate pipeline (already covered by test_ft_aggregate / test_non_vector).
This file verifies:
  * Local-only command flow (single-instance, LOCALONLY) and the refusal
    of the synchronous path (MULTI/EXEC, Lua).
  * Score-alias propagation through the aggregate pipeline.
  * Cross-clause framing: SEARCH / VSIM / COMBINE / POLICY / aggregate suffix.
  * Reserved-feature rejections (NOCONTENT, a DIALECT other than 2).
  * VSIM RANGE parsed-but-not-implemented.
  * SEARCH-arm vector content, which belongs to VSIM and is refused.
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

# The exact refusal FT.HYBRID returns when dispatch would take the synchronous
# local path. Mirrors query::kSyncLocalUnsupportedMsg in src/query/multi_search.h.
SYNC_LOCAL_NOT_SUPPORTED_ERR = (
    "FT.HYBRID is not supported inside MULTI/EXEC or a Lua script, or when "
    "the reader thread pool is disabled. Both force synchronous execution, on "
    "which the per-arm results cannot be revalidated against concurrent "
    "mutations before they are fused.")


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
        # Indexing is asynchronous; wait for all 10 docs to be searchable before
        # tests assert exact counts, otherwise they race the background indexer.
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == 10,
            timeout=10)

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
        with pytest.raises(ResponseError, match=r"missing value for ALPHA"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "COMBINE", "LINEAR", "2", "BETA", "0.3",
                "PARAMS", "2", "q", self.Q,
            )

    def test_combine_linear_with_no_weights_uses_the_defaults(self):
        """Neither weight written is not the same as one written: it takes
        ALPHA 0.3 / BETA 0.7, which is what the reference does."""
        client = self.server.get_new_client()
        self.setup_index(client)
        defaulted = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "COMBINE", "LINEAR", "0",
            "PARAMS", "2", "q", self.Q,
        )
        explicit = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "COMBINE", "LINEAR", "4", "ALPHA", "0.3", "BETA", "0.7",
            "PARAMS", "2", "q", self.Q,
        )
        assert defaulted == explicit
        assert defaulted[0] == 10

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
        """The COMBINE YIELD_SCORE_AS alias is reachable by the aggregate
        stages. The per-arm aliases are named here too, but only so that
        naming them does not disturb the fused one: a per-arm alias is not
        itself resolvable in a stage (see
        test_sortby_per_arm_score_alias_is_rejected)."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "sscore",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
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
    # The synchronous dispatch path, which MULTI/EXEC and a server without
    # parallel queries both take. FT.HYBRID has no correct implementation of
    # it -- nothing there revalidates a fused arm against a queued mutation --
    # so what it has to prove now is that it is refused, not that it answers.
    # ---------------------------------------------------------------------

    def _wide_index(self, client: Valkey, n: int = 40) -> bytes:
        """More documents than any per-arm default cap, all matching the text
        arm, so an arm that is quietly capped comes back short."""
        client.execute_command(
            "FT.CREATE", "wide", "ON", "HASH", "PREFIX", "1", "w:",
            "SCHEMA", "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        for i in range(1, n + 1):
            client.hset(f"w:{i:03d}", mapping={
                "title": "hello world",
                "vec": _vec(float(i), 0.0, 0.0, 0.0)})
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", "wide", "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == n,
            timeout=10)
        return _vec(0.0, 0.0, 0.0, 0.0)

    @staticmethod
    def _wide_query(q: bytes, n: int):
        return ["FT.HYBRID", "wide",
                "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
                "VSIM", "@vec", "$q", "KNN", "2", "K", str(n),
                "YIELD_SCORE_AS", "v",
                "COMBINE", "RRF", "4", "WINDOW", "100",
                "YIELD_SCORE_AS", "h",
                "LIMIT", "0", "100",
                "PARAMS", "2", "q", q]

    def test_multi_exec_is_refused_and_the_direct_reply_is_not(self):
        """The same query answers outside a transaction and is refused inside
        one. Was: the two replies had to match; the synchronous path that
        produced the transaction's reply is gone, so the only thing left to
        compare is answered-vs-refused."""
        client = self.server.get_new_client()
        n = 40
        q = self._wide_index(client, n)
        cmd = self._wide_query(q, n)

        direct = client.execute_command(*cmd)
        assert direct[0] == n, f"direct returned {direct[0]} of {n}"

        assert client.execute_command("MULTI") == b"OK"
        assert client.execute_command(*cmd) == b"QUEUED"
        queued = client.execute_command("EXEC")
        assert isinstance(queued[0], ResponseError), queued[0]
        assert SYNC_LOCAL_NOT_SUPPORTED_ERR in str(queued[0]), queued[0]

        # The refusal leaves the connection usable, and the command still
        # answers on it.
        assert client.ping()
        assert client.execute_command(*cmd)[0] == n

    def test_multi_exec_is_refused_rather_than_crashing(self):
        """A transaction must get an error back, not a closed connection.
        Was: the arms on the synchronous path had to carry a real cancellation
        token or the server died reading it. The path is refused now, so the
        crash is out of reach -- the surviving connection is what proves it."""
        client = self.server.get_new_client()
        q = self._wide_index(client, 12)
        assert client.execute_command("MULTI") == b"OK"
        assert client.execute_command(
            *(self._wide_query(q, 12) + ["TIMEOUT", "10000"])) == b"QUEUED"
        result = client.execute_command("EXEC")
        assert client.ping()
        assert isinstance(result[0], ResponseError), result[0]
        assert SYNC_LOCAL_NOT_SUPPORTED_ERR in str(result[0]), result[0]

    def test_lua_is_refused(self):
        """Lua shares the MULTI/EXEC trigger (vmsdk::MultiOrLua), so it must
        get the same refusal."""
        client = self.server.get_new_client()
        q = self._wide_index(client, 8)
        cmd = self._wide_query(q, 8)
        with pytest.raises(ResponseError) as err:
            client.eval("return redis.call(unpack(ARGV))", 0, *cmd)
        assert SYNC_LOCAL_NOT_SUPPORTED_ERR in str(err.value), err.value

    # ---------------------------------------------------------------------
    # SORTBY over the fused record. Order is asserted here and only here:
    # the compatibility suite re-sorts both replies on a derived key before
    # comparing them, so it pins which rows and values come back, never the
    # sequence the engine emitted them in.
    # ---------------------------------------------------------------------

    def _fused_rows(self, client, *tail):
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "sscore",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
            "YIELD_SCORE_AS", "vscore",
            "COMBINE", "RRF", "4", "WINDOW", "20", "YIELD_SCORE_AS", "hscore",
            *tail,
            "LIMIT", "0", "10",
            "PARAMS", "2", "q", self.Q,
        )
        return [self._rec_to_dict(rec) for rec in result[1:]]

    def test_sortby_fused_score_orders_the_reply(self):
        """Ascending and descending both order the rows, and reversing one
        gives the other's scores."""
        client = self.server.get_new_client()
        self.setup_index(client)

        descending = [float(r[b"hscore"])
                      for r in self._fused_rows(
                          client, "SORTBY", "2", "@hscore", "DESC")]
        ascending = [float(r[b"hscore"])
                     for r in self._fused_rows(
                         client, "SORTBY", "2", "@hscore", "ASC")]

        assert len(descending) == 10
        assert descending == sorted(descending, reverse=True)
        assert ascending == sorted(ascending)
        # Ties do not break this: it compares the score sequence, not the keys.
        assert ascending == list(reversed(descending))

    def test_sortby_document_key_orders_the_reply(self):
        """The document key is sortable even though it is not a schema
        field."""
        client = self.server.get_new_client()
        self.setup_index(client)

        ascending = [r[b"__key"] for r in self._fused_rows(
            client, "SORTBY", "2", "@__key", "ASC")]
        descending = [r[b"__key"] for r in self._fused_rows(
            client, "SORTBY", "2", "@__key", "DESC")]

        assert len(ascending) == 10
        assert ascending == sorted(ascending)
        assert descending == list(reversed(ascending))

    def test_sortby_applied_column_orders_the_reply(self):
        """A column APPLY derived from the fused score sorts like the score it
        was derived from, which is what makes a stage's output usable by the
        stage after it."""
        client = self.server.get_new_client()
        self.setup_index(client)

        rows = self._fused_rows(
            client,
            "APPLY", "@hscore * 1000", "AS", "scaled",
            "SORTBY", "2", "@scaled", "DESC")
        scaled = [float(r[b"scaled"]) for r in rows]
        assert len(scaled) == 10
        assert scaled == sorted(scaled, reverse=True)
        # And it really is the fused score, scaled.
        for row in rows:
            assert abs(float(row[b"scaled"]) -
                       float(row[b"hscore"]) * 1000) < 1e-6

    def test_sortby_per_arm_score_orders_by_that_arm(self):
        """Sorting by an arm's own alias orders by that arm's score, not by
        the fused one.

        Asserting the order rather than merely that the command is accepted:
        the column is filled by parsing the text fusion wrote, and a column
        typed as a string would sort 0.9 above 0.53 while still looking like
        a successful sort.
        """
        client = self.server.get_new_client()
        self.setup_index(client)
        for alias in ("vscore", "sscore"):
            for direction, reverse in (("DESC", True), ("ASC", False)):
                rows = self._fused_rows(client, "SORTBY", "2", f"@{alias}",
                                        direction)
                got = [float(r[alias.encode()]) for r in rows
                       if alias.encode() in r]
                assert got == sorted(got, reverse=reverse), \
                    f"@{alias} {direction} came back as {got}"

    def test_sortby_per_arm_score_puts_rows_without_it_last(self):
        """A document only one arm found has no alias for the other arm. Those
        rows sort after every row that has a value, ascending and descending
        alike, rather than tying with them.

        This is the half of the behaviour that is easy to get wrong and hard
        to see: a tie leaves the row wherever it already was, so the reply
        still looks sorted until you notice the gaps."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for alias in ("vscore", "sscore"):
            for direction in ("DESC", "ASC"):
                rows = self._fused_rows(client, "SORTBY", "2", f"@{alias}",
                                        direction)
                present = [alias.encode() in r for r in rows]
                assert present == sorted(present, reverse=True), \
                    (f"@{alias} {direction}: rows carrying the alias are "
                     f"{present}, which interleaves them with rows that do "
                     f"not carry it")
                assert any(present), f"no row carried @{alias}"

    def test_per_arm_score_alias_is_not_reachable_under_load_all(self):
        """`LOAD *` projects the document's own fields, and a fused score is
        not one of them. The reference refuses the reference there, so this
        one stays refused."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for alias in ("vscore", "sscore"):
            with pytest.raises(ResponseError,
                               match=rf"Index field `{alias}` does not exist"):
                self._fused_rows(client, "LOAD", "*",
                                 "SORTBY", "2", f"@{alias}", "DESC")

    def test_groupby_per_arm_score(self):
        """Grouping by an arm's alias groups by that arm's score."""
        client = self.server.get_new_client()
        self.setup_index(client)
        rows = self._fused_rows(client, "GROUPBY", "1", "@vscore",
                                "REDUCE", "COUNT", "0", "AS", "cnt")
        assert rows, "grouping by a per-arm alias returned nothing"
        for r in rows:
            assert b"cnt" in r, r

    def test_arm_alias_naming_the_fused_score_is_rejected(self):
        """An arm naming the fused score's column would write its own score
        into it, and the caller could not tell which they were reading."""
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"collides with the fused"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello", "YIELD_SCORE_AS", "hscore",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
                "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "hscore",
                "LIMIT", "0", "3", "PARAMS", "2", "q", self.Q)
        # The default name is the same question without a COMBINE alias.
        with pytest.raises(ResponseError, match=r"collides with the fused"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello", "YIELD_SCORE_AS", "__score",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
                "LIMIT", "0", "3", "PARAMS", "2", "q", self.Q)

    # ---------------------------------------------------------------------
    # COMBINE FUNCTION: user-defined scoring expression over per-arm scores.
    # ---------------------------------------------------------------------

    @staticmethod
    def _rec_to_dict(rec):
        return {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}

    def test_combine_function_uses_vsim_score(self):
        """COMBINE FUNCTION computes the fused score from a user expression.
        With EXPR '@v + 1', the fused score equals each doc's VSIM score + 1 --
        the text arm's score is not referenced by the expression."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10", "YIELD_SCORE_AS", "v",
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
        """All arms' scores are available to the function: verify the fused
        score equals f(@s,@v) read back from the per-arm aliases, with @s the
        text arm's relevance score and @v the vector arm's distance.

        This used to make both arms vector queries, which the SEARCH clause no
        longer accepts -- the vector arm is VSIM's. What it is really about,
        that the function sees *every* arm's score rather than only the vector
        one (that is test_combine_function_uses_vsim_score), a text SEARCH arm
        tests just as well."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            # Text arm (arm score = @s).
            "SEARCH", "@title:hello", "SCORER", "BM25STD",
            "YIELD_SCORE_AS", "s",
            # Vector arm (arm score = @v).
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10", "YIELD_SCORE_AS", "v",
            "COMBINE", "FUNCTION", "4", "EXPR", "@s * 10 + @v",
            "YIELD_SCORE_AS", "h",
            "PARAMS", "2", "q", self.Q,
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
        # Both arms must contribute a non-trivial score. Without the @s half
        # the arithmetic proves nothing about the text arm: if BM25 returned 0
        # for every row, h == v would still satisfy the check above, and the
        # test would pass while the function never saw the SEARCH arm at all.
        assert saw_nonzero_s, "the text arm contributed 0 to every row"
        assert saw_nonzero_v, "the vector arm contributed 0 to every row"

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
        # POLICY sits inside the VSIM clause, so the token after it still
        # belongs to VSIM. This used to end the clause and leave
        # YIELD_SCORE_AS to be read as a top-level keyword.
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "POLICY", "BATCHES", "YIELD_SCORE_AS", "vs",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 10
        # The 5 docs the vector arm returned carry the alias; the rest of the
        # union came from the text arm alone and have no vector score.
        assert sum(b"vs" in self._rec_to_dict(rec) for rec in result[1:]) == 5

    # ---------------------------------------------------------------------
    # Reserved-feature rejections.
    # ---------------------------------------------------------------------

    def test_dialect_two_accepted_others_rejected(self):
        """DIALECT is range-checked as FT.SEARCH and FT.AGGREGATE check it,
        except that FT.HYBRID implements the one dialect."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "DIALECT", "2",
            "PARAMS", "2", "q", self.Q,
        )
        assert isinstance(result, list)
        assert result[0] == 10
        for dialect in ["1", "3"]:
            with pytest.raises(ResponseError,
                               match=r"DIALECT requires a non negative "
                                     r"integer >=2 and <= 2"):
                client.execute_command(
                    "FT.HYBRID", self.INDEX,
                    "SEARCH", "@title:hello",
                    "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                    "DIALECT", dialect,
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
    # VSIM clause shape. What the parse settled on is unit-tested in
    # testing/ft_hybrid_parser_test.cc; these cover what it does to a reply.
    # ---------------------------------------------------------------------

    def test_k_bounds_the_vector_arm_only(self):
        """K caps how many documents the VSIM arm contributes. It says nothing
        about the SEARCH arm, which keeps returning everything it matches."""
        client = self.server.get_new_client()
        self.setup_index(client)
        # `@title:hello` matches all 10 documents, so any cap the vector arm's
        # K imposed on it would be visible as a shrinking `s` count.
        for k, expected_vector_arm in [(1, 1), (3, 3), (10, 10)]:
            result = client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
                "VSIM", "@vec", "$q", "KNN", "2", "K", str(k),
                "YIELD_SCORE_AS", "v",
                "COMBINE", "RRF", "4", "WINDOW", "100", "YIELD_SCORE_AS", "h",
                "LIMIT", "0", "100",
                "PARAMS", "2", "q", self.Q,
            )
            rows = [self._rec_to_dict(rec) for rec in result[1:]]
            text_arm = sum(1 for d in rows if b"s" in d)
            vector_arm = sum(1 for d in rows if b"v" in d)
            assert text_arm == 10, \
                f"K={k} narrowed the text arm to {text_arm}"
            assert vector_arm == expected_vector_arm, \
                f"K={k} gave {vector_arm} vector-arm rows"

    def test_knn_block_may_be_omitted(self):
        """With no KNN block the arm runs with the default K of 10, which is
        every document here."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "YIELD_SCORE_AS", "v",
            "COMBINE", "RRF", "4", "WINDOW", "100", "YIELD_SCORE_AS", "h",
            "LIMIT", "0", "100",
            "PARAMS", "2", "q", self.Q,
        )
        rows = [self._rec_to_dict(rec) for rec in result[1:]]
        assert sum(1 for d in rows if b"v" in d) == 10

    def test_shard_k_ratio_is_accepted_and_ignored(self):
        """SHARD_K_RATIO tunes how much of K each shard returns during a
        fanout. This implementation does not use it, but a command written for
        Redis must not be rejected -- and the value must not change the
        result."""
        client = self.server.get_new_client()
        self.setup_index(client)

        def fused_scores(*knn_args):
            result = client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", *knn_args, "YIELD_SCORE_AS", "v",
                "COMBINE", "RRF", "4", "WINDOW", "100", "YIELD_SCORE_AS", "h",
                "LOAD", "1", "@title",
                "SORTBY", "2", "@h", "DESC",
                "LIMIT", "0", "100",
                "PARAMS", "2", "q", self.Q,
            )
            return [self._rec_to_dict(rec).get(b"h") for rec in result[1:]]

        baseline = fused_scores("KNN", "2", "K", "5")
        for ratio in ("0.1", "0.5", "1.0"):
            assert fused_scores(
                "KNN", "4", "K", "5", "SHARD_K_RATIO", ratio) == baseline, \
                f"SHARD_K_RATIO {ratio} changed the result"

    def test_yield_score_as_inside_the_knn_block_is_rejected(self):
        """YIELD_SCORE_AS names the arm, not the KNN search, so it belongs
        after the block. Redis rejects this spelling too."""
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"Unknown VSIM KNN sub-arg"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                # count 4 pulls YIELD_SCORE_AS and its alias into the block.
                "VSIM", "@vec", "$q", "KNN", "4", "K", "5",
                "YIELD_SCORE_AS", "v",
                "PARAMS", "2", "q", self.Q,
            )

    def test_ef_runtime_is_accepted_on_the_vsim_arm(self):
        """EF_RUNTIME is an HNSW search-effort knob; it must not change which
        documents come back for a K this small, only how hard the index
        works to find them."""
        client = self.server.get_new_client()
        self.setup_index(client)

        def scores(*knn_args):
            result = client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello",
                "VSIM", "@vec", "$q", *knn_args, "YIELD_SCORE_AS", "v",
                "COMBINE", "RRF", "4", "WINDOW", "100", "YIELD_SCORE_AS", "h",
                "SORTBY", "2", "@h", "DESC",
                "LIMIT", "0", "100",
                "PARAMS", "2", "q", self.Q,
            )
            return [self._rec_to_dict(rec).get(b"v") for rec in result[1:]]

        # Omitted (index default) and overridden agree on this corpus.
        assert scores("KNN", "2", "K", "5") == \
            scores("KNN", "4", "K", "5", "EF_RUNTIME", "200")

    # ---------------------------------------------------------------------
    # SEARCH-arm vector content: the vector arm is VSIM's, and a vector query
    # written into SEARCH is refused rather than run by a path that the VSIM
    # clause's K/EF_RUNTIME bounds and scoring rules never see.
    # ---------------------------------------------------------------------

    def test_search_arm_vector_query_rejected(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError,
                           match=r"A vector query is not supported in the "
                                 r"SEARCH clause; use VSIM"):
            client.execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "*=>[KNN 5 @vec $q]",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "PARAMS", "2", "q", self.Q,
            )

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


class TestFtHybridLoad(ValkeySearchTestCaseDebugMode):
    """The LOAD clause. FT.HYBRID routes it through the same resolution
    FT.AGGREGATE uses, so the clause names the columns the reply carries, `AS`
    renames them, and a rename is visible to the stages after it. Pinned
    against Redis by integration/compatibility/generate_hybrid.py.

    `LOAD ... AS` is gated on search.emulate-release >= 1.3.0 (see
    COMPATIBILITY.md), which is why this runs under debug-mode: the ceiling has
    to be lifted to the release that carries the fix."""

    INDEX = "idx"
    Q = _vec(1.0, 0.0, 0.0, 0.0)
    LOAD_AS_RELEASE = "1.3.0"

    def client_at_load_as_release(self) -> Valkey:
        client: Valkey = self.server.get_new_client()
        client.execute_command(
            "CONFIG", "SET", "search.emulate-release", self.LOAD_AS_RELEASE)
        return client

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX,
            "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT", "NOSTEM",
            "color", "TAG",
            "price", "NUMERIC",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        for i in range(5):
            client.hset(
                f"doc:{i}",
                mapping={
                    "title": "hello world",
                    "color": ["red", "blue"][i % 2],
                    "price": i * 10,
                    "vec": _vec(1.0 + i, 0.0, 0.0, 0.0),
                },
            )
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == 5,
            timeout=10)

    def _rows(self, client, *extra):
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "ts",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
            "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "hs",
            *extra,
            "LIMIT", "0", "10",
            "PARAMS", "2", "q", self.Q,
        )
        return [
            {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}
            for rec in result[1:]
        ]

    def test_load_names_the_columns_returned(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        for load, expected in [
            (["LOAD", "1", "@price"], {b"price"}),
            (["LOAD", "2", "@price", "@color"], {b"price", b"color"}),
            (["LOAD", "1", "@__key"], {b"__key"}),
        ]:
            for row in self._rows(client, *load):
                # The score aliases ride along with whatever was loaded.
                assert set(row) - {b"hs", b"ts"} == expected, \
                    f"{load} returned {sorted(row)}"

    def test_load_star_returns_every_field(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        for row in self._rows(client, "LOAD", "*"):
            assert {b"title", b"color", b"price", b"vec"} <= set(row), sorted(row)

    def test_no_load_returns_key_and_scores_only(self):
        """With no LOAD clause, the reply is the document key and the score
        aliases -- no database field is fetched at all."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for row in self._rows(client):
            assert set(row) - {b"hs", b"ts"} == {b"__key"}, sorted(row)

    def test_load_as_renames_an_existing_field(self):
        client = self.client_at_load_as_release()
        self.setup_index(client)
        rows = self._rows(client, "LOAD", "3", "@price", "AS", "cost")
        assert rows
        for row in rows:
            assert b"cost" in row, sorted(row)
            assert b"price" not in row, "the original name must not also appear"
        # The renamed column still carries the field's values.
        assert {int(r[b"cost"]) for r in rows} == {0, 10, 20, 30, 40}

    def test_load_as_renames_onto_another_fields_name(self):
        """An alias that collides with a different indexed field resolves to
        the renamed column, not to the field it shadows."""
        client = self.client_at_load_as_release()
        self.setup_index(client)
        rows = self._rows(client, "LOAD", "3", "@price", "AS", "color")
        assert rows
        # `color` now carries prices, not the tag values it would otherwise.
        assert {int(r[b"color"]) for r in rows} == {0, 10, 20, 30, 40}

    def test_load_as_rename_is_visible_to_later_stages(self):
        client = self.client_at_load_as_release()
        self.setup_index(client)
        rows = self._rows(
            client, "LOAD", "3", "@price", "AS", "cost",
            "APPLY", "@cost * 2", "AS", "doubled")
        assert rows
        for row in rows:
            assert int(row[b"doubled"]) == 2 * int(row[b"cost"]), sorted(row)

        rows = self._rows(
            client, "LOAD", "3", "@price", "AS", "cost",
            "SORTBY", "2", "@cost", "ASC")
        assert [int(r[b"cost"]) for r in rows] == [0, 10, 20, 30, 40]

        rows = self._rows(
            client, "LOAD", "6", "@price", "AS", "amount", "@color", "AS", "hue",
            "GROUPBY", "1", "@hue", "REDUCE", "SUM", "1", "@amount", "AS", "total")
        assert {r[b"hue"]: int(r[b"total"]) for r in rows} == {
            b"red": 0 + 20 + 40, b"blue": 10 + 30}

    def test_load_of_an_unknown_field_is_rejected(self):
        """Redis returns no column for a field the index does not have;
        valkey-search rejects the command. Tracked in
        integration/compatibility/unsupported_tests.md section 5.1."""
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError, match=r"does not exist"):
            self._rows(client, "LOAD", "1", "@nosuchfield")

class _ScoreAliasOverAStoredFieldTests:
    """A YIELD_SCORE_AS alias naming a field the document also carries.

    `LOAD *` emits whatever each key holds, which a query cannot be expected
    to know in advance, so such a clash is no longer refused at parse time.
    It is resolved at runtime under one rule: an explicitly named score --
    per-arm or fused -- beats a database field of the same name. The column
    appears once and holds the score.

    Mixed into both the standalone and the cluster fixture: the two take
    different code paths to attach the score (local RestoreAliases vs the
    coordinator's fanout merge) and the point of the rule is that they agree.
    """

    INDEX = "aliasidx"
    Q = _vec(1.0, 0.0, 0.0, 0.0)
    PRICES = (0, 10, 20, 30, 40)
    # A hash field that is not in the schema: only `LOAD *` surfaces it, and
    # the old parse-time guard could not have caught a clash with it.
    STORED = tuple(f"stored-{i}".encode() for i in range(5))

    def _create_and_seed(self, command_client, write_client) -> None:
        command_client.execute_command(
            "FT.CREATE", self.INDEX,
            "ON", "HASH", "PREFIX", "1", "adoc:",
            "SCHEMA",
            "title", "TEXT", "NOSTEM",
            "price", "NUMERIC",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        for i in range(5):
            write_client.hset(
                f"adoc:{i}",
                mapping={
                    "title": "hello world",
                    "price": self.PRICES[i],
                    "extra": self.STORED[i].decode(),
                    "vec": _vec(1.0 + i, 0.0, 0.0, 0.0),
                },
            )
        waiters.wait_for_true(
            lambda: command_client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == 5,
            timeout=15)

    def _setup(self):
        """Returns the client the FT.HYBRID commands run on."""
        raise NotImplementedError

    def _command(self, *, search_alias=None, vsim_alias=None,
                 combine_alias=None, load=("LOAD", "*")):
        cmd = ["FT.HYBRID", self.INDEX, "SEARCH", "@title:hello"]
        if search_alias is not None:
            cmd += ["YIELD_SCORE_AS", search_alias]
        cmd += ["VSIM", "@vec", "$q", "KNN", "2", "K", "10"]
        if vsim_alias is not None:
            cmd += ["YIELD_SCORE_AS", vsim_alias]
        if combine_alias is not None:
            cmd += ["COMBINE", "RRF", "2", "YIELD_SCORE_AS", combine_alias]
        cmd += list(load)
        cmd += ["LIMIT", "0", "10", "PARAMS", "2", "q", self.Q]
        return cmd

    @staticmethod
    def _rows(client, cmd):
        """[(column names in reply order, {name: value}), ...]"""
        result = client.execute_command(*cmd)
        rows = []
        for rec in result[1:]:
            names = [bytes(n) for n in rec[0::2]]
            rows.append((names, dict(zip(names, rec[1::2]))))
        return rows

    def _assert_alias_holds_the_score(self, rows, alias, shadowed):
        """`alias` is emitted once per row and carries a score, not the
        database field of the same name that `shadowed` lists the values of."""
        assert len(rows) == 5, f"expected all 5 docs, got {len(rows)}"
        for names, row in rows:
            assert names.count(alias) == 1, \
                f"`{alias.decode()}` appears {names.count(alias)} times: {names}"
            value = row[alias]
            assert value not in shadowed, \
                f"`{alias.decode()}` holds the stored field {value!r}"
            float(value)  # a score is a number; raises if the field won

    def test_load_all_search_arm_alias_beats_a_stored_field(self):
        client = self._setup()
        rows = self._rows(client, self._command(search_alias="extra"))
        self._assert_alias_holds_the_score(rows, b"extra", self.STORED)

    def test_load_all_vsim_arm_alias_beats_a_stored_field(self):
        client = self._setup()
        rows = self._rows(client, self._command(vsim_alias="extra"))
        self._assert_alias_holds_the_score(rows, b"extra", self.STORED)

    def test_load_all_combine_alias_beats_a_stored_field(self):
        client = self._setup()
        rows = self._rows(client, self._command(combine_alias="extra"))
        self._assert_alias_holds_the_score(rows, b"extra", self.STORED)

    def test_load_all_alias_naming_an_indexed_field_is_served(self):
        """The case the parse-time guard used to refuse: the alias names a
        field of the schema, which under `LOAD *` is certain to be emitted.
        Served now, and resolved the same way."""
        client = self._setup()
        prices = tuple(str(p).encode() for p in self.PRICES)
        for placement in ("search_alias", "vsim_alias", "combine_alias"):
            rows = self._rows(client, self._command(**{placement: "price"}))
            self._assert_alias_holds_the_score(rows, b"price", prices)

    def test_named_load_with_a_combine_alias_over_a_stored_field(self):
        """A named LOAD alongside a fused-score alias that names a field the
        LOAD clause does not mention. The LOAD column and the score column
        both come back, each once, on either fixture."""
        client = self._setup()
        rows = self._rows(client, self._command(
            combine_alias="extra", load=("LOAD", "1", "@price")))
        assert len(rows) == 5
        assert {int(row[b"price"]) for _, row in rows} == set(self.PRICES)
        self._assert_alias_holds_the_score(rows, b"extra", self.STORED)
        for names, _ in rows:
            assert names.count(b"price") == 1, names


class TestFtHybridScoreAliasOverAStoredField(
        _ScoreAliasOverAStoredFieldTests, ValkeySearchTestCaseBase):

    def _setup(self):
        client: Valkey = self.server.get_new_client()
        self._create_and_seed(client, client)
        return client


class TestFtHybridScoreAliasOverAStoredFieldCluster(
        _ScoreAliasOverAStoredFieldTests, ValkeySearchClusterTestCase):

    def _setup(self):
        cluster: ValkeyCluster = self.new_cluster_client()
        client: Valkey = self.new_client_for_primary(0)
        self._create_and_seed(client, cluster)
        return client


class TestFtHybridScoreShape(ValkeySearchTestCaseBase):
    """Result framing and score conventions: how many rows come back, what
    WINDOW bounds, what number the VSIM arm reports, and how LINEAR combines
    the arms. These are the points where FT.HYBRID differs from FT.AGGREGATE,
    and each is pinned against the Redis behavior the compatibility suite
    captures (integration/compatibility/generate_hybrid.py)."""

    INDEX = "idx"
    NUM_DOCS = 25
    Q = _vec(1.0, 0.0, 0.0, 0.0)

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX,
            "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA",
            "title", "TEXT", "NOSTEM",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2",
        )
        for i in range(self.NUM_DOCS):
            client.hset(
                f"doc:{i:02d}",
                mapping={
                    # Every document matches `hello`, so the SEARCH arm alone
                    # returns more rows than the default page.
                    "title": " ".join(["hello"] * (1 + i % 3) + ["world"] * i),
                    "vec": _vec(1.0 + i, 0.0, 0.0, 0.0),
                },
            )
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == self.NUM_DOCS,
            timeout=10)

    def _hybrid(self, client, *extra):
        return client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10",
            *extra,
            "PARAMS", "2", "q", self.Q,
        )

    def test_default_limit_is_ten(self):
        """With no LIMIT, FT.HYBRID returns one page of 10 -- unlike
        FT.AGGREGATE, which returns everything."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = self._hybrid(client)
        assert result[0] == 10, f"expected the default page of 10, got {result[0]}"

    def test_explicit_limit_overrides_the_default(self):
        client = self.server.get_new_client()
        self.setup_index(client)
        assert self._hybrid(client, "LIMIT", "0", "3")[0] == 3
        # The whole fused set is the SEARCH arm unioned with the VSIM top-10.
        # WINDOW is raised past the corpus so the default of 20 does not cap
        # the text arm first.
        assert self._hybrid(
            client, "COMBINE", "RRF", "2", "WINDOW", "100",
            "LIMIT", "0", "100")[0] == self.NUM_DOCS

    def test_window_bounds_the_arms_not_the_fused_list(self):
        """WINDOW caps how much of each arm reaches fusion. It does not cap the
        fused list, so two arms with disjoint windows can produce more rows
        than WINDOW itself."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = self._hybrid(
            client, "COMBINE", "RRF", "2", "WINDOW", "6", "LIMIT", "0", "100")
        # 6 from the text arm plus up to 6 from the vector arm; the two
        # rankings disagree here, so the union exceeds the window.
        assert result[0] > 6, \
            f"WINDOW must not truncate the fused list; got {result[0]} rows"
        assert result[0] <= 12

    def test_sortby_returns_every_row_the_limit_asks_for(self):
        """A SORTBY does not cap the page; the LIMIT does.

        The sort stage carries a retention bound that defaults to 10, and
        `ResolveSortByBounds` raises it from the LIMIT beside it. FT.AGGREGATE
        asks for that resolution at the end of its own parse; FT.HYBRID drives
        the aggregate parser itself and has to ask separately, which is what
        this pins. Without it a sorted FT.HYBRID returned 10 rows whatever the
        LIMIT said, and the reference returned the whole fused set.
        """
        client = self.server.get_new_client()
        self.setup_index(client)
        # The resolution is gated: without this the sort keeps its parse-time
        # bound of 10 and the reply is capped there, which is the behaviour
        # this test exists to say is gone.
        client.execute_command("CONFIG", "SET", "search.emulate-release",
                               "1.3.0")

        def keys(*tail):
            reply = self._hybrid(client, "COMBINE", "RRF", "2", "WINDOW",
                                 "100", *tail)
            return [self._rec_to_dict(rec)[b"__key"] for rec in reply[1:]]

        everything = keys("LIMIT", "0", "100")
        assert len(everything) > 10, \
            f"the fused set must exceed 10 to be worth testing, got " \
            f"{len(everything)}"

        sorted_wide = keys("SORTBY", "2", "@__key", "ASC", "LIMIT", "0", "100")
        assert sorted_wide == sorted(everything), \
            (f"a SORTBY under a wide LIMIT returned {len(sorted_wide)} of "
             f"{len(everything)} rows")

        # A narrower LIMIT is still the page, and it is the sorted head of the
        # set rather than an arbitrary ten.
        page = keys("SORTBY", "2", "@__key", "ASC", "LIMIT", "0", "4")
        assert page == sorted(everything)[:4], page

        # MAX raises the sort's bound and never lowers it below the LIMIT, so
        # on this command it cannot shrink the page. The reference refuses the
        # clause outright here; see unsupported_tests.md 5.4d.
        for n in ("1", "5", "1000"):
            capped = keys("SORTBY", "2", "@__key", "ASC", "MAX", n,
                          "LIMIT", "0", "100")
            assert capped == sorted(everything), \
                f"MAX {n} shrank the page to {len(capped)} rows"

    def test_window_zero_means_unlimited(self):
        """`WINDOW 0` lifts the cap rather than removing every candidate.

        The parser resolves zero to max-combine-window, so what is observable
        here is that it behaves like a window wider than the corpus. The
        reference rejects `WINDOW 0`, so nothing outside this repo pins it,
        and a reading of zero as "take no rows" would return an empty reply
        instead of the whole union.
        """
        client = self.server.get_new_client()
        self.setup_index(client)
        capped = self._hybrid(client, "COMBINE", "RRF", "2", "WINDOW", "3",
                              "LIMIT", "0", "100")
        unlimited = self._hybrid(client, "COMBINE", "RRF", "2", "WINDOW", "0",
                                 "LIMIT", "0", "100")
        wide = self._hybrid(client, "COMBINE", "RRF", "2", "WINDOW", "1000",
                            "LIMIT", "0", "100")
        assert unlimited[0] == wide[0], (
            f"WINDOW 0 returned {unlimited[0]} rows, a window wider than the "
            f"corpus returned {wide[0]}")
        assert unlimited[0] > capped[0], (
            f"WINDOW 0 ({unlimited[0]} rows) should not be capped like "
            f"WINDOW 3 ({capped[0]} rows)")

    def test_malformed_fusion_arguments_are_rejected(self):
        """A number the caller did not write must never be acted on.

        Each of these was previously accepted and silently truncated, because
        the shared scalar parser keeps whatever numeric prefix it finds:
        `WINDOW 20abc` became 20 and `CONSTANT 1.5` became 1.
        """
        client = self.server.get_new_client()
        self.setup_index(client)
        cases = [
            ["COMBINE", "RRF", "2", "WINDOW", "20abc"],
            ["COMBINE", "RRF", "2", "WINDOW", "1.5"],
            ["COMBINE", "RRF", "2", "WINDOW", "-1"],
            ["COMBINE", "RRF", "2", "CONSTANT", "60abc"],
            ["COMBINE", "RRF", "2", "CONSTANT", "-1"],
            ["COMBINE", "RRF", "2", "CONSTANT", "inf"],
            ["COMBINE", "RRF", "2", "CONSTANT", "nan"],
            ["COMBINE", "LINEAR", "4", "ALPHA", "inf", "BETA", "0.5"],
            ["COMBINE", "LINEAR", "4", "ALPHA", "nan", "BETA", "0.5"],
            ["COMBINE", "LINEAR", "4", "ALPHA", "0.5", "BETA", "1e400"],
        ]
        for extra in cases:
            with pytest.raises(ResponseError):
                self._hybrid(client, *extra, "LIMIT", "0", "3")

    def test_fractional_rrf_constant_ranks_between_its_neighbours(self):
        """`CONSTANT 1.5` is a real constant, not one truncated to 1.

        Fusion divides by `constant + rank`, so a larger constant flattens
        every score. A fractional value therefore has to land strictly between
        the two integers around it -- which is what catches a truncation that
        a single-value check would not.
        """
        client = self.server.get_new_client()
        self.setup_index(client)

        def top_score(constant):
            reply = self._hybrid(
                client, "COMBINE", "RRF", "4", "CONSTANT", constant,
                "YIELD_SCORE_AS", "h", "LIMIT", "0", "1")
            return float(self._rec_to_dict(reply[1])[b"h"])

        one, one_and_a_half, two = (top_score("1"), top_score("1.5"),
                                    top_score("2"))
        assert one > one_and_a_half > two, (
            f"CONSTANT 1.5 scored {one_and_a_half}, outside "
            f"({two}, {one}) -- it was probably truncated")

    def test_vsim_score_is_similarity_not_distance(self):
        """The VSIM arm reports 1 / (1 + distance): higher is better, and the
        document sitting on the query vector scores exactly 1."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10", "YIELD_SCORE_AS", "v",
            "COMBINE", "RRF", "4", "WINDOW", "100", "YIELD_SCORE_AS", "h",
            # The title is read back below, so it has to be loaded: without a
            # LOAD clause the reply is the key and the score aliases only.
            "LOAD", "*",
            "LIMIT", "0", "100",
            "PARAMS", "2", "q", self.Q,
        )
        seen = {}
        for rec in result[1:]:
            d = self._rec_to_dict(rec)
            if b"v" in d:
                seen[d[b"title"]] = float(d[b"v"])
        assert seen, "no document carried the VSIM score alias"
        # doc:00's vector is the query vector, so distance 0 -> similarity 1.
        exact = [v for t, v in seen.items() if t == b"hello"]
        assert exact and abs(exact[0] - 1.0) < 1e-6, \
            f"document on the query vector should score 1.0, got {exact}"
        # Every reported similarity lies in (0, 1].
        assert all(0.0 < v <= 1.0 for v in seen.values()), seen

    def test_linear_is_an_unnormalized_weighted_sum(self):
        """LINEAR sums ALPHA * text_score + BETA * vector_score over the arms'
        raw scores -- no per-arm normalization, so the fused score is
        reproducible from the two per-arm aliases alone."""
        client = self.server.get_new_client()
        self.setup_index(client)
        alpha, beta = 0.3, 0.7
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "10", "YIELD_SCORE_AS", "v",
            "COMBINE", "LINEAR", "8",
            "ALPHA", str(alpha), "BETA", str(beta), "WINDOW", "100",
            "YIELD_SCORE_AS", "h",
            "LIMIT", "0", "100",
            "PARAMS", "2", "q", self.Q,
        )
        checked = 0
        for rec in result[1:]:
            d = self._rec_to_dict(rec)
            assert b"h" in d, f"missing fused score in {rec}"
            s = float(d[b"s"]) if b"s" in d else 0.0
            v = float(d[b"v"]) if b"v" in d else 0.0
            h = float(d[b"h"])
            assert abs(h - (alpha * s + beta * v)) < 1e-5, \
                f"h={h} != {alpha}*{s} + {beta}*{v}"
            checked += 1
        assert checked == self.NUM_DOCS

    def test_named_fused_score_survives_every_load_shape(self):
        """COMBINE ... YIELD_SCORE_AS is an explicit request, so the column it
        names is in the reply whatever the LOAD clause says."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for load in ([], ["LOAD", "*"], ["LOAD", "1", "@title"]):
            reply = self._hybrid(
                client, "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "h",
                *load, "LIMIT", "0", "3")
            for rec in reply[1:]:
                assert b"h" in self._rec_to_dict(rec), \
                    f"missing alias under {load or 'no LOAD'}: {rec}"

    def test_unnamed_fused_score_follows_the_load_clause(self):
        """With no alias the fused score is named `__score`, and it is in the
        reply only when the caller gave no LOAD clause.

        A LOAD clause replaces the default projection outright, which is the
        rule the reference follows: `LOAD 1 @title` drops the score just as
        `LOAD *` does. Both spellings are here because the narrower one is the
        easy one to get wrong.
        """
        client = self.server.get_new_client()
        self.setup_index(client)

        for extra in (
            ["COMBINE", "RRF", "0", "LIMIT", "0", "3"],   # COMBINE, no alias
            ["LIMIT", "0", "3"],                          # no COMBINE at all
        ):
            reply = self._hybrid(client, *extra)
            for rec in reply[1:]:
                assert b"__score" in self._rec_to_dict(rec), \
                    f"no default score column without a LOAD clause: {rec}"

        for load in (["LOAD", "*"], ["LOAD", "1", "@title"]):
            reply = self._hybrid(client, "COMBINE", "RRF", "0", *load,
                                 "LIMIT", "0", "3")
            for rec in reply[1:]:
                keys = set(self._rec_to_dict(rec))
                assert b"__score" not in keys, \
                    f"default score survived {load}: {keys}"

    def test_a_load_naming_the_score_column_brings_it_back(self):
        """A LOAD replaces the default projection the score comes from, so
        naming `@__score` in the clause is the only way to get the score
        beside a chosen set of fields."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for load in (["LOAD", "1", "@__score"],
                     ["LOAD", "2", "@title", "@__score"],
                     ["LOAD", "2", "@__score", "@__key"]):
            reply = self._hybrid(client, "COMBINE", "RRF", "0", *load,
                                 "LIMIT", "0", "3")
            for rec in reply[1:]:
                assert b"__score" in self._rec_to_dict(rec), \
                    f"{load} did not bring the score back: {rec}"
        # The control: a LOAD that names something else still hides it.
        reply = self._hybrid(client, "COMBINE", "RRF", "0",
                             "LOAD", "1", "@title", "LIMIT", "0", "3")
        for rec in reply[1:]:
            assert b"__score" not in self._rec_to_dict(rec), rec

    def test_yielding_the_default_score_name_without_a_load_is_rejected(self):
        """`YIELD_SCORE_AS __score` names the column the default projection
        already generates. Accepted once a LOAD clause has removed it."""
        client = self.server.get_new_client()
        self.setup_index(client)
        with pytest.raises(ResponseError):
            self._hybrid(client, "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                         "__score", "LIMIT", "0", "3")
        reply = self._hybrid(client, "COMBINE", "RRF", "2", "YIELD_SCORE_AS",
                             "__score", "LOAD", "*", "LIMIT", "0", "3")
        for rec in reply[1:]:
            assert b"__score" in self._rec_to_dict(rec), rec

    @staticmethod
    def _rec_to_dict(rec):
        return {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}


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
        # Wait for indexing to complete before asserting, instead of a
        # fixed sleep that is both flaky under load and slow on fast runs.
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == 30,
            timeout=15)
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
            # Explicit LIMIT: FT.HYBRID returns 10 rows by default, and this
            # test is about the size of the cross-shard union, not the page.
            "LIMIT", "0", "100",
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
            "LIMIT", "0", "100",  # see test_fanout_basic_rrf
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
        # (Also under the default LIMIT of 10.)
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
        # Wait for indexing to complete before asserting, instead of a
        # fixed sleep that is both flaky under load and slow on fast runs.
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == 15,
            timeout=15)
        return cluster, client

    def _hybrid(self, client: Valkey):
        return client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
            "PARAMS", "2", "q", self.Q,
        )

    def test_index_fingerprint_mismatch_fails_fanout(self):
        """A forced index-fingerprint mismatch makes every shard's per-arm
        consistency check fail. With partial results refused, that has to
        surface as an error rather than as a quietly short reply."""
        _, client = self._setup()
        assert self._hybrid(client)[0] > 0  # sanity: nominal fanout works

        client.execute_command(
            "CONFIG", "SET", "search.enable-partial-results", "no")
        client.execute_command(
            "ft._debug", "CONTROLLED_VARIABLE", "set",
            "ForceInvalidIndexFingerprint", "yes")
        try:
            with pytest.raises(ResponseError):
                self._hybrid(client)
        finally:
            client.execute_command(
                "ft._debug", "CONTROLLED_VARIABLE", "set",
                "ForceInvalidIndexFingerprint", "no")
            client.execute_command(
                "CONFIG", "SET", "search.enable-partial-results", "yes")

    def test_partial_results_setting_is_live_for_hybrid(self):
        """`search.enable-partial-results` reaches FT.HYBRID rather than being
        hardcoded. The same forced failure is an error when partial results are
        refused and a short reply when they are allowed, so this fails in one
        direction or the other if the setting is ever pinned again."""
        _, client = self._setup()
        nominal = self._hybrid(client)[0]
        assert nominal > 0

        client.execute_command(
            "ft._debug", "CONTROLLED_VARIABLE", "set",
            "ForceInvalidIndexFingerprint", "yes")
        try:
            client.execute_command(
                "CONFIG", "SET", "search.enable-partial-results", "no")
            with pytest.raises(ResponseError):
                self._hybrid(client)

            client.execute_command(
                "CONFIG", "SET", "search.enable-partial-results", "yes")
            tolerated = self._hybrid(client)
            # A reply rather than an error, carrying whatever survived the
            # shards that failed their check.
            assert isinstance(tolerated, list)
            assert tolerated[0] < nominal
        finally:
            client.execute_command(
                "ft._debug", "CONTROLLED_VARIABLE", "set",
                "ForceInvalidIndexFingerprint", "no")
            client.execute_command(
                "CONFIG", "SET", "search.enable-partial-results", "yes")


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
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        hset_thread, _, _ = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "content", "updated", "vec", vec2))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing") > 0,
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
        client.execute_command("FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        hset_thread.join()
        search_thread.join()
        assert search_err[0] is None
        # doc:1 no longer matches "hello" after the mutation; fused result is
        # empty for the SEARCH arm, leaving only the VSIM match for doc:1.
        assert isinstance(search_res[0], list)
        assert search_res[0][0] == 1  # exactly one fused record survives
        assert len(search_res[0]) == 2  # count + the single VSIM record


# =============================================================================
# A COSINE arm, whose distance can come back negative.
#
# A vector arm's distance becomes the similarity every fusion method reads, via
# `1 / (1 + distance)`. Cosine distance is nominally in [0, 2], but for a
# document whose vector matches the query it lands just below zero -- about
# -1.6e-07 -- because the arithmetic does not cancel exactly. Every other
# FT.HYBRID test in this file uses L2, where a distance is never negative, so
# the sign is only exercised here.
#
# COMBINE FUNCTION is deliberately not covered: there the fused score is
# whatever the user's expression returns, so there is no formula of ours to
# check against.
# =============================================================================
class TestFtHybridCosineNegativeDistance(ValkeySearchTestCaseBase):
    INDEX = "cosidx"
    Q = _vec(1.0, 2.0, 3.0, 4.0)

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "c:",
            "SCHEMA", "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "COSINE")
        # Identical to the query, the same direction at twice the length,
        # orthogonal, and opposed: cosine distances of roughly 0, 0, 1 and 2.
        for key, vector in [
                ("c:same", _vec(1.0, 2.0, 3.0, 4.0)),
                ("c:scaled", _vec(2.0, 4.0, 6.0, 8.0)),
                ("c:orth", _vec(-2.0, 1.0, 0.0, 0.0)),
                ("c:opposed", _vec(-1.0, -2.0, -3.0, -4.0))]:
            client.hset(key, mapping={"title": "hello world", "vec": vector})
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == 4,
            timeout=10)

    @staticmethod
    def _rec_to_dict(rec):
        return {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}

    def _rows(self, client: Valkey, method: str, *opts):
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "4", "YIELD_SCORE_AS", "v",
            "COMBINE", method, str(len(opts) + 2), *opts,
            "YIELD_SCORE_AS", "h",
            "LOAD", "1", "@__key", "LIMIT", "0", "4",
            "PARAMS", "2", "q", self.Q)
        return {r[b"__key"]: r for r in
                (self._rec_to_dict(rec) for rec in result[1:])}

    def test_the_distance_really_is_negative(self):
        """The premise. Without this the two tests below would pass on an
        index that never produced a negative distance at all."""
        client = self.server.get_new_client()
        self.setup_index(client)
        result = client.execute_command(
            "FT.SEARCH", self.INDEX, "*=>[KNN 4 @vec $q AS d]",
            "RETURN", "1", "d", "DIALECT", "2", "PARAMS", "2", "q", self.Q)
        by_key = {}
        for i in range(1, len(result), 2):
            fields = self._rec_to_dict(result[i + 1])
            by_key[result[i]] = float(fields[b"d"])
        assert by_key[b"c:same"] < 0.0, \
            f"expected a negative self-distance, got {by_key[b'c:same']}"
        assert abs(by_key[b"c:same"]) < 1e-5  # negative, but only just
        assert abs(by_key[b"c:orth"] - 1.0) < 1e-5
        assert abs(by_key[b"c:opposed"] - 2.0) < 1e-5

    def test_similarity_follows_the_cosine_formula(self):
        """A cosine arm reports `1 - distance / 2`, which is what the reference
        engine reports and what keeps the value in [0, 1] across the metric's
        whole [0, 2] range.

        A negative distance therefore lands just above 1 rather than being
        clamped, and an opposed vector lands at 0 rather than at the 1/3 a
        `1 / (1 + d)` mapping would give it."""
        client = self.server.get_new_client()
        self.setup_index(client)
        rows = self._rows(client, "RRF", "CONSTANT", "60")

        same = float(rows[b"c:same"][b"v"])
        orth = float(rows[b"c:orth"][b"v"])
        opposed = float(rows[b"c:opposed"][b"v"])

        # The self-distance is slightly negative, so the similarity is slightly
        # above 1 -- not clamped, and not 1 exactly.
        assert 1.0 < same < 1.001, f"expected just above 1, got {same}"
        assert abs(orth - 0.5) < 1e-5, f"orthogonal should be 0.5, got {orth}"
        # Opposed is distance 2, so similarity 0 to within float error.
        assert abs(opposed) < 1e-5, f"opposed should be 0, got {opposed}"
        assert same > orth > opposed
        # And every fused score is a finite number.
        for key, row in rows.items():
            h = float(row[b"h"])
            assert h == h and abs(h) != float("inf"), f"{key}: h={h}"

    def test_linear_over_a_negative_distance(self):
        """LINEAR sums the two arms' scores with the given weights, so the
        fused score is reproducible from the aliases -- including for the
        document whose distance was negative."""
        client = self.server.get_new_client()
        self.setup_index(client)
        rows = self._rows(client, "LINEAR", "ALPHA", "0.25", "BETA", "0.75")

        assert float(rows[b"c:same"][b"v"]) > 1.0
        for key, row in rows.items():
            expected = 0.25 * float(row[b"s"]) + 0.75 * float(row[b"v"])
            actual = float(row[b"h"])
            assert abs(actual - expected) < 1e-5, \
                f"{key}: fused {actual} != 0.25*s + 0.75*v = {expected}"


# =============================================================================
# An inner-product arm, whose distance is unbounded below.
#
# IP distance is `1 - dot`, so a document whose dot product with the query is
# 500 arrives at -499. That is the only metric that reaches far below zero, and
# the similarity it maps to is `(1 + d) / 2` -- an affine map, because no
# bounded one exists over an unbounded domain. A single `1 / (1 + d)` mapping
# used to drive those through a pole and clamp them, so the three best matches
# all reported the same similarity and the worst reported the largest.
# =============================================================================
class TestFtHybridInnerProduct(ValkeySearchTestCaseBase):
    INDEX = "ipidx"
    Q = _vec(10.0, 0.0, 0.0, 0.0)
    # dot with the query, and therefore distance 1 - dot: 0, -9, -49, -499.
    DOCS = {
        "p:small": _vec(0.1, 0.0, 0.0, 0.0),
        "p:unit": _vec(1.0, 0.0, 0.0, 0.0),
        "p:big": _vec(5.0, 0.0, 0.0, 0.0),
        "p:huge": _vec(50.0, 0.0, 0.0, 0.0),
    }

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "p:",
            "SCHEMA", "title", "TEXT",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "IP")
        for key, vector in self.DOCS.items():
            client.hset(key, mapping={"title": "hello world", "vec": vector})
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == len(self.DOCS),
            timeout=10)

    @staticmethod
    def _rec_to_dict(rec):
        return {bytes(rec[i]): rec[i + 1] for i in range(0, len(rec), 2)}

    def _distances(self, client: Valkey):
        result = client.execute_command(
            "FT.SEARCH", self.INDEX, "*=>[KNN 4 @vec $q AS d]",
            "RETURN", "1", "d", "DIALECT", "2", "PARAMS", "2", "q", self.Q)
        return {result[i]: float(self._rec_to_dict(result[i + 1])[b"d"])
                for i in range(1, len(result), 2)}

    def _arm_scores(self, client: Valkey, method: str, *opts):
        result = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "4", "YIELD_SCORE_AS", "v",
            "COMBINE", method, str(len(opts) + 2), *opts,
            "YIELD_SCORE_AS", "h",
            "LOAD", "1", "@__key", "LIMIT", "0", "4",
            "PARAMS", "2", "q", self.Q)
        return [self._rec_to_dict(rec) for rec in result[1:]]

    def test_distances_really_go_below_minus_one(self):
        """The premise, and the thing no other metric provides."""
        client = self.server.get_new_client()
        self.setup_index(client)
        d = self._distances(client)
        assert abs(d[b"p:small"] - 0.0) < 1e-4
        assert abs(d[b"p:unit"] - -9.0) < 1e-4
        assert abs(d[b"p:big"] - -49.0) < 1e-4
        assert abs(d[b"p:huge"] - -499.0) < 1e-3

    def test_similarity_follows_the_inner_product_formula(self):
        """`(1 + d) / 2`, for every document including the three whose distance
        is below -1. Those used to be clamped to a single value, which lost the
        ordering between them entirely."""
        client = self.server.get_new_client()
        self.setup_index(client)
        distances = self._distances(client)
        rows = {r[b"__key"]: r for r in self._arm_scores(client, "RRF",
                                                         "CONSTANT", "60")}
        for key, distance in distances.items():
            actual = float(rows[key][b"v"])
            expected = (1.0 + distance) / 2.0
            assert abs(actual - expected) < 1e-3, (
                f"{key}: distance {distance} should give {expected}, "
                f"got {actual}")
        # All four are distinct, which is what the clamp destroyed.
        values = {float(r[b"v"]) for r in rows.values()}
        assert len(values) == len(self.DOCS), f"collapsed to {values}"

    def test_linear_sums_the_unbounded_similarity(self):
        """LINEAR reads the similarity rather than the rank, so an unbounded
        one has to flow through the weighted sum unaltered."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for row in self._arm_scores(client, "LINEAR",
                                    "ALPHA", "0.5", "BETA", "0.5"):
            expected = 0.5 * float(row[b"s"]) + 0.5 * float(row[b"v"])
            actual = float(row[b"h"])
            assert abs(actual - expected) < 1e-3, (
                f"{row[b'__key']}: fused {actual} != {expected}")


# =============================================================================
# Parallel-arm execution + per-arm consistency under concurrent mutations.
#
# Property the implementation must hold: an FT.HYBRID reply describes one
# state of the data, and it is the state the reply's own content comes from.
#
# It does not get there from a shared snapshot. The arms run in parallel and
# each one takes its own reader lock on the index's time-sliced mutex for the
# duration of its own search; nothing spans the arms, so a writer can and does
# land between them. Cross-arm correctness comes from the post-search
# validation, once every arm has reported: ArmGate runs ONE contention check
# over every (arm, key) probe and parks the whole envelope until the mutation
# queue is quiescent, then RevalidateArmsBeforeFusion re-checks each
# neighbor's sequence number against the index and drops, re-verifies or
# rescores it, and only then does fusion run. An arm's stale entry is
# corrected, not inherited. (The case this does not cover is a document the
# mutation made newly-matching between two arms' searches;
# RevalidateArmsBeforeFusion is deliberately one-directional about that.)
#
# So a document appears in an arm exactly when it matches that arm after the
# mutation, carrying the score it earns there, and the fused ranking follows.
# A document that stopped matching one arm drops out of that arm alone; one
# whose score moved is re-ranked rather than left where it was.
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

    # -------- TEST 2 : a mutation that changes nothing about matching --------
    def test_doc_stays_in_both_arms_when_the_mutation_preserves_matching(self):
        """doc:1 matches both arms, and the queued mutation rewrites it without
        changing that: same title, a different vector. The mutation processor
        is stalled so it cannot apply while the arms run.

        The arms park behind the mutation, it applies, and each arm is
        revalidated against the result. doc:1 still matches both, so it must
        still carry both per-arm aliases. A document is never left holding one
        arm's alias when it belongs to both."""
        client: Valkey = self.server.get_new_client()
        q = self._setup_index(client, n=1)  # only doc:1, matched by both arms

        # Pause mutation processing → any HSET queues but does not apply.
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        mut_thread, _, mut_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "title", "hello world",
                "vec", _vec(99.0, 99.0, 99.0, 99.0)))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing")
            >= 1,
            timeout=5)

        blocked_before = client.info("SEARCH").get(
            "search_text_query_blocked_count", 0)
        # Issue FT.HYBRID — both arms run against the still-pre-mutation index.
        hyb_thread, res, err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "FT.HYBRID", self.INDEX,
                "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
                "VSIM", "@vec", "$q", "KNN", "2", "K", "5",
                "YIELD_SCORE_AS", "v",
                "PARAMS", "2", "q", q))
        # The query must be parked behind the mutation before it is released.
        # Without this wait the release frequently wins the race, both arms
        # then run against the post-mutation index, and doc:1 legitimately
        # appears in the VSIM arm alone -- which looks like a violation but is
        # only a test that let the mutation land first.
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_text_query_blocked_count"]
            >= blocked_before + 1,
            timeout=5)
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

        rows = [self._rec_to_dict(r) for r in res[0][1:]]
        doc1 = next((r for r in rows
                     if r.get(b"__key") == b"doc:1"
                     or b"doc:1" in r.values()), None)
        assert doc1 is not None, "doc:1 still matches both arms"
        assert b"s" in doc1 and b"v" in doc1, \
            f"doc:1 lost an arm it still matches: {doc1}"

    # -------- TESTS 3 & 4 : a mutated document is re-ranked, not just kept ---

    def _ranking_index(self, client: Valkey) -> bytes:
        """Three documents that both arms match, ordered the same way by each:
        p:1 has the most occurrences of `hello` and the nearest vector, p:3 the
        fewest and the farthest."""
        client.execute_command(
            "FT.CREATE", "rank_idx", "ON", "HASH", "PREFIX", "1", "p:",
            "SCHEMA", "title", "TEXT", "NOSTEM",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        client.execute_command("HSET", "p:1", "title",
                               "hello hello hello hello",
                               "vec", _vec(1.0, 0.0, 0.0, 0.0))
        client.execute_command("HSET", "p:2", "title", "hello there",
                               "vec", _vec(2.0, 0.0, 0.0, 0.0))
        client.execute_command("HSET", "p:3", "title", "hello world",
                               "vec", _vec(3.0, 0.0, 0.0, 0.0))
        IndexingTestHelper.is_indexing_complete_on_node(client, "rank_idx")
        return _vec(0.0, 0.0, 0.0, 0.0)

    def _ranked(self, q: bytes):
        """One FT.HYBRID over the ranking index, as a list of row dicts."""
        result = self.server.get_new_client().execute_command(
            "FT.HYBRID", "rank_idx",
            "SEARCH", "@title:hello", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "3", "YIELD_SCORE_AS", "v",
            "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "h",
            "PARAMS", "2", "q", q)
        return [self._rec_to_dict(r) for r in result[1:]]

    def _run_across_mutation(self, client: Valkey, q: bytes, mutation):
        """Park an FT.HYBRID behind `mutation`, release it, and return the
        parked query's rows next to a fresh query's rows.

        The mutation cannot apply while the arms run, so the arms produce a
        pre-mutation result and the reply is assembled after the mutation
        lands. Those rows have to agree with a query issued afterwards."""
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        mut_thread, _, mut_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(*mutation))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing") >= 1,
            timeout=5)
        blocked_before = client.info("SEARCH").get(
            "search_text_query_blocked_count", 0)
        hyb_thread, res, err = run_in_thread(lambda: self._ranked(q))
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_text_query_blocked_count"]
            >= blocked_before + 1,
            timeout=5)
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        mut_thread.join()
        hyb_thread.join()
        assert mut_err[0] is None
        assert err[0] is None
        return res[0], self._ranked(q)

    @staticmethod
    def _order(rows):
        return [r.get(b"__key") for r in rows]

    def test_mutation_drops_a_document_from_the_arm_it_left(self):
        """p:1 leads both arms, then stops matching the text arm and its vector
        moves to the far end. The parked query must rank it where it now
        belongs -- last, on the vector arm alone -- not where it was when the
        arms ran."""
        client: Valkey = self.server.get_new_client()
        q = self._ranking_index(client)
        assert self._order(self._ranked(q)) == [b"p:1", b"p:2", b"p:3"]

        parked, fresh = self._run_across_mutation(
            client, q,
            ("HSET", "p:1", "title", "goodbye",
             "vec", _vec(99.0, 0.0, 0.0, 0.0)))

        assert self._order(parked) == [b"p:2", b"p:3", b"p:1"]
        assert self._order(parked) == self._order(fresh)
        # p:1 left the text arm, so it carries no text alias any more, and its
        # vector score and fused score are the post-mutation ones.
        moved = next(r for r in parked if r[b"__key"] == b"p:1")
        moved_fresh = next(r for r in fresh if r[b"__key"] == b"p:1")
        assert b"s" not in moved
        assert moved[b"v"] == moved_fresh[b"v"]
        assert [r[b"h"] for r in parked] == [r[b"h"] for r in fresh]

    def test_mutation_that_only_moves_a_vector_repositions_the_row(self):
        """The document still matches both arms; only its vector moved. The
        text arm keeps it, the vector arm has to rank it by where it is now."""
        client: Valkey = self.server.get_new_client()
        q = self._ranking_index(client)

        parked, fresh = self._run_across_mutation(
            client, q,
            ("HSET", "p:1", "title", "hello hello hello hello",
             "vec", _vec(99.0, 0.0, 0.0, 0.0)))

        moved = next(r for r in parked if r[b"__key"] == b"p:1")
        moved_fresh = next(r for r in fresh if r[b"__key"] == b"p:1")
        # Still in both arms.
        assert b"s" in moved and b"v" in moved
        assert moved[b"v"] == moved_fresh[b"v"]
        assert self._order(parked) == self._order(fresh)
        # And it is no longer the nearest vector, so it is no longer first.
        assert self._order(parked)[0] != b"p:1"

    # -------- TEST 5 : stress probe — no split-arm result under concurrent
    #                   mutations across many trials --------
    def test_concurrent_mutations_never_split_arms(self):
        """The "both arms or neither" guarantee is a *per-query atomicity*
        property: a key cannot be in one arm's result and absent from the
        other's just because a writer slipped in between the arms' searches.
        The arms do not share an index snapshot — each takes its own reader
        lock for its own search — so this property is produced after the fact,
        by the post-search validation. (A doc that legitimately stopped
        matching SEARCH after a real content change would land in just the
        VSIM arm — that's correct semantics, not a split-arm violation.)

        To probe atomicity in isolation, the mutator REFRESHES each doc in
        place with the same values it already holds: this exercises the
        mutation pipeline (writer lock + index remove/re-add) without ever
        changing the doc's match status for either arm. Every doc matches
        both arms throughout the run, so any split-arm row is by construction
        caused by a writer time-slicing between the two arms' independent
        reader locks and the post-search validation failing to repair it:
        ArmGate's single contention check over every (arm, key) probe should
        park the envelope until the mutation queue is quiescent, and
        RevalidateArmsBeforeFusion should then re-check every neighbor's
        sequence number and drop, re-verify or rescore it before fusion
        runs."""
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
                    "VSIM", "@vec", "$q", "KNN", "2", "K", str(N),
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
        # Wait for indexing to complete before asserting, instead of a
        # fixed sleep that is both flaky under load and slow on fast runs.
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT.SEARCH", self.INDEX, "@title:hello",
                "NOCONTENT", "LIMIT", "0", "0")[0] == n_docs,
            timeout=15)
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
            "VSIM", "@vec", "$q", "KNN", "2", "K", str(n_docs * 4),
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
            "VSIM", "@vec", "$q", "KNN", "2", "K", str(n_docs * 4),
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
