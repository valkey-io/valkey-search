"""A KNN reply describes the documents as they are, not as they were.

A document rewritten while a query is running is re-checked against the query's
filter at content-fetch time. These cover the other half of that: its distance
is recomputed against the vector it holds now, and the reply is put back in
order once it has been. The last case covers FT.HYBRID, whose VSIM arm does the
same work in RevalidateArmsBeforeFusion before the arms are fused.
"""

import struct

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper, run_in_thread


def _vec(*xs: float) -> bytes:
    return struct.pack(f"<{len(xs)}f", *xs)


class TestVectorMutationRescore(ValkeySearchTestCaseDebugMode):
    INDEX = "idx"
    # The origin, so a document's distance is the square of its first
    # coordinate and the ordering is obvious by eye.
    Q = _vec(0.0, 0.0, 0.0, 0.0)

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        # A second writer thread so the parked mutation does not starve the
        # rest of the pool.
        args["search.writer-threads"] = "2"
        # FT.HYBRID dispatches its arms in parallel and needs room for both;
        # the KNN cases here do not care either way.
        args["search.reader-threads"] = "4"
        return args

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "d:",
            "SCHEMA",
            "price", "NUMERIC",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        for i in range(1, 5):
            client.hset(f"d:{i}", mapping={
                "price": i, "vec": _vec(float(i), 0.0, 0.0, 0.0)})
        IndexingTestHelper.wait_for_indexing_complete_on_node(
            client, self.INDEX)

    def knn(self, client: Valkey, *extra):
        """(key, distance) per row, in the order the reply carried them."""
        result = client.execute_command(
            "FT.SEARCH", self.INDEX, "@price:[0 100]=>[KNN 4 @vec $q AS dist]",
            "RETURN", "2", "dist", "price", *extra,
            "DIALECT", "2", "PARAMS", "2", "q", self.Q)
        rows = []
        for i in range(1, len(result), 2):
            fields = {result[i + 1][j]: result[i + 1][j + 1]
                      for j in range(0, len(result[i + 1]), 2)}
            rows.append((result[i], fields[b"dist"]))
        return rows

    def _park_mutation(self, client: Valkey, *hset_args):
        """Queue an HSET and hold it at the mutation pausepoint.

        The write lands in the database immediately; only the index update
        waits. So while this is parked the index still describes the old
        document and the database already holds the new one, which is the
        window a query has to get right.
        """
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "SET", "mutation_processing")
        thread, _, err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(*hset_args))
        waiters.wait_for_true(
            lambda: client.execute_command(
                "FT._DEBUG", "PAUSEPOINT", "TEST", "mutation_processing") >= 1,
            timeout=5)
        return thread, err

    def _release(self, client: Valkey, thread, err):
        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        thread.join()
        assert err[0] is None
        IndexingTestHelper.wait_for_indexing_complete_on_node(
            client, self.INDEX)

    def test_distance_is_recomputed_and_the_reply_reordered(self):
        """d:1 is nearest, then its vector moves to the far end. The reply has
        to report where it is now, and rank it there."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)
        assert [k for k, _ in self.knn(client)] == [b"d:1", b"d:2", b"d:3",
                                                    b"d:4"]

        thread, err = self._park_mutation(
            client, "HSET", "d:1", "price", "1", "vec", _vec(99.0, 0, 0, 0))
        during = self.knn(client)
        self._release(client, thread, err)
        after = self.knn(client)

        assert during == after, (
            f"reply taken across the mutation differs from a settled one: "
            f"{during} vs {after}")
        # 99 squared, and last.
        assert during[-1][0] == b"d:1"
        assert abs(float(during[-1][1]) - 9801.0) < 1e-3

    def test_distance_is_recomputed_when_the_vector_moves_closer(self):
        """The same in the other direction: a document that moved nearer has
        to rise, not merely report a smaller number."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)

        thread, err = self._park_mutation(
            client, "HSET", "d:4", "price", "4", "vec", _vec(0.5, 0, 0, 0))
        during = self.knn(client)
        self._release(client, thread, err)
        after = self.knn(client)

        assert during == after
        assert during[0][0] == b"d:4"
        assert abs(float(during[0][1]) - 0.25) < 1e-3

    def test_an_explicit_sortby_sees_the_recomputed_distance(self):
        """SORTBY on the distance alias runs after the reply path has
        refreshed it, so it sorts on the fresh value."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)

        thread, err = self._park_mutation(
            client, "HSET", "d:1", "price", "1", "vec", _vec(99.0, 0, 0, 0))
        during = self.knn(client, "SORTBY", "dist", "DESC")
        self._release(client, thread, err)
        after = self.knn(client, "SORTBY", "dist", "DESC")

        assert during == after
        # Furthest first, and d:1 is now the furthest.
        assert during[0][0] == b"d:1"

    def test_a_document_that_stopped_matching_is_still_dropped(self):
        """The re-filter half is unchanged: a document whose mutation takes it
        out of the query's filter leaves the reply, rather than being rescored
        into a new position."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)

        thread, err = self._park_mutation(
            client, "HSET", "d:1", "price", "999", "vec", _vec(1.0, 0, 0, 0))
        during = self.knn(client)
        self._release(client, thread, err)

        assert [k for k, _ in during] == [b"d:2", b"d:3", b"d:4"]

    def _hybrid_rows(self, client: Valkey):
        """{key: fields} from FT.HYBRID.

        The VSIM arm yields its score under `vdist`, and a per-arm alias is
        attached only for documents that arm actually contributed. So a key
        present without `vdist` is one the vector arm dropped -- which the
        fused reply still carries, because the numeric SEARCH arm matches every
        document and fusion is a union.
        """
        reply = client.execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "@price:[0 100]",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "4",
            "YIELD_SCORE_AS", "vdist",
            "COMBINE", "RRF", "2", "WINDOW", "100",
            "LIMIT", "0", "100",
            "PARAMS", "2", "q", self.Q)
        rows = {}
        for rec in reply[1:]:
            fields = {rec[i]: rec[i + 1] for i in range(0, len(rec), 2)}
            rows[fields[b"__key"]] = fields
        return rows

    def test_a_vector_the_mutation_made_unusable_is_dropped(self):
        """A mutation that leaves the vector field present but the wrong length
        has no current distance, so the document leaves the reply.

        RecomputeDistance rejects the value on a size check, which is the only
        way it fails for a well-formed key: the field is there, it just no
        longer matches the index dimensions. Scoring the document on the
        distance the search computed against the vector it no longer holds
        would rank it somewhere it does not belong, so it is dropped -- the
        same answer the neighbouring branch gives when the field is missing
        altogether.
        """
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)
        before = self._hybrid_rows(client)
        assert sorted(before) == [b"d:1", b"d:2", b"d:3", b"d:4"]
        assert all(b"vdist" in r for r in before.values()), before

        # Three floats where the index wants four.
        thread, err = self._park_mutation(
            client, "HSET", "d:1", "price", "1", "vec", _vec(1.0, 0.0, 0.0))
        # Unlike the FT.SEARCH cases above, the query cannot simply be run
        # while the mutation is held: FT.HYBRID's ArmGate parks the whole
        # envelope until the mutation queue is quiescent, so it would block
        # until the client's deadline (see
        # test_fused_result_blocks_on_inflight_mutation). Start it, then let
        # the mutation through, so revalidation runs against the new value.
        qthread, out, qerr = run_in_thread(
            lambda: self._hybrid_rows(self.server.get_new_client()))
        self._release(client, thread, err)
        qthread.join()
        assert qerr[0] is None, qerr[0]

        during = out[0]
        assert b"vdist" not in during.get(b"d:1", {}), (
            f"the vector arm kept a document it cannot score, with the "
            f"pre-mutation distance: {during.get(b'd:1')}")
        for key in (b"d:2", b"d:3", b"d:4"):
            assert b"vdist" in during[key], (
                f"{key} was dropped from the vector arm too: {during}")

    def test_an_untouched_reply_is_left_alone(self):
        """With nothing in flight the reply is exactly what the search
        produced, in the order it produced it."""
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)
        rows = self.knn(client)
        assert [k for k, _ in rows] == [b"d:1", b"d:2", b"d:3", b"d:4"]
        assert [float(d) for _, d in rows] == [1.0, 4.0, 9.0, 16.0]
