"""A reply's relevance score must describe the document the reply carries.

Two shapes get this wrong. Both are races: the document is rewritten between
the background index search and the main-thread content fetch, so they need
`FT._DEBUG PAUSEPOINT` to park the mutation and are not reproducible from a
plain valkey-cli session.

  * V4 -- `text=>[KNN k @vec $q]` through FT.SEARCH. That query ranks on BM25
    (the reference engine does too), but a mutated document keeps its
    pre-mutation BM25 score and its pre-mutation rank, next to fresh content
    and a freshly recomputed distance.

  * V5 -- a match-all (`*`) SEARCH arm of FT.HYBRID. A match-all score is a
    pure function of the document's text length, so rewriting the text has to
    move it. It does not: the score, and the arm's rank, stay pre-mutation.
    RRF ranks positionally, so one stale row moves the whole fused order.
"""

import struct

import pytest
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401
from valkeytestframework.util import waiters
from utils import IndexingTestHelper, run_in_thread


def _vec(*xs: float) -> bytes:
    return struct.pack(f"<{len(xs)}f", *xs)


def _words(prefix: str, n: int) -> str:
    return " ".join(f"{prefix}{i}" for i in range(n))


class _ParkedMutation:
    """Mixin: run a query while a mutation is parked at `mutation_processing`.

    The HSET lands in the database at once; only the index update waits. The
    query therefore runs its background search against the pre-mutation index,
    parks behind the queued mutation at the contention check, and assembles its
    reply after the mutation has applied -- which is the window in which the
    reply has to describe the document as it is now.
    """

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        # A second writer thread so the parked mutation does not starve the
        # rest of the pool, and enough readers for FT.HYBRID's two arms.
        args["search.writer-threads"] = "2"
        args["search.reader-threads"] = "4"
        return args

    def _run_across_mutation(self, client: Valkey, query, mutation):
        """Returns (rows seen by the parked query, rows seen afterwards)."""
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
        q_thread, res, err = run_in_thread(query)
        # The query must be parked behind the mutation before it is released,
        # or it simply runs after the mutation and proves nothing.
        waiters.wait_for_true(
            lambda: client.info("SEARCH")["search_text_query_blocked_count"]
            >= blocked_before + 1,
            timeout=5)

        client.execute_command(
            "FT._DEBUG", "PAUSEPOINT", "RESET", "mutation_processing")
        mut_thread.join()
        q_thread.join()
        assert mut_err[0] is None
        assert err[0] is None
        IndexingTestHelper.wait_for_indexing_complete_on_node(
            client, self.INDEX)
        return res[0], query()


@pytest.mark.skip(reason=(
    "Defect V4: https://github.com/valkey-io/valkey-search/issues/1415"
    " -- a `text=>[KNN ...]` reply keeps the pre-mutation BM25 score and the"
    " pre-mutation rank for a document rewritten mid-query, beside fresh"
    " content and a freshly recomputed distance."))
class TestStaleTextScoreOnVectorQuery(_ParkedMutation,
                                      ValkeySearchTestCaseDebugMode):
    """V4. Demonstrated through FT.SEARCH, because no FT.HYBRID arm can be
    written in this shape: the SEARCH arm refuses a vector query outright
    ("A vector query is not supported in the SEARCH clause; use VSIM") and a
    VSIM arm carrying a text FILTER sets `vector_score_only`, which keeps its
    score a distance and takes it out of this defect."""

    INDEX = "v4_idx"
    Q = _vec(0.0, 0.0, 0.0, 0.0)

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "p:",
            "SCHEMA", "title", "TEXT", "NOSTEM",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        # Ranked by BM25: p:1 has the most `hello`, p:3 the fewest in the
        # longest document. The vectors run the other way, so a reply ordered
        # by distance is impossible to mistake for one ordered by relevance.
        client.execute_command("HSET", "p:1", "title", "hello hello hello",
                               "vec", _vec(3.0, 0.0, 0.0, 0.0))
        client.execute_command("HSET", "p:2", "title", "hello hello other",
                               "vec", _vec(2.0, 0.0, 0.0, 0.0))
        client.execute_command("HSET", "p:3", "title", "hello a b c d e f g",
                               "vec", _vec(1.0, 0.0, 0.0, 0.0))
        IndexingTestHelper.wait_for_indexing_complete_on_node(
            client, self.INDEX)

    def _search(self):
        """[(key, score, dist, title)] in reply order."""
        result = self.server.get_new_client().execute_command(
            "FT.SEARCH", self.INDEX, "@title:hello=>[KNN 3 @vec $q AS dist]",
            "WITHSCORES", "RETURN", "2", "dist", "title",
            "DIALECT", "2", "PARAMS", "2", "q", self.Q)
        rows = []
        for i in range(1, len(result), 3):
            fields = {result[i + 2][j]: result[i + 2][j + 1]
                      for j in range(0, len(result[i + 2]), 2)}
            rows.append((result[i], float(result[i + 1]),
                         float(fields[b"dist"]), fields[b"title"]))
        return rows

    def test_bm25_score_and_rank_follow_the_mutation(self):
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)
        assert [r[0] for r in self._search()] == [b"p:1", b"p:2", b"p:3"]

        # p:1 keeps matching but becomes the least relevant document: one
        # `hello` in a long body. Its vector moves at the same time, so the
        # reply's three descriptions of it can be compared against each other.
        new_title = "hello " + _words("f", 40)
        parked, fresh = self._run_across_mutation(
            client, self._search,
            ("HSET", "p:1", "title", new_title,
             "vec", _vec(9.0, 0.0, 0.0, 0.0)))

        parked_by_key = {r[0]: r for r in parked}
        fresh_by_key = {r[0]: r for r in fresh}

        # The reply already carries the new body and the new distance...
        assert parked_by_key[b"p:1"][3] == new_title.encode(), (
            "content fetch did not see the mutation; the test parked nothing")
        assert parked_by_key[b"p:1"][2] == pytest.approx(81.0), (
            f"distance was not recomputed against the new vector: "
            f"{parked_by_key[b'p:1'][2]}")

        # ...so the score beside them has to be the new body's score.
        assert parked_by_key[b"p:1"][1] == pytest.approx(
            fresh_by_key[b"p:1"][1]), (
            f"stale BM25 score for the mutated document: the reply carries "
            f"the new body and the new distance "
            f"{parked_by_key[b'p:1'][2]} beside score "
            f"{parked_by_key[b'p:1'][1]}, but the settled score for that "
            f"body is {fresh_by_key[b'p:1'][1]}")

        # And the row has to sit where that score puts it.
        assert [r[0] for r in parked] == [r[0] for r in fresh], (
            f"stale rank across the mutation: parked "
            f"{[r[0] for r in parked]} vs settled {[r[0] for r in fresh]}")

        # Every score in the reply is the pre-mutation one, not only the
        # mutated document's: the corpus statistics moved too.
        parked_scores = {r[0]: r[1] for r in parked}
        fresh_scores = {r[0]: r[1] for r in fresh}
        assert parked_scores == pytest.approx(fresh_scores), (
            f"stale BM25 scores across the mutation: parked {parked_scores} "
            f"vs settled {fresh_scores}")


@pytest.mark.skip(reason=(
    "Defect V5: https://github.com/valkey-io/valkey-search/issues/1416"
    " -- a match-all (`*`) FT.HYBRID SEARCH arm keeps the pre-mutation score"
    " for a document rewritten mid-query, which moves the fused RRF order."))
class TestStaleMatchAllScore(_ParkedMutation, ValkeySearchTestCaseDebugMode):
    """V5, through the FT.HYBRID `SEARCH "*"` arm: RRF ranks positionally, so
    one stale row in an arm corrupts the whole fused ranking."""

    INDEX = "v5_idx"
    Q = _vec(0.0, 0.0, 0.0, 0.0)

    def setup_index(self, client: Valkey) -> None:
        client.execute_command(
            "FT.CREATE", self.INDEX, "ON", "HASH", "PREFIX", "1", "p:",
            "SCHEMA", "title", "TEXT", "NOSTEM",
            "vec", "VECTOR", "HNSW", "6",
            "TYPE", "FLOAT32", "DIM", "4", "DISTANCE_METRIC", "L2")
        # A match-all score is a pure function of the document's text length,
        # so these three are strictly ordered p:1 > p:2 > p:3, and the vectors
        # agree with that order.
        client.execute_command("HSET", "p:1", "title", "alpha",
                               "vec", _vec(1.0, 0.0, 0.0, 0.0))
        client.execute_command("HSET", "p:2", "title", _words("w", 10),
                               "vec", _vec(2.0, 0.0, 0.0, 0.0))
        client.execute_command("HSET", "p:3", "title", _words("z", 40),
                               "vec", _vec(3.0, 0.0, 0.0, 0.0))
        IndexingTestHelper.wait_for_indexing_complete_on_node(
            client, self.INDEX)

    def _hybrid(self):
        """[{field: value}] in fused order."""
        result = self.server.get_new_client().execute_command(
            "FT.HYBRID", self.INDEX,
            "SEARCH", "*", "YIELD_SCORE_AS", "s",
            "VSIM", "@vec", "$q", "KNN", "2", "K", "3", "YIELD_SCORE_AS", "v",
            "COMBINE", "RRF", "2", "YIELD_SCORE_AS", "h",
            "PARAMS", "2", "q", self.Q)
        return [{rec[i]: rec[i + 1] for i in range(0, len(rec), 2)}
                for rec in result[1:]]

    def test_match_all_score_and_fused_order_follow_the_mutation(self):
        client: Valkey = self.server.get_new_client()
        self.setup_index(client)
        assert [r[b"__key"] for r in self._hybrid()] == [b"p:1", b"p:2",
                                                         b"p:3"]

        # p:1 grows from the shortest document to the longest, so its match-all
        # score must fall from best to worst and take its rank with it. The
        # vector is untouched, so the VSIM arm is unchanged and every
        # difference below comes from the SEARCH arm alone.
        parked, fresh = self._run_across_mutation(
            client, self._hybrid, ("HSET", "p:1", "title", _words("g", 60)))

        parked_s = {r[b"__key"]: float(r[b"s"]) for r in parked}
        fresh_s = {r[b"__key"]: float(r[b"s"]) for r in fresh}
        assert parked_s[b"p:1"] == pytest.approx(fresh_s[b"p:1"]), (
            f"stale match-all score for the mutated document: parked "
            f"{parked_s[b'p:1']} vs settled {fresh_s[b'p:1']}")
        assert [r[b"__key"] for r in parked] == [r[b"__key"] for r in fresh], (
            f"stale fused RRF order across the mutation: parked "
            f"{[r[b'__key'] for r in parked]} vs settled "
            f"{[r[b'__key'] for r in fresh]}")
        assert parked_s == pytest.approx(fresh_s), (
            f"stale match-all scores across the mutation: parked {parked_s} "
            f"vs settled {fresh_s}")
