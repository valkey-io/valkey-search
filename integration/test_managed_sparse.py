"""Managed sparse indexing, mutation, persistence and dense/sparse rank fusion."""

import math
import random
import struct

import pytest
from valkey.exceptions import ResponseError
from valkey_search_test_case import ValkeySearchClusterTestCase, ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


def sparse(entries):
    """Encode sorted dimensions using the portable HASH wire format."""
    return b"".join(struct.pack("<If", dim, weight) for dim, weight in entries)


def search(client, query, k=10, predicate="*", field="s"):
    """Return (key, distance) pairs from the normal FT.SEARCH KNN path."""
    result = client.execute_command(
        "FT.SEARCH", "idx", f"({predicate})=>[KNN {k} @{field} $q AS d]",
        "PARAMS", 2, "q", query, "RETURN", 1, "d", "LIMIT", 0, k,
    )
    return [(result[i], float(dict(zip(result[i+1][::2], result[i+1][1::2]))[b"d"]))
            for i in range(1, len(result), 2)]


def check_malformed_queries(client):
    """A malformed final pair fails even on an empty shard or beside a valid arm."""
    prefix = sparse((i, 1) for i in range(1024))
    bad = prefix + sparse([(1023, 1)])
    term = "strictly increasing dimensions"
    with pytest.raises(ResponseError, match=term):
        search(client, bad)
    with pytest.raises(ResponseError, match=term):
        client.execute_command("FT.SEARCH", "idx", "*=>[KNN 1 @s $q]", "INKEYS", 0,
                               "PARAMS", 2, "q", bad)
    for clause in ([], ["FILTER", 1, "@region:{one}"]):
        with pytest.raises(ResponseError, match=term):
            client.execute_command(
                "FT.HYBRID", "idx", "VSIM", "@v", "$v", "KNN", 2, "K", 1,
                "VSIM", "@s", "$q", "KNN", 2, "K", 1, *clause, "COMBINE", "RRF", 0,
                "PARAMS", 4, "q", bad, "v", struct.pack("<2f", 1, 0),
            )


class TestManagedSparse(ValkeySearchTestCaseBase):
    """Exercise the managed index through public commands on a real server."""

    def setup_index(self, client):
        """Create sparse, dense and region fields sharing the same documents."""
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "doc:",
            "SCHEMA", "s", "SPARSE", "region", "TAG",
            "v", "VECTOR", "FLAT", 6,
            "TYPE", "FLOAT32", "DIM", 2, "DISTANCE_METRIC", "L2",
        )

    def test_mutations_filters_and_reload(self):
        """Updates/deletes remove old postings; RDB rebuild preserves results."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for key, entries, region in [
            ("a", [(1, 2), (7, 1)], "one"),
            ("b", [(1, 4)], "two"),
            ("c", [(7, -2)], "one"),
        ]:
            client.hset(f"doc:{key}", mapping={"s": sparse(entries), "region": region})
        q = sparse([(1, 1), (7, 2)])
        waiters.wait_for_true(lambda: len(search(client, q)) == 3, timeout=10)
        assert search(client, q) == [(b"doc:a", -4), (b"doc:b", -4), (b"doc:c", 4)]
        assert search(client, q, 1, "@region:{one}") == [(b"doc:a", -4)]
        client.hset("doc:a", "s", sparse([(9, 10)]))
        waiters.wait_for_true(lambda: b"doc:a" not in dict(search(client, q)), timeout=10)
        client.delete("doc:b")
        assert b"doc:b" not in dict(search(client, q))
        waiters.wait_for_true(lambda: search(client, q) == [(b"doc:c", 4)], timeout=10)
        client.execute_command("SAVE")
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        waiters.wait_for_true(lambda: search(client, q) == [(b"doc:c", 4)], timeout=10)
        client.hdel("doc:c", "s")
        waiters.wait_for_true(lambda: search(client, q) == [], timeout=10)

    def test_validation_and_invalid_update(self):
        """Malformed query values fail and invalid updates disappear."""
        client = self.server.get_new_client()
        self.setup_index(client)
        for blob in [b"x", sparse([(2, 1), (1, 2)]), sparse([(1, math.inf)]),
                     sparse([(1, math.nan)]), sparse([(1, 1), (1, 2)])]:
            with pytest.raises(ResponseError, match="SPARSE"):
                search(client, blob)
        client.hset("doc:a", "s", sparse([(1, 3)]))
        q = sparse([(1, 1)])
        waiters.wait_for_true(lambda: len(search(client, q)) == 1, timeout=10)
        client.hset("doc:a", "s", b"invalid")
        waiters.wait_for_true(lambda: search(client, q) == [], timeout=10)
        client.hset("doc:a", "s", sparse([(1, 5)]))
        waiters.wait_for_true(lambda: search(client, q) == [(b"doc:a", -5)], timeout=10)
        with pytest.raises(ResponseError):
            search(client, b"")
        assert search(client, sparse([(1, 0)])) == []

    @pytest.mark.parametrize("partial", ["yes", "no"])
    def test_malformed_sparse_request(self, partial):
        """Local readers return validation errors in either partial-results mode."""
        client = self.server.get_new_client()
        self.setup_index(client)
        client.config_set("search.enable-partial-results", partial)
        check_malformed_queries(client)
        # MULTI forces FT.SEARCH down the synchronous execution path.
        with client.pipeline(transaction=True) as pipe:
            pipe.execute_command("FT.SEARCH", "idx", "*=>[KNN 1 @s $q]", "PARAMS",
                                 2, "q", sparse([(1, 1), (2, math.nan)]))
            with pytest.raises(ResponseError, match="finite weights"):
                pipe.execute()

    def test_sparse_query_budget(self):
        """Sparse arms share a byte budget, including repeated and filtered inputs."""
        client = self.server.get_new_client()
        self.setup_index(client)
        half = sparse((i, 1) for i in range(4096))
        full = half + sparse((i, 1) for i in range(4096, 8192))
        extra = sparse([(8192, 1)])
        assert search(client, full) == []
        with pytest.raises(ResponseError, match="64 KiB"):
            search(client, full + extra)
        arm = ["VSIM", "@s", "$q", "KNN", 2, "K", 1]
        for clause in ([], ["FILTER", 1, "@region:{one}"]):
            args = ["FT.HYBRID", "idx", *arm, *arm, *clause, "COMBINE", "RRF", 0,
                    "PARAMS", 2, "q"]
            assert client.execute_command(*args, half) == [0]
            with pytest.raises(ResponseError, match="64 KiB"):
                client.execute_command(*args, half + extra)
            with pytest.raises(ResponseError, match="uint32/FLOAT32 pairs"):
                client.execute_command(*args, half[:-1])

        client.hset("doc:long", "s", full + extra)
        waiters.wait_for_true(lambda: search(client, extra) == [(b"doc:long", -1)])

    def test_random_dot_products_and_dense_sparse_rrf(self):
        """Compare both retrieval and fusion with an independent exhaustive oracle."""
        client = self.server.get_new_client()
        self.setup_index(client)
        rng = random.Random(16621)
        docs = []
        for i in range(100):
            entries = [(d, float(rng.randrange(-8, 9)))
                       for d in sorted(rng.sample(range(32), 8))]
            dense = [float(i), float(100-i)]
            key = f"doc:{i:03d}".encode()
            docs.append((key, dict(entries), dense))
            client.hset(key, mapping={"s": sparse(entries), "v": struct.pack("<2f", *dense),
                                      "region": str(i % 3)})
        q = [(1, 1), (4, 2), (16, -1), (25, 3)]
        blob = sparse(q)
        oracle = sorted(((-sum(w * entries.get(d, 0) for d, w in q), key)
                         for key, entries, _ in docs
                         if any(entries.get(d, 0) for d, _ in q)))
        waiters.wait_for_true(lambda: len(search(client, blob, 100)) == len(oracle), timeout=10)
        assert search(client, blob, 20) == [(key, score) for score, key in oracle[:20]]
        dense_q = struct.pack("<2f", 31.25, 68.75)
        dense_rank = sorted((sum((x-y)**2 for x, y in zip(v, [31.25, 68.75])), key)
                            for key, _, v in docs)
        expected = {}
        for ranking in [oracle[:20], dense_rank[:20]]:
            for rank, (_, key) in enumerate(ranking, 1):
                expected[key] = expected.get(key, 0) + 1 / (60 + rank)
        # The shared fusion result type stores the final score as FLOAT32.
        rounded = {key: struct.unpack("<f", struct.pack("<f", score))[0]
                   for key, score in expected.items()}
        expected_keys = sorted(rounded, key=lambda key: (-rounded[key], key))[:10]
        response = client.execute_command(
            "FT.HYBRID", "idx", "VSIM", "@s", "$s", "KNN", 2, "K", 20,
            "YIELD_SCORE_AS", "sparse_score",
            "VSIM", "@v", "$v", "KNN", 2, "K", 20,
            "COMBINE", "RRF", 4, "CONSTANT", 60, "WINDOW", 20,
            "LOAD", 1, "@__key", "LIMIT", 0, 10,
            "PARAMS", 4, "s", blob, "v", dense_q,
        )
        rows = [dict(zip(row[::2], row[1::2])) for row in response[1:]]
        assert [row[b"__key"] for row in rows] == expected_keys
        sparse_scores = {key: -distance for distance, key in oracle[:20]}
        for row in rows:
            if row[b"__key"] in sparse_scores:
                assert float(row[b"sparse_score"]) == sparse_scores[row[b"__key"]]

    def test_sparse_arm_options(self):
        c = self.server.get_new_client()
        self.setup_index(c)
        arm = ["VSIM", "@s", "$q", "KNN", 2, "K", 1]
        params = ["PARAMS", 2, "q", sparse([(1, 1)])]
        with pytest.raises(ResponseError, match="Duplicate per-arm"):
            c.execute_command("FT.HYBRID", "idx", *(arm + ["YIELD_SCORE_AS", "same"])*2,
                              "COMBINE", "RRF", 0, *params)
        for policy in ("ADHOC", "BATCHES"):
            with pytest.raises(ResponseError, match="SPARSE does not support"):
                c.execute_command("FT.HYBRID", "idx", *arm, *arm, "FILTER", 3,
                                  "@region:{one}", "POLICY", policy, "COMBINE", "RRF", 0, *params)

    @pytest.mark.parametrize("n", [3, 5, 6])
    def test_multiple_sparse_arms(self, n):
        c = self.server.get_new_client()
        self.setup_index(c)
        for i in range(n):
            c.hset(f"doc:{i}", "s", sparse([(i, 1)]))
        waiters.wait_for_true(lambda: c.execute_command("FT.SEARCH", "idx", "*", "LIMIT", 0, 0)[0] == n)
        arms, params = [], []
        for i in range(n):
            arms.extend(["VSIM", "@s", f"$q{i}", "KNN", 2, "K", 1])
            params.extend([f"q{i}", sparse([(i, 1)])])
        args = ["FT.HYBRID", "idx", *arms, "COMBINE", "RRF", 0, "LOAD", 1, "@__key",
                "LIMIT", 0, n, "PARAMS", len(params), *params]
        if n == 6:
            with pytest.raises(ResponseError, match="at most 5"):
                c.execute_command(*args)
        else:
            rows = c.execute_command(*args)[1:]
            assert {dict(zip(row[::2], row[1::2]))[b"__key"] for row in rows} == {
                f"doc:{i}".encode() for i in range(n)}


class TestManagedSparseCluster(ValkeySearchClusterTestCase):
    def test_malformed_sparse_fanout(self):
        """Remote responders must report bad input even when partials are allowed."""
        client = self.client_for_primary(0)
        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "s", "SPARSE", "region", "TAG",
            "v", "VECTOR", "FLAT", 6, "TYPE", "FLOAT32", "DIM", 2, "DISTANCE_METRIC", "L2",
        )
        for partial in ("yes", "no"):
            client.config_set("search.enable-partial-results", partial)
            check_malformed_queries(client)

    def test_filtered_global_topk_after_updates(self):
        """All three coordinators return the same filtered top-K after HASH mutations."""
        client = self.new_cluster_client()
        client.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", 1, "doc:",
                               "SCHEMA", "s", "SPARSE", "region", "TAG")
        weights = {}
        for i in range(96):
            key = f"doc:{i:03d}".encode()
            weight = i % 9 - 4
            client.hset(key, mapping={"s": sparse([(1, weight), (7, 1)]),
                                      "region": str(i % 2)})
            if i % 2:
                weights[key] = weight
        nodes = [self.client_for_primary(i) for i in range(self.CLUSTER_SIZE)]
        assert all(node.dbsize() > 0 for node in nodes)
        query = sparse([(1, 1), (7, 2)])
        def correct():
            expected = sorted(((key, -(w + 2)) for key, w in weights.items()),
                              key=lambda hit: (hit[1], hit[0]))[:10]
            return all(search(node, query, 10, "@region:{1}") == expected for node in nodes)
        waiters.wait_for_true(correct, timeout=15)
        client.hset(b"doc:001", "s", sparse([(1, 100), (7, 1)]))
        weights[b"doc:001"] = 100
        waiters.wait_for_true(correct, timeout=15)
        client.delete(b"doc:017")
        assert all(b"doc:017" not in dict(search(node, query, 10, "@region:{1}")) for node in nodes)
        del weights[b"doc:017"]
        waiters.wait_for_true(correct, timeout=15)
