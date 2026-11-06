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
        client.delete("doc:b")
        waiters.wait_for_true(lambda: search(client, q) == [(b"doc:c", 4)], timeout=10)
        client.execute_command("SAVE")
        self.server.restart(remove_rdb=False)
        client = self.server.get_new_client()
        waiters.wait_for_true(lambda: search(client, q) == [(b"doc:c", 4)], timeout=10)
        client.hdel("doc:c", "s")
        waiters.wait_for_true(lambda: search(client, q) == [], timeout=10)

    def test_validation_and_invalid_update(self):
        """Malformed query values fail before dispatch and invalid updates disappear."""
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

class TestManagedSparseCluster(ValkeySearchClusterTestCase):
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
        client.delete(b"doc:017")
        del weights[b"doc:017"]
        waiters.wait_for_true(correct, timeout=15)
