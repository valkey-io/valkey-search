import random
import struct

import pytest

from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector
from util import waiters

DIM = 16
NUM_VECS = 500
NUM_CHURN = 500
NUM_QUERIES = 50
K = 10
# Recall with allow-replace-deleted=yes measured 0.886-0.920 over 8 seeds after
# 500 churns (0.94-0.95 before churn, 0.946-0.970 with =no).
MIN_RECALL = 0.85


def random_vec(rng: random.Random) -> bytes:
    return struct.pack(f'<{DIM}f', *[rng.random() for _ in range(DIM)])


def knn_keys(client: Valkey, index_name: str, query: bytes) -> list:
    result = client.execute_command(
        "FT.SEARCH", index_name, f"*=>[KNN {K} @vector $q]",
        "PARAMS", "2", "q", query, "NOCONTENT", "LIMIT", "0", str(K),
    )
    return result[1:]


def recall(client: Valkey, queries: list) -> float:
    """Fraction of exact (FLAT) top-K neighbors that HNSW also returns."""
    hits = 0
    for q in queries:
        truth = knn_keys(client, "churn_flat", q)
        assert len(truth) == K
        hits += len(set(truth) & set(knn_keys(client, "churn_idx", q)))
    return hits / (K * len(queries))


class TestHNSWOverwriteDelete(ValkeySearchTestCaseDebugMode):
    """
    Churn an HNSW index with delete/insert pairs at a constant population and
    verify whether deleted slots are reused instead of growing the index.
    """

    @pytest.mark.parametrize("allow_replace_deleted", ["yes", "no"])
    def test_delete_insert_churn(self, allow_replace_deleted):
        client: Valkey = self.server.get_new_client()
        # Captured when the index is created, so set it first.
        client.config_set("search.hnsw-allow-replace-deleted",
                          allow_replace_deleted)
        rng = random.Random(1234)

        # INITIAL_CAP == population, so the index is full after the preload
        # and any insert that does not reuse a deleted slot forces a resize.
        index = Index(
            "churn_idx",
            [Vector("vector", DIM, type="HNSW", distance="L2",
                    initialcap=NUM_VECS)],
            prefixes=["doc:"],
        )
        index.create(client)
        # Exact-search index on the same keys, used as recall ground truth.
        flat_index = Index(
            "churn_flat",
            [Vector("vector", DIM, type="FLAT", distance="L2")],
            prefixes=["doc:"],
        )
        flat_index.create(client)

        live = []
        for i in range(NUM_VECS):
            key = f"doc:{i}"
            client.hset(key, mapping={"vector": random_vec(rng)})
            live.append(key)
        waiters.wait_for_equal(lambda: index.info(client).num_docs, NUM_VECS)
        waiters.wait_for_equal(
            lambda: flat_index.info(client).num_docs, NUM_VECS)
        queries = [random_vec(rng) for _ in range(NUM_QUERIES)]
        recall_before = recall(client, queries)
        assert recall_before >= MIN_RECALL

        def capacity():
            attr = index.info(client).get_attribute_by_name("vector")
            return int(attr["index"]["capacity"])

        assert capacity() == NUM_VECS
        exc_before = int(client.info("SEARCH").get(
            "search_hnsw_add_exceptions_count", 0))

        next_id = NUM_VECS
        for _ in range(NUM_CHURN):
            victim = live.pop(rng.randrange(len(live)))
            assert client.delete(victim) == 1
            key = f"doc:{next_id}"
            next_id += 1
            client.hset(key, mapping={"vector": random_vec(rng)})
            live.append(key)

        waiters.wait_for_equal(lambda: index.info(client).num_docs, NUM_VECS)
        waiters.wait_for_equal(
            lambda: flat_index.info(client).num_docs, NUM_VECS)

        exc_after = int(client.info("SEARCH").get(
            "search_hnsw_add_exceptions_count", 0))
        assert exc_after == exc_before

        if allow_replace_deleted == "yes":
            # Every insert reclaimed a tombstoned slot; no resize happened.
            assert capacity() == NUM_VECS
        else:
            # Control: without slot reuse the same workload must grow the
            # index, proving the capacity check above is meaningful.
            assert capacity() > NUM_VECS

        # The most recently inserted vector must be its own nearest neighbor.
        last_key = live[-1]
        last_vec = client.hget(last_key, "vector")
        result = client.execute_command(
            "FT.SEARCH", "churn_idx", "*=>[KNN 1 @vector $q]",
            "PARAMS", "2", "q", last_vec, "NOCONTENT",
        )
        assert result[0] == 1
        assert result[1] == last_key.encode()

        recall_after = recall(client, queries)
        assert recall_after >= MIN_RECALL, \
            (f"[allow_replace_deleted={allow_replace_deleted}] recall "
             f"{recall_after} after churn (was {recall_before} before)")

        client.execute_command("FT.DROPINDEX", "churn_idx")
        client.execute_command("FT.DROPINDEX", "churn_flat")
