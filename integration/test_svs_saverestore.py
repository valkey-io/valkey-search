"""
Integration test for SVS RDB persistence.

Verifies that an SVS Vamana index can be saved (SAVE) and restored
on restart with recall >= 0.95 for a KNN query.

Requires the module to be built with ENABLE_SVS=ON. Tests are skipped
automatically if the SVS algorithm is not available.
"""

import os
import struct
import numpy as np
from valkey import ResponseError
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser
import pytest

NUM_VECTORS = 1000
DIM = 128
K = 10


def random_vectors(n, dim, seed=42):
    rng = np.random.default_rng(seed)
    return rng.random((n, dim), dtype=np.float32)


def float_vector_to_bytes(vec):
    return struct.pack(f"<{len(vec)}f", *vec)


def compute_l2_distances(query, vectors):
    diff = vectors - query
    return np.sum(diff * diff, axis=1)


def brute_force_knn(query, vectors, k):
    distances = compute_l2_distances(query, vectors)
    indices = np.argsort(distances)[:k]
    return set(indices.tolist())


class TestSVSSaveRestore(ValkeySearchTestCaseBase):
    """Test SVS index persistence through SAVE/restart cycle."""

    def create_svs_index(self, client):
        cmd = [
            "FT.CREATE", "svs_idx",
            "ON", "HASH",
            "PREFIX", "1", "vec:",
            "SCHEMA",
            "v", "VECTOR", "SVS",
            str(6 + 2),
            "TYPE", "FLOAT32",
            "DIM", str(DIM),
            "DISTANCE_METRIC", "L2",
            "GRAPH_MAX_DEGREE", "32",
        ]
        try:
            client.execute_command(*cmd)
        except ResponseError as e:
            err_msg = str(e)
            if "Unsupported algorithm" in err_msg or "Unknown argument" in err_msg:
                pytest.skip("SVS not available (module built without ENABLE_SVS)")
            raise

    def load_vectors(self, client, vectors):
        for i, vec in enumerate(vectors):
            client.hset(f"vec:{i:06d}", mapping={
                "v": float_vector_to_bytes(vec)
            })

    def knn_search(self, client, query_vec, k):
        query_bytes = float_vector_to_bytes(query_vec)
        cmd = [
            "FT.SEARCH", "svs_idx",
            f"*=>[KNN {k} @v $query_vec]",
            "PARAMS", "2", "query_vec", query_bytes,
            "LIMIT", "0", str(k),
        ]
        result = client.execute_command(*cmd)
        keys = []
        # Result format: [count, key1, [fields1], key2, [fields2], ...]
        for i in range(1, len(result), 2):
            key = result[i]
            if isinstance(key, bytes):
                key = key.decode()
            idx = int(key.split(":")[1])
            keys.append(idx)
        return set(keys)

    def wait_for_index_ready(self, client):
        waiters.wait_for_true(
            lambda: self._backfill_complete(client),
            timeout=60,
        )
        # Wait for SVS buffer flush — num_docs reflects indexed vectors
        waiters.wait_for_true(
            lambda: self._docs_indexed(client, NUM_VECTORS),
            timeout=60,
        )

    def _docs_indexed(self, client, expected):
        try:
            info = FTInfoParser(
                client.execute_command("FT.INFO", "svs_idx"))
            return info.num_docs >= expected
        except Exception:
            return False

    def test_svs_save_restore_recall(self):
        """Insert vectors, SAVE, restart, verify recall >= 0.95."""
        vectors = random_vectors(NUM_VECTORS, DIM)

        self.create_svs_index(self.client)
        self.load_vectors(self.client, vectors)
        self.wait_for_index_ready(self.client)

        # Verify search works before save
        query = vectors[0]
        expected = brute_force_knn(query, vectors, K)
        result_before = self.knn_search(self.client, query, K)
        recall_before = len(result_before & expected) / K
        assert recall_before >= 0.9, (
            f"Pre-save recall too low: {recall_before}"
        )

        # Save and restart
        self.client.execute_command("SAVE")
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        self.client.ping()
        self.wait_for_index_ready(self.client)

        # Verify search after restore
        result_after = self.knn_search(self.client, query, K)
        recall_after = len(result_after & expected) / K
        assert recall_after >= 0.95, (
            f"Post-restore recall too low: {recall_after}"
        )

        # Verify with a second query
        query2 = vectors[NUM_VECTORS // 2]
        expected2 = brute_force_knn(query2, vectors, K)
        result2 = self.knn_search(self.client, query2, K)
        recall2 = len(result2 & expected2) / K
        assert recall2 >= 0.95, (
            f"Post-restore recall (query 2) too low: {recall2}"
        )

    def test_svs_save_restore_count(self):
        """Verify all vectors are present after restore."""
        vectors = random_vectors(NUM_VECTORS, DIM)

        self.create_svs_index(self.client)
        self.load_vectors(self.client, vectors)
        self.wait_for_index_ready(self.client)

        info = FTInfoParser(
            self.client.execute_command("FT.INFO", "svs_idx"))
        assert info.num_docs == NUM_VECTORS

        # Save and restart
        self.client.execute_command("SAVE")
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        self.client.ping()
        self.wait_for_index_ready(self.client)

        info = FTInfoParser(
            self.client.execute_command("FT.INFO", "svs_idx"))
        assert info.num_docs == NUM_VECTORS

    def test_svs_bgsave_no_crash(self):
        """BGSAVE with a populated SVS index must complete without crashing.

        Current expected behavior: the SVS graph is not persisted
        (has_graph_data=0) due to a C++ ABI incompatibility under
        RTLD_DEEPBIND; the sigsetjmp workaround in PreSerializeForRDB
        intercepts the resulting SIGABRT and allows BGSAVE to complete
        safely. The server must respond to PING after BGSAVE finishes.
        """
        vectors = random_vectors(NUM_VECTORS, DIM)
        self.create_svs_index(self.client)
        self.load_vectors(self.client, vectors)
        self.wait_for_index_ready(self.client)

        self.client.execute_command("BGSAVE")
        # Wait for BGSAVE to finish before pinging.
        waiters.wait_for_false(
            lambda: self.client.info("persistence")["rdb_bgsave_in_progress"],
            timeout=30,
        )
        # If BGSAVE caused an unhandled SIGABRT the server would be dead
        # and this PING would raise a connection error.
        assert self.client.ping()

    def _backfill_complete(self, client):
        try:
            info = FTInfoParser(
                client.execute_command("FT.INFO", "svs_idx"))
            return not info.backfill_in_progress
        except Exception:
            return False
