"""
Integration test: SVS SQ8 compression validation.

Validates that:
1. SVS accepts COMPRESSION SQ8 and reports it in FT.INFO
2. SQ8 compressed index maintains acceptable search recall

Uses the "load keys first, create index after" pattern so backfill
ingests vectors without hitting the blocked-client write path.

Requires ENABLE_SVS=ON. Skipped if SVS is not available.
"""

import struct
from typing import List
import numpy as np
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from ft_info_parser import FTInfoParser
import pytest

NUM_VECTORS = 1000
DIM = 128
K = 10
GRAPH_MAX_DEGREE = 32


def random_vectors(n, dim, seed=42):
    rng = np.random.default_rng(seed)
    return rng.random((n, dim), dtype=np.float32)


def float_vector_to_bytes(vec):
    return struct.pack(f"<{len(vec)}f", *vec)


def brute_force_knn(query, vectors, k):
    diff = vectors - query
    distances = np.sum(diff * diff, axis=1)
    indices = np.argsort(distances)[:k]
    return set(indices.tolist())


class TestSVSSQ8Compression(ValkeySearchTestCaseBase):
    """Test that SVS SQ8 compression is configured and functional."""

    def get_config_file_lines(self, testdir, port) -> List[str]:
        lines = super().get_config_file_lines(testdir, port)
        lines.append("save \"\"")
        return lines

    def create_svs_index(self, client, index_name, compression="NONE"):
        """Create an SVS index with specified compression."""
        num_args = 6 + 2 + 2
        cmd = [
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", f"{index_name}:",
            "SCHEMA",
            "v", "VECTOR", "SVS", str(num_args),
            "TYPE", "FLOAT32",
            "DIM", str(DIM),
            "DISTANCE_METRIC", "L2",
            "GRAPH_MAX_DEGREE", str(GRAPH_MAX_DEGREE),
            "COMPRESSION", compression,
        ]
        try:
            client.execute_command(*cmd)
        except ResponseError as e:
            err_msg = str(e)
            if "Unsupported algorithm" in err_msg or "Unknown argument" in err_msg:
                pytest.skip("SVS not available (module built without ENABLE_SVS)")
            raise

    def load_vectors(self, client, prefix, vectors):
        """Load vectors into hash keys with the given prefix."""
        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"{prefix}:{i:06d}", mapping={
                "v": float_vector_to_bytes(vec)
            })
            if (i + 1) % 500 == 0:
                pipe.execute()
                pipe = client.pipeline(transaction=False)
        pipe.execute()

    def wait_for_docs_indexed(self, client, index_name, expected):
        """Wait for num_docs to reach expected count."""
        waiters.wait_for_true(
            lambda: self._check_docs(client, index_name, expected),
            timeout=120,
        )

    def _check_docs(self, client, index_name, expected):
        try:
            info = FTInfoParser(
                client.execute_command("FT.INFO", index_name))
            return info.num_docs >= expected
        except Exception:
            return False

    def knn_search(self, client, index_name, query_vec, k):
        """Execute KNN search and return set of result indices."""
        query_bytes = float_vector_to_bytes(query_vec)
        result = client.execute_command(
            "FT.SEARCH", index_name,
            f"*=>[KNN {k} @v $query_vec]",
            "PARAMS", "2", "query_vec", query_bytes,
            "LIMIT", "0", str(k),
        )
        keys = []
        for i in range(1, len(result), 2):
            key = result[i]
            if isinstance(key, bytes):
                key = key.decode()
            idx = int(key.split(":")[1])
            keys.append(idx)
        return set(keys)

    def test_sq8_compression_reported_in_info(self):
        """
        Verify FT.INFO reports compression=SQ8 for an SQ8-configured index.
        """
        index_name = "svs_sq8_info"
        self.create_svs_index(self.client, index_name, compression="SQ8")

        info = FTInfoParser(
            self.client.execute_command("FT.INFO", index_name))
        algo = info.get_vector_algorithm("v")

        assert algo is not None, "Algorithm section not found in FT.INFO"
        assert algo.get("name") == "SVS", (
            f"Expected algorithm SVS, got {algo.get('name')}")
        assert algo.get("compression") == "SQ8", (
            f"Expected compression SQ8, got {algo.get('compression')}")

    def test_sq8_maintains_recall(self):
        """
        Verify SQ8 compressed index produces reasonable search recall.

        Loads vectors BEFORE creating the index so backfill handles
        ingestion (avoids the blocked-client write path).
        """
        vectors = random_vectors(NUM_VECTORS, DIM, seed=123)
        index_name = "svs_sq8_recall"

        # Load vectors first (prefix must match the index PREFIX)
        self.load_vectors(self.client, index_name, vectors)

        # Create index — backfill will ingest the existing keys
        self.create_svs_index(self.client, index_name, compression="SQ8")
        self.wait_for_docs_indexed(self.client, index_name, NUM_VECTORS)

        # Run KNN queries and measure recall
        num_queries = 10
        total_recall = 0.0
        for qi in range(num_queries):
            query = vectors[qi * (NUM_VECTORS // num_queries)]
            expected = brute_force_knn(query, vectors, K)
            result = self.knn_search(self.client, index_name, query, K)
            recall = len(result & expected) / K
            total_recall += recall

        avg_recall = total_recall / num_queries
        assert avg_recall >= 0.80, (
            f"SQ8 average recall too low: {avg_recall:.3f} "
            f"(expected >= 0.80 for 128-dim random data)"
        )

    def test_fp32_and_sq8_both_return_results(self):
        """
        Verify both FP32 and SQ8 indexes return search results for the
        same dataset, confirming SQ8 doesn't silently fail.
        """
        vectors = random_vectors(NUM_VECTORS, DIM, seed=456)
        fp32_index = "svs_fp32_cmp"
        sq8_index = "svs_sq8_cmp"

        # Load vectors with prefixes matching each index
        self.load_vectors(self.client, fp32_index, vectors)
        self.load_vectors(self.client, sq8_index, vectors)

        # Create both indexes — backfill ingests
        self.create_svs_index(self.client, fp32_index, compression="NONE")
        self.create_svs_index(self.client, sq8_index, compression="SQ8")
        self.wait_for_docs_indexed(self.client, fp32_index, NUM_VECTORS)
        self.wait_for_docs_indexed(self.client, sq8_index, NUM_VECTORS)

        # Both should return K results
        query = vectors[0]
        fp32_results = self.knn_search(self.client, fp32_index, query, K)
        sq8_results = self.knn_search(self.client, sq8_index, query, K)

        assert len(fp32_results) == K, (
            f"FP32 returned {len(fp32_results)} results, expected {K}")
        assert len(sq8_results) == K, (
            f"SQ8 returned {len(sq8_results)} results, expected {K}")

        # Both should find the query vector itself (index 0) as nearest neighbor
        assert 0 in fp32_results, "FP32 didn't find the query vector itself"
        assert 0 in sq8_results, "SQ8 didn't find the query vector itself"
