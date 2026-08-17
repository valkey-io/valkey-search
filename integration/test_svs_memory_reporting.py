"""
Integration test: SVS memory reporting via used_memory_bytes.

Verifies that INFO SEARCH used_memory_bytes increases after SVS vectors
are flushed to the graph, and decreases after FT.DROPINDEX.

Uses the "load keys first, create index after" pattern so backfill
ingests vectors without hitting the blocked-client write path.

Requires ENABLE_SVS=ON. Skipped automatically if SVS is not available.
"""

import struct
from typing import List
import numpy as np
from valkey import ResponseError
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
import pytest

# Exactly kBufferSize so at least one FlushBuffer() fires during backfill.
NUM_VECTORS = 10000
DIM = 128
GRAPH_MAX_DEGREE = 32


def random_vectors(n, dim, seed=42):
    rng = np.random.default_rng(seed)
    return rng.random((n, dim), dtype=np.float32)


def float_vector_to_bytes(vec):
    return struct.pack(f"<{len(vec)}f", *vec)


class TestSVSMemoryReporting(ValkeySearchTestCaseBase):
    """Verify SVS graph memory is visible in INFO SEARCH used_memory_bytes."""

    def get_config_file_lines(self, testdir, port) -> List[str]:
        lines = super().get_config_file_lines(testdir, port)
        lines.append("save \"\"")
        return lines

    def _create_svs_index(self, client, index_name):
        cmd = [
            "FT.CREATE", index_name,
            "ON", "HASH",
            "PREFIX", "1", f"{index_name}:",
            "SCHEMA",
            "v", "VECTOR", "SVS", "8",
            "TYPE", "FLOAT32",
            "DIM", str(DIM),
            "DISTANCE_METRIC", "L2",
            "GRAPH_MAX_DEGREE", str(GRAPH_MAX_DEGREE),
        ]
        try:
            client.execute_command(*cmd)
        except ResponseError as e:
            err_msg = str(e)
            if "Unsupported algorithm" in err_msg or "Unknown argument" in err_msg:
                pytest.skip("SVS not available (module built without ENABLE_SVS)")
            raise

    def _load_vectors(self, client, prefix, vectors):
        pipe = client.pipeline(transaction=False)
        for i, vec in enumerate(vectors):
            pipe.hset(f"{prefix}:{i:06d}", mapping={"v": float_vector_to_bytes(vec)})
            if (i + 1) % 500 == 0:
                pipe.execute()
                pipe = client.pipeline(transaction=False)
        pipe.execute()

    def _wait_for_indexed(self, client, index_name, expected):
        def check():
            try:
                info = client.execute_command("FT.INFO", index_name)
                pairs = dict(zip(info[::2], info[1::2]))
                return int(pairs.get(b"num_docs", 0)) >= expected
            except Exception:
                return False
        waiters.wait_for_true(check, timeout=120)

    def _used_memory_bytes(self, client):
        info = client.info("search")
        return int(info["search_used_memory_bytes"])

    def test_used_memory_bytes_increases_after_svs_flush(self):
        """used_memory_bytes grows by at least raw vector bytes after a flush."""
        vectors = random_vectors(NUM_VECTORS, DIM, seed=0)
        index_name = "svs_mem_increase"

        self._load_vectors(self.client, index_name, vectors)
        memory_before = self._used_memory_bytes(self.client)

        self._create_svs_index(self.client, index_name)
        self._wait_for_indexed(self.client, index_name, NUM_VECTORS)

        memory_after = self._used_memory_bytes(self.client)
        delta = memory_after - memory_before

        # SVS graph must occupy at least the raw vector data.
        min_expected = NUM_VECTORS * DIM * 4  # float32 = 4 bytes
        assert delta >= min_expected, (
            f"used_memory_bytes delta {delta} < minimum expected {min_expected} "
            f"({NUM_VECTORS} vectors × {DIM} dims × 4 bytes)"
        )

    def test_used_memory_bytes_decreases_after_dropindex(self):
        """used_memory_bytes decreases after FT.DROPINDEX and never underflows."""
        vectors = random_vectors(NUM_VECTORS, DIM, seed=1)
        index_name = "svs_mem_drop"

        self._load_vectors(self.client, index_name, vectors)
        self._create_svs_index(self.client, index_name)
        self._wait_for_indexed(self.client, index_name, NUM_VECTORS)

        memory_before_drop = self._used_memory_bytes(self.client)
        self.client.execute_command("FT.DROPINDEX", index_name)

        vector_bytes = NUM_VECTORS * DIM * 4
        edge_bytes = NUM_VECTORS * GRAPH_MAX_DEGREE * 4
        min_freed = vector_bytes + edge_bytes

        waiters.wait_for_true(
            lambda: (memory_before_drop - self._used_memory_bytes(self.client))
            >= min_freed,
            timeout=30,
        )

        memory_after_drop = self._used_memory_bytes(self.client)
        freed = memory_before_drop - memory_after_drop
        assert freed >= min_freed, (
            f"DROPINDEX freed only {freed} bytes; "
            f"expected >= {min_freed} "
            f"(vectors={vector_bytes} + edges={edge_bytes})"
        )
