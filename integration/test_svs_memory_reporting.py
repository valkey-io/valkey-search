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
from ft_info_parser import FTInfoParser
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker  # noqa: F401 — required by base class fixture
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
        waiters.wait_for_true(
            lambda: self._check_docs(client, index_name, expected),
            timeout=120,
        )

    def _check_docs(self, client, index_name, expected):
        try:
            info = FTInfoParser(client.execute_command("FT.INFO", index_name))
            return info.num_docs >= expected
        except Exception:
            return False

    def _get_num_docs(self, client, index_name):
        try:
            info = FTInfoParser(client.execute_command("FT.INFO", index_name))
            return info.num_docs
        except Exception:
            return -1

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

    def test_memory_accounting_full_lifecycle(self):
        """
        Validates used_memory_bytes at every stage of the SVS index lifecycle:
          baseline → FT.CREATE → load+flush → delete subset → FT.DROPINDEX → near baseline.
        """
        vectors = random_vectors(NUM_VECTORS, DIM, seed=2)
        index_name = "svs_mem_lifecycle"
        n_delete = 100

        vector_bytes = NUM_VECTORS * DIM * 4
        edge_bytes = NUM_VECTORS * GRAPH_MAX_DEGREE * 4
        min_graph_bytes = vector_bytes + edge_bytes

        # Stage 1: baseline before any SVS activity.
        baseline = self._used_memory_bytes(self.client)

        # Stage 2: FT.CREATE — empty graph overhead visible immediately.
        self._create_svs_index(self.client, index_name)
        after_create = self._used_memory_bytes(self.client)
        assert after_create > baseline, (
            f"FT.CREATE did not increase used_memory_bytes: "
            f"baseline={baseline} after_create={after_create}"
        )

        # Stage 3: load all vectors and wait for flush.
        self._load_vectors(self.client, index_name, vectors)
        self._wait_for_indexed(self.client, index_name, NUM_VECTORS)
        after_load = self._used_memory_bytes(self.client)
        assert after_load - baseline >= min_graph_bytes, (
            f"used_memory_bytes delta after load ({after_load - baseline}) "
            f"< expected minimum {min_graph_bytes} "
            f"(vectors={vector_bytes} + edges={edge_bytes})"
        )

        # Stage 4: delete a subset of vectors.
        # Vamana is a dense graph — removes tombstone rather than shrink,
        # so memory should stay flat or decrease slightly.
        pipe = self.client.pipeline(transaction=False)
        for i in range(n_delete):
            pipe.delete(f"{index_name}:{i:06d}")
        pipe.execute()
        waiters.wait_for_true(
            lambda: self._get_num_docs(self.client, index_name)
            <= NUM_VECTORS - n_delete,
            timeout=30,
        )
        after_delete = self._used_memory_bytes(self.client)
        assert after_delete <= after_load, (
            f"used_memory_bytes grew after {n_delete} deletes: "
            f"before={after_load} after={after_delete}"
        )

        # Stage 5: FT.DROPINDEX — wait for full memory reclaim (graph + intern store).
        # DROPINDEX is asynchronous; the destructor frees both the SVS graph and
        # the intern store on a background thread. Wait until used_memory_bytes
        # returns near baseline before asserting.
        self.client.execute_command("FT.DROPINDEX", index_name)
        tolerance = max(512 * 1024, int(baseline * 0.05))
        waiters.wait_for_true(
            lambda: self._used_memory_bytes(self.client) <= baseline + tolerance,
            timeout=60,
        )
        after_drop = self._used_memory_bytes(self.client)
        assert after_drop <= baseline + tolerance, (
            f"used_memory_bytes after DROPINDEX ({after_drop}) did not return near "
            f"baseline ({baseline} ± {tolerance})"
        )
