"""
Save/restore tests for FP16 (FLOAT16) vector indexes.

The base test_saverestore.py only exercises FLOAT32 vectors. These tests
cover the FP16 HNSW (and FLAT) save/restore code paths: that the index
schema and tracked vectors survive RDB save+restart and that KNN search
returns the same nearest neighbors as before the restart.
"""

import os
import struct
from typing import List, Tuple

import pytest
from valkey import Valkey

from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector, Numeric, Tag, float16_to_bytes
from util import waiters


DIM = 4

# Vector count for HNSW tests: chosen to reliably populate multiple layers of
# the HNSW graph so that the upper-layer link lists, per-node level array,
# enterpoint_node_, and maxlevel_ all participate in save/restore.
#
# hnswlib level distribution: P(level >= L) = (1/M)^L. With M=4 and 1000
# vectors the expected populations are L>=1 ~250, L>=2 ~62, L>=3 ~16,
# L>=4 ~4. hnswlib's level RNG is seeded deterministically (default seed
# 100), so given the same insertion order this is reproducible.
NUM_VECTORS_HNSW = 1000

# FLAT has no layered graph, so a smaller count is enough and keeps the
# parametrized matrix fast.
NUM_VECTORS_FLAT = 100

# K values for the KNN probes. Several are picked so an upper-layer
# regression that misroutes the search would show up in at least one
# (small K is sensitive to the entry point; large K stresses the lower
# layer fan-out).
KNN_K_VALUES = (1, 5, 25, 100)


# M=4 is intentionally small. hnswlib's level normalizer mL = 1/ln(M),
# so smaller M produces taller graphs for a given vector count, which
# maximizes the number of upper-layer connections we serialize.
def fp16_hnsw_index(name: str = "fp16_hnsw_index") -> Index:
    return Index(
        name,
        [
            Vector(
                "v",
                DIM,
                type="HNSW",
                m=4,
                efc=40,
                data_type="FLOAT16",
            ),
            Numeric("n"),
            Tag("t"),
        ],
    )


def fp16_hnsw_vector_only_index(name: str = "fp16_hnsw_vector_only") -> Index:
    return Index(
        name,
        [Vector("v", DIM, type="HNSW", m=4, efc=40, data_type="FLOAT16")],
    )


def fp16_flat_index(name: str = "fp16_flat_index") -> Index:
    return Index(
        name,
        [Vector("v", DIM, type="FLAT", data_type="FLOAT16"), Numeric("n"), Tag("t")],
    )


# (factory, num_vectors) pairs used by the parametrized matrix tests.
INDEX_VARIANTS = [
    (fp16_hnsw_index, NUM_VECTORS_HNSW),
    (fp16_hnsw_vector_only_index, NUM_VECTORS_HNSW),
    (fp16_flat_index, NUM_VECTORS_FLAT),
]


def load_data(client: Valkey, index: Index, num_vectors: int) -> int:
    """Write num_vectors hashes for index. Returns count written."""
    for i in range(num_vectors):
        index.write_data(client, i, index.make_data(i))
    return num_vectors


def query_vector_for_row(row: int) -> bytes:
    """Build the FP16-packed query vector that exactly matches the data
    written by Vector.make_value for the given row (column index = 0)."""
    return float16_to_bytes([float(i + row) for i in range(DIM)])


def knn_search(client: Valkey, index: Index, query_vec: bytes, k: int) -> List[bytes]:
    """Return the list of keys + distances returned by a KNN query."""
    res = client.execute_command(
        "FT.SEARCH",
        index.name,
        f"*=>[KNN {k} @v $query_vector AS knn_score]",
        "PARAMS",
        "2",
        "query_vector",
        query_vec,
        "RETURN",
        "1",
        "knn_score",
        "LIMIT",
        "0",
        str(k),
    )
    # Result format: [count, key1, [field, value, ...], key2, [field, value, ...], ...]
    pairs = []
    for i in range(1, len(res), 2):
        key = res[i]
        fields = res[i + 1]
        score = None
        for j in range(0, len(fields), 2):
            if fields[j] == b"knn_score":
                score = fields[j + 1]
        pairs.append((key, score))
    # KNN results are unordered in the wire format; sort by distance for stable
    # comparison across save/restore.
    pairs.sort(key=lambda kv: (float(kv[1]) if kv[1] is not None else 0.0, kv[0]))
    return pairs


def collect_knn_results(
    client: Valkey, index: Index, num_vectors: int
) -> List[Tuple[int, int, List[Tuple[bytes, bytes]]]]:
    """Run several KNN probes anchored at different rows and K values.

    Anchoring queries at multiple rows scattered across the dataset
    ensures the search routes through different parts of the upper-layer
    graph, so a corrupted upper layer would surface as a result mismatch
    in at least one probe.
    """
    anchor_rows = (0, num_vectors // 4, num_vectors // 2, num_vectors - 1)
    results = []
    for row in anchor_rows:
        for k in KNN_K_VALUES:
            if k > num_vectors:
                continue
            ranked = knn_search(client, index, query_vector_for_row(row), k)
            results.append((row, k, ranked))
    return results


def verify_doc_count(client: Valkey, index: Index, expected: int):
    info = index.info(client)
    assert info.num_docs == expected, (
        f"index {index.name}: num_docs={info.num_docs}, expected {expected}"
    )


def do_fp16_save_restore(test, index: Index, num_vectors: int):
    """Common driver: create index, load data, save, restart, re-verify."""
    index.create(test.client, wait_for_backfill=True)
    load_data(test.client, index, num_vectors)
    waiters.wait_for_equal(
        lambda: index.info(test.client).num_docs, num_vectors, timeout=30
    )

    pre_results = collect_knn_results(test.client, index, num_vectors)
    verify_doc_count(test.client, index, num_vectors)

    test.client.config_set("search.rdb-validate-on-write", "yes")
    test.client.execute_command("save")

    os.environ["SKIPLOGCLEAN"] = "1"
    test.server.restart(remove_rdb=False)
    assert test.client.ping()

    waiters.wait_for_true(lambda: index.backfill_complete(test.client), timeout=60)

    verify_doc_count(test.client, index, num_vectors)
    post_results = collect_knn_results(test.client, index, num_vectors)

    assert pre_results == post_results, (
        f"FP16 KNN results changed across save/restore for index {index.name}\n"
        f"before: {pre_results}\nafter:  {post_results}"
    )


# --------------------------------------------------------------------------
# v1/v1, v1/v2, v2/v1, v2/v2 RDB write/read combinations
# --------------------------------------------------------------------------

class TestFP16SaveRestore_v1_v1(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "no"
        return args

    @pytest.mark.parametrize(
        "index_factory,num_vectors",
        INDEX_VARIANTS,
        ids=[f.__name__ for f, _ in INDEX_VARIANTS],
    )
    def test_fp16_saverestore_v1_v1(self, index_factory, num_vectors):
        do_fp16_save_restore(self, index_factory(), num_vectors)


class TestFP16SaveRestore_v1_v2(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.rdb_write_v2"] = "no"
        args["search.rdb_read_v2"] = "yes"
        return args

    @pytest.mark.parametrize(
        "index_factory,num_vectors",
        INDEX_VARIANTS,
        ids=[f.__name__ for f, _ in INDEX_VARIANTS],
    )
    def test_fp16_saverestore_v1_v2(self, index_factory, num_vectors):
        do_fp16_save_restore(self, index_factory(), num_vectors)


class TestFP16SaveRestore_v2_v1(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "no"
        return args

    @pytest.mark.parametrize(
        "index_factory,num_vectors",
        INDEX_VARIANTS,
        ids=[f.__name__ for f, _ in INDEX_VARIANTS],
    )
    def test_fp16_saverestore_v2_v1(self, index_factory, num_vectors):
        do_fp16_save_restore(self, index_factory(), num_vectors)


class TestFP16SaveRestore_v2_v2(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args

    @pytest.mark.parametrize(
        "index_factory,num_vectors",
        INDEX_VARIANTS,
        ids=[f.__name__ for f, _ in INDEX_VARIANTS],
    )
    def test_fp16_saverestore_v2_v2(self, index_factory, num_vectors):
        do_fp16_save_restore(self, index_factory(), num_vectors)


# --------------------------------------------------------------------------
# Schema-only checks: the index definition (FLOAT16 + dim + algorithm) must
# survive a save/restore even when no documents have been written. This
# catches regressions in the proto/schema serialization path independently
# of the vector storage layer.
# --------------------------------------------------------------------------

class TestFP16SaveRestoreSchemaOnly(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args

    def test_fp16_hnsw_schema_survives_restart_with_no_data(self):
        index = fp16_hnsw_index("fp16_hnsw_empty")
        index.create(self.client, wait_for_backfill=True)
        verify_doc_count(self.client, index, 0)

        self.client.execute_command("save")
        os.environ["SKIPLOGCLEAN"] = "1"
        self.server.restart(remove_rdb=False)
        assert self.client.ping()

        # Index must come back with FP16 still in effect: writing FLOAT32
        # bytes (4*DIM) into a FLOAT16 (2*DIM) field is a hard rejection
        # downstream, so a successful FP16 round-trip means we can write
        # FP16-sized payloads after restart.
        waiters.wait_for_true(lambda: index.backfill_complete(self.client), timeout=10)
        load_data(self.client, index, NUM_VECTORS_HNSW)
        waiters.wait_for_equal(
            lambda: index.info(self.client).num_docs, NUM_VECTORS_HNSW, timeout=30
        )

        ranked = knn_search(self.client, index, query_vector_for_row(0), 3)
        # Closest match to the row-0 query vector should be row 0 itself.
        assert ranked[0][0] == index.keyname(0).encode(), (
            f"unexpected nearest neighbor after schema-only restore: {ranked}"
        )

