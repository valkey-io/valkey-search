"""
Basic ingest + KNN smoke tests across the full vector data-type matrix.

Covers the cartesian product
    data_type  ∈ {FLOAT32, FLOAT16, BFLOAT16}
    algorithm  ∈ {HNSW, FLAT}
    key_kind   ∈ {HASH,  JSON}

For each combination the test ingests a deterministic small dataset and
asserts the basic KNN invariant: a query equal to row N's stored vector
returns row N as its top-1 nearest neighbor. This catches any data-type
misroute (FP16<->BF16 confusion, JSON parser dispatching by element size
instead of by the VectorDataType enum, FLAT/HNSW per-type plumbing
breakage, etc.) on the simplest possible workload.

Three test methods build the matrix at different dimensions:
  - test_small_dataset_ingest_query  : DIM=4,  N=50
  - test_short_dim                   : DIM ∈ {2,3,7}, N=50
  - test_max_dim                     : DIM=32768 (engine default cap), N=32
"""

import json
import math
import random
from typing import List, Tuple

import pytest
from valkey import Valkey

from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from indexes import (
    Index,
    Vector,
    KeyDataType,
    float_to_bytes,
    float16_to_bytes,
    bfloat16_to_bytes,
)


DATA_TYPES = ["FLOAT32", "FLOAT16", "BFLOAT16"]
ALGORITHMS = ["HNSW", "FLAT"]
KEY_KINDS = [KeyDataType.HASH, KeyDataType.JSON]

# Engine's default --max-vector-dimensions ceiling (src/commands/ft_create_parser.cc).
MAX_DIM = 32768


def _encode_query(values: List[float], data_type: str) -> bytes:
    """Encode a vector into the BLOB the server expects on KNN query.

    All query vectors travel as BLOBs regardless of HASH vs JSON storage;
    only the on-disk format differs.
    """
    if data_type == "FLOAT16":
        return float16_to_bytes(values)
    if data_type == "BFLOAT16":
        return bfloat16_to_bytes(values)
    return float_to_bytes(values)


def _make_vectors(num_vectors: int, dim: int, seed: int) -> List[List[float]]:
    """Generate small-magnitude deterministic vectors safe for FP16/BF16.

    FP16 saturates at ~65504 and BF16 has only 7 mantissa bits, so any
    generator whose values scale with dim+row would saturate at the
    max-dim case. We use random values in [-0.5, 0.5] which sit well
    inside FP16's normal range and round losslessly across BF16's mantissa
    for the exact-self-match invariant we test.
    """
    rng = random.Random(seed)
    return [
        [rng.uniform(-0.5, 0.5) for _ in range(dim)] for _ in range(num_vectors)
    ]


def _build_index(name: str, dim: int, algorithm: str, data_type: str,
                 key_kind: KeyDataType) -> Index:
    # Use small HNSW params: fast to build, sufficient for the tiny matrices
    # this file exercises. Distance metric L2 matches the indexes.py default
    # but is named here for clarity.
    if algorithm == "HNSW":
        vec = Vector("v", dim, type="HNSW", distance="L2", m=8, efc=32,
                     data_type=data_type)
    else:
        vec = Vector("v", dim, type="FLAT", distance="L2", data_type=data_type)
    return Index(name, [vec], type=key_kind)


def _write_one(client: Valkey, index: Index, row: int, vec: List[float],
               data_type: str):
    """Store row `row` of the dataset in the right native format."""
    key = index.keyname(row)
    if index.type == KeyDataType.HASH:
        if data_type == "FLOAT16":
            payload = {"v": float16_to_bytes(vec)}
        elif data_type == "BFLOAT16":
            payload = {"v": bfloat16_to_bytes(vec)}
        else:
            payload = {"v": float_to_bytes(vec)}
        client.hset(key, mapping=payload)
    else:
        # JSON path: store native floats; server casts to the index data type.
        client.execute_command("JSON.SET", key, "$", json.dumps({"v": vec}))


def _knn_top1_key(client: Valkey, index: Index, query_blob: bytes) -> bytes:
    res = client.execute_command(
        "FT.SEARCH",
        index.name,
        "*=>[KNN 1 @v $q AS knn_score]",
        "PARAMS",
        "2",
        "q",
        query_blob,
        "DIALECT",
        "2",
        "LIMIT",
        "0",
        "1",
    )
    # Wire format: [count, key, [field, value, ...]]
    assert res[0] >= 1, f"KNN returned no results: {res!r}"
    return res[1]


def _ingest_and_assert_self_match(client: Valkey, index: Index,
                                  data_type: str,
                                  vectors: List[List[float]],
                                  sample_rows: List[int]):
    """Ingest `vectors`, then for each row in `sample_rows` assert KNN(1)
    of that row's vector returns its own key.

    `sample_rows` exists because at MAX_DIM with N=32, scanning every row
    is cheap; at smaller dims we use it identically. Kept explicit so a
    future caller can probe a subset without changing semantics.
    """
    index.create(client, wait_for_backfill=True)
    for i, v in enumerate(vectors):
        _write_one(client, index, i, v, data_type)

    # Wait for the index to reflect every row before querying.
    from util import waiters
    waiters.wait_for_equal(
        lambda: index.info(client).num_docs, len(vectors), timeout=30
    )

    for row in sample_rows:
        query_blob = _encode_query(vectors[row], data_type)
        top1 = _knn_top1_key(client, index, query_blob)
        expected = index.keyname(row).encode()
        assert top1 == expected, (
            f"[{index.name}] KNN top-1 for row {row} returned {top1!r}, "
            f"expected {expected!r}"
        )


class TestVectorBasicIngestQuery(ValkeySearchTestCaseBase):

    @pytest.mark.parametrize("key_kind", KEY_KINDS, ids=[k.name for k in KEY_KINDS])
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    @pytest.mark.parametrize("data_type", DATA_TYPES)
    def test_small_dataset_ingest_query(self, data_type, algorithm, key_kind):
        client = self.server.get_new_client()
        dim = 4
        num_vectors = 50
        vectors = _make_vectors(num_vectors, dim, seed=1)

        name = f"basic_{data_type}_{algorithm}_{key_kind.name}".lower()
        index = _build_index(name, dim, algorithm, data_type, key_kind)
        _ingest_and_assert_self_match(
            client, index, data_type, vectors,
            sample_rows=list(range(num_vectors)),
        )

    @pytest.mark.parametrize("key_kind", KEY_KINDS, ids=[k.name for k in KEY_KINDS])
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    @pytest.mark.parametrize("data_type", DATA_TYPES)
    @pytest.mark.parametrize("dim", [2, 3, 7])
    def test_short_dim(self, dim, data_type, algorithm, key_kind):
        client = self.server.get_new_client()
        num_vectors = 50
        vectors = _make_vectors(num_vectors, dim, seed=2 + dim)

        name = f"shortd{dim}_{data_type}_{algorithm}_{key_kind.name}".lower()
        index = _build_index(name, dim, algorithm, data_type, key_kind)
        _ingest_and_assert_self_match(
            client, index, data_type, vectors,
            sample_rows=list(range(num_vectors)),
        )

    @pytest.mark.parametrize("key_kind", KEY_KINDS, ids=[k.name for k in KEY_KINDS])
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    @pytest.mark.parametrize("data_type", DATA_TYPES)
    def test_max_dim(self, data_type, algorithm, key_kind):
        client = self.server.get_new_client()
        # MAX_DIM is the engine's default cap (configurable to 64000 via
        # --max-vector-dimensions, but we test at default).
        num_vectors = 32
        vectors = _make_vectors(num_vectors, MAX_DIM, seed=3)

        name = f"maxd_{data_type}_{algorithm}_{key_kind.name}".lower()
        index = _build_index(name, MAX_DIM, algorithm, data_type, key_kind)
        _ingest_and_assert_self_match(
            client, index, data_type, vectors,
            sample_rows=list(range(num_vectors)),
        )
