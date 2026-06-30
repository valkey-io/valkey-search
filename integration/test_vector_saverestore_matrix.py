"""
Save+restore parity matrix across the full vector data-type cartesian product.

Covers
    data_type ∈ {FLOAT32, FLOAT16, BFLOAT16}
    algorithm ∈ {HNSW, FLAT}
    key_kind  ∈ {HASH, JSON}

on the RDB v2/v2 (current default) read/write combination. The existing
test_fp16_saverestore.py and test_bfloat16_saverestore.py already exercise
the full v1/v2 RDB read/write matrix for the low-precision types on HASH
keys; this file is the orthogonal axis -- it adds FP32 parity and JSON
coverage so all 12 combinations of the data-type × algorithm × key-kind
matrix have round-trip protection.

For each combination the test:
  1. creates the index
  2. ingests a small deterministic dataset (DIM=4, N=80)
  3. takes a KNN-rank fingerprint at several anchor rows and K values
  4. SAVEs, restarts the server, waits for backfill
  5. asserts the post-restart fingerprint matches the pre-save fingerprint
     exactly (rank order and reported distances)

If a code path mis-dispatches by element size instead of by the
VectorDataType enum (e.g. parsing JSON BF16 as if it were FP16), the
post-restart distances would shift and the fingerprint match would fail.
"""

import json
import os
import random
from typing import List, Tuple

import pytest
from valkey import Valkey

from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.conftest import resource_port_tracker
from indexes import (
    Index,
    Vector,
    KeyDataType,
    float_to_bytes,
    float16_to_bytes,
    bfloat16_to_bytes,
)
from util import waiters


DATA_TYPES = ["FLOAT32", "FLOAT16", "BFLOAT16"]
ALGORITHMS = ["HNSW", "FLAT"]
KEY_KINDS = [KeyDataType.HASH, KeyDataType.JSON]

DIM = 4
NUM_VECTORS = 80
# KNN K values -- mix of small (sensitive to entry point / top of HNSW
# graph) and large (stresses the lower-layer fan-out).
KNN_K_VALUES = (1, 5, 25, 60)


def _encode_query(values: List[float], data_type: str) -> bytes:
    if data_type == "FLOAT16":
        return float16_to_bytes(values)
    if data_type == "BFLOAT16":
        return bfloat16_to_bytes(values)
    return float_to_bytes(values)


def _make_vectors(num_vectors: int, dim: int, seed: int) -> List[List[float]]:
    # Small magnitudes so FP16/BF16 round-trips don't saturate.
    rng = random.Random(seed)
    return [
        [rng.uniform(-0.5, 0.5) for _ in range(dim)] for _ in range(num_vectors)
    ]


def _write_one(client: Valkey, index: Index, row: int, vec: List[float],
               data_type: str):
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
        client.execute_command("JSON.SET", key, "$", json.dumps({"v": vec}))


def _knn_search(client: Valkey, index: Index, query_vec: bytes, k: int
                ) -> List[Tuple[bytes, bytes]]:
    res = client.execute_command(
        "FT.SEARCH",
        index.name,
        f"*=>[KNN {k} @v $q AS knn_score]",
        "PARAMS",
        "2",
        "q",
        query_vec,
        "RETURN",
        "1",
        "knn_score",
        "DIALECT",
        "2",
        "LIMIT",
        "0",
        str(k),
    )
    pairs = []
    for i in range(1, len(res), 2):
        key = res[i]
        fields = res[i + 1]
        score = None
        for j in range(0, len(fields), 2):
            if fields[j] == b"knn_score":
                score = fields[j + 1]
        pairs.append((key, score))
    # KNN wire order is unspecified -- sort by distance then key for a
    # stable fingerprint across save/restore.
    pairs.sort(key=lambda kv: (float(kv[1]) if kv[1] is not None else 0.0, kv[0]))
    return pairs


def _collect_fingerprint(client: Valkey, index: Index,
                         vectors: List[List[float]], data_type: str
                         ) -> List[Tuple[int, int, List[Tuple[bytes, bytes]]]]:
    anchor_rows = (0, len(vectors) // 4, len(vectors) // 2, len(vectors) - 1)
    out = []
    for row in anchor_rows:
        q = _encode_query(vectors[row], data_type)
        for k in KNN_K_VALUES:
            if k > len(vectors):
                continue
            ranked = _knn_search(client, index, q, k)
            out.append((row, k, ranked))
    return out


def _build_index(name: str, algorithm: str, data_type: str,
                 key_kind: KeyDataType) -> Index:
    if algorithm == "HNSW":
        # M=4 deliberately small to maximize multi-layer graph use even on
        # 80 vectors -- mirrors the existing FP16 saverestore harness.
        vec = Vector("v", DIM, type="HNSW", distance="L2", m=4, efc=40,
                     data_type=data_type)
    else:
        vec = Vector("v", DIM, type="FLAT", distance="L2", data_type=data_type)
    return Index(name, [vec], type=key_kind)


def _do_save_restore(test, data_type: str, algorithm: str,
                     key_kind: KeyDataType):
    client = test.client
    vectors = _make_vectors(NUM_VECTORS, DIM, seed=hash((data_type, algorithm,
                                                        key_kind.name)) & 0xFFFF)
    name = f"sr_{data_type}_{algorithm}_{key_kind.name}".lower()
    index = _build_index(name, algorithm, data_type, key_kind)
    index.create(client, wait_for_backfill=True)

    for i, v in enumerate(vectors):
        _write_one(client, index, i, v, data_type)

    waiters.wait_for_equal(
        lambda: index.info(client).num_docs, NUM_VECTORS, timeout=30
    )

    pre = _collect_fingerprint(client, index, vectors, data_type)
    assert index.info(client).num_docs == NUM_VECTORS

    client.config_set("search.rdb-validate-on-write", "yes")
    client.execute_command("save")

    os.environ["SKIPLOGCLEAN"] = "1"
    test.server.restart(remove_rdb=False)
    assert client.ping()

    waiters.wait_for_true(lambda: index.backfill_complete(client), timeout=60)
    assert index.info(client).num_docs == NUM_VECTORS, (
        f"[{name}] num_docs={index.info(client).num_docs}, expected {NUM_VECTORS}"
    )

    post = _collect_fingerprint(client, index, vectors, data_type)
    assert pre == post, (
        f"[{name}] KNN fingerprint changed across save/restore\n"
        f"pre:  {pre}\npost: {post}"
    )


class TestVectorSaveRestoreMatrix(ValkeySearchTestCaseDebugMode):
    def append_startup_args(self, args):
        args = super().append_startup_args(args)
        # Pin RDB to current default (v2 write, v2 read). The existing
        # fp16/bf16 saverestore files already cover the v1/v2 read/write
        # matrix for the low-precision types on HASH keys.
        args["search.rdb_write_v2"] = "yes"
        args["search.rdb_read_v2"] = "yes"
        return args

    @pytest.mark.parametrize("key_kind", KEY_KINDS, ids=[k.name for k in KEY_KINDS])
    @pytest.mark.parametrize("algorithm", ALGORITHMS)
    @pytest.mark.parametrize("data_type", DATA_TYPES)
    def test_saverestore(self, data_type, algorithm, key_kind):
        _do_save_restore(self, data_type, algorithm, key_kind)
