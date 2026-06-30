"""
Cohere recall benchmark across FLOAT32 / FLOAT16 / BFLOAT16.

Loads a 10K-row + 100-query subset of the publicly available
`ashraq/cohere-wiki-embedding-100k` HuggingFace dataset (the smallest open
Cohere-embeddings parquet we could find), builds an HNSW index for each
of the three vector data types, and measures recall@10 against an exact
numpy-computed FLOAT32 ground truth.

Recall@10 is asserted to be >= MIN_RECALL_* (absolute thresholds; see
constants below) for each data type. The measured values are printed in
the test output so the constants can be retuned if the embedding
distribution changes.

First run downloads ~258 MB of parquet, extracts a ~33 MB numpy subset
to integration/test-data/cohere_subset.npz, then discards the parquet.
Subsequent runs reuse the .npz cache and skip the download.

If neither the cache nor a working network is available, or pyarrow is
missing, the test is skipped with a clear message.
"""

import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
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
from util import waiters


# ---- Dataset configuration -----------------------------------------------

# `ashraq/cohere-wiki-embedding-100k` -- one of the few openly accessible
# Cohere-embeddings datasets on HuggingFace (no auth required). 768-dim
# Cohere multilingual embeddings of English Wikipedia paragraphs.
COHERE_PARQUET_URL = (
    "https://huggingface.co/datasets/ashraq/cohere-wiki-embedding-100k/"
    "resolve/main/data/train-00000-of-00002-039513d189a50a66.parquet"
)
COHERE_PARQUET_BYTES_HINT = 270_000_000  # ~258 MB, used only for a sanity log

DIM = 768
NUM_BASE = 10_000
NUM_QUERY = 100
K = 10

# HNSW build params. Modest, but enough that FP32 recall@10 lands ~0.95+
# on this dataset (calibrated empirically).
HNSW_M = 16
HNSW_EFC = 200
HNSW_EFR = 50

# Absolute recall thresholds. Calibrated empirically on this dataset
# with the HNSW params above:
#   measured FP32 recall@10  = 0.985
#   measured FP16 recall@10  = 0.985
#   measured BF16 recall@10  = 0.988
# Thresholds are set at ~0.9 x measured ("fail if recall falls below 90%
# of the first measured value"). All three landed in the same band at
# this config -- the Cohere-Wikipedia embedding distribution stays well
# inside FP16/BF16 representable range, so storage-side precision loss
# is negligible compared to HNSW approximation.
# If the dataset, HNSW params, or vector distribution change, re-measure
# and update these constants.
MIN_RECALL_FLOAT32 = 0.88
MIN_RECALL_FLOAT16 = 0.88
MIN_RECALL_BFLOAT16 = 0.88


# Cache location -- the test_data dir is part of the repo so the cached
# .npz survives `git clean -fdx` only if explicitly excluded. We leave it
# in place; it's small (~33 MB) and reused across local runs.
_THIS_DIR = Path(__file__).resolve().parent
CACHE_PATH = _THIS_DIR / "test-data" / "cohere_subset.npz"


# ---- Dataset acquisition -------------------------------------------------

def _download_and_extract() -> Optional[Tuple[np.ndarray, np.ndarray]]:
    """Download the parquet, extract base+query subset, write .npz cache.

    Returns (base, query) on success, None on any failure (network down,
    pyarrow missing, parsing error). The caller decides whether to skip.
    """
    try:
        import pyarrow.parquet as pq  # noqa: F401
    except ImportError:
        return None

    # Use a scratch path for the parquet -- don't pollute test-data with
    # the raw 258 MB. Drop it once the npz cache is written.
    scratch_dir = Path(os.environ.get("TMPDIR", "/tmp")) / "valkey-search-cohere"
    scratch_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = scratch_dir / "cohere_raw.parquet"

    print(f"Downloading {COHERE_PARQUET_URL} -> {parquet_path}", flush=True)
    try:
        with urllib.request.urlopen(COHERE_PARQUET_URL, timeout=120) as resp:
            with open(parquet_path, "wb") as out:
                while True:
                    chunk = resp.read(1 << 20)  # 1 MB
                    if not chunk:
                        break
                    out.write(chunk)
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"download failed: {e!r}", flush=True)
        return None

    actual_size = parquet_path.stat().st_size
    print(f"downloaded {actual_size} bytes (hint ~{COHERE_PARQUET_BYTES_HINT})",
          flush=True)

    import pyarrow.parquet as pq
    pf = pq.ParquetFile(str(parquet_path))
    needed = NUM_BASE + NUM_QUERY
    assert pf.metadata.num_rows >= needed, (
        f"parquet has {pf.metadata.num_rows} rows, need {needed}"
    )

    # Stream rows; stop once we've collected enough.
    collected = []
    for batch in pf.iter_batches(batch_size=2048, columns=["emb"]):
        col = batch.column("emb")
        for i in range(len(col)):
            collected.append(col[i].as_py())
            if len(collected) >= needed:
                break
        if len(collected) >= needed:
            break

    arr = np.array(collected, dtype=np.float32)
    assert arr.shape == (needed, DIM), f"got {arr.shape}, expected {(needed, DIM)}"

    base = arr[:NUM_BASE]
    query = arr[NUM_BASE:NUM_BASE + NUM_QUERY]

    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    np.savez(str(CACHE_PATH), base=base, query=query)
    print(f"cached subset to {CACHE_PATH} (base={base.shape}, query={query.shape})",
          flush=True)

    # Discard the parquet -- we have the npz now.
    try:
        parquet_path.unlink()
    except OSError:
        pass

    return base, query


def _load_dataset() -> Optional[Tuple[np.ndarray, np.ndarray]]:
    """Return (base, query) numpy arrays or None if unavailable."""
    if CACHE_PATH.exists():
        d = np.load(str(CACHE_PATH))
        base = d["base"]
        query = d["query"]
        if base.shape == (NUM_BASE, DIM) and query.shape == (NUM_QUERY, DIM):
            return base.astype(np.float32, copy=False), \
                   query.astype(np.float32, copy=False)
        print(f"cache at {CACHE_PATH} has unexpected shape; re-downloading",
              flush=True)
    return _download_and_extract()


# ---- Ground truth + recall -----------------------------------------------

def _exact_knn_l2(base: np.ndarray, query: np.ndarray, k: int) -> np.ndarray:
    """For each query, return the indices of the k nearest base vectors
    by squared L2 distance. Shape: (num_queries, k).

    Uses the identity ||q - b||^2 = ||q||^2 + ||b||^2 - 2 q.b. Done in
    numpy fp32; for our sizes (100 x 10000 x 768) this is ~10 ms.
    """
    base_norm = np.sum(base * base, axis=1)        # (NB,)
    query_norm = np.sum(query * query, axis=1)     # (NQ,)
    # d2[i,j] = q[i].q[i] + b[j].b[j] - 2 q[i].b[j]
    cross = query @ base.T                          # (NQ, NB)
    d2 = query_norm[:, None] + base_norm[None, :] - 2.0 * cross
    # argpartition is O(NB) for k-NN; argsort the partition window for stable order.
    part = np.argpartition(d2, k, axis=1)[:, :k]
    rows = np.arange(part.shape[0])[:, None]
    sorted_idx = np.argsort(d2[rows, part], axis=1)
    return part[rows, sorted_idx]


# ---- Server-side indexing helpers ----------------------------------------

def _encode_query(values: np.ndarray, data_type: str) -> bytes:
    vals_list = values.astype(np.float32).tolist()
    if data_type == "FLOAT16":
        return float16_to_bytes(vals_list)
    if data_type == "BFLOAT16":
        return bfloat16_to_bytes(vals_list)
    return float_to_bytes(vals_list)


def _build_index(name: str, data_type: str) -> Index:
    vec = Vector("v", DIM, type="HNSW", distance="L2",
                 m=HNSW_M, efc=HNSW_EFC, ef=HNSW_EFR, data_type=data_type)
    return Index(name, [vec])


def _ingest_base(client: Valkey, index: Index, base: np.ndarray,
                 data_type: str):
    if data_type == "FLOAT16":
        encode = float16_to_bytes
    elif data_type == "BFLOAT16":
        encode = bfloat16_to_bytes
    else:
        encode = float_to_bytes
    for i in range(base.shape[0]):
        key = index.keyname(i)
        client.hset(key, mapping={"v": encode(base[i].astype(np.float32).tolist())})


def _query_topk(client: Valkey, index: Index, query_blob: bytes, k: int
                ) -> list:
    res = client.execute_command(
        "FT.SEARCH",
        index.name,
        f"*=>[KNN {k} @v $q AS knn_score]",
        "PARAMS",
        "2",
        "q",
        query_blob,
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
                score = float(fields[j + 1])
        pairs.append((key, score))
    pairs.sort(key=lambda kv: (kv[1] if kv[1] is not None else 0.0, kv[0]))
    return [k_ for k_, _ in pairs]


def _row_index_from_key(key_bytes: bytes, index: Index) -> Optional[int]:
    # index.keyname(i) -> ":<index_name>:<i:08d>" with prefix `:` (no prefixes list).
    s = key_bytes.decode()
    tail = s.rsplit(":", 1)[-1]
    try:
        return int(tail)
    except ValueError:
        return None


def _recall_at_k(retrieved_idxs: list, ground_truth_row: np.ndarray, k: int) -> float:
    gt_set = set(int(x) for x in ground_truth_row.tolist())
    hit = sum(1 for x in retrieved_idxs if x is not None and x in gt_set)
    return hit / k


# ---- Test ---------------------------------------------------------------

class TestRecallCohere(ValkeySearchTestCaseBase):

    def _measure_recall_for_type(self, client: Valkey, base: np.ndarray,
                                 query: np.ndarray, gt: np.ndarray,
                                 data_type: str) -> float:
        name = f"cohere_{data_type}".lower()
        index = _build_index(name, data_type)
        index.create(client, wait_for_backfill=True)
        _ingest_base(client, index, base, data_type)
        waiters.wait_for_equal(
            lambda: index.info(client).num_docs, NUM_BASE, timeout=120
        )

        recalls = []
        for qi in range(NUM_QUERY):
            blob = _encode_query(query[qi], data_type)
            keys = _query_topk(client, index, blob, K)
            retrieved = [_row_index_from_key(k_, index) for k_ in keys]
            recalls.append(_recall_at_k(retrieved, gt[qi], K))
        mean_recall = float(np.mean(recalls))

        # Drop the index + keys so the next data type starts clean.
        client.execute_command("FT.DROPINDEX", index.name)
        # HSET keys live under prefix ":<name>:" -- nuke them so the next
        # ingest doesn't trip over leftover hashes.
        for i in range(NUM_BASE):
            client.delete(index.keyname(i))
        return mean_recall

    def test_cohere_recall(self):
        loaded = _load_dataset()
        if loaded is None:
            pytest.skip(
                "Cohere dataset unavailable: no cache at "
                f"{CACHE_PATH} and could not download / parse "
                f"{COHERE_PARQUET_URL} (pyarrow may be missing, or no "
                "network)."
            )
        base, query = loaded
        client = self.server.get_new_client()

        gt = _exact_knn_l2(base, query, K)

        measured = {}
        for data_type in ("FLOAT32", "FLOAT16", "BFLOAT16"):
            r = self._measure_recall_for_type(client, base, query, gt, data_type)
            measured[data_type] = r
            print(f"recall@{K} {data_type}: {r:.4f}", flush=True)

        # Thresholds calibrated at ~0.9 x measured (see module constants).
        assert measured["FLOAT32"] >= MIN_RECALL_FLOAT32, (
            f"FLOAT32 recall@{K}={measured['FLOAT32']:.4f} below "
            f"threshold {MIN_RECALL_FLOAT32}"
        )
        assert measured["FLOAT16"] >= MIN_RECALL_FLOAT16, (
            f"FLOAT16 recall@{K}={measured['FLOAT16']:.4f} below "
            f"threshold {MIN_RECALL_FLOAT16}"
        )
        assert measured["BFLOAT16"] >= MIN_RECALL_BFLOAT16, (
            f"BFLOAT16 recall@{K}={measured['BFLOAT16']:.4f} below "
            f"threshold {MIN_RECALL_BFLOAT16}"
        )
