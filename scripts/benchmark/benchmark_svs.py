#!/usr/bin/env python3
"""
SVS vs HNSW Benchmark using VectorDBBench Valkey client.
Synthetic dataset: 10K vectors, 128-dim, L2 distance.
Measures: insertion throughput, search latency (p50/p95/p99), recall@10.

Usage:
    python3 benchmark_svs.py [--host 127.0.0.1] [--port 6399] [--num-vectors 10000]
"""
import argparse
import os
import sys
import time

import numpy as np

# Force unbuffered output for background execution
os.environ["PYTHONUNBUFFERED"] = "1"

# Add VectorDBBench to path
sys.path.insert(0, "/home/ubuntu/projects/cee-valkey-svs/VectorDBBench")

from pydantic import SecretStr
from vectordb_bench.backend.clients.valkey_search.valkey_search import ValkeySearch
from vectordb_bench.backend.clients.valkey_search.config import (
    ValkeySearchConfig,
    ValkeySearchHNSWConfig,
    ValkeySearchSVSConfig,
)

# ---------- Configuration ----------
SEED = 42
K = 10
BATCH_SIZE = 5000


def generate_data(n, dim, seed):
    rng = np.random.default_rng(seed)
    return rng.standard_normal((n, dim)).astype(np.float32)


def brute_force_knn(data, queries, k):
    results = []
    for q in queries:
        dists = np.sum((data - q) ** 2, axis=1)
        idx = np.argpartition(dists, k)[:k]
        results.append(set(idx.tolist()))
    return results


def compute_recall(predicted, ground_truth):
    recalls = []
    for pred, gt in zip(predicted, ground_truth):
        recalls.append(len(pred & gt) / len(gt))
    return np.mean(recalls)


def run_benchmark(name, db_config_dict, case_config, dim, data, queries,
                  ground_truth, k, extra_index_params=None):
    print(f"\n{'='*60}")
    print(f"  {name}")
    print(f"{'='*60}")

    # 1. Create index (drops old)
    t0 = time.perf_counter()
    client = ValkeySearch(
        dim=dim,
        db_config=db_config_dict,
        db_case_config=case_config,
        drop_old=True,
        extra_index_params=extra_index_params,
    )
    create_time = time.perf_counter() - t0
    print(f"  Index creation:     {create_time*1000:.1f} ms")

    # 2. Insert vectors
    metadata = list(range(len(data)))
    embeddings = [row.tolist() for row in data]

    t0 = time.perf_counter()
    with client.init():
        # Insert in batches
        total_inserted = 0
        for start in range(0, len(embeddings), BATCH_SIZE):
            end = min(start + BATCH_SIZE, len(embeddings))
            batch_emb = embeddings[start:end]
            batch_meta = metadata[start:end]
            count, err = client.insert_embeddings(batch_emb, batch_meta)
            if err:
                print(f"  ERROR inserting batch: {err}")
                return None
            total_inserted += count

    # Wait for async indexing
    time.sleep(2)
    insert_time = time.perf_counter() - t0
    throughput = total_inserted / insert_time
    print(f"  Inserted:           {total_inserted} vectors in {insert_time:.2f}s "
          f"({throughput:.0f} vec/s)")

    # 3. Search
    latencies = []
    predicted = []
    with client.init():
        for q in queries:
            t0 = time.perf_counter()
            result_ids = client.search_embedding(q.tolist(), k=k)
            lat = time.perf_counter() - t0
            latencies.append(lat)
            predicted.append(set(result_ids))

    latencies = np.array(latencies)
    p50 = np.percentile(latencies, 50) * 1000
    p95 = np.percentile(latencies, 95) * 1000
    p99 = np.percentile(latencies, 99) * 1000
    mean_lat = np.mean(latencies) * 1000

    print(f"  Search latency:     p50={p50:.2f}ms  p95={p95:.2f}ms  "
          f"p99={p99:.2f}ms  mean={mean_lat:.2f}ms")

    # 4. Recall
    recall = compute_recall(predicted, ground_truth)
    print(f"  Recall@{k}:          {recall:.4f}")

    return {
        "name": name,
        "create_ms": create_time * 1000,
        "insert_s": insert_time,
        "throughput": throughput,
        "p50_ms": p50,
        "p95_ms": p95,
        "p99_ms": p99,
        "mean_ms": mean_lat,
        "recall": recall,
    }


def main():
    parser = argparse.ArgumentParser(description="SVS vs HNSW Benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6399)
    parser.add_argument("--num-vectors", type=int, default=10_000)
    parser.add_argument("--num-queries", type=int, default=100)
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--k", type=int, default=K)
    args = parser.parse_args()

    num_vectors = args.num_vectors
    num_queries = args.num_queries
    dim = args.dim
    k = args.k

    print(f"Benchmark: {num_vectors} vectors, {dim}-dim, L2, K={k}")
    print(f"Server: {args.host}:{args.port}")

    # Generate data
    print("\nGenerating data...")
    data = generate_data(num_vectors, dim, SEED)
    queries = generate_data(num_queries, dim, SEED + 1)

    print("Computing ground truth (brute force)...")
    ground_truth = brute_force_knn(data, queries, k)

    db_config = {
        "host": args.host,
        "port": args.port,
        "password": None,
    }

    # --- Benchmark configurations ---
    # Use moderate params to keep insertion tractable on single-thread
    configs = [
        (
            "HNSW (M=16, ef_c=64, ef_r=10)",
            ValkeySearchHNSWConfig(M=16, efConstruction=64, ef=10),
        ),
        (
            "SVS FP32 (deg=32, cws=64, sws=10)",
            ValkeySearchSVSConfig(
                graph_max_degree=32,
                construction_window_size=64,
                search_window_size=10,
                alpha=1.2,
            ),
        ),
        (
            "SVS LVQ4X8 (deg=32, cws=64, sws=10)",
            ValkeySearchSVSConfig(
                graph_max_degree=32,
                construction_window_size=64,
                search_window_size=10,
                alpha=1.2,
            ),
            # LVQ4X8 compression added as extra index params
            {"COMPRESSION": "LVQ4X8"},
        ),
    ]

    results = []
    for entry in configs:
        name = entry[0]
        case_config = entry[1]
        extra_index_params = entry[2] if len(entry) > 2 else None

        result = run_benchmark(
            name, db_config, case_config,
            dim, data, queries, ground_truth, k,
            extra_index_params=extra_index_params,
        )
        if result:
            results.append(result)

    # --- Comparison Table ---
    if len(results) >= 2:
        print(f"\n{'='*72}")
        print(f"  COMPARISON — {num_vectors} vectors, {dim}-dim, L2, K={k}")
        print(f"{'='*72}")
        header = f"  {'Metric':<22}"
        for r in results:
            header += f" {r['name'][:16]:>16}"
        print(header)
        print(f"  {'-'*22}" + (" " + "-"*16) * len(results))
        for metric, label in [
            ("create_ms", "Create (ms)"),
            ("throughput", "Insert (vec/s)"),
            ("p50_ms", "Search p50 (ms)"),
            ("p95_ms", "Search p95 (ms)"),
            ("p99_ms", "Search p99 (ms)"),
            ("mean_ms", "Search mean (ms)"),
            ("recall", f"Recall@{k}"),
        ]:
            row = f"  {label:<22}"
            for r in results:
                v = r[metric]
                fmt = ".4f" if metric == "recall" else ".2f" if "ms" in metric else ".0f"
                row += f" {v:>16{fmt}}"
            print(row)
        print()


if __name__ == "__main__":
    sys.exit(main() or 0)
