#!/usr/bin/env python3
"""
HNSW Performance Benchmark (Ingestion and Search) for Valkey Search.

Benchmarks concurrent ingestion into an HNSW vector index, measures background
indexing completion time, and evaluates search throughput (QPS) and latency percentiles
after ingestion is complete.

"""

import argparse
import concurrent.futures
import json
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
from typing import Dict, List, Tuple

import numpy as np
import redis


def find_free_port() -> int:
    """Find an available TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def create_temp_conf(module_path: str, port: int, work_dir: str) -> str:
    """Generate a temporary valkey-server configuration file."""
    conf_file = tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".conf",
        prefix="valkey_bench_",
        dir=work_dir,
        delete=False,
    )
    conf_file.write(f"port {port}\n")
    conf_file.write("daemonize no\n")
    conf_file.write("save \"\"\n")
    conf_file.write(f"dir {work_dir}\n")
    conf_file.write(f"loadmodule {os.path.abspath(module_path)}\n")
    conf_file.flush()
    conf_file.close()
    return conf_file.name


def start_server(server_path: str, conf_path: str, port: int) -> subprocess.Popen:
    """Launch the valkey-server process with the specified configuration."""
    abs_server = os.path.abspath(server_path)
    if not os.path.exists(abs_server):
        raise FileNotFoundError(f"Valkey server binary not found at: {abs_server}")

    cmd = [abs_server, conf_path]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        preexec_fn=os.setsid,
    )

    # Wait for server to become responsive
    deadline = time.time() + 15
    while time.time() < deadline:
        if proc.poll() is not None:
            _, stderr = proc.communicate()
            raise RuntimeError(
                f"Server failed to start (exit code {proc.returncode}): {stderr.decode('utf-8', errors='replace')}"
            )
        try:
            client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=1)
            if client.ping():
                return proc
        except Exception:
            time.sleep(0.1)

    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait()
    raise TimeoutError(f"Server at port {port} failed to start within timeout.")


def generate_vectors(num_vectors: int, dim: int, rng: np.random.Generator) -> np.ndarray:
    """Generate random float32 vectors deterministically using the provided RNG."""
    return rng.random((num_vectors, dim), dtype=np.float32)


def ingest_chunk(
    pool: redis.ConnectionPool,
    index_name: str,
    vectors: np.ndarray,
    start_idx: int,
    end_idx: int,
    batch_size: int,
) -> int:
    """Ingest a slice of vectors using pipelined HSET operations."""
    client = redis.Redis(connection_pool=pool)
    pipeline = client.pipeline(transaction=False)
    ops = 0

    for i in range(start_idx, end_idx):
        vec_bytes = vectors[i].tobytes()
        pipeline.hset(f"doc:{i}", mapping={"vec": vec_bytes})
        ops += 1
        if ops % batch_size == 0:
            pipeline.execute()

    if ops % batch_size != 0:
        pipeline.execute()

    return ops


def run_search_worker(
    pool: redis.ConnectionPool,
    index_name: str,
    query_bytes_list: List[bytes],
    duration: float,
    k: int,
    worker_id: int,
) -> Tuple[int, List[float]]:
    """Execute KNN search queries continuously for the given duration."""
    client = redis.Redis(connection_pool=pool)
    query_str = f"*=>[KNN {k} @vec $query_vec]"
    num_queries = len(query_bytes_list)
    idx = worker_id % num_queries

    latencies: List[float] = []
    ops = 0
    start_time = time.perf_counter()

    while time.perf_counter() - start_time < duration:
        vec_bytes = query_bytes_list[idx % num_queries]
        idx += 1
        t0 = time.perf_counter()
        try:
            client.execute_command(
                "FT.SEARCH",
                index_name,
                query_str,
                "PARAMS",
                "2",
                "query_vec",
                vec_bytes,
                "DIALECT",
                "2",
            )
        except Exception as e:
            # Fail the whole benchmark rather than reporting partial results.
            raise RuntimeError(f"[Worker {worker_id}] Search error: {e}") from e
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000.0)  # ms
        ops += 1

    return ops, latencies


def parse_ft_info(info_raw: list) -> Dict[str, str]:
    """Parse key-value list output from FT.INFO into a dictionary."""
    result = {}
    for i in range(0, len(info_raw), 2):
        key = (
            info_raw[i].decode("utf-8", errors="replace")
            if isinstance(info_raw[i], bytes)
            else str(info_raw[i])
        )
        val = (
            info_raw[i + 1].decode("utf-8", errors="replace")
            if isinstance(info_raw[i + 1], bytes)
            else str(info_raw[i + 1])
        )
        result[key] = val
    return result


def run_benchmark(args) -> Dict:
    """Run full benchmark sequence: Ingest -> Index Wait -> Search."""
    port = args.port if args.port > 0 else find_free_port()
    work_dir = tempfile.mkdtemp(prefix="valkey_bench_")
    conf_path = create_temp_conf(args.module, port, work_dir)

    print("=" * 80)
    print("VALKEY SEARCH BENCHMARK")
    print("=" * 80)
    print(f"Server binary:     {args.server}")
    print(f"Module path:       {args.module}")
    print(f"Port:              {port}")
    print(f"Dimensions:        {args.dim}")
    print(f"Metric:            {args.distance_metric}")
    print(f"Num Vectors:       {args.num_vectors:,}")
    print(f"Ingest Threads:    {args.ingest_threads}")
    print(f"Search Threads:    {args.search_threads}")
    print(f"Search Duration:   {args.search_duration}s")
    print(f"Search K:          {args.k}")
    print("-" * 80)

    server_proc = None
    pool = None
    try:
        server_proc = start_server(args.server, conf_path, port)
        pool = redis.ConnectionPool(
            host="127.0.0.1", port=port, decode_responses=False, max_connections=64
        )
        client = redis.Redis(connection_pool=pool)

        index_name = "idx_bench"
        print(f"Creating HNSW index '{index_name}'...")
        client.execute_command(
            "FT.CREATE",
            index_name,
            "SCHEMA",
            "vec",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            str(args.dim),
            "DISTANCE_METRIC",
            args.distance_metric,
        )

        rng = np.random.default_rng(args.seed)
        print(f"Generating {args.num_vectors:,} vectors of dim {args.dim} (seed={args.seed})...")
        vectors = generate_vectors(args.num_vectors, args.dim, rng)

        # -------------------------------------------------------------
        # Phase 1: Ingestion
        # -------------------------------------------------------------
        print(f"\n[Phase 1] Ingesting {args.num_vectors:,} vectors ({args.ingest_threads} threads)...")
        chunk_size = args.num_vectors // args.ingest_threads
        ingest_start = time.perf_counter()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.ingest_threads
        ) as executor:
            futures = []
            for t in range(args.ingest_threads):
                s_idx = t * chunk_size
                e_idx = (
                    args.num_vectors if t == args.ingest_threads - 1 else (t + 1) * chunk_size
                )
                futures.append(
                    executor.submit(
                        ingest_chunk,
                        pool,
                        index_name,
                        vectors,
                        s_idx,
                        e_idx,
                        args.batch_size,
                    )
                )
            total_ingested = sum(f.result() for f in futures)

        ingest_elapsed = time.perf_counter() - ingest_start
        ingest_throughput = total_ingested / ingest_elapsed
        print(
            f"  -> Ingestion completed: {total_ingested:,} vectors in {ingest_elapsed:.2f}s "
            f"({ingest_throughput:,.1f} vectors/sec)"
        )

        # -------------------------------------------------------------
        # Phase 2: Wait for background indexing
        # -------------------------------------------------------------
        print("\n[Phase 2] Waiting for background indexing to complete...")
        index_wait_start = time.perf_counter()
        index_deadline = index_wait_start + args.index_timeout
        while True:
            info = parse_ft_info(client.execute_command("FT.INFO", index_name))
            failures = int(info.get("hash_indexing_failures", 0))
            if failures > 0:
                raise RuntimeError(f"{failures} hash indexing failures reported")

            num_docs = int(info.get("num_docs", 0))
            mutation_queue = int(info.get("mutation_queue_size", 0))
            if num_docs >= args.num_vectors and mutation_queue == 0:
                break
            if time.perf_counter() > index_deadline:
                raise TimeoutError(
                    f"Indexing did not complete within {args.index_timeout}s "
                    f"(num_docs={num_docs}, mutation_queue_size={mutation_queue})"
                )
            time.sleep(0.1)

        index_wait_elapsed = time.perf_counter() - index_wait_start
        total_index_time = ingest_elapsed + index_wait_elapsed
        effective_indexing_throughput = args.num_vectors / total_index_time
        print(
            f"  -> Indexing 100% complete in {index_wait_elapsed:.2f}s "
            f"(Total time from start: {total_index_time:.2f}s, "
            f"Effective Indexing Rate: {effective_indexing_throughput:,.1f} docs/sec)"
        )

        # -------------------------------------------------------------
        # Phase 3: Search Benchmark (executed after ingestion is done)
        # -------------------------------------------------------------
        print(
            f"\n[Phase 3] Running search benchmark ({args.search_threads} threads, "
            f"{args.search_duration}s duration, KNN={args.k})..."
        )
        num_query_samples = min(2000, len(vectors))
        query_indices = rng.choice(len(vectors), size=num_query_samples, replace=False)
        query_bytes_list = [vectors[i].tobytes() for i in query_indices]

        search_start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.search_threads
        ) as executor:
            futures = [
                executor.submit(
                    run_search_worker,
                    pool,
                    index_name,
                    query_bytes_list,
                    args.search_duration,
                    args.k,
                    t,
                )
                for t in range(args.search_threads)
            ]
            worker_results = [f.result() for f in futures]

        search_elapsed = time.perf_counter() - search_start
        total_queries = sum(r[0] for r in worker_results)
        search_qps = total_queries / search_elapsed

        all_latencies: List[float] = []
        for r in worker_results:
            all_latencies.extend(r[1])

        if all_latencies:
            lat_arr = np.array(all_latencies)
            p50 = float(np.percentile(lat_arr, 50))
            p95 = float(np.percentile(lat_arr, 95))
            p99 = float(np.percentile(lat_arr, 99))
            avg_lat = float(np.mean(lat_arr))
        else:
            p50, p95, p99, avg_lat = 0.0, 0.0, 0.0, 0.0

        print(f"  -> Completed {total_queries:,} queries in {search_elapsed:.2f}s")
        print(f"  -> Search Throughput: {search_qps:,.1f} QPS")
        print(
            f"  -> Latency: avg={avg_lat:.2f}ms, p50={p50:.2f}ms, "
            f"p95={p95:.2f}ms, p99={p99:.2f}ms"
        )

        results = {
            "config": {
                "server": args.server,
                "module": args.module,
                "dim": args.dim,
                "distance_metric": args.distance_metric,
                "num_vectors": args.num_vectors,
                "ingest_threads": args.ingest_threads,
                "search_threads": args.search_threads,
                "search_duration": args.search_duration,
                "k": args.k,
                "seed": args.seed,
            },
            "ingestion": {
                "total_vectors": total_ingested,
                "elapsed_sec": ingest_elapsed,
                "throughput_vectors_per_sec": ingest_throughput,
            },
            "indexing": {
                "background_wait_sec": index_wait_elapsed,
                "total_time_to_indexed_sec": total_index_time,
                "effective_rate_docs_per_sec": effective_indexing_throughput,
            },
            "search": {
                "total_queries": total_queries,
                "elapsed_sec": search_elapsed,
                "throughput_qps": search_qps,
                "latency_ms": {
                    "avg": avg_lat,
                    "p50": p50,
                    "p95": p95,
                    "p99": p99,
                },
            },
        }

        if args.output_json:
            with open(args.output_json, "w") as jf:
                json.dump(results, jf, indent=2)
            print(f"\nResults written to: {args.output_json}")

        return results

    finally:
        if pool is not None:
            pool.disconnect()
        if server_proc is not None and server_proc.poll() is None:
            try:
                os.killpg(os.getpgid(server_proc.pid), signal.SIGTERM)
                server_proc.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(server_proc.pid), signal.SIGKILL)
                    server_proc.wait(timeout=5)
                except Exception:
                    pass
        # conf_path lives inside work_dir.
        shutil.rmtree(work_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(
        description="Run Valkey Search HNSW Ingestion and Search Benchmark."
    )
    parser.add_argument(
        "--server",
        default=".build-release/valkey-server/.build-release/bin/valkey-server",
        help="Path to valkey-server executable",
    )
    parser.add_argument(
        "--module",
        default=".build-release/libsearch.so",
        help="Path to Valkey Search module .so",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port for valkey-server (0 to select free ephemeral port)",
    )
    parser.add_argument(
        "--dim", type=int, default=768, help="Vector dimension count (default: 768)"
    )
    parser.add_argument(
        "--num_vectors",
        type=int,
        default=50000,
        help="Number of vectors to ingest (default: 50,000)",
    )
    parser.add_argument(
        "--distance_metric",
        default="COSINE",
        choices=["COSINE", "L2", "IP"],
        help="Distance metric (default: COSINE)",
    )
    parser.add_argument(
        "--ingest_threads",
        type=int,
        default=8,
        help="Concurrent client threads for ingestion (default: 8)",
    )
    parser.add_argument(
        "--search_threads",
        type=int,
        default=8,
        help="Concurrent client threads for search (default: 8)",
    )
    parser.add_argument(
        "--search_duration",
        type=int,
        default=30,
        help="Duration of search phase in seconds (default: 30)",
    )
    parser.add_argument(
        "--k", type=int, default=10, help="KNN result count per query (default: 10)"
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=500,
        help="Pipeline batch size for ingestion (default: 500)",
    )
    parser.add_argument(
        "--output_json",
        default="",
        help="Optional path to output results in JSON format",
    )
    parser.add_argument(
        "--index_timeout",
        type=int,
        default=600,
        help="Max seconds to wait for background indexing (default: 600)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible vector and query generation (default: 42)",
    )

    args = parser.parse_args()
    run_benchmark(args)


if __name__ == "__main__":
    main()
