#!/usr/bin/env python3
"""
End-to-End Benchmark for Valkey-Search Text Index.
Benchmarks Ingestion Speed, Search QPS, Latency, and Memory Footprint across
various reader/writer and client thread counts.
"""

import os
import sys
import time
import argparse
import subprocess
import threading
import glob
import statistics
import csv
import redis

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../.."))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
QUERIES_FILE = os.path.join(SCRIPT_DIR, "queries.txt")

DEFAULT_SERVER = os.path.join(PROJECT_ROOT, ".build-release/valkey-server/.build-release/bin/valkey-server")
DEFAULT_MODULE = os.path.join(PROJECT_ROOT, ".build-release/libsearch.so")
DEFAULT_CLI = os.path.join(PROJECT_ROOT, ".build-release/valkey-server/.build-release/bin/valkey-cli")
DEFAULT_CSV = os.path.join(PROJECT_ROOT, "e2e_rax_bench_stats.csv")

def parse_info_search(client):
    info = client.execute_command("INFO", "SEARCH")
    res = {}
    if isinstance(info, dict):
        return info
    text = info.decode("utf-8", errors="ignore") if isinstance(info, bytes) else str(info)
    for line in text.splitlines():
        line = line.strip()
        if ":" in line and not line.startswith("#"):
            k, v = line.split(":", 1)
            res[k.strip()] = v.strip()
    return res

def parse_info_memory(client):
    info = client.info("memory")
    return info

def start_server(server_bin, module_bin, reader_threads, writer_threads, port=6399):
    conf_path = f"/tmp/valkey_e2e_{port}.conf"
    pid_path = f"/tmp/valkey_e2e_{port}.pid"
    log_path = f"/tmp/valkey_e2e_{port}.log"
    
    # Clean up previous instances if any
    try:
        subprocess.run(["pkill", "-9", "-f", f"valkey_e2e_{port}.conf"], check=False)
        time.sleep(0.5)
    except Exception:
        pass

    with open(conf_path, "w") as f:
        f.write(f"""port {port}
save ""
appendonly no
pidfile {pid_path}
logfile {log_path}
loadmodule {module_bin} --reader-threads {reader_threads} --writer-threads {writer_threads}
""")
    
    proc = subprocess.Popen([server_bin, conf_path, "--daemonize", "yes"])
    proc.wait()
    
    # Wait for server to be responsive
    client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=10)
    for _ in range(50):
        try:
            if client.ping():
                return client, conf_path
        except Exception:
            time.sleep(0.1)
            
    raise RuntimeError(f"Failed to start valkey-server on port {port}. Log: {log_path}")

def stop_server(client, conf_path):
    try:
        client.shutdown(save=False)
    except Exception:
        pass
    time.sleep(0.5)
    if os.path.exists(conf_path):
        os.remove(conf_path)

def load_dataset(data_dir):
    db_file = os.path.join(data_dir, "documents.txt")
    if not os.path.exists(db_file):
        raise RuntimeError(f"Database file not found: {db_file}. Run generate_dataset.py first.")
    
    print(f"Loading documents from {db_file} into memory...")
    docs = []
    with open(db_file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            text = line.strip()
            if text:
                docs.append((f"doc:{i:05d}", f"Document Title {i:05d}", text))
    print(f"Loaded {len(docs)} documents.")
    return docs

def load_queries(queries_file):
    if not os.path.exists(queries_file):
        raise RuntimeError(f"Queries file not found: {queries_file}")
    with open(queries_file, "r", encoding="utf-8") as f:
        queries = [line.strip() for line in f if line.strip()]
    return queries

def run_benchmark_for_threads(server_bin, module_bin, docs, queries, num_threads, port=6399):
    print(f"\n========================================================")
    print(f"Running E2E Benchmark with {num_threads} Reader/Writer & Client Threads")
    print(f"========================================================")
    
    client, conf_path = start_server(server_bin, module_bin, num_threads, num_threads, port)
    
    # 1. Create Index
    print("Creating text index 'bench_idx'...")
    client.execute_command("FT.CREATE", "bench_idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT", "body", "TEXT")
    
    # 2. Ingestion Benchmark
    num_docs = len(docs)
    chunk_size = (num_docs + num_threads - 1) // num_threads
    
    ingest_latencies = []
    lat_lock = threading.Lock()
    
    def ingest_worker(thread_idx, doc_subset):
        thread_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30)
        local_lats = []
        for key, title, body in doc_subset:
            t0 = time.perf_counter()
            thread_client.hset(key, mapping={"title": title, "body": body})
            t1 = time.perf_counter()
            local_lats.append((t1 - t0) * 1000.0)  # ms
        with lat_lock:
            ingest_latencies.extend(local_lats)

    threads = []
    print(f"Ingesting {num_docs} documents with {num_threads} client threads...")
    ingest_start = time.perf_counter()
    for t_idx in range(num_threads):
        start_idx = t_idx * chunk_size
        end_idx = min(num_docs, (t_idx + 1) * chunk_size)
        subset = docs[start_idx:end_idx]
        t = threading.Thread(target=ingest_worker, args=(t_idx, subset))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
    ingest_end = time.perf_counter()
    ingest_duration = ingest_end - ingest_start
    ingest_rate = num_docs / ingest_duration
    token_rate = (num_docs * 500) / ingest_duration
    
    ingest_p50 = statistics.median(ingest_latencies) if ingest_latencies else 0.0
    ingest_p95 = statistics.quantiles(ingest_latencies, n=20)[18] if len(ingest_latencies) >= 20 else ingest_p50
    ingest_p99 = statistics.quantiles(ingest_latencies, n=100)[98] if len(ingest_latencies) >= 100 else ingest_p95

    print(f"Ingestion completed in {ingest_duration:.2f}s | Throughput: {ingest_rate:.1f} docs/s ({token_rate:,.1f} tokens/s) | Latency p50={ingest_p50:.2f}ms, p95={ingest_p95:.2f}ms, p99={ingest_p99:.2f}ms")

    # 3. Wait for indexing to complete & Measure Memory
    # Wait until FT.INFO reports num_docs == 10000
    for _ in range(100):
        try:
            info_idx = client.execute_command("FT.INFO", "bench_idx")
            info_dict = {}
            if isinstance(info_idx, list):
                for i in range(0, len(info_idx), 2):
                    k = info_idx[i].decode() if isinstance(info_idx[i], bytes) else str(info_idx[i])
                    info_dict[k] = info_idx[i+1]
            indexed_docs = int(info_dict.get("num_docs", 0))
            if indexed_docs >= num_docs:
                break
        except Exception:
            pass
        time.sleep(0.2)
        
    time.sleep(0.5)  # brief settle time
    
    search_info = parse_info_search(client)
    mem_info = parse_info_memory(client)
    
    search_mem_bytes = int(search_info.get("search_used_memory_bytes", 0))
    search_mem_human = search_info.get("search_used_memory_human", f"{search_mem_bytes/(1024*1024):.2f}M")
    used_mem_bytes = int(mem_info.get("used_memory", 0))
    used_mem_rss_bytes = int(mem_info.get("used_memory_rss", 0))
    
    print(f"Memory Measured After Ingestion:")
    print(f"  - Search Used Memory: {search_mem_human} ({search_mem_bytes:,} bytes)")
    print(f"  - Total Process Memory: {used_mem_bytes/(1024*1024):.2f} MB (RSS: {used_mem_rss_bytes/(1024*1024):.2f} MB)")

    # 4. Search Benchmark
    TOTAL_SEARCH_QUERIES = 5000
    queries_per_thread = TOTAL_SEARCH_QUERIES // num_threads
    search_latencies = []
    search_lat_lock = threading.Lock()
    
    def search_worker(thread_idx, count):
        thread_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30)
        local_lats = []
        num_q = len(queries)
        for i in range(count):
            q = queries[(thread_idx * count + i) % num_q]
            t0 = time.perf_counter()
            thread_client.execute_command("FT.SEARCH", "bench_idx", q, "LIMIT", "0", "10")
            t1 = time.perf_counter()
            local_lats.append((t1 - t0) * 1000.0)  # ms
        with search_lat_lock:
            search_latencies.extend(local_lats)

    search_threads = []
    print(f"Executing {TOTAL_SEARCH_QUERIES} search queries across {num_threads} client threads...")
    search_start = time.perf_counter()
    for t_idx in range(num_threads):
        t = threading.Thread(target=search_worker, args=(t_idx, queries_per_thread))
        search_threads.append(t)
        t.start()
        
    for t in search_threads:
        t.join()
    search_end = time.perf_counter()
    search_duration = search_end - search_start
    total_searches_done = len(search_latencies)
    search_qps = total_searches_done / search_duration if search_duration > 0 else 0.0
    
    search_p50 = statistics.median(search_latencies) if search_latencies else 0.0
    search_p95 = statistics.quantiles(search_latencies, n=20)[18] if len(search_latencies) >= 20 else search_p50
    search_p99 = statistics.quantiles(search_latencies, n=100)[98] if len(search_latencies) >= 100 else search_p95

    print(f"Search completed in {search_duration:.2f}s | QPS: {search_qps:,.1f} queries/s | Latency p50={search_p50:.2f}ms, p95={search_p95:.2f}ms, p99={search_p99:.2f}ms")

    stop_server(client, conf_path)
    
    return {
        "threads": num_threads,
        "ingest_duration_sec": round(ingest_duration, 3),
        "ingest_throughput_docs_sec": round(ingest_rate, 1),
        "ingest_tokens_sec": round(token_rate, 1),
        "ingest_latency_p50_ms": round(ingest_p50, 3),
        "ingest_latency_p95_ms": round(ingest_p95, 3),
        "ingest_latency_p99_ms": round(ingest_p99, 3),
        "search_used_memory_bytes": search_mem_bytes,
        "search_used_memory_mb": round(search_mem_bytes / (1024 * 1024), 2),
        "total_used_memory_mb": round(used_mem_bytes / (1024 * 1024), 2),
        "used_memory_rss_mb": round(used_mem_rss_bytes / (1024 * 1024), 2),
        "search_qps": round(search_qps, 1),
        "search_latency_p50_ms": round(search_p50, 3),
        "search_latency_p95_ms": round(search_p95, 3),
        "search_latency_p99_ms": round(search_p99, 3),
    }

def main():
    parser = argparse.ArgumentParser(description="Valkey-Search E2E Text Index Benchmark")
    parser.add_argument("--branch-name", default="current", help="Branch or label identifier for results")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="Path to valkey-server binary")
    parser.add_argument("--module", default=DEFAULT_MODULE, help="Path to libsearch.so")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Output CSV file path")
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 4, 8, 16], help="Thread counts to test")
    parser.add_argument("--append", action="store_true", help="Append results to existing CSV instead of overwriting")
    args = parser.parse_args()

    if not os.path.exists(args.server):
        raise FileNotFoundError(f"valkey-server not found at {args.server}")
    if not os.path.exists(args.module):
        raise FileNotFoundError(f"libsearch.so not found at {args.module}")

    docs = load_dataset(DATA_DIR)
    queries = load_queries(QUERIES_FILE)

    all_results = []
    for t in args.threads:
        res = run_benchmark_for_threads(args.server, args.module, docs, queries, t)
        res["branch"] = args.branch_name
        all_results.append(res)

    # Write/Append CSV
    file_exists = os.path.exists(args.csv) and args.append
    fieldnames = [
        "branch", "threads", "ingest_throughput_docs_sec", "ingest_tokens_sec",
        "ingest_duration_sec", "ingest_latency_p50_ms", "ingest_latency_p95_ms", "ingest_latency_p99_ms",
        "search_used_memory_bytes", "search_used_memory_mb", "total_used_memory_mb", "used_memory_rss_mb",
        "search_qps", "search_latency_p50_ms", "search_latency_p95_ms", "search_latency_p99_ms"
    ]

    mode = "a" if file_exists else "w"
    with open(args.csv, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for r in all_results:
            writer.writerow(r)

    print(f"\nBenchmark results successfully written to {args.csv}")

if __name__ == "__main__":
    main()
