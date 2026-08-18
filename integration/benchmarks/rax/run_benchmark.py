#!/usr/bin/env python3
"""
End-to-End Benchmark for Valkey-Search Text & Tag Index.
Evaluates Ingestion Speed, Mutation/Update Churn Throughput, Search QPS, Latency,
and Memory Footprint across thread counts comparing optimized_rax vs main.

Emits structured CSV results with timestamps and prints
human-friendly formatted tables to stdout with detailed setup summaries.
Incrementally persists results to disk after each benchmark run.
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
try:
    import redis
except ImportError:
    import valkey as redis
import json
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../.."))
DATASET_DIR = os.path.join(SCRIPT_DIR, "dataset")
QUERIES_FILE = os.path.join(DATASET_DIR, "queries.txt")

DEFAULT_SERVER = os.environ.get("VALKEY_SERVER_PATH", os.path.join(PROJECT_ROOT, ".build-release/valkey-server/.build-release/bin/valkey-server"))
DEFAULT_MODULE = os.environ.get("VALKEY_SEARCH_PATH", os.path.join(PROJECT_ROOT, ".build-release/libsearch.so"))
DEFAULT_CLI = os.environ.get("VALKEY_CLI_PATH", os.path.join(PROJECT_ROOT, ".build-release/valkey-server/.build-release/bin/valkey-cli"))

def get_default_csv_path():
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    return f"/tmp/e2e_rax_bench_stats_{timestamp}.csv"

RAW_FIELDNAMES = [
    "branch", "setup", "threads", "ingest_throughput_docs_sec", "ingest_tokens_sec",
    "ingest_duration_sec", "ingest_latency_p50_ms", "ingest_latency_p95_ms", "ingest_latency_p99_ms",
    "mutation_throughput_docs_sec", "mutation_latency_p50_ms",
    "search_used_memory_bytes", "search_used_memory_mb", "total_used_memory_mb", "used_memory_rss_mb",
    "search_qps", "search_latency_p50_ms", "search_latency_p95_ms", "search_latency_p99_ms"
]

BENEFIT_FIELDNAMES = [
    "setup", "threads", "ingest_throughput_benefit_pct", "mutation_throughput_benefit_pct",
    "ingest_duration_reduction_pct", "ingest_latency_p50_reduction_pct",
    "search_used_memory_savings_pct", "total_used_memory_savings_pct",
    "used_memory_rss_savings_pct", "search_qps_benefit_pct", "search_latency_p50_reduction_pct"
]

SETUPS = {
    "text_only": {
        "name": "TextOnly_LinearIngest",
        "title": "Setup 1: Pure TEXT Index (Linear Ingestion -> Pure Text Searches)",
        "summary": "Evaluates pure TEXT indexing without TAG fields. Performs single-pass document ingestion, measures post-ingestion memory footprint, and runs representative single-term text search queries.",
        "schema_type": "text_only",
        "has_mutation": False,
        "search_mode": "text_only",
    },
    "text_tag_mixed": {
        "name": "TextTag_MixedWorkload",
        "title": "Setup 2: TEXT + TAG Index (Standard Mixed Workload)",
        "summary": "Evaluates combined TEXT and TAG indexing. Ingests full documents with multi-value tags, measures post-ingestion memory, and executes representative tag and term search queries.",
        "schema_type": "text_tag",
        "has_mutation": False,
        "search_mode": "mixed",
    },
    "text_tag_churn_prefix": {
        "name": "TextTag_MutationChurn_Prefix",
        "title": "Setup 3: TEXT + TAG Index (Mutation Churn + Pure Prefix Subtree Traversal)",
        "summary": "Evaluates memory churn and radix tree subtree traversal. Ingests full documents, overwrites/mutates 50% of the documents with updated content, measures post-mutation memory fragmentation, and executes representative prefix wildcard searches.",
        "schema_type": "text_tag",
        "has_mutation": True,
        "search_mode": "prefix_only",
    },
}

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

def find_free_port():
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def start_server(server_bin, module_bin, reader_threads, writer_threads, port):
    conf_path = f"/tmp/valkey_e2e_{port}.conf"
    pid_path = f"/tmp/valkey_e2e_{port}.pid"
    log_path = f"/tmp/valkey_e2e_{port}.log"
    
    try:
        tmp_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=2)
        tmp_client.shutdown(save=False)
        time.sleep(0.3)
    except Exception:
        pass
    try:
        subprocess.run(["pkill", "-9", "-f", f"valkey_e2e_{port}.conf"], check=False)
        time.sleep(0.3)
    except Exception:
        pass

    with open(conf_path, "w") as f:
        f.write(f"""port {port}
save ""
appendonly no
io-threads 4
io-threads-do-reads yes
pidfile {pid_path}
logfile {log_path}
loadmodule {module_bin} --reader-threads {reader_threads} --writer-threads {writer_threads}
""")
    
    proc = subprocess.Popen([server_bin, conf_path, "--daemonize", "yes"])
    proc.wait()
    
    client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=10)
    for _ in range(60):
        try:
            if client.ping():
                client.flushall()
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

def generate_dataset_if_missing(dataset_dir, num_docs=15000):
    db_file = os.path.join(dataset_dir, "documents.txt")
    queries_file = os.path.join(dataset_dir, "queries.txt")
    if os.path.exists(db_file) and os.path.exists(queries_file):
        return

    os.makedirs(dataset_dir, exist_ok=True)
    print(f"[INFO] Dataset missing in '{dataset_dir}'. Auto-generating {num_docs} synthetic benchmark documents...")

    import random, string
    rng = random.Random(42)

    vocab = [''.join(rng.choices(string.ascii_lowercase, k=rng.randint(4, 10))) for _ in range(2000)]
    prefix_vocab = ['pref_' + ''.join(rng.choices(string.ascii_lowercase, k=5)) for _ in range(100)]
    all_vocab = vocab + prefix_vocab

    if not os.path.exists(db_file):
        with open(db_file, "w", encoding="utf-8") as f:
            for i in range(num_docs):
                cat = f"cat_{i % 100:03d}"
                status = ["active", "pending", "draft", "archived", "verified"][i % 5]
                region = f"region_{i % 50:02d}"
                dept = f"dept_{i % 200:03d}"
                sku = f"sku_{i % 1000:04d}"
                tags = f"{cat},{status},{region},{dept},{sku}"

                doc_words = rng.choices(all_vocab, k=300)
                body = " ".join(doc_words)
                f.write(f"{tags}\t{body}\n")

    if not os.path.exists(queries_file):
        with open(queries_file, "w", encoding="utf-8") as f:
            sample_queries = rng.choices(vocab, k=50) + rng.choices(prefix_vocab, k=50)
            for q in sample_queries:
                f.write(f"{q}\n")

def load_dataset(dataset_dir):
    generate_dataset_if_missing(dataset_dir)
    db_file = os.path.join(dataset_dir, "documents.txt")
    if not os.path.exists(db_file):
        raise RuntimeError(f"Database file not found: {db_file}")
    
    print(f"Loading documents from {db_file} into memory...")
    docs = []
    with open(db_file, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            if "\t" in line:
                tags, body = line.split("\t", 1)
            else:
                tags, body = "default_tag", line
            docs.append((f"doc:{i:05d}", f"Document Title {i:05d}", tags, body))
    print(f"Loaded {len(docs)} documents.")
    return docs

def load_queries(queries_file):
    dataset_dir = os.path.dirname(queries_file)
    generate_dataset_if_missing(dataset_dir)
    if not os.path.exists(queries_file):
        raise RuntimeError(f"Queries file not found: {queries_file}")
    with open(queries_file, "r", encoding="utf-8") as f:
        queries = [line.strip() for line in f if line.strip()]
    return queries

def init_stats_file(csv_path, append=False):
    os.makedirs(os.path.dirname(os.path.abspath(csv_path)), exist_ok=True)
    if append and os.path.exists(csv_path):
        print(f"\n[Benchmark Stats File Attached (Append Mode)]: {os.path.abspath(csv_path)}")
        return
        
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_FIELDNAMES)
        writer.writeheader()
        f.flush()
    print(f"\n[Benchmark Stats File Initialized]: {os.path.abspath(csv_path)}")

def append_single_result(csv_path, result_dict):
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_FIELDNAMES)
        writer.writerow(result_dict)
        f.flush()
    print(f"  -> Persisted incremental stats for [{result_dict.get('setup')}] ({result_dict.get('threads')} threads) to {csv_path}")

def run_benchmark_for_threads(server_bin, module_bin, docs, queries, num_threads, setup_key="text_tag_mixed", port=None):
    setup_info = SETUPS[setup_key]
    setup_name = setup_info["name"]
    
    if port is None:
        port = find_free_port()
        
    print(f"\n" + "=" * 80)
    print(f"{setup_info['title']}")
    print(f"Summary : {setup_info['summary']}")
    print(f"Threads : {num_threads} Server Worker Threads | {num_threads} Client Worker Threads (port {port})")
    print("=" * 80)
    
    client, conf_path = start_server(server_bin, module_bin, num_threads, num_threads, port)
    try:
        # 1. Create Index based on setup
        if setup_info["schema_type"] == "text_only":
            print("Creating pure TEXT index 'bench_idx'...")
            client.execute_command(
                "FT.CREATE", "bench_idx", "ON", "HASH", "PREFIX", "1", "doc:", "STOPWORDS", "0",
                "SCHEMA", "title", "TEXT", "body", "TEXT"
            )
        else:
            print("Creating TEXT + TAG index 'bench_idx'...")
            client.execute_command(
                "FT.CREATE", "bench_idx", "ON", "HASH", "PREFIX", "1", "doc:", "STOPWORDS", "0",
                "SCHEMA", "title", "TEXT", "tags", "TAG", "SEPARATOR", ",", "body", "TEXT"
            )
        
        # 2. Ingestion Phase
        num_docs = len(docs)
        chunk_size = (num_docs + num_threads - 1) // num_threads
        
        ingest_latencies = []
        lat_lock = threading.Lock()
        
        def ingest_worker(thread_idx, doc_subset):
            thread_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=300)
            local_lats = []
            for key, title, tags, body in doc_subset:
                t0 = time.perf_counter()
                if setup_info["schema_type"] == "text_only":
                    thread_client.hset(key, mapping={"title": title, "body": body})
                else:
                    thread_client.hset(key, mapping={"title": title, "tags": tags, "body": body})
                t1 = time.perf_counter()
                local_lats.append((t1 - t0) * 1000.0)  # ms
            with lat_lock:
                ingest_latencies.extend(local_lats)

        threads = []
        print(f"Phase 1: Ingesting {num_docs} documents with {num_threads} client threads...")
        ingest_start = time.perf_counter()
        for t_idx in range(num_threads):
            start_idx = t_idx * chunk_size
            end_idx = min(num_docs, (t_idx + 1) * chunk_size)
            subset = docs[start_idx:end_idx]
            if subset:
                t = threading.Thread(target=ingest_worker, args=(t_idx, subset))
                threads.append(t)
                t.start()
            
        for t in threads:
            t.join()
        ingest_end = time.perf_counter()
        ingest_duration = ingest_end - ingest_start
        ingest_rate = num_docs / ingest_duration if ingest_duration > 0 else 0.0
        total_tokens = sum(len(title.split()) + len(body.split()) for _, title, _, body in docs)
        token_rate = total_tokens / ingest_duration if ingest_duration > 0 else 0.0
        
        ingest_p50 = statistics.median(ingest_latencies) if ingest_latencies else 0.0
        ingest_p95 = statistics.quantiles(ingest_latencies, n=20)[18] if len(ingest_latencies) >= 20 else ingest_p50
        ingest_p99 = statistics.quantiles(ingest_latencies, n=100)[98] if len(ingest_latencies) >= 100 else ingest_p95

        print(f"Ingestion completed in {ingest_duration:.2f}s | Throughput: {ingest_rate:,.1f} docs/s ({token_rate:,.1f} tokens/s) | Latency p50={ingest_p50:.2f}ms, p95={ingest_p95:.2f}ms, p99={ingest_p99:.2f}ms")

        # 3. Mutation Phase (only in text_tag_churn_prefix setup)
        update_rate = 0.0
        update_p50 = 0.0
        if setup_info["has_mutation"]:
            NUM_UPDATES = num_docs // 2
            update_chunk_size = (NUM_UPDATES + num_threads - 1) // num_threads
            update_latencies = []
            update_lat_lock = threading.Lock()
            
            def update_worker(thread_idx, doc_subset):
                thread_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=300)
                local_lats = []
                for key, title, tags, body in doc_subset:
                    updated_tags = tags + ",updated_tag,v2"
                    updated_body = body[:250] + " updated_content " + body[250:]
                    t0 = time.perf_counter()
                    thread_client.hset(key, mapping={"title": f"Updated {title}", "tags": updated_tags, "body": updated_body})
                    t1 = time.perf_counter()
                    local_lats.append((t1 - t0) * 1000.0)
                with update_lat_lock:
                    update_latencies.extend(local_lats)

            update_threads = []
            print(f"Phase 2: Updating {NUM_UPDATES} documents with {num_threads} threads (mutation churn)...")
            update_start = time.perf_counter()
            for t_idx in range(num_threads):
                start_idx = t_idx * update_chunk_size
                end_idx = min(NUM_UPDATES, (t_idx + 1) * update_chunk_size)
                subset = docs[start_idx:end_idx]
                if subset:
                    t = threading.Thread(target=update_worker, args=(t_idx, subset))
                    update_threads.append(t)
                    t.start()
                    
            for t in update_threads:
                t.join()
            update_end = time.perf_counter()
            update_duration = update_end - update_start
            update_rate = NUM_UPDATES / update_duration if update_duration > 0 else 0.0
            update_p50 = statistics.median(update_latencies) if update_latencies else 0.0
            print(f"Updates completed in {update_duration:.2f}s | Mutation Throughput: {update_rate:,.1f} updates/s | Latency p50={update_p50:.2f}ms")

        # 4. Wait for indexing to settle & Measure Memory
        settled = False
        last_info_error = None
        indexed_docs = 0
        for _ in range(120):
            try:
                info_idx = client.execute_command("FT.INFO", "bench_idx")
                info_dict = {}
                if isinstance(info_idx, list):
                    for i in range(0, len(info_idx), 2):
                        k = info_idx[i].decode() if isinstance(info_idx[i], bytes) else str(info_idx[i])
                        info_dict[k] = info_idx[i+1]
                elif isinstance(info_idx, dict):
                    info_dict = {k.decode() if isinstance(k, bytes) else str(k): v for k, v in info_idx.items()}
                else:
                    raise ValueError(f"Unexpected FT.INFO output type: {type(info_idx)}")

                raw_docs = info_dict.get("num_docs") or info_dict.get(b"num_docs", 0)
                indexed_docs = int(raw_docs)
                if indexed_docs >= num_docs:
                    settled = True
                    break
            except Exception as e:
                last_info_error = e
            time.sleep(0.2)

        if not settled:
            err_msg = f"Indexing failed to settle: indexed {indexed_docs}/{num_docs} documents before timeout."
            if last_info_error:
                err_msg += f" Last FT.INFO error: {last_info_error}"
            print(f"[ERROR] {err_msg}")
            raise RuntimeError(err_msg)

        time.sleep(0.5)
        
        search_info = parse_info_search(client)
        mem_info = parse_info_memory(client)
        
        search_mem_bytes = int(search_info.get("search_used_memory_bytes", 0))
        search_mem_human = search_info.get("search_used_memory_human", f"{search_mem_bytes/(1024*1024):.2f}M")
        used_mem_bytes = int(mem_info.get("used_memory", 0))
        used_mem_rss_bytes = int(mem_info.get("used_memory_rss", 0))
        
        print(f"Memory Measured:")
        print(f"  - Search Used Memory: {search_mem_human} ({search_mem_bytes:,} bytes)")
        print(f"  - Total Process Memory: {used_mem_bytes/(1024*1024):.2f} MB (RSS: {used_mem_rss_bytes/(1024*1024):.2f} MB)")

        # 5. Search Benchmark Phase using memtier_benchmark (16 threads, 1 client per thread)
        pure_terms = [q for q in queries if not q.startswith("@tags:") and not q.endswith("*") and " " not in q and len(q) >= 3]
        prefix_terms = [q for q in queries if q.endswith("*")]

        active_queries = []
        if setup_info["search_mode"] == "prefix_only":
            print(f"Phase 3: Generating single-prefix queries for memtier_benchmark...")
            for p in prefix_terms[:50]:
                active_queries.append(f"FT.SEARCH bench_idx {p} NOCONTENT LIMIT 0 10")
        elif setup_info["search_mode"] == "text_only":
            print(f"Phase 2: Generating single-term text queries for memtier_benchmark...")
            for w in pure_terms[:50]:
                active_queries.append(f"FT.SEARCH bench_idx {w} NOCONTENT LIMIT 0 10")
        else:
            print(f"Phase 2: Generating tag and term queries for memtier_benchmark...")
            for i, w in enumerate(pure_terms[:30]):
                c = f"cat_{i%100:03d}"
                active_queries.append(f"FT.SEARCH bench_idx @tags:{{{c}}} NOCONTENT LIMIT 0 10")
                active_queries.append(f"FT.SEARCH bench_idx {w} NOCONTENT LIMIT 0 10")

        active_queries = active_queries[:50]
        if not active_queries:
            active_queries = ["FT.SEARCH bench_idx * NOCONTENT LIMIT 0 10"]

        TOTAL_SEARCH_QUERIES = 5000
        MEMTIER_THREADS = 16
        MEMTIER_CLIENTS = 1
        requests_per_client = (TOTAL_SEARCH_QUERIES + (MEMTIER_THREADS * MEMTIER_CLIENTS) - 1) // (MEMTIER_THREADS * MEMTIER_CLIENTS)
        
        json_filename = f".memtier_out_{port}.json"
        json_out_path = os.path.join(PROJECT_ROOT, json_filename)
        if os.path.exists(json_out_path):
            os.remove(json_out_path)
            
        docker_runner = os.path.join(PROJECT_ROOT, ".devcontainer/run_in_docker.sh")
        memtier_cmd = [
            docker_runner, "memtier_benchmark",
            "--server=127.0.0.1",
            f"--port={port}",
            "--protocol=redis",
            f"--threads={MEMTIER_THREADS}",
            f"--clients={MEMTIER_CLIENTS}",
            f"--requests={requests_per_client}",
            "--print-percentiles=50,95,99",
            "--hide-histogram",
            f"--json-out-file={json_filename}"
        ]
        memtier_cmd.append(f"--command={active_queries[0]}")
        print(f"Executing memtier_benchmark ({MEMTIER_THREADS} threads, {MEMTIER_CLIENTS} client/thread, representative query '{active_queries[0]}', target={TOTAL_SEARCH_QUERIES} queries)...")
        search_start = time.perf_counter()
        subprocess.run(memtier_cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        search_end = time.perf_counter()
        search_duration = search_end - search_start
        
        if not os.path.exists(json_out_path):
            raise RuntimeError(f"memtier_benchmark JSON output file not found at '{json_out_path}'.")

        with open(json_out_path, "r") as f:
            data = json.load(f)

        all_stats = data.get("ALL STATS")
        if not all_stats or "Totals" not in all_stats:
            raise RuntimeError("Malformed memtier_benchmark JSON output: 'ALL STATS.Totals' section missing.")

        totals = all_stats["Totals"]
        if "Ops/sec" not in totals:
            raise RuntimeError("Malformed memtier_benchmark JSON output: 'Ops/sec' metric missing from Totals.")
        search_qps = float(totals["Ops/sec"])

        percentiles = totals.get("Percentile Latencies")
        if not percentiles:
            raise RuntimeError("Malformed memtier_benchmark JSON output: 'Percentile Latencies' missing from Totals.")

        p50_val = percentiles.get("p50.000", percentiles.get("p50.00"))
        p95_val = percentiles.get("p95.000", percentiles.get("p95.00"))
        p99_val = percentiles.get("p99.000", percentiles.get("p99.00"))

        if p50_val is None or p95_val is None or p99_val is None:
            raise RuntimeError(f"Malformed memtier_benchmark JSON output: Missing required percentiles (p50.000, p95.000, p99.000) in {percentiles}")

        search_p50 = float(p50_val)
        search_p95 = float(p95_val)
        search_p99 = float(p99_val)

        try:
            os.remove(json_out_path)
        except OSError:
            pass
                
        print(f"Search completed in {search_duration:.2f}s | QPS: {search_qps:,.1f} queries/s | Latency p50={search_p50:.2f}ms, p95={search_p95:.2f}ms, p99={search_p99:.2f}ms")

        return {
            "setup": setup_name,
            "threads": num_threads,
            "ingest_throughput_docs_sec": round(ingest_rate, 1),
            "ingest_tokens_sec": round(token_rate, 1),
            "ingest_duration_sec": round(ingest_duration, 3),
            "ingest_latency_p50_ms": round(ingest_p50, 3),
            "ingest_latency_p95_ms": round(ingest_p95, 3),
            "ingest_latency_p99_ms": round(ingest_p99, 3),
            "mutation_throughput_docs_sec": round(update_rate, 1) if setup_info["has_mutation"] else "N/A",
            "mutation_latency_p50_ms": round(update_p50, 3) if setup_info["has_mutation"] else "N/A",
            "search_used_memory_bytes": search_mem_bytes,
            "search_used_memory_mb": round(search_mem_bytes / (1024 * 1024), 2),
            "total_used_memory_mb": round(used_mem_bytes / (1024 * 1024), 2),
            "used_memory_rss_mb": round(used_mem_rss_bytes / (1024 * 1024), 2),
            "search_qps": round(search_qps, 1),
            "search_latency_p50_ms": round(search_p50, 3),
            "search_latency_p95_ms": round(search_p95, 3),
            "search_latency_p99_ms": round(search_p99, 3),
        }
    finally:
        stop_server(client, conf_path)

def format_table(headers, rows):
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
            
    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator_line = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    
    table_lines = [header_line, separator_line]
    for row in rows:
        table_lines.append(" | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row)))
    return "\n".join(table_lines)

def finalize_and_print_stats(csv_path, baseline_csv=None):
    raw_rows = []
    def is_valid_row(r):
        if not r or not r.get("branch") or r["branch"].startswith("#"):
            return False
        try:
            float(r.get("ingest_throughput_docs_sec", ""))
            return True
        except (ValueError, TypeError):
            return False

    if os.path.exists(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if is_valid_row(row):
                    raw_rows.append(row)

    if baseline_csv and os.path.exists(baseline_csv):
        with open(baseline_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if is_valid_row(row):
                    if not any(r.get("branch") == row["branch"] and r.get("setup") == row.get("setup") and str(r.get("threads")) == str(row.get("threads")) for r in raw_rows):
                        raw_rows.append(row)

    branches = list(dict.fromkeys([r["branch"] for r in raw_rows]))
    opt_branch = "current" if "current" in branches else ("optimized_rax" if "optimized_rax" in branches else (branches[0] if branches else "current"))
    other_branches = [b for b in branches if b != opt_branch]
    base_branch = "main" if "main" in other_branches else ("optimized_rax" if "optimized_rax" in other_branches else (other_branches[0] if other_branches else None))

    opt_by_key = {(r.get("setup", "TextTag_MixedWorkload"), int(r["threads"])): r for r in raw_rows if r["branch"] == opt_branch}
    base_by_key = {(r.get("setup", "TextTag_MixedWorkload"), int(r["threads"])): r for r in raw_rows if r["branch"] == base_branch} if base_branch else {}

    benefit_rows = []
    for (s_name, t) in sorted(opt_by_key.keys(), key=lambda x: (x[0], x[1])):
        if (s_name, t) in base_by_key:
            opt = opt_by_key[(s_name, t)]
            base = base_by_key[(s_name, t)]
            
            opt_ingest = float(opt["ingest_throughput_docs_sec"])
            base_ingest = float(base["ingest_throughput_docs_sec"])
            ingest_tput_pct = ((opt_ingest - base_ingest) / base_ingest) * 100.0 if base_ingest else 0.0
            
            opt_mut_str = opt.get("mutation_throughput_docs_sec", "N/A")
            base_mut_str = base.get("mutation_throughput_docs_sec", "N/A")
            if opt_mut_str not in ("N/A", "") and base_mut_str not in ("N/A", ""):
                opt_mut = float(opt_mut_str)
                base_mut = float(base_mut_str)
                mut_pct = ((opt_mut - base_mut) / base_mut) * 100.0 if base_mut else 0.0
                mut_fmt = f"{mut_pct:+.2f}%" if mut_pct != 0 else "0.00%"
            else:
                mut_fmt = "N/A"
            
            opt_dur = float(opt["ingest_duration_sec"])
            base_dur = float(base["ingest_duration_sec"])
            dur_pct = ((base_dur - opt_dur) / base_dur) * 100.0 if base_dur else 0.0
            
            opt_ing_lat = float(opt["ingest_latency_p50_ms"])
            base_ing_lat = float(base["ingest_latency_p50_ms"])
            ing_lat_pct = ((base_ing_lat - opt_ing_lat) / base_ing_lat) * 100.0 if base_ing_lat else 0.0
            
            opt_smem = float(opt["search_used_memory_bytes"])
            base_smem = float(base["search_used_memory_bytes"])
            smem_pct = ((base_smem - opt_smem) / base_smem) * 100.0 if base_smem else 0.0
            
            opt_tot_mem = float(opt["total_used_memory_mb"])
            base_tot_mem = float(base["total_used_memory_mb"])
            tot_mem_pct = ((base_tot_mem - opt_tot_mem) / base_tot_mem) * 100.0 if base_tot_mem else 0.0
            
            opt_rss = float(opt["used_memory_rss_mb"])
            base_rss = float(base["used_memory_rss_mb"])
            rss_pct = ((base_rss - opt_rss) / base_rss) * 100.0 if base_rss else 0.0
            
            opt_qps = float(opt["search_qps"])
            base_qps = float(base["search_qps"])
            qps_pct = ((opt_qps - base_qps) / base_qps) * 100.0 if base_qps else 0.0
            
            opt_srch_lat = float(opt["search_latency_p50_ms"])
            base_srch_lat = float(base["search_latency_p50_ms"])
            srch_lat_pct = ((base_srch_lat - opt_srch_lat) / base_srch_lat) * 100.0 if base_srch_lat else 0.0
            
            def fmt(v):
                return f"{v:+.2f}%" if v != 0 else "0.00%"
                
            benefit_rows.append({
                "setup": s_name,
                "threads": t,
                "ingest_throughput_benefit_pct": fmt(ingest_tput_pct),
                "mutation_throughput_benefit_pct": mut_fmt,
                "ingest_duration_reduction_pct": fmt(dur_pct),
                "ingest_latency_p50_reduction_pct": fmt(ing_lat_pct),
                "search_used_memory_savings_pct": fmt(smem_pct),
                "total_used_memory_savings_pct": fmt(tot_mem_pct),
                "used_memory_rss_savings_pct": fmt(rss_pct),
                "search_qps_benefit_pct": fmt(qps_pct),
                "search_latency_p50_reduction_pct": fmt(srch_lat_pct),
            })

    # Re-write the complete finalized file atomically via a temporary file
    tmp_csv_path = f"{csv_path}.tmp"
    try:
        with open(tmp_csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=RAW_FIELDNAMES, extrasaction="ignore")
            writer.writeheader()
            for r in raw_rows:
                writer.writerow(r)
                
            if benefit_rows:
                f.write(f"\n# Calculated Percentage Benefits ({opt_branch} vs {base_branch})\n")
                bwriter = csv.DictWriter(f, fieldnames=BENEFIT_FIELDNAMES, extrasaction="ignore")
                bwriter.writeheader()
                for br in benefit_rows:
                    bwriter.writerow(br)
        os.replace(tmp_csv_path, csv_path)
    except Exception:
        if os.path.exists(tmp_csv_path):
            try:
                os.remove(tmp_csv_path)
            except OSError:
                pass
        raise

    # Print Human-Friendly Formatted Version to stdout
    print("\n" + "=" * 110)
    print(f"BENCHMARK RESULTS SUMMARY (Saved to: {os.path.abspath(csv_path)})")
    print("=" * 110)
    
    raw_headers = ["Branch", "Setup", "Threads", "Ingest (docs/s)", "Ingest p50", "Mutations/s", "Search QPS", "Search p50", "Search RAM (MB)", "Process RSS (MB)"]
    raw_display_rows = []
    for r in raw_rows:
        raw_display_rows.append([
            r["branch"],
            r["setup"],
            r["threads"],
            f"{float(r['ingest_throughput_docs_sec']):,.1f}",
            f"{float(r['ingest_latency_p50_ms']):.2f}ms",
            f"{float(r['mutation_throughput_docs_sec']):,.1f}" if r["mutation_throughput_docs_sec"] != "N/A" else "N/A",
            f"{float(r['search_qps']):,.1f}",
            f"{float(r['search_latency_p50_ms']):.2f}ms",
            f"{float(r['search_used_memory_mb']):,.1f}",
            f"{float(r['used_memory_rss_mb']):,.1f}",
        ])
    print(format_table(raw_headers, raw_display_rows))
    
    if benefit_rows:
        print("\n" + "=" * 110)
        print(f"CALCULATED PERCENTAGE BENEFITS ({opt_branch} vs {base_branch})")
        print("=" * 110)
        b_headers = ["Setup", "Threads", "Ingest Throughput", "Mutation Rate", "Search QPS", "Search p50 Latency", "Search RAM Savings", "RSS Savings"]
        b_display_rows = []
        for br in benefit_rows:
            b_display_rows.append([
                br["setup"],
                br["threads"],
                br["ingest_throughput_benefit_pct"],
                br["mutation_throughput_benefit_pct"],
                br["search_qps_benefit_pct"],
                br["search_latency_p50_reduction_pct"],
                br["search_used_memory_savings_pct"],
                br["used_memory_rss_savings_pct"],
            ])
        print(format_table(b_headers, b_display_rows))
    print("=" * 110 + "\n")

def main():
    parser = argparse.ArgumentParser(description="Valkey-Search E2E Text & Tag Index Benchmark")
    parser.add_argument("--branch-name", default="current", help="Branch or label identifier for results")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="Path to valkey-server binary")
    parser.add_argument("--module", default=DEFAULT_MODULE, help="Path to libsearch.so")
    parser.add_argument("--csv", default=None, help="Output CSV file path (default: /tmp/e2e_rax_bench_stats_<TIMESTAMP>.csv)")
    parser.add_argument("--baseline-csv", default=None, help="Path to pre-captured baseline benchmark CSV to reuse historical results without re-running baseline benchmarks")
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 4, 8, 16], help="Thread counts to test")
    parser.add_argument("--setups", nargs="+", choices=["text_only", "text_tag_mixed", "text_tag_churn_prefix", "all"], default=["all"], help="Benchmark setups to run")
    parser.add_argument("--append", action="store_true", help="Append results to existing CSV instead of overwriting")
    args = parser.parse_args()

    if not os.path.exists(args.server):
        raise FileNotFoundError(f"valkey-server not found at {args.server}")
    if not os.path.exists(args.module):
        print(f"[INFO] libsearch.so not found at '{args.module}'. Building automatically...")
        build_script = os.path.join(PROJECT_ROOT, "build.sh")
        subprocess.run([build_script], check=True, cwd=PROJECT_ROOT)

    csv_path = os.path.abspath(args.csv if args.csv else get_default_csv_path())
    
    # 1. Initialize stats file at the beginning of execution and emit path
    init_stats_file(csv_path, append=args.append)

    docs = load_dataset(DATASET_DIR)
    queries = load_queries(QUERIES_FILE)

    chosen_setups = ["text_only", "text_tag_mixed", "text_tag_churn_prefix"] if "all" in args.setups else args.setups

    # 2. Incrementally execute setups and update file after each run
    for s_key in chosen_setups:
        for t in args.threads:
            res = run_benchmark_for_threads(args.server, args.module, docs, queries, t, setup_key=s_key)
            res["branch"] = args.branch_name
            append_single_result(csv_path, res)

    # 3. Finalize summary and print human-friendly tables
    finalize_and_print_stats(csv_path, baseline_csv=args.baseline_csv)
    print(f"[Generated Stats File Full Path]: {csv_path}\n")

if __name__ == "__main__":
    main()
