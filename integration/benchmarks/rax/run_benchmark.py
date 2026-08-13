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
import redis
import json
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../.."))
DATASET_DIR = os.path.join(SCRIPT_DIR, "dataset")
QUERIES_FILE = os.path.join(DATASET_DIR, "queries.txt")

DEFAULT_SERVER = os.path.join(PROJECT_ROOT, ".build-release/valkey-server/.build-release/bin/valkey-server")
DEFAULT_MODULE = os.path.join(PROJECT_ROOT, ".build-release/libsearch.so")
DEFAULT_CLI = os.path.join(PROJECT_ROOT, ".build-release/valkey-server/.build-release/bin/valkey-cli")

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
        "summary": "Evaluates pure TEXT indexing without TAG fields. Performs single-pass document ingestion, measures post-ingestion memory footprint, and runs pure text search queries.",
        "schema_type": "text_only",
        "has_mutation": False,
        "search_mode": "text_only",
    },
    "text_tag_mixed": {
        "name": "TextTag_MixedWorkload",
        "title": "Setup 2: TEXT + TAG Index (Standard Mixed Workload)",
        "summary": "Evaluates combined TEXT and TAG indexing. Ingests full documents with multi-value tags, measures post-ingestion memory, and executes mixed search queries (Exact terms, TAG filters, and Prefix wildcards).",
        "schema_type": "text_tag",
        "has_mutation": False,
        "search_mode": "mixed",
    },
    "text_tag_churn_prefix": {
        "name": "TextTag_MutationChurn_Prefix",
        "title": "Setup 3: TEXT + TAG Index (Mutation Churn + Pure Prefix Subtree Traversal)",
        "summary": "Evaluates memory churn and radix tree subtree traversal. Ingests full documents, overwrites/mutates 50% of the documents with updated content, measures post-mutation memory fragmentation, and executes pure prefix wildcard searches (term*).",
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

def load_dataset(dataset_dir):
    db_file = os.path.join(dataset_dir, "documents.txt")
    if not os.path.exists(db_file):
        raise RuntimeError(f"Database file not found: {db_file}. Run generate_dataset.py first.")
    
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
        thread_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30)
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
    token_rate = (num_docs * 500) / ingest_duration if ingest_duration > 0 else 0.0
    
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
            thread_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30)
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
    for _ in range(120):
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

    # 5. Search Benchmark Phase
    if setup_info["search_mode"] == "prefix_only":
        active_queries = [q for q in queries if q.endswith("*")]
        if not active_queries:
            active_queries = queries
        print(f"Phase 3: Running Pure Prefix Searches ({len(active_queries)} distinct prefix* queries)...")
    elif setup_info["search_mode"] == "text_only":
        active_queries = [q for q in queries if not q.startswith("@tags:")]
        if not active_queries:
            active_queries = queries
        print(f"Phase 2: Running Pure Text Searches ({len(active_queries)} distinct queries)...")
    else:
        active_queries = queries
        print(f"Phase 2: Running Mixed Searches ({len(active_queries)} distinct queries)...")

    TOTAL_SEARCH_QUERIES = 20000
    queries_per_thread = TOTAL_SEARCH_QUERIES // num_threads
    search_latencies = []
    search_lat_lock = threading.Lock()
    
    def search_worker(thread_idx, count):
        thread_client = redis.Redis(host="127.0.0.1", port=port, socket_timeout=30)
        local_lats = []
        num_q = len(active_queries)
        for i in range(count):
            q = active_queries[(thread_idx * count + i) % num_q]
            t0 = time.perf_counter()
            try:
                thread_client.execute_command("FT.SEARCH", "bench_idx", q, "LIMIT", "0", "10")
                t1 = time.perf_counter()
                local_lats.append((t1 - t0) * 1000.0)  # ms
            except Exception:
                pass
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

def finalize_and_print_stats(csv_path):
    raw_rows = []
    if os.path.exists(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("branch") and not row["branch"].startswith("#"):
                    raw_rows.append(row)

    branches = list(dict.fromkeys([r["branch"] for r in raw_rows]))
    opt_branch = "optimized_rax" if "optimized_rax" in branches else (branches[0] if branches else "optimized_rax")
    other_branches = [b for b in branches if b != opt_branch]
    base_branch = "main" if "main" in other_branches else (other_branches[0] if other_branches else None)

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

    # Re-write the complete finalized file
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=RAW_FIELDNAMES)
        writer.writeheader()
        for r in raw_rows:
            writer.writerow(r)
            
        if benefit_rows:
            f.write(f"\n# Calculated Percentage Benefits ({opt_branch} vs {base_branch})\n")
            bwriter = csv.DictWriter(f, fieldnames=BENEFIT_FIELDNAMES)
            bwriter.writeheader()
            for br in benefit_rows:
                bwriter.writerow(br)

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
    parser.add_argument("--threads", nargs="+", type=int, default=[1, 4, 8, 16], help="Thread counts to test")
    parser.add_argument("--setups", nargs="+", choices=["text_only", "text_tag_mixed", "text_tag_churn_prefix", "all"], default=["all"], help="Benchmark setups to run")
    parser.add_argument("--append", action="store_true", help="Append results to existing CSV instead of overwriting")
    args = parser.parse_args()

    if not os.path.exists(args.server):
        raise FileNotFoundError(f"valkey-server not found at {args.server}")
    if not os.path.exists(args.module):
        raise FileNotFoundError(f"libsearch.so not found at {args.module}")

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
    finalize_and_print_stats(csv_path)
    print(f"[Generated Stats File Full Path]: {csv_path}\n")

if __name__ == "__main__":
    main()
