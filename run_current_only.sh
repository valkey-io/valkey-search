#!/bin/bash
set -e

SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN=".build-release/libsearch.so"

echo "=== RUNNING OPTIMIZED_RAX BENCHMARK (SECOND RUN) ==="
pgrep valkey-server | xargs kill -9 || true
python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name current2 \
    --setups all \
    --threads 1 4 8 16 \
    --baseline-csv /tmp/main_baseline.csv \
    --csv /tmp/final_report_run2.csv > /tmp/opt_bench_run2.log 2>&1

echo "=== ALL FINISHED ==="
