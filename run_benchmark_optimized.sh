#!/bin/bash
set -e

echo "Waiting for build and tests to finish..."
while pgrep -f ctest > /dev/null; do sleep 2; done

echo "=== RUNNING OPTIMIZED BENCHMARK ==="
SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN=".build-release/libsearch.so"

pgrep valkey-server | xargs kill -9 || true
python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name current_tcache \
    --setups all \
    --threads 1 4 8 16 \
    --baseline-csv /tmp/main_baseline.csv \
    --csv /tmp/final_report_run3.csv > /tmp/opt_bench_run3.log 2>&1

echo "=== ALL FINISHED ==="
