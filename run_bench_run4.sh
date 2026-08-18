#!/bin/bash
set -e

SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN=".build-release/libsearch.so"

echo "Building module..."
cd .build-release && ninja && cd ..

echo "Running full benchmark..."
pgrep valkey-server | xargs kill -9 || true
python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name current_tcache \
    --setups all \
    --threads 1 4 8 16 \
    --baseline-csv /tmp/main_baseline.csv \
    --csv /tmp/final_report_run4.csv > /tmp/opt_bench_run4.log 2>&1

echo "Parsing and writing report..."
python3 parse_and_write4.py
