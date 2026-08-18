#!/bin/bash
set -e

SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN="/usr/local/google/home/yairg/work/valkey-search4/.build-release/libsearch.so"
REPORT_CSV="/tmp/aligned_64k_report.csv"

echo "Building module with 64KB boundary aligned allocator..."
cd .build-release && ninja && cd ..

echo "Running full benchmark matrix (1, 4, 8, 16 threads)..."
pgrep valkey-server | xargs kill -9 || true

python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name aligned_64k_tcache \
    --threads 1 4 8 16 \
    --csv "$REPORT_CSV" > /tmp/aligned_64k.log 2>&1

echo "Parsing and writing final report..."
python3 parse_and_write5.py
