#!/bin/bash
set -e

SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN=".build-release/libsearch.so"

echo "Building module with removed null terminator..."
cd .build-release && ninja && cd ..

echo "Running memory benchmark..."
pgrep valkey-server | xargs kill -9 || true
python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name no_null_term \
    --setups text_only text_tag_mixed \
    --threads 1 \
    --csv /tmp/mem_bench.csv > /tmp/mem_bench.log 2>&1

cat /tmp/mem_bench.csv
