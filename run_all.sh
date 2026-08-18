#!/bin/bash
set -e

# Path variables
SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN=".build-release/libsearch.so"

echo "=== PREPARING MAIN BRANCH ==="
git checkout main
# We need the benchmark script and dataset halving from optimized_rax
git checkout optimized_rax -- integration/benchmarks/rax .devcontainer/run_in_docker.sh
ninja -C .build-release libsearch.so

echo "=== RUNNING MAIN BENCHMARK ==="
pgrep valkey-server | xargs kill -9 || true
python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name main \
    --setups all \
    --threads 1 4 8 16 \
    --csv /tmp/main_baseline.csv > /tmp/main_bench.log 2>&1

echo "=== PREPARING OPTIMIZED_RAX BRANCH ==="
git checkout optimized_rax
# The fix is already applied on optimized_rax!
ninja -C .build-release libsearch.so

echo "=== RUNNING OPTIMIZED_RAX BENCHMARK ==="
pgrep valkey-server | xargs kill -9 || true
python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name current \
    --setups all \
    --threads 1 4 8 16 \
    --baseline-csv /tmp/main_baseline.csv \
    --csv /tmp/final_report.csv > /tmp/opt_bench.log 2>&1

echo "=== ALL FINISHED ==="
