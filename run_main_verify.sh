#!/bin/bash
set -e
git stash
git checkout main
git checkout optimized_rax -- integration/benchmarks/rax/run_benchmark.py
SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN=".build-release/libsearch.so"
python3 -u integration/benchmarks/rax/run_benchmark.py \
    --server "$SERVER_BIN" \
    --module "$MODULE_BIN" \
    --branch-name main \
    --setups text_only \
    --threads 1 \
    --csv /tmp/verify_main_1t_real2.csv > /tmp/verify_main_1t_real2.log 2>&1
cat /tmp/verify_main_1t_real2.csv
git checkout -- integration/benchmarks/rax/run_benchmark.py
git checkout optimized_rax
git stash pop
