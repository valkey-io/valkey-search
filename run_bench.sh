#!/bin/bash
set -e
ninja -C .build-release libsearch.so
pgrep valkey-server | xargs kill -9 || true
echo "Running fixed benchmark..."
python3 -u integration/benchmarks/rax/run_benchmark.py --server /usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server --module .build-release/libsearch.so --threads 1 --setups text_tag_churn_prefix > /tmp/bench_fixed_1t.log 2>&1
echo "Done!"
