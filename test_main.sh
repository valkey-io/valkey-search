#!/bin/bash
git checkout main
git checkout optimized_rax -- integration/benchmarks/rax/run_benchmark.py integration/benchmarks/rax/dataset/ .devcontainer/run_in_docker.sh
ninja -C .build-release libsearch.so
pgrep valkey-server | xargs kill -9 || true
python3 -u integration/benchmarks/rax/run_benchmark.py --server /usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server --module .build-release/libsearch.so --threads 1 --setups text_tag_churn_prefix > /tmp/bench_main_1t.log 2>&1
