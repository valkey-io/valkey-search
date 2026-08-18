#!/bin/bash
pgrep valkey-server | xargs kill -9 || true
perf record -g -o /tmp/perf_fixed_2.data -- python3 -u integration/benchmarks/rax/run_benchmark.py --server /usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server --module .build-release/libsearch.so --threads 1 --setups text_tag_churn_prefix
perf report -i /tmp/perf_fixed_2.data --stdio > /tmp/perf_fixed_2_report.txt
