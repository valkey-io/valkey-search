#!/bin/bash
set -e
git stash || true
git checkout main
git checkout optimized_rax -- integration/benchmarks/rax/run_benchmark.py .devcontainer/run_in_docker.sh

# Patch daemonize out and perf in
sed -i 's/\[server_bin, conf_path, "--daemonize", "yes"\]/["perf", "record", "-g", "-o", "\/tmp\/perf_main.data", "--", server_bin, conf_path]/g' integration/benchmarks/rax/run_benchmark.py
sed -i 's/proc.wait()//g' integration/benchmarks/rax/run_benchmark.py

ninja -C .build-release libsearch.so
pgrep valkey-server | xargs kill -9 || true
echo "Running main benchmark with perf..."
python3 -u integration/benchmarks/rax/run_benchmark.py --server /usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server --module .build-release/libsearch.so --threads 1 --setups text_tag_churn_prefix > /tmp/perf_main_bench.log 2>&1
perf report -i /tmp/perf_main.data --stdio | head -n 40 > /tmp/perf_report_main.txt

# Cleanup
git checkout -- integration/benchmarks/rax/run_benchmark.py .devcontainer/run_in_docker.sh
git checkout optimized_rax
git stash pop || true
echo "Done!"
