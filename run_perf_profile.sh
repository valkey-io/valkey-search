#!/bin/bash
set -e

SERVER_BIN="/usr/local/google/home/yairg/work/valkey_ms_unstable4-1/src/valkey-server"
MODULE_BIN=".build-release/libsearch.so"

pgrep valkey-server | xargs kill -9 || true
sleep 1

"$SERVER_BIN" --loadmodule "$MODULE_BIN" --port 60099 > /tmp/valkey_perf.log 2>&1 &
SERVER_PID=$!
sleep 2

echo "Server running on PID $SERVER_PID"

python3 run_ingest_perf.py "$SERVER_PID"

kill -9 $SERVER_PID || true
