#!/bin/bash
# Benchmark script to reproduce the assetlookup cluster overload scenario.
# Tests the local-search-max-priority fix under heavy write + read contention.
#
# Setup: Standalone server (not cluster) to isolate the thread pool behavior.
# The key insight is that the overload happens PER NODE — each node's reader
# thread pool gets saturated. We don't need a real cluster to test the fix.
#
# Workload ratio (matching customer):
#   - Writes: ~6,800 upserts/min = ~113/sec
#   - Reads:  ~417 queries/min = ~7/sec (but each generates work on the reader pool)
#   - The reads are tag + text AND queries (expensive)
#
# Usage: ./bench-overload.sh <module_path> [with-fix|without-fix]

MODULE=${1:-".build-release/libsearch.so"}
MODE=${2:-"without-fix"}

SERVER=/home/karsubba/valkey/src/valkey-server
CLI=/home/karsubba/valkey/src/valkey-cli
BENCH=/home/karsubba/valkey/src/valkey-benchmark
PORT=6399
DURATION=30
NUM_DOCS=100000
READER_THREADS=8  # Smaller than customer (48) to hit overload faster

if [ ! -f "$MODULE" ]; then
  echo "ERROR: Module not found at $MODULE"
  exit 1
fi

cleanup() {
  kill $WRITE_PID 2>/dev/null
  timeout 2 $CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null
}
trap cleanup EXIT

# Kill any existing server on this port
timeout 2 $CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null
sleep 1

echo "=== Overload Benchmark (mode=$MODE) ==="
echo "Module: $MODULE"
echo "Reader threads: $READER_THREADS"
echo ""

# Start server with the module
LOCAL_PRIORITY_FLAG=""
if [ "$MODE" = "with-fix" ]; then
  LOCAL_PRIORITY_FLAG="--local-search-max-priority yes"
fi

$SERVER --port $PORT \
  --daemonize yes \
  --logfile /tmp/overload_bench.log \
  --save "" \
  --loglevel notice \
  --loadmodule "$MODULE"

# Wait for server
for i in $(seq 1 30); do
  if timeout 1 $CLI -p $PORT PING 2>/dev/null | grep -q PONG; then break; fi
  sleep 0.1
done

if ! timeout 1 $CLI -p $PORT PING 2>/dev/null | grep -q PONG; then
  echo "ERROR: Server failed to start. Check /tmp/overload_bench.log"
  cat /tmp/overload_bench.log 2>/dev/null | tail -20
  exit 1
fi

# Configure module settings at runtime
$CLI -p $PORT CONFIG SET search.reader-threads $READER_THREADS >/dev/null 2>&1
$CLI -p $PORT CONFIG SET search.writer-threads $READER_THREADS >/dev/null 2>&1
$CLI -p $PORT CONFIG SET search.default-timeout-ms 5000 >/dev/null 2>&1
$CLI -p $PORT CONFIG SET search.enable-partial-results yes >/dev/null 2>&1
if [ "$MODE" = "with-fix" ]; then
  $CLI -p $PORT CONFIG SET search.local-search-max-priority yes >/dev/null 2>&1
  echo "  local-search-max-priority: ENABLED"
else
  echo "  local-search-max-priority: DISABLED"
fi

echo "[1/4] Creating index with 15 tag + 3 numeric + 1 text attributes..."

# Create index matching customer schema: 15 tag, 3 numeric, 1 text
$CLI -p $PORT FT.CREATE idx-asset ON HASH PREFIX 1 asset: SCHEMA \
  title TEXT \
  tag1 TAG tag2 TAG tag3 TAG tag4 TAG tag5 TAG \
  tag6 TAG tag7 TAG tag8 TAG tag9 TAG tag10 TAG \
  tag11 TAG tag12 TAG tag13 TAG tag14 TAG tag15 TAG \
  num1 NUMERIC num2 NUMERIC num3 NUMERIC \
  2>&1 | grep -v "^$"

echo "[2/4] Loading $NUM_DOCS documents..."

# Generate and load documents
seq 1 $NUM_DOCS | awk '{
  # Distribute tag values to create realistic cardinality
  t1 = ($1 % 10 == 0) ? "rare" : "common"
  t2 = ($1 % 100 == 0) ? "vip" : "standard"
  t3 = ($1 % 5 == 0) ? "active" : "inactive"
  printf "HSET asset:%d title \"device asset number %d with description\" tag1 %s tag2 %s tag3 %s tag4 device tag5 us-east-1 tag6 prod tag7 linux tag8 x86 tag9 running tag10 managed tag11 team-a tag12 shard-%d tag13 rack-%d tag14 dc-1 tag15 fleet-main num1 %d num2 %d num3 %d\n", \
    $1, $1, t1, t2, t3, ($1 % 20), ($1 % 50), $1, ($1 * 7 % 1000), ($1 % 365)
}' | $CLI -p $PORT --pipe 2>&1 | grep -v "^$"

# Wait for indexing to settle
sleep 2
echo "  Indexed docs: $($CLI -p $PORT FT.SEARCH idx-asset "*" NOCONTENT LIMIT 0 0 2>/dev/null | head -1)"

echo "[3/4] Starting continuous writes in background (~150 writes/sec)..."

# Background writer: continuous HSET updates at high rate
(
  while true; do
    seq 1 500 | awk -v seed=$RANDOM '{
      doc_id = int(rand() * 100000) + 1
      printf "HSET asset:%d tag1 updated tag3 active num1 %d num2 %d\n", doc_id, int(rand()*10000), int(rand()*1000)
    }' | $CLI -p $PORT --pipe >/dev/null 2>&1
    # ~500 writes per batch, small sleep to get ~150/sec sustained
    sleep 0.1
  done
) &
WRITE_PID=$!

echo "  Writer PID: $WRITE_PID"
sleep 2

echo "[4/4] Running read benchmark for ${DURATION}s (tag AND queries)..."
echo ""

# Multi-tag AND query (expensive — matches customer pattern)
# This creates multi-predicate evaluation across the candidate set
echo "--- Tag AND query: @tag1:{common} @tag3:{active} ---"
$BENCH -p $PORT -c 50 --duration $DURATION \
  -- FT.SEARCH idx-asset "@tag1:{common} @tag3:{active}" LIMIT 0 10 2>&1 | \
  grep -E "throughput|latency|requests"

echo ""
echo "--- Tag + Text AND query: @tag1:{common} @title:device ---"
$BENCH -p $PORT -c 50 --duration $DURATION \
  -- FT.SEARCH idx-asset "@tag1:{common} @title:device" LIMIT 0 10 2>&1 | \
  grep -E "throughput|latency|requests"

echo ""
echo "=== Post-benchmark stats ==="
echo ""
$CLI -p $PORT INFO MODULES 2>/dev/null | grep -E "queue_size|successful_requests|failure_requests|cancel-timeout|time_slice_queries|time_slice_upserts|used_read_cpu|used_write_cpu"

echo ""
echo "=== Done ==="

# Cleanup writer
kill $WRITE_PID 2>/dev/null
