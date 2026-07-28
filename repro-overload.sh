#!/bin/bash
# Reproduce the queue growth death spiral.
# Strategy: Few reader threads + massive concurrent load + heavy writes
# All protection configs DISABLED to reproduce the bug.

MODULE=${1:-".build-release/libsearch.so"}
SERVER=/home/karsubba/valkey/src/valkey-server
CLI=/home/karsubba/valkey/src/valkey-cli
BENCH=/home/karsubba/valkey/src/valkey-benchmark
PORT=6399
NUM_DOCS=500000
READER_THREADS=4  # Deliberately low to saturate quickly
BENCH_CLIENTS=200 # High concurrency to overwhelm
BENCH_DURATION=60

cleanup() {
  kill $WRITE_PID $BENCH_PID2 2>/dev/null
  timeout 2 $CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null
}
trap cleanup EXIT

timeout 2 $CLI -p $PORT SHUTDOWN NOSAVE 2>/dev/null
sleep 1

echo "=== REPRO: Queue Growth Death Spiral ==="
echo "Module: $MODULE"
echo "Reader threads: $READER_THREADS (deliberately low)"
echo "Benchmark clients: $BENCH_CLIENTS"
echo "Docs: $NUM_DOCS"
echo "All overload protections: DISABLED"
echo ""

# Start server
LD_LIBRARY_PATH=/home/karsubba/.local/GCCStandalone/lib64 \
$SERVER --port $PORT \
  --daemonize yes \
  --logfile /tmp/repro_bench.log \
  --save "" \
  --loglevel notice \
  --loadmodule "$MODULE"

for i in $(seq 1 30); do
  if timeout 1 $CLI -p $PORT PING 2>/dev/null | grep -q PONG; then break; fi
  sleep 0.1
done

if ! timeout 1 $CLI -p $PORT PING 2>/dev/null | grep -q PONG; then
  echo "ERROR: Server failed to start"
  cat /tmp/repro_bench.log 2>/dev/null | tail -20
  exit 1
fi

# Configure: low threads, all protections OFF
$CLI -p $PORT CONFIG SET search.reader-threads $READER_THREADS >/dev/null 2>&1
$CLI -p $PORT CONFIG SET search.writer-threads 4 >/dev/null 2>&1
$CLI -p $PORT CONFIG SET search.default-timeout-ms 5000 >/dev/null 2>&1
$CLI -p $PORT CONFIG SET search.enable-partial-results yes >/dev/null 2>&1
# Protections OFF:
$CLI -p $PORT CONFIG SET search.local-search-max-priority no >/dev/null 2>&1
$CLI -p $PORT CONFIG SET search.max-query-queue-depth 0 >/dev/null 2>&1

echo "[1/5] Creating index (15 tag + 3 numeric + 1 text)..."
$CLI -p $PORT FT.CREATE idx-asset ON HASH PREFIX 1 asset: SCHEMA \
  title TEXT \
  tag1 TAG tag2 TAG tag3 TAG tag4 TAG tag5 TAG \
  tag6 TAG tag7 TAG tag8 TAG tag9 TAG tag10 TAG \
  tag11 TAG tag12 TAG tag13 TAG tag14 TAG tag15 TAG \
  num1 NUMERIC num2 NUMERIC num3 NUMERIC \
  2>&1 | grep -v "^$"

echo "[2/5] Loading $NUM_DOCS documents..."
seq 1 $NUM_DOCS | awk '{
  t1 = ($1 % 10 == 0) ? "rare" : "common"
  t2 = ($1 % 100 == 0) ? "vip" : "standard"
  t3 = ($1 % 5 == 0) ? "active" : "inactive"
  printf "HSET asset:%d title \"device asset number %d with long description text for searching\" tag1 %s tag2 %s tag3 %s tag4 device tag5 us-east-1 tag6 prod tag7 linux tag8 x86 tag9 running tag10 managed tag11 team-a tag12 shard-%d tag13 rack-%d tag14 dc-1 tag15 fleet-main num1 %d num2 %d num3 %d\n", \
    $1, $1, t1, t2, t3, ($1 % 20), ($1 % 50), $1, ($1 * 7 % 1000), ($1 % 365)
}' | $CLI -p $PORT --pipe 2>&1 | grep "replies"
sleep 3

INDEXED=$($CLI -p $PORT FT.SEARCH idx-asset "*" NOCONTENT LIMIT 0 0 2>/dev/null | head -1)
echo "  Indexed: $INDEXED docs"

echo "[3/5] Starting heavy writes in background..."
# Aggressive writer: ~500+ writes/sec
(
  while true; do
    seq 1 200 | awk '{
      doc_id = int(rand() * 500000) + 1
      printf "HSET asset:%d tag1 updated-%d tag3 active num1 %d num2 %d title \"updated device %d\"\n", doc_id, NR, int(rand()*10000), int(rand()*1000), doc_id
    }' | $CLI -p $PORT --pipe >/dev/null 2>&1
  done
) &
WRITE_PID=$!

echo "[4/5] Starting read benchmark ($BENCH_CLIENTS clients, ${BENCH_DURATION}s)..."
echo "  Query: @tag1:{common} @tag3:{active} (multi-tag AND)"
echo ""

# Take initial snapshot
Q1=$($CLI -p $PORT INFO MODULES 2>/dev/null | grep "search_query_queue_size" | cut -d: -f2)
S1=$($CLI -p $PORT INFO MODULES 2>/dev/null | grep "search_successful_requests_count" | cut -d: -f2)
echo "  [T=0]  queue=$Q1  successful=$S1"

# Start the benchmark in background
$BENCH -p $PORT -c $BENCH_CLIENTS --duration $BENCH_DURATION \
  -- FT.SEARCH idx-asset "@tag1:{common} @tag3:{active}" LIMIT 0 10 >/dev/null 2>&1 &
BENCH_PID2=$!

# Also start a second bench with text queries to add more pressure
$BENCH -p $PORT -c $BENCH_CLIENTS --duration $BENCH_DURATION \
  -- FT.SEARCH idx-asset "@tag1:{common} @title:device" LIMIT 0 10 >/dev/null 2>&1 &

# Monitor queue every 10 seconds
for t in 10 20 30 40 50 60; do
  sleep 10
  Q=$($CLI -p $PORT INFO MODULES 2>/dev/null | grep "search_query_queue_size" | cut -d: -f2)
  S=$($CLI -p $PORT INFO MODULES 2>/dev/null | grep "search_successful_requests_count" | cut -d: -f2)
  T=$($CLI -p $PORT INFO MODULES 2>/dev/null | grep "search_cancel-timeouts" | cut -d: -f2)
  CPU=$($CLI -p $PORT INFO MODULES 2>/dev/null | grep "search_used_read_cpu" | cut -d: -f2)
  echo "  [T=${t}s] queue=$Q  successful=$S  timeouts=$T  read_cpu=$CPU"
done

echo ""
echo "[5/5] Final stats:"
$CLI -p $PORT INFO MODULES 2>/dev/null | grep -E "queue_size|successful_requests|failure_requests|cancel-timeout|time_slice_queries|time_slice_upserts|used_read_cpu"

kill $WRITE_PID $BENCH_PID2 2>/dev/null
wait 2>/dev/null
echo ""
echo "=== Done ==="
