#!/bin/bash
# Measure valkey-search SVS memory footprint.
#
# Runs through stages: empty → hashes loaded → indexed, capturing
# VmRSS + used_memory at each stage.
#
# Environment variables (all required unless noted):
#   VALKEY_SERVER  — path to valkey-server binary
#   VALKEY_CLI    — path to valkey-cli binary
#   MODULE        — path to libsearch.so
#   SVS_LIB       — directory containing libsvs_runtime.so (for LD_LIBRARY_PATH)
#   N             — number of vectors (default: 100000)
#   DIM           — dimensions (default: 768)
#   PORT          — server port (default: 6399)
#   LOGDIR        — output directory for snapshots (default: /tmp/mem_measure)
#   ALGO          — algorithm config: svs_fp32 | svs_lvq4x8 | hnsw (default: svs_fp32)

set -o pipefail

: "${VALKEY_SERVER:?VALKEY_SERVER must be set}"
: "${VALKEY_CLI:?VALKEY_CLI must be set}"
: "${MODULE:?MODULE must be set}"
: "${SVS_LIB:?SVS_LIB must be set}"
: "${N:=100000}"
: "${DIM:=768}"
: "${PORT:=6399}"
: "${LOGDIR:=/tmp/mem_measure}"
: "${ALGO:=svs_fp32}"
: "${PYTHON:=python3}"

mkdir -p "$LOGDIR"

start_server() {
  pkill -9 -f "valkey-server \*:$PORT" 2>/dev/null || true
  sleep 1
  rm -f dump.rdb
  LD_LIBRARY_PATH="$SVS_LIB:${LD_LIBRARY_PATH:-}" \
    "$VALKEY_SERVER" \
    --loadmodule "$MODULE" \
    --port "$PORT" \
    --save "" \
    --maxmemory 0 \
    --appendonly no \
    --loglevel warning \
    > "$LOGDIR/server.log" 2>&1 &
  sleep 3
  "$VALKEY_CLI" -p "$PORT" PING > /dev/null || { echo "FATAL: server didn't start"; exit 1; }
  "$VALKEY_CLI" -p "$PORT" CONFIG SET search.info-developer-visible yes > /dev/null 2>&1 || true
}

svr_pid() { pgrep -f "valkey-server \*:$PORT" | head -1; }

snapshot() {
  local stage="$1"
  local out="$LOGDIR/${ALGO}_${stage}.txt"
  local pid
  pid=$(svr_pid)

  {
    echo "===== algo=$ALGO stage=$stage N=$N DIM=$DIM ====="
    echo ""
    echo "--- /proc/$pid/status ---"
    grep -E "^(VmRSS|VmSize|VmData|VmHWM):" /proc/"$pid"/status
    echo ""
    echo "--- INFO memory ---"
    "$VALKEY_CLI" -p "$PORT" INFO memory
    echo ""
    echo "--- DBSIZE ---"
    "$VALKEY_CLI" -p "$PORT" DBSIZE
    echo ""
    echo "--- FT.INFO idx ---"
    "$VALKEY_CLI" -p "$PORT" FT.INFO idx 2>/dev/null || echo "(no idx)"
  } > "$out"

  local vmrss_kb
  vmrss_kb=$(grep "^VmRSS:" /proc/"$pid"/status | awk '{print $2}')
  local used_memory
  used_memory=$("$VALKEY_CLI" -p "$PORT" INFO memory | grep -oP "used_memory:\K[0-9]+")

  printf "  [%s/%s] VmRSS=%s KB  used_memory=%s B\n" "$ALGO" "$stage" "$vmrss_kb" "$used_memory"

  # Write machine-readable summary line
  echo "${ALGO},${stage},${vmrss_kb},${used_memory}" >> "$LOGDIR/summary.csv"
}

load_hashes() {
  echo "  [load_hashes N=$N DIM=$DIM]"
  "$PYTHON" - <<PYEOF
import redis, numpy as np
r = redis.Redis(port=$PORT, decode_responses=False)
np.random.seed(1)
batch = 1000
pipe = r.pipeline(transaction=False)
for i in range($N):
    v = np.random.rand($DIM).astype(np.float32).tobytes()
    pipe.hset(f"doc:{i}", "vec", v)
    if (i+1) % batch == 0:
        pipe.execute()
pipe.execute()
print(f"  loaded {$N} hashes")
PYEOF
}

create_index() {
  echo "  [create_index algo=$ALGO]"
  case "$ALGO" in
    hnsw)
      "$VALKEY_CLI" -p "$PORT" FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR HNSW 10 TYPE FLOAT32 DIM "$DIM" \
        DISTANCE_METRIC L2 M 32 EF_CONSTRUCTION 128 > /dev/null
      ;;
    svs_fp32)
      "$VALKEY_CLI" -p "$PORT" FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR SVS 12 TYPE FLOAT32 DIM "$DIM" \
        DISTANCE_METRIC L2 GRAPH_MAX_DEGREE 32 \
        CONSTRUCTION_WINDOW_SIZE 128 SEARCH_WINDOW_SIZE 50 > /dev/null
      ;;
    svs_lvq4x8)
      "$VALKEY_CLI" -p "$PORT" FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR SVS 14 TYPE FLOAT32 DIM "$DIM" \
        DISTANCE_METRIC L2 GRAPH_MAX_DEGREE 32 \
        CONSTRUCTION_WINDOW_SIZE 128 SEARCH_WINDOW_SIZE 50 \
        COMPRESSION LVQ4X8 > /dev/null
      ;;
    *)
      echo "Unknown ALGO=$ALGO"; exit 1 ;;
  esac

  # Wait for backfill to complete
  for _ in $(seq 1 600); do
    local indexing
    indexing=$("$VALKEY_CLI" -p "$PORT" INFO 2>/dev/null | grep -oP "search_number_of_active_indexes_indexing:\K[0-9]+" || echo "0")
    if [ "${indexing:-0}" = "0" ]; then
      sleep 2
      return 0
    fi
    sleep 2
  done
  echo "  TIMEOUT waiting for backfill"
  return 1
}

echo "============================================"
echo "  measure_memory.sh: ALGO=$ALGO N=$N DIM=$DIM"
echo "============================================"

# Initialize CSV header if file doesn't exist
if [ ! -f "$LOGDIR/summary.csv" ]; then
  echo "algo,stage,vmrss_kb,used_memory_bytes" > "$LOGDIR/summary.csv"
fi

start_server
snapshot "stage0_empty"

load_hashes
snapshot "stage1_hashes_only"

create_index
snapshot "stage2_indexed"

pkill -9 -f "valkey-server \*:$PORT" 2>/dev/null || true
echo "  Done. Snapshots in $LOGDIR/"
