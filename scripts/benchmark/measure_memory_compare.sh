#!/bin/bash
# Measure valkey-search memory footprint, attributing RSS to:
#   - Valkey core (hash values + core bookkeeping)
#   - valkey-search intern store (module-owned shared vector bytes)
#   - Graph payload (index-specific storage)
#
# Runs three configurations (HNSW M=32, SVS FP32 GMD=32, SVS LVQ4X8 GMD=32)
# against the same synthetic dataset so numbers are directly comparable.
#
# Each config goes through four checkpoint stages:
#   stage 0: fresh server, no data, no index (module loaded)
#   stage 1: 100k vectors loaded via HSET only, no index
#   stage 2: FT.CREATE <algo> on the same data (waits for backfill)
#   stage 3: (optional) add 1K more vectors via HSET to confirm steady-state
#
# At each stage we capture:
#   - RSS of the valkey-server process (ps rss)
#   - used_memory and used_memory_rss from INFO memory
#   - every search_* field from INFO (module-reported)
#   - DBSIZE (proof of how many keys exist)
#   - FT.INFO idx (for stage 2+)

# No set -e/-u: we do a lot of grep/tail that may exit non-zero on
# missing fields and we don't want that to abort the whole run.

BASEDIR="/home/ubuntu/projects/cee-valkey-svs"
cd "$BASEDIR"

PORT=6399
VALKEY=valkey/src/valkey-server
CLI=valkey/src/valkey-cli
MODULE=valkey-search-svs/.build-release/libsearch.so
SVS_LIB=valkey-search-svs/.build-release/_deps/svs-src/lib

LOGDIR=/tmp/memory_experiment
mkdir -p "$LOGDIR"
echo "logdir=$LOGDIR"

N=${N:-100000}
DIM=${DIM:-768}

start_server() {
  echo "  [start_server]"
  pkill -9 valkey-server 2>/dev/null || true
  sleep 1
  rm -f dump.rdb
  LD_LIBRARY_PATH="$SVS_LIB:${LD_LIBRARY_PATH:-}" \
    "$VALKEY" \
    --loadmodule "$MODULE" \
    --port $PORT \
    --save "" \
    --maxmemory 0 \
    --appendonly no \
    --loglevel warning \
    > "$LOGDIR/server.log" 2>&1 &
  sleep 3
  $CLI -p $PORT PING > /dev/null
  # Developer-visible INFO fields (for svs_* metrics)
  $CLI -p $PORT CONFIG SET search.info-developer-visible yes > /dev/null
}

svr_pid() { pgrep -f "valkey-server \*:$PORT" | head -1; }

# Snapshot: stage name + config, writes all the numbers to a text file.
snapshot() {
  local config="$1"
  local stage="$2"
  local out="$LOGDIR/${config}_${stage}.txt"
  local pid=$(svr_pid)

  {
    echo "===== config=$config stage=$stage ====="
    echo ""
    echo "--- ps ---"
    ps -o pid,rss,vsz,command -p "$pid"
    echo ""
    echo "--- /proc/$pid/status (key fields) ---"
    grep -E "^(VmRSS|VmSize|VmData|VmHWM):" /proc/"$pid"/status
    echo ""
    echo "--- DBSIZE ---"
    $CLI -p $PORT DBSIZE
    echo ""
    echo "--- INFO memory ---"
    $CLI -p $PORT INFO memory
    echo ""
    echo "--- INFO everything (search_* and other module-only fields) ---"
    $CLI -p $PORT INFO everything | grep -E "^(# search_|search_|used_memory)"
    echo ""
    echo "--- FT._LIST ---"
    $CLI -p $PORT FT._LIST 2>/dev/null || true
    echo ""
    echo "--- FT.INFO idx ---"
    $CLI -p $PORT FT.INFO idx 2>/dev/null || echo "(no idx)"
  } > "$out"

  local rss=$(ps -o rss= -p "$pid" | tr -d ' ')
  local used=$($CLI -p $PORT INFO memory | grep -oE "used_memory:[0-9]+" | cut -d: -f2)
  local svc_used=$($CLI -p $PORT INFO | grep -oE "search_used_memory_bytes:[0-9]+" | cut -d: -f2 || echo 0)
  local extern=$($CLI -p $PORT INFO | grep -oE "search_vector_externing_entry_count:[0-9]+" | cut -d: -f2 || echo 0)
  local dbsize=$($CLI -p $PORT DBSIZE)
  printf "  [%s/%s] rss=%s KB  used_memory=%s B  search_module=%s B  extern_entries=%s  dbsize=%s\n" \
    "$config" "$stage" "$rss" "$used" "$svc_used" "$extern" "$dbsize"
}

# Load N HSETs of random FP32 vectors. Uses the valkey-cli PIPE mode
# so loading is fast enough. Each HSET: key=doc:i, field=vec=<bytes>.
# Keys are ONLY the docs so we can subtract core overhead cleanly.
load_hashes() {
  echo "  [load_hashes n=$N dim=$DIM]"
  python3 - <<PYEOF
import redis, numpy as np, sys
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

# Create an index over an already-populated keyspace.
#   $1 = algo: hnsw | svs_fp32 | svs_lvq4x8
create_index() {
  local algo="$1"
  echo "  [create_index algo=$algo]"
  case "$algo" in
    hnsw)
      $CLI -p $PORT FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR HNSW 10 TYPE FLOAT32 DIM $DIM \
        DISTANCE_METRIC L2 M 32 EF_CONSTRUCTION 128 > /dev/null
      ;;
    svs_fp32)
      $CLI -p $PORT FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR SVS 12 TYPE FLOAT32 DIM $DIM \
        DISTANCE_METRIC L2 GRAPH_MAX_DEGREE 32 \
        CONSTRUCTION_WINDOW_SIZE 128 SEARCH_WINDOW_SIZE 50 > /dev/null
      ;;
    svs_lvq4x8)
      $CLI -p $PORT FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR SVS 14 TYPE FLOAT32 DIM $DIM \
        DISTANCE_METRIC L2 GRAPH_MAX_DEGREE 32 \
        CONSTRUCTION_WINDOW_SIZE 128 SEARCH_WINDOW_SIZE 50 \
        COMPRESSION LVQ4X8 > /dev/null
      ;;
  esac
  # Wait until backfill completes (number_of_active_indexes_indexing==0).
  for _ in $(seq 1 600); do
    local indexing=$($CLI -p $PORT INFO | grep -oE "search_number_of_active_indexes_indexing:[0-9]+" | cut -d: -f2 || echo "0")
    if [ "${indexing:-0}" = "0" ]; then
      # Also make sure FT.INFO says state=ready
      local state=$($CLI -p $PORT FT.INFO idx 2>/dev/null | awk '/^state$/{getline; print}' || true)
      if [ "$state" = "ready" ]; then
        # Allow a beat for stats to settle
        sleep 2
        return 0
      fi
    fi
    sleep 2
  done
  echo "  [create_index] TIMEOUT waiting for backfill"
  return 1
}

run_config() {
  local algo="$1"
  echo ""
  echo "======================================================"
  echo "  CONFIG: $algo  (N=$N, DIM=$DIM)"
  echo "======================================================"

  start_server
  snapshot "$algo" "stage0_empty"

  load_hashes
  snapshot "$algo" "stage1_hashes_only"

  create_index "$algo"
  snapshot "$algo" "stage2_indexed"
}

for algo in hnsw svs_fp32 svs_lvq4x8; do
  run_config "$algo"
done

echo ""
echo "All stages complete. Snapshots in $LOGDIR/"
ls -la "$LOGDIR"
pkill -9 valkey-server 2>/dev/null || true
