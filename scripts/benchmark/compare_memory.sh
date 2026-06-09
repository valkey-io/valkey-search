#!/bin/bash
# Compare SVS memory footprint before and after the raw_vectors_ removal.
#
# Methodology: follows SVS_MEMORY_FOOTPRINT_INVESTIGATION.md — uses VmRSS
# as ground truth (captures heap + mmap), runs MEMORY PURGE before
# measurement to reduce fragmentation noise, and captures smaps
# decomposition for attribution.
#
# Usage:
#   ./scripts/benchmark/compare_memory.sh
#
# Environment overrides:
#   BEFORE_REF     — git ref for "before" build (default: HEAD~1)
#   AFTER_REF      — git ref for "after" build (default: HEAD)
#   N              — number of vectors (default: 100000)
#   DIM            — dimensions (default: 768)
#   VALKEY_TAG     — valkey release tag to build (default: 9.1.0-rc1)
#   SVS_URL_BEFORE — SVS runtime URL for "before" build
#   SVS_URL_AFTER  — SVS runtime URL for "after" build
#   WORKDIR        — scratch directory (default: /tmp/mem_compare)
#   PYTHON         — python3 with redis+numpy installed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

: "${BEFORE_REF:=HEAD~1}"
: "${AFTER_REF:=HEAD}"
: "${N:=100000}"
: "${DIM:=768}"
: "${VALKEY_TAG:=9.1.0-rc1}"
: "${WORKDIR:=/tmp/mem_compare}"
: "${SVS_URL_BEFORE:=https://github.com/intel/ScalableVectorSearch/releases/download/v0.2.0/svs-cpp-runtime-bindings-0.2.0.tar.gz}"
: "${SVS_URL_AFTER:=file:///home/enpicket/svs-runtime/svs-cpp-runtime-bindings.tar.gz}"
: "${PYTHON:=$REPO_DIR/.venv/bin/python3}"

PORT=6399

mkdir -p "$WORKDIR"

echo "============================================"
echo "  compare_memory.sh"
echo "  BEFORE_REF=$BEFORE_REF"
echo "  AFTER_REF=$AFTER_REF"
echo "  N=$N DIM=$DIM"
echo "  WORKDIR=$WORKDIR"
echo "============================================"
echo ""

# --- Helper: Build ICU static libs ---
build_icu() {
  local worktree="$1"
  local build_dir="$2"
  local icu_source="$worktree/third_party/icu/source"
  local icu_install="$build_dir/icu/install"

  if [ -f "$icu_install/lib/libicudata.a" ]; then
    return 0
  fi

  echo "    Building ICU static libs..."
  local icu_build="$build_dir/icu"
  rm -rf "$icu_build"
  mkdir -p "$icu_build"
  (
    cd "$icu_build"
    "$icu_source/configure" \
      --enable-static --disable-shared \
      --with-data-packaging=static \
      --disable-extras --disable-icuio --disable-layout \
      --disable-tests --disable-samples --enable-tools \
      --prefix="$icu_install" \
      CFLAGS="-O2 -fPIC" CXXFLAGS="-O2 -fPIC" \
      > "$build_dir/icu_configure.log" 2>&1
    make PKGDATA_MODE=static -j"$(nproc)" > "$build_dir/icu_build.log" 2>&1
    make install PKGDATA_MODE=static > "$build_dir/icu_install.log" 2>&1
  )
}

# --- Helper: Run measurement for one build ---
# Runs the full stage0 → stage1 → stage2 pipeline for a given algo,
# captures VmRSS + smaps at stage2 after MEMORY PURGE.
run_measurement() {
  local label="$1"    # e.g. "before" or "after"
  local module="$2"   # path to libsearch.so
  local svs_lib="$3"  # directory with libsvs_runtime.so
  local algo="$4"     # hnsw | svs_fp32 | svs_lvq4x8 | svs_leanvec*
  local valkey_server="$5"
  local valkey_cli="$6"
  local outdir="$WORKDIR/results_${label}"

  mkdir -p "$outdir"

  # Start server
  pkill -9 -f "valkey-server \*:$PORT" 2>/dev/null || true
  sleep 1
  rm -f dump.rdb

  LD_LIBRARY_PATH="$svs_lib:${LD_LIBRARY_PATH:-}" \
    "$valkey_server" \
    --loadmodule "$module" \
    --port "$PORT" \
    --save "" \
    --maxmemory 0 \
    --appendonly no \
    --loglevel warning \
    > "$outdir/server_${algo}.log" 2>&1 &
  sleep 3
  "$valkey_cli" -p "$PORT" PING > /dev/null || { echo "FATAL: server didn't start"; return 1; }
  "$valkey_cli" -p "$PORT" CONFIG SET search.info-developer-visible yes > /dev/null 2>&1 || true

  # Load hashes
  "$PYTHON" - <<PYEOF
import redis, numpy as np
r = redis.Redis(port=$PORT, decode_responses=False)
np.random.seed(1)
algo = "$algo"
is_leanvec = algo.startswith("svs_leanvec")
threshold = 10000
batch = 1000
if is_leanvec:
    for i in range(min($N, threshold)):
        v = np.random.rand($DIM).astype(np.float32).tobytes()
        r.hset(f"doc:{i}", "vec", v)
    start = threshold
    print(f"  [${label}/${algo}] loaded {min($N, threshold)} hashes (non-pipelined, LeanVec training)")
else:
    start = 0
pipe = r.pipeline(transaction=False)
for i in range(start, $N):
    v = np.random.rand($DIM).astype(np.float32).tobytes()
    pipe.hset(f"doc:{i}", "vec", v)
    if (i+1) % batch == 0: pipe.execute()
pipe.execute()
print(f"  [${label}/${algo}] loaded {$N} hashes total")
PYEOF

  # Create index
  case "$algo" in
    hnsw)
      "$valkey_cli" -p "$PORT" FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR HNSW 10 TYPE FLOAT32 DIM "$DIM" \
        DISTANCE_METRIC L2 M 16 EF_CONSTRUCTION 128 > /dev/null
      ;;
    svs_*)
      local args=(TYPE FLOAT32 DIM "$DIM" DISTANCE_METRIC L2
                  GRAPH_MAX_DEGREE 32 CONSTRUCTION_WINDOW_SIZE 128
                  SEARCH_WINDOW_SIZE 50)
      case "$algo" in
        svs_fp32) ;;
        svs_lvq4x8) args+=(COMPRESSION LVQ4X8) ;;
        svs_leanvec*)
          local comp="${algo#svs_leanvec}"
          args+=(COMPRESSION "LEANVEC${comp^^}"
                 LEANVEC_DIMS "$((DIM / 4))"
                 LEANVEC_TRAINING_THRESHOLD 10000) ;;
      esac
      "$valkey_cli" -p "$PORT" FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA \
        vec VECTOR SVS "${#args[@]}" "${args[@]}" > /dev/null
      ;;
  esac

  # Wait for backfill/training to complete
  for _ in $(seq 1 1800); do
    local backfill
    backfill=$("$valkey_cli" -p "$PORT" FT.INFO idx 2>/dev/null | awk '/^backfill_in_progress$/{getline; print; exit}' || true)
    local state
    state=$("$valkey_cli" -p "$PORT" FT.INFO idx 2>/dev/null | awk '/^state$/{getline; print; exit}' || true)
    if [ "$backfill" = "0" ] && [ "$state" = "ready" ]; then
      sleep 2
      break
    fi
    sleep 2
  done

  # MEMORY PURGE to reduce fragmentation noise (per investigation methodology)
  "$valkey_cli" -p "$PORT" MEMORY PURGE > /dev/null
  sleep 1

  # Capture VmRSS
  local pid
  pid=$(pgrep -f "valkey-server \*:$PORT" | head -1)
  local vmrss_kb
  vmrss_kb=$(awk '/^VmRSS:/{print $2}' /proc/"$pid"/status)

  # Capture smaps decomposition
  PID="$pid" "$PYTHON" <<'PYEOF' > "$outdir/${algo}_smaps.txt"
import os, re
pid = os.environ['PID']
buckets = {}
cur = None
with open(f'/proc/{pid}/smaps') as f:
    for line in f:
        m = re.match(r'^([0-9a-f]+)-([0-9a-f]+)\s+(\S+)\s+\S+\s+\S+\s+\S+\s*(.*)$', line)
        if m:
            if cur:
                b = cur['path'] or '(anon)'
                if b == '(anon)': b = '(anon, mmap)'
                elif '[heap]' in b: b = '[heap] (glibc main arena)'
                elif 'libsvs_runtime' in b: b = 'libsvs_runtime.so (code)'
                elif 'libsearch' in b: b = 'libsearch.so (code)'
                elif '[' in b: pass
                elif '.so' in b: b = 'other .so (code)'
                else: b = b.rsplit('/',1)[-1]
                buckets.setdefault(b, {'size':0, 'rss':0})
                buckets[b]['size'] += cur['size']
                buckets[b]['rss'] += cur.get('rss', 0)
            sz = int(m.group(2), 16) - int(m.group(1), 16)
            cur = {'size': sz, 'path': m.group(4).strip()}
        elif line.startswith('Rss:'):
            cur['rss'] = int(line.split()[1]) * 1024

print(f"{'bucket':<40} {'virt_MB':>10} {'rss_MB':>10}")
total = 0
for k, v in sorted(buckets.items(), key=lambda kv: -kv[1]['rss']):
    if v['rss'] > 0:
        print(f"{k:<40} {v['size']/1048576:>9.1f}  {v['rss']/1048576:>9.1f}")
    total += v['rss']
print(f"{'total RSS':<40} {'':>10}  {total/1048576:>9.1f}")
PYEOF

  # Capture used_memory for reference
  local used_memory
  used_memory=$("$valkey_cli" -p "$PORT" INFO memory | grep -oP "used_memory:\K[0-9]+")

  printf "  [%s/%s] VmRSS=%s KB (%s MiB)  used_memory=%s B (%s MiB)\n" \
    "$label" "$algo" "$vmrss_kb" "$((vmrss_kb / 1024))" \
    "$used_memory" "$((used_memory / 1048576))"

  # Write machine-readable result
  echo "${label},${algo},${vmrss_kb},${used_memory}" >> "$WORKDIR/results.csv"

  # Stop server
  pkill -9 -f "valkey-server \*:$PORT" 2>/dev/null || true
  sleep 1
}

# --- Step 1: Build valkey-server ---
VALKEY_DIR="$WORKDIR/valkey"
if [ ! -x "$VALKEY_DIR/src/valkey-server" ]; then
  echo "[1/5] Building valkey-server ($VALKEY_TAG)..."
  if [ ! -d "$VALKEY_DIR" ]; then
    git clone --depth 1 --branch "$VALKEY_TAG" https://github.com/valkey-io/valkey.git "$VALKEY_DIR"
  fi
  make -C "$VALKEY_DIR" -j"$(nproc)" > "$WORKDIR/valkey_build.log" 2>&1
  echo "  Built: $VALKEY_DIR/src/valkey-server"
else
  echo "[1/5] valkey-server already built, skipping."
fi

VALKEY_SERVER="$VALKEY_DIR/src/valkey-server"
VALKEY_CLI="$VALKEY_DIR/src/valkey-cli"

# --- Step 2: Build "before" libsearch.so ---
BEFORE_BUILD="$WORKDIR/build_before"
echo ""
echo "[2/5] Building libsearch.so from $BEFORE_REF..."
if [ ! -f "$BEFORE_BUILD/libsearch.so" ]; then
  BEFORE_WORKTREE="$WORKDIR/worktree_before"
  rm -rf "$BEFORE_WORKTREE"
  git -C "$REPO_DIR" worktree add "$BEFORE_WORKTREE" "$BEFORE_REF" --detach 2>/dev/null
  build_icu "$BEFORE_WORKTREE" "$BEFORE_BUILD"
  cmake -S "$BEFORE_WORKTREE" -B "$BEFORE_BUILD" \
    -DCMAKE_BUILD_TYPE=Release -DENABLE_SVS=ON \
    -DSVS_URL="$SVS_URL_BEFORE" \
    -G Ninja > "$WORKDIR/cmake_before.log" 2>&1
  ninja -C "$BEFORE_BUILD" libsearch.so > "$WORKDIR/build_before.log" 2>&1
  git -C "$REPO_DIR" worktree remove "$BEFORE_WORKTREE" --force 2>/dev/null || true
  echo "  Built: $BEFORE_BUILD/libsearch.so"
else
  echo "  Already built, skipping."
fi

# --- Step 3: Build "after" libsearch.so ---
AFTER_BUILD="$WORKDIR/build_after"
echo ""
echo "[3/5] Building libsearch.so from $AFTER_REF..."
if [ ! -f "$AFTER_BUILD/libsearch.so" ]; then
  AFTER_WORKTREE="$WORKDIR/worktree_after"
  rm -rf "$AFTER_WORKTREE"
  git -C "$REPO_DIR" worktree add "$AFTER_WORKTREE" "$AFTER_REF" --detach 2>/dev/null
  build_icu "$AFTER_WORKTREE" "$AFTER_BUILD"
  cmake -S "$AFTER_WORKTREE" -B "$AFTER_BUILD" \
    -DCMAKE_BUILD_TYPE=Release -DENABLE_SVS=ON \
    -DSVS_URL="$SVS_URL_AFTER" \
    -G Ninja > "$WORKDIR/cmake_after.log" 2>&1
  ninja -C "$AFTER_BUILD" libsearch.so > "$WORKDIR/build_after.log" 2>&1
  git -C "$REPO_DIR" worktree remove "$AFTER_WORKTREE" --force 2>/dev/null || true
  echo "  Built: $AFTER_BUILD/libsearch.so"
else
  echo "  Already built, skipping."
fi

# Locate SVS runtime .so directories
SVS_LIB_BEFORE=$(find "$BEFORE_BUILD" -name "libsvs_runtime.so" -printf "%h\n" | head -1)
SVS_LIB_AFTER=$(find "$AFTER_BUILD" -name "libsvs_runtime.so" -printf "%h\n" | head -1)

# --- Step 4: Run measurements ---
echo ""
echo "[4/5] Running measurements..."

# Clear previous results
rm -f "$WORKDIR/results.csv"
echo "label,algo,vmrss_kb,used_memory_bytes" > "$WORKDIR/results.csv"

for algo in hnsw svs_fp32 svs_lvq4x8 svs_leanvec4x4 svs_leanvec4x8 svs_leanvec8x8; do
  echo ""
  echo "  --- BEFORE ($BEFORE_REF) / $algo ---"
  run_measurement "before" "$BEFORE_BUILD/libsearch.so" "$SVS_LIB_BEFORE" "$algo" "$VALKEY_SERVER" "$VALKEY_CLI"

  echo ""
  echo "  --- AFTER ($AFTER_REF) / $algo ---"
  run_measurement "after" "$AFTER_BUILD/libsearch.so" "$SVS_LIB_AFTER" "$algo" "$VALKEY_SERVER" "$VALKEY_CLI"
done

# --- Step 5: Compare results ---
echo ""
echo "[5/5] Comparison (VmRSS is the authoritative metric):"
echo ""
printf "%-16s | %12s | %12s | %10s | %16s | %16s | %10s\n" \
  "Config" "Before VmRSS" "After VmRSS" "RSS Delta" "Before used_mem" "After used_mem" "heap Delta"
printf "%-16s-+-%12s-+-%12s-+-%10s-+-%16s-+-%16s-+-%10s\n" \
  "----------------" "------------" "------------" "----------" "----------------" "----------------" "----------"

PASS=true
declare -A AFTER_RSS_KB_MAP

for algo in hnsw svs_fp32 svs_lvq4x8 svs_leanvec4x4 svs_leanvec4x8 svs_leanvec8x8; do
  B_LINE=$(grep "^before,${algo}," "$WORKDIR/results.csv" | tail -1)
  A_LINE=$(grep "^after,${algo}," "$WORKDIR/results.csv" | tail -1)

  B_RSS_KB=$(echo "$B_LINE" | cut -d, -f3)
  A_RSS_KB=$(echo "$A_LINE" | cut -d, -f3)
  B_USED=$(echo "$B_LINE" | cut -d, -f4)
  A_USED=$(echo "$A_LINE" | cut -d, -f4)

  AFTER_RSS_KB_MAP["$algo"]="$A_RSS_KB"

  DELTA_RSS_MIB=$(( (A_RSS_KB - B_RSS_KB) / 1024 ))
  DELTA_USED_MIB=$(( (A_USED - B_USED) / 1048576 ))

  B_RSS_MIB=$((B_RSS_KB / 1024))
  A_RSS_MIB=$((A_RSS_KB / 1024))
  B_USED_MIB=$((B_USED / 1048576))
  A_USED_MIB=$((A_USED / 1048576))

  printf "%-16s | %9s MiB | %9s MiB | %7s MiB | %13s MiB | %13s MiB | %7s MiB\n" \
    "$algo" "$B_RSS_MIB" "$A_RSS_MIB" "$DELTA_RSS_MIB" \
    "$B_USED_MIB" "$A_USED_MIB" "$DELTA_USED_MIB"

  # Sanity gate: expect at least 250 MiB RSS reduction for SVS configs
  # HNSW is a reference baseline — no expected reduction between builds
  if [ "$algo" != "hnsw" ] && [ "$DELTA_RSS_MIB" -gt -250 ]; then
    echo "  WARNING: $algo VmRSS reduction ($DELTA_RSS_MIB MiB) is less than expected (-250 MiB)"
    PASS=false
  fi
done

# Ordering check: expect HNSW <= LEANVEC4X4 <= LEANVEC4X8 <= LEANVEC8X8 <= LVQ4X8 <= SVS_FP32
echo ""
echo "Ordering check (after-build RSS):"
EXPECTED_ORDER=(hnsw svs_leanvec4x4 svs_leanvec4x8 svs_leanvec8x8 svs_lvq4x8 svs_fp32)
prev_algo=""
prev_rss=0
for algo in "${EXPECTED_ORDER[@]}"; do
  rss="${AFTER_RSS_KB_MAP[$algo]:-0}"
  if [ "$prev_algo" != "" ] && [ "$rss" -lt "$prev_rss" ]; then
    echo "  WARNING: $algo (${rss} KB) < $prev_algo (${prev_rss} KB) — unexpected ordering"
    PASS=false
  fi
  prev_algo="$algo"
  prev_rss="$rss"
done
if [ "$PASS" = true ]; then
  echo "  OK: RSS ordering matches expected progression"
fi

echo ""
echo "Smaps reports:"
ls "$WORKDIR"/results_*/*_smaps.txt 2>/dev/null
echo ""
echo "Heap breakdown (before vs after):"
for algo in hnsw svs_fp32 svs_lvq4x8 svs_leanvec4x4 svs_leanvec4x8 svs_leanvec8x8; do
  echo "  --- $algo ---"
  echo "  BEFORE heap:"
  grep "heap" "$WORKDIR/results_before/${algo}_smaps.txt" 2>/dev/null || echo "    (not found)"
  echo "  AFTER heap:"
  grep "heap" "$WORKDIR/results_after/${algo}_smaps.txt" 2>/dev/null || echo "    (not found)"
done

echo ""
if [ "$PASS" = true ]; then
  echo "PASS: Memory reduction meets expectations."
  exit 0
else
  echo "FAIL: Memory reduction below threshold. Check smaps for attribution."
  exit 1
fi
