#!/bin/bash
# Capture smaps-based memory breakdown (heap vs mmap) for each algo.
#
# This runs as a follow-up to measure_memory.sh to get process-map-level
# attribution that INFO cannot give us (SVS allocates via mmap, not
# through Valkey's allocator).

BASEDIR="/home/ubuntu/projects/cee-valkey-svs"
cd "$BASEDIR"
PORT=6399
CLI=valkey/src/valkey-cli
VALKEY=valkey/src/valkey-server
MODULE=valkey-search-svs/.build-release/libsearch.so
SVS_LIB=valkey-search-svs/.build-release/_deps/svs-src/lib
N=${N:-100000}
DIM=${DIM:-768}
OUT=/tmp/memory_experiment

start() {
  pkill -9 valkey-server 2>/dev/null; sleep 1
  rm -f dump.rdb
  LD_LIBRARY_PATH=$SVS_LIB:$LD_LIBRARY_PATH \
    $VALKEY --loadmodule $MODULE --port $PORT --save "" \
    --appendonly no --loglevel warning > /tmp/svr_smaps.log 2>&1 &
  sleep 3
  $CLI -p $PORT CONFIG SET search.info-developer-visible yes > /dev/null
}

load_hashes() {
  python3 <<PYEOF
import redis, numpy as np
r = redis.Redis(port=$PORT, decode_responses=False)
np.random.seed(1)
pipe = r.pipeline(transaction=False)
for i in range($N):
    v = np.random.rand($DIM).astype(np.float32).tobytes()
    pipe.hset(f"doc:{i}", "vec", v)
    if (i+1) % 1000 == 0: pipe.execute()
pipe.execute()
PYEOF
}

wait_ready() {
  for _ in $(seq 1 120); do
    state=$($CLI -p $PORT FT.INFO idx 2>/dev/null | awk '/^state$/{getline; print}')
    [ "$state" = "ready" ] && { sleep 2; return; }
    sleep 2
  done
  echo "TIMEOUT"
}

snapshot_smaps() {
  local label="$1"
  local pid=$(pgrep -f "valkey-server \*:$PORT" | head -1)
  local out=$OUT/${label}_smaps.txt
  {
    echo "=== ${label} ==="
    echo "VmRSS: $(awk '/^VmRSS:/{print $2" kB"}' /proc/$pid/status)"
    PID=$pid python3 <<'PYEOF'
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
                buckets.setdefault(b, {'size':0, 'rss':0, 'count':0})
                buckets[b]['size'] += cur['size']
                buckets[b]['rss'] += cur.get('rss', 0)
                buckets[b]['count'] += 1
            sz = int(m.group(2), 16) - int(m.group(1), 16)
            cur = {'size': sz, 'path': m.group(4).strip()}
        elif line.startswith('Rss:'):
            cur['rss'] = int(line.split()[1]) * 1024

print(f"{'bucket':<35} {'virt_MB':>10} {'rss_MB':>10}")
total=0
for k,v in sorted(buckets.items(), key=lambda kv: -kv[1]['rss']):
    print(f"{k:<35} {v['size']/1048576:>9.1f}  {v['rss']/1048576:>9.1f}")
    total += v['rss']
print(f"{'total':<35} {'':>10}  {total/1048576:>9.1f}")
PYEOF
  } > "$out"
  tail -3 "$out"
}

for algo in hnsw svs_fp32 svs_lvq4x8; do
  echo "=== $algo ==="
  start
  load_hashes
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
  wait_ready
  $CLI -p $PORT MEMORY PURGE > /dev/null
  sleep 1
  snapshot_smaps "$algo"
done

pkill -9 valkey-server 2>/dev/null
echo ""
echo "smaps reports in $OUT/*_smaps.txt"
ls $OUT/*_smaps.txt
