#!/bin/bash
set -e
cd "$(dirname "$0")"

N=${N:-100000}
DIM=${DIM:-768}
OUT=${OUT_DIR:-/tmp/pure_lib_bench}
export OUT_DIR="$OUT"

mkdir -p "$OUT"
echo "N=$N DIM=$DIM OUT=$OUT"

# Three pure-library configs that mirror the valkey-search measurements.
./bench_hnsw $N $DIM
./bench_svs  $N $DIM fp32
./bench_svs  $N $DIM lvq4x8

# Bonus: show the effect of SVS's blocksize_exp default.
# 30 = 1 GiB (default, used above), 22 = 4 MiB.
BLOCKSIZE_EXP=22 ./bench_svs $N $DIM lvq4x8

echo ""
echo "Done. Snapshots at $OUT/$N/"
ls "$OUT/$N" 2>/dev/null || true
