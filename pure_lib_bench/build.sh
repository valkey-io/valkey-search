#!/bin/bash
# Compile the two pure-library benchmarks.
#
# - bench_hnsw links against vendored upstream hnswlib (reused from
#   svs_integration_tests/hnsw/third_party/).
# - bench_svs links against libsvs_runtime.so from the main
#   valkey-search build (.build-release/_deps/svs-src/).
#
# Prereqs: a completed valkey-search build (`ninja -C .build-release
# libsearch.so`) so the SVS runtime is populated.

set -e
cd "$(dirname "$0")"

REPO_ROOT="$(cd .. && pwd)"
SVS_DEPS="$REPO_ROOT/.build-release/_deps/svs-src"
HNSWLIB_DIR="$REPO_ROOT/svs_integration_tests/hnsw/third_party"

if [ ! -f "$SVS_DEPS/lib/libsvs_runtime.so" ]; then
  echo "missing $SVS_DEPS/lib/libsvs_runtime.so"
  echo "run: ninja -C .build-release libsearch.so"
  exit 1
fi

if [ ! -d "$HNSWLIB_DIR/hnswlib" ]; then
  echo "missing $HNSWLIB_DIR/hnswlib (vendored hnswlib headers)"
  exit 1
fi

CXXFLAGS="-std=c++20 -O2 -pthread -Wall -Wno-unused-variable -g -rdynamic"

echo "[build] bench_hnsw"
g++ $CXXFLAGS \
  -I. -I"$HNSWLIB_DIR" \
  bench_hnsw.cc -o bench_hnsw

echo "[build] bench_svs"
g++ $CXXFLAGS \
  -I. -I"$SVS_DEPS/include" \
  bench_svs.cc -o bench_svs \
  -L"$SVS_DEPS/lib" -lsvs_runtime \
  -Wl,-rpath,"$SVS_DEPS/lib"

echo "[build] mmap_trace.so"
gcc -O2 -shared -fPIC -rdynamic mmap_trace.c -o mmap_trace.so -ldl

echo "[build] done."
ls -la bench_hnsw bench_svs mmap_trace.so
