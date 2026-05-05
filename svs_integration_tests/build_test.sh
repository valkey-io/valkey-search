#!/bin/bash
# Build a single SVS-integration test (HNSW side or SVS side), or all.
#
# Usage:
#   ./build_test.sh hnsw/test_02_concurrent_search
#   ./build_test.sh svs/test_02_concurrent_search
#   ./build_test.sh all
#
# Layout:
#   svs_integration_tests/
#     hnsw/  — tests linked against vendored upstream hnswlib v0.8.0
#              (header-only, no external deps)
#     svs/   — tests linked against libsvs_runtime.so (from the main build)
set -e

THIS_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$THIS_DIR/.." && pwd)"
SVS_DEPS="$REPO_ROOT/.build-release/_deps/svs-src"

CXX=${CXX:-g++}
# C++20 is required because the SVS runtime headers use std::span.
CXXFLAGS="-std=c++20 -O2 -Wall -Wno-unused-function -pthread"

build_hnsw() {
  # HNSW tests only need the vendored header-only hnswlib.
  # No OpenMP, no external library.
  local name="$1"
  local src="$THIS_DIR/hnsw/${name}.cc"
  local out="$THIS_DIR/hnsw/${name}"
  if [ ! -f "$src" ]; then
    echo "ERROR: $src does not exist" >&2
    return 1
  fi
  echo "[hnsw] $name"
  "$CXX" $CXXFLAGS -I"$THIS_DIR/hnsw" -I"$THIS_DIR" "$src" -o "$out"
}

build_svs() {
  # SVS tests link against libsvs_runtime.so fetched during the main build.
  local name="$1"
  local src="$THIS_DIR/svs/${name}.cc"
  local out="$THIS_DIR/svs/${name}"
  if [ ! -f "$src" ]; then
    echo "ERROR: $src does not exist" >&2
    return 1
  fi
  if [ ! -d "$SVS_DEPS/include" ] || [ ! -d "$SVS_DEPS/lib" ]; then
    echo "ERROR: SVS runtime not found at $SVS_DEPS" >&2
    echo "Build valkey-search first: ninja -C .build-release libsearch.so" >&2
    return 1
  fi
  echo "[svs ] $name"
  "$CXX" $CXXFLAGS -fopenmp \
    -I"$SVS_DEPS/include" -I"$THIS_DIR" \
    "$src" \
    -L"$SVS_DEPS/lib" -Wl,-rpath,"$SVS_DEPS/lib" -lsvs_runtime \
    -o "$out"
}

build_one() {
  local path="$1"                       # e.g. hnsw/test_02_concurrent_search
  local subdir="${path%%/*}"            # hnsw | svs
  local name="${path##*/}"              # test_02_concurrent_search
  case "$subdir" in
    hnsw) build_hnsw "$name" ;;
    svs)  build_svs  "$name" ;;
    *) echo "ERROR: path must start with hnsw/ or svs/" >&2; return 1 ;;
  esac
}

if [ "$1" = "all" ]; then
  rc=0
  for f in "$THIS_DIR"/hnsw/test_*.cc; do
    [ -e "$f" ] || continue
    name=$(basename "$f" .cc)
    build_hnsw "$name" || rc=1
  done
  for f in "$THIS_DIR"/svs/test_*.cc; do
    [ -e "$f" ] || continue
    name=$(basename "$f" .cc)
    build_svs "$name" || rc=1
  done
  exit $rc
fi

if [ -z "$1" ]; then
  echo "Usage: $0 {hnsw|svs}/<test_name> | all" >&2
  exit 1
fi

build_one "$1"
