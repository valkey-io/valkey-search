#!/bin/bash
# Orchestrator script: build and run all fuzz targets, collect results.
#
# Usage:
#   ./fuzzing/run_all_fuzzers.sh [--build] [--asan] [--duration <seconds>] [--build-dir <path>]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
DURATION=300
BUILD_DIR=""
DO_BUILD="no"
USE_ASAN="no"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      DO_BUILD="yes"
      shift
      ;;
    --asan)
      USE_ASAN="yes"
      shift
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --build-dir)
      BUILD_DIR="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--build] [--asan] [--duration <seconds>] [--build-dir <path>]"
      echo ""
      echo "Options:"
      echo "  --build      Build with AFL instrumentation before fuzzing"
      echo "  --asan       Enable address sanitizer (AFL_USE_ASAN=1)"
      echo "  --duration   Seconds to run each fuzzer (default: 300)"
      echo "  --build-dir  Build output directory (default: .build-fuzz or .build-fuzz-asan)"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Set build directory default based on asan flag
if [[ -z "$BUILD_DIR" ]]; then
  if [[ "$USE_ASAN" == "yes" ]]; then
    BUILD_DIR=".build-fuzz-asan"
  else
    BUILD_DIR=".build-fuzz"
  fi
fi

# ---------------------------------------------------------------------------
# Build with AFL instrumentation if requested
# ---------------------------------------------------------------------------
if [[ "$DO_BUILD" == "yes" ]]; then
  echo "=== Building with AFL instrumentation ==="
  BUILD_ARGS="--fuzz"
  if [[ "$USE_ASAN" == "yes" ]]; then
    BUILD_ARGS="$BUILD_ARGS --asan"
  fi
  cd "$PROJECT_ROOT"
  ./build.sh $BUILD_ARGS
fi

# ---------------------------------------------------------------------------
# Fuzz target definitions
# Each entry: "<target_name> <binary_path> <seeds_dir> <dict_file>"
# ---------------------------------------------------------------------------
FUZZ_TARGETS=(
  "query_parser ${BUILD_DIR}/tests/fuzz_query_parser fuzzing/query_parser/seeds fuzzing/query_parser/query.dict"
)

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------
if ! command -v afl-fuzz &>/dev/null; then
  echo "ERROR: afl-fuzz not found. Install AFL++ first."
  echo "  See: https://github.com/AFLplusplus/AFLplusplus"
  exit 1
fi

for entry in "${FUZZ_TARGETS[@]}"; do
  read -r name binary seeds dict <<< "$entry"
  if [[ ! -x "$PROJECT_ROOT/$binary" ]]; then
    echo "ERROR: Binary not found: $PROJECT_ROOT/$binary"
    echo "  Build with: ./build.sh --fuzz"
    echo "  Or run:     $0 --build"
    exit 1
  fi
  if [[ ! -d "$PROJECT_ROOT/$seeds" ]]; then
    echo "ERROR: Seeds directory not found: $PROJECT_ROOT/$seeds"
    exit 1
  fi
done

# Verify the binary has AFL instrumentation
FIRST_BINARY="${PROJECT_ROOT}/$(echo "${FUZZ_TARGETS[0]}" | awk '{print $2}')"
if ! strings "$FIRST_BINARY" 2>/dev/null | grep -q "__afl"; then
  echo "WARNING: Binary does not appear to have AFL instrumentation."
  echo "  Rebuild with: ./build.sh --fuzz"
  echo "  Or run:       $0 --build"
  echo ""
fi

# ---------------------------------------------------------------------------
# System setup (requires root)
# ---------------------------------------------------------------------------
if [[ $EUID -eq 0 ]]; then
  echo "Setting up AFL system settings..."
  echo core > /proc/sys/kernel/core_pattern 2>/dev/null || true
  echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null || true
else
  echo "NOTE: Not running as root. If AFL complains about core_pattern or"
  echo "CPU scaling, run: sudo $0 $*"
fi

# ---------------------------------------------------------------------------
# Run each fuzzer
# ---------------------------------------------------------------------------
RESULTS_FILE="$SCRIPT_DIR/fuzz_results.txt"
echo "Fuzzing Results - $(date)" > "$RESULTS_FILE"
echo "Duration per target: ${DURATION}s" >> "$RESULTS_FILE"
echo "Build directory: ${BUILD_DIR}" >> "$RESULTS_FILE"
if [[ "$USE_ASAN" == "yes" ]]; then
  echo "Address sanitizer: enabled" >> "$RESULTS_FILE"
fi
echo "========================================" >> "$RESULTS_FILE"

cd "$PROJECT_ROOT"

for entry in "${FUZZ_TARGETS[@]}"; do
  read -r name binary seeds dict <<< "$entry"
  OUT_DIR="fuzzing/afl_out/$name"
  mkdir -p "$OUT_DIR"

  echo ""
  echo "=== Running fuzzer: $name (${DURATION}s) ==="
  echo "  Binary: $binary"
  echo "  Seeds:  $seeds"
  echo "  Dict:   $dict"
  echo "  Output: $OUT_DIR"

  DICT_FLAG=""
  if [[ -f "$dict" ]]; then
    DICT_FLAG="-x $dict"
  fi

  # Run with timeout; afl-fuzz returns non-zero on timeout, which is expected
  timeout "${DURATION}s" afl-fuzz \
    -i "$seeds" \
    -o "$OUT_DIR" \
    $DICT_FLAG \
    -- "$binary" \
    2>&1 || true

  # Collect stats
  echo "" >> "$RESULTS_FILE"
  echo "Target: $name" >> "$RESULTS_FILE"
  echo "----------------------------------------" >> "$RESULTS_FILE"

  STATS_FILE="$OUT_DIR/default/fuzzer_stats"
  if [[ -f "$STATS_FILE" ]]; then
    total_execs=$(grep "^execs_done" "$STATS_FILE" | awk '{print $3}')
    paths_found=$(grep "^corpus_count" "$STATS_FILE" | awk '{print $3}')
    crashes=$(grep "^saved_crashes" "$STATS_FILE" | awk '{print $3}')
    hangs=$(grep "^saved_hangs" "$STATS_FILE" | awk '{print $3}')
    exec_speed=$(grep "^execs_per_sec" "$STATS_FILE" | awk '{print $3}')

    echo "  Total execs:  ${total_execs:-N/A}" >> "$RESULTS_FILE"
    echo "  Paths found:  ${paths_found:-N/A}" >> "$RESULTS_FILE"
    echo "  Crashes:      ${crashes:-N/A}" >> "$RESULTS_FILE"
    echo "  Hangs:        ${hangs:-N/A}" >> "$RESULTS_FILE"
    echo "  Exec speed:   ${exec_speed:-N/A}/sec" >> "$RESULTS_FILE"

    echo "  Results: total_execs=${total_execs:-N/A} paths=${paths_found:-N/A} crashes=${crashes:-N/A} hangs=${hangs:-N/A} speed=${exec_speed:-N/A}/sec"
  else
    echo "  (no fuzzer_stats found)" >> "$RESULTS_FILE"
    echo "  WARNING: No fuzzer_stats found at $STATS_FILE"
  fi
done

echo ""
echo "========================================" >> "$RESULTS_FILE"
echo ""
echo "All fuzzers complete. Results saved to: $RESULTS_FILE"
