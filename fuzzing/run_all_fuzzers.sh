#!/bin/bash
# Orchestrator script: build and run all fuzz targets, collect results.
#
# Usage:
#   ./fuzzing/run_all_fuzzers.sh [--duration <seconds>] [--build-dir <path>]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Defaults
DURATION=300
BUILD_DIR=".build-fuzz-asan"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --build-dir)
      BUILD_DIR="$2"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--duration <seconds>] [--build-dir <path>]"
      echo ""
      echo "Options:"
      echo "  --duration   Seconds to run each fuzzer (default: 300)"
      echo "  --build-dir  Build output directory (default: .build-fuzz-asan)"
      echo ""
      echo "Automatically builds AFL+ASAN instrumented binaries if not already present."
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Fuzz target definitions
# Each entry: "<target_name> <binary_name> <seeds_dir> <dict_file>"
# ---------------------------------------------------------------------------
FUZZ_TARGETS=(
  "query_parser fuzz_query_parser fuzzing/query_parser/seeds fuzzing/query_parser/query.dict"
  "ft_create_parser fuzz_ft_create_parser fuzzing/ft_create_parser/seeds fuzzing/ft_create_parser/ft_create.dict"
  "ft_search_parser fuzz_ft_search_parser fuzzing/ft_search_parser/seeds fuzzing/ft_search_parser/ft_search.dict"
  "ft_aggregate_parser fuzz_ft_aggregate_parser fuzzing/ft_aggregate_parser/seeds fuzzing/ft_aggregate_parser/ft_aggregate.dict"
)

# ---------------------------------------------------------------------------
# Preflight: ensure afl-fuzz is available
# ---------------------------------------------------------------------------
if ! command -v afl-fuzz &>/dev/null; then
  echo "ERROR: afl-fuzz not found. Install AFL++ first."
  echo "  See: https://github.com/AFLplusplus/AFLplusplus"
  exit 1
fi

# ---------------------------------------------------------------------------
# Auto-build AFL+ASAN binaries if any are missing
# ---------------------------------------------------------------------------
NEED_BUILD="no"
for entry in "${FUZZ_TARGETS[@]}"; do
  read -r _name bin_name _seeds _dict <<< "$entry"
  if [[ ! -x "$PROJECT_ROOT/$BUILD_DIR/tests/$bin_name" ]]; then
    NEED_BUILD="yes"
    break
  fi
done

if [[ "$NEED_BUILD" == "yes" ]]; then
  echo "=== Building AFL+ASAN instrumented binaries ==="
  cd "$PROJECT_ROOT"
  ./build.sh --fuzz --asan
fi

# ---------------------------------------------------------------------------
# Verify all binaries and seeds exist
# ---------------------------------------------------------------------------
for entry in "${FUZZ_TARGETS[@]}"; do
  read -r name bin_name seeds dict <<< "$entry"
  binary="$BUILD_DIR/tests/$bin_name"
  if [[ ! -x "$PROJECT_ROOT/$binary" ]]; then
    echo "ERROR: Binary not found after build: $PROJECT_ROOT/$binary"
    exit 1
  fi
  if [[ ! -d "$PROJECT_ROOT/$seeds" ]]; then
    echo "ERROR: Seeds directory not found: $PROJECT_ROOT/$seeds"
    exit 1
  fi
done

# Verify the binary has AFL instrumentation
FIRST_BIN_NAME=$(echo "${FUZZ_TARGETS[0]}" | awk '{print $2}')
FIRST_BINARY="$PROJECT_ROOT/$BUILD_DIR/tests/$FIRST_BIN_NAME"
if ! strings "$FIRST_BINARY" 2>/dev/null | grep -q "__afl"; then
  echo "WARNING: Binary does not appear to have AFL instrumentation."
  echo "  Rebuild with: ./build.sh --fuzz --asan"
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
echo "Address sanitizer: enabled" >> "$RESULTS_FILE"
echo "========================================" >> "$RESULTS_FILE"

cd "$PROJECT_ROOT"

trap 'echo ""; echo "Interrupted."; exit 130' INT TERM

for entry in "${FUZZ_TARGETS[@]}"; do
  read -r name bin_name seeds dict <<< "$entry"
  binary="$BUILD_DIR/tests/$bin_name"
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

  # Save terminal state before running afl-fuzz
  STTY_SAVED=$(stty -g 2>/dev/null) || true

  # Run afl-fuzz in a new session (setsid -w) to prevent signal propagation
  # to the parent shell. Redirect stdin from /dev/null and capture output to
  # a log file to prevent afl-fuzz's TUI from corrupting the terminal.
  AFL_LOG="$OUT_DIR/afl_log.txt"
  setsid -w timeout "${DURATION}s" afl-fuzz \
    -i "$seeds" \
    -o "$OUT_DIR" \
    $DICT_FLAG \
    -- "$binary" \
    </dev/null >"$AFL_LOG" 2>&1 &
  wait $! 2>/dev/null || true

  # Restore terminal state in case afl-fuzz corrupted it
  if [[ -n "$STTY_SAVED" ]]; then
    stty "$STTY_SAVED" 2>/dev/null || true
  fi

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
