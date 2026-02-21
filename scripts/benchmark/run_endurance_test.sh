#!/bin/bash
# ============================================================================
# Stability Test Runner Script
# ============================================================================
# This script runs the comprehensive stability tests for valkey-search.
# The stability tests include:
# - Multiple index types (FLAT, HNSW, TEXT, TAG, NUMERIC)
# - Concurrent memtier operations (HSET, DEL, EXPIRE, FT.SEARCH, FT.INFO, FT._LIST)
# - Background tasks (BGSAVE, FT.CREATE, FT.DROPINDEX, FLUSHDB)
# - Optional failover testing with replicas
# ============================================================================

set -Eeuo pipefail

# Default configuration
VALKEY_SERVER_PATH="${VALKEY_SERVER_PATH:-valkey/src/valkey-server}"
VALKEY_CLI_PATH="${VALKEY_CLI_PATH:-valkey/src/valkey-cli}"
VALKEY_SEARCH_PATH="${VALKEY_SEARCH_PATH:-.build-release/libsearch.so}"
MEMTIER_PATH="${MEMTIER_PATH:-memtier_benchmark}"
TEST_TMPDIR="${TEST_TMPDIR:-/tmp/stability_test}"
TEST_UNDECLARED_OUTPUTS_DIR="${TEST_UNDECLARED_OUTPUTS_DIR:-/tmp/stability_test_outputs}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# Print configuration
echo "=== Stability Test Configuration ==="
echo "Valkey Server: ${VALKEY_SERVER_PATH}"
echo "Valkey CLI: ${VALKEY_CLI_PATH}"
echo "Valkey Search Module: ${VALKEY_SEARCH_PATH}"
echo "Memtier Path: ${MEMTIER_PATH}"
echo "Python: ${PYTHON_BIN}"
echo ""

# ============================================================================
# FUNCTION: Check Prerequisites
# ============================================================================
check_prerequisites() {
    echo "=== Checking Prerequisites ==="
    
    local missing_deps=()
    
    # Check Python
    if ! command -v "${PYTHON_BIN}" &> /dev/null; then
        missing_deps+=("${PYTHON_BIN}")
    fi
    
    # Check memtier_benchmark
    if ! command -v "${MEMTIER_PATH}" &> /dev/null; then
        missing_deps+=("memtier_benchmark")
    fi
    
    # Check if valkey server exists
    if [[ ! -f "${VALKEY_SERVER_PATH}" ]]; then
        echo "WARNING: Valkey server not found at ${VALKEY_SERVER_PATH}"
        echo "Attempting to build valkey..."
        build_valkey
    fi
    
    # Check if valkey-search module exists
    if [[ ! -f "${VALKEY_SEARCH_PATH}" ]]; then
        echo "WARNING: Valkey-search module not found at ${VALKEY_SEARCH_PATH}"
        echo "Attempting to build valkey-search..."
        build_valkey_search
    fi
    
    if [ ${#missing_deps[@]} -ne 0 ]; then
        echo "ERROR: Missing required dependencies: ${missing_deps[*]}"
        echo ""
        echo "Please install the missing dependencies:"
        echo "  - Python 3: apt-get install python3 python3-pip"
        echo "  - memtier_benchmark: apt-get install memtier-benchmark or build from source"
        return 1
    fi
    
    echo "All prerequisites satisfied"
}

# ============================================================================
# FUNCTION: Build Valkey
# ============================================================================
build_valkey() {
    echo "=== Building Valkey ==="
    
    local valkey_dir="valkey"
    local original_dir="$(pwd)"
    
    if [[ ! -d "${valkey_dir}" ]]; then
        echo "Cloning valkey repository..."
        if ! git clone https://github.com/valkey-io/valkey.git "${valkey_dir}"; then
            echo "ERROR: Failed to clone valkey repository"
            return 1
        fi
    fi
    
    cd "${valkey_dir}"
    
    echo "Building valkey..."
    if ! make BUILD_TLS=yes -j$(nproc); then
        cd "${original_dir}"  # Restore directory before returning error
        echo "ERROR: Failed to build valkey"
        return 1
    fi
    
    cd "${original_dir}"  # Always restore original directory
    
    VALKEY_SERVER_PATH="${original_dir}/${valkey_dir}/src/valkey-server"
    VALKEY_CLI_PATH="${original_dir}/${valkey_dir}/src/valkey-cli"
    
    echo "Valkey built successfully"
}

# ============================================================================
# FUNCTION: Build Valkey-Search
# ============================================================================
build_valkey_search() {
    echo "=== Building Valkey-Search ==="
    
    if [[ ! -f "build.sh" ]]; then
        echo "ERROR: build.sh not found. Are you in the valkey-search directory?"
        return 1
    fi
    
    echo "Running build script..."
    if ! ./build.sh; then
        echo "ERROR: Failed to build valkey-search"
        return 1
    fi
    
    # build.sh creates .build-release/libsearch.so
    VALKEY_SEARCH_PATH="$(pwd)/.build-release/libsearch.so"
    
    if [[ ! -f "${VALKEY_SEARCH_PATH}" ]]; then
        echo "ERROR: valkey-search module not found after build at ${VALKEY_SEARCH_PATH}"
        return 1
    fi
    
    echo "Valkey-search built successfully at ${VALKEY_SEARCH_PATH}"
}

# ============================================================================
# FUNCTION: Setup Test Environment
# ============================================================================
setup_test_environment() {
    echo "=== Setting Up Test Environment ==="
    
    # Create test directories
    mkdir -p "${TEST_TMPDIR}"
    mkdir -p "${TEST_UNDECLARED_OUTPUTS_DIR}"
    
    # Export environment variables for the Python tests
    export VALKEY_SERVER_PATH="$(realpath "${VALKEY_SERVER_PATH}")"
    export VALKEY_CLI_PATH="$(realpath "${VALKEY_CLI_PATH}")"
    export VALKEY_SEARCH_PATH="$(realpath "${VALKEY_SEARCH_PATH}")"
    export MEMTIER_PATH="${MEMTIER_PATH}"
    export TEST_TMPDIR="$(realpath "${TEST_TMPDIR}")"
    export TEST_UNDECLARED_OUTPUTS_DIR="$(realpath "${TEST_UNDECLARED_OUTPUTS_DIR}")"
    
    # VALKEY_JSON_PATH is optional - set to empty string if not provided
    # This prevents KeyError in tests that check for this environment variable
    export VALKEY_JSON_PATH="${VALKEY_JSON_PATH:-}"
    
    echo "Environment configured:"
    echo "  VALKEY_SERVER_PATH=${VALKEY_SERVER_PATH}"
    echo "  VALKEY_CLI_PATH=${VALKEY_CLI_PATH}"
    echo "  VALKEY_SEARCH_PATH=${VALKEY_SEARCH_PATH}"
    echo "  MEMTIER_PATH=${MEMTIER_PATH}"
    echo "  TEST_TMPDIR=${TEST_TMPDIR}"
    echo "  TEST_UNDECLARED_OUTPUTS_DIR=${TEST_UNDECLARED_OUTPUTS_DIR}"
    echo "  VALKEY_JSON_PATH=${VALKEY_JSON_PATH}"
}

# ============================================================================
# FUNCTION: Run Stability Tests
# ============================================================================
run_stability_tests() {
    echo "=== Running Stability Tests ==="
    
    cd testing/integration
    
    # Run the stability tests (absltest doesn't support --test_filter flag)
    # To run specific tests, pass test name directly: python3 stability_test.py StabilityTests.test_valkeyquery_stability_<test_name>
    echo "Executing: ${PYTHON_BIN} stability_test.py"
    echo ""
    
    if "${PYTHON_BIN}" stability_test.py; then
        echo ""
        echo "=== Stability Tests PASSED ==="
        return 0
    else
        echo ""
        echo "=== Stability Tests FAILED ==="
        return 1
    fi
}

# ============================================================================
# FUNCTION: Cleanup
# ============================================================================
cleanup() {
    echo "=== Cleanup ==="
    
    # Kill any remaining valkey processes
    pkill -f "valkey-server" 2>/dev/null || true
    
    # Wait a moment for processes to terminate
    sleep 2
    
    echo "Cleanup completed"
}

# ============================================================================
# FUNCTION: Show Help
# ============================================================================
show_help() {
    cat << EOF
Usage: $0 [OPTIONS]

Run the valkey-search stability tests (all tests).

OPTIONS:
    --valkey-server PATH        Path to valkey-server binary (default: valkey/src/valkey-server)
    --valkey-cli PATH          Path to valkey-cli binary (default: valkey/src/valkey-cli)
    --valkey-search PATH       Path to valkey-search module (default: .build-release/libsearch.so)
    --memtier PATH             Path to memtier_benchmark (default: memtier_benchmark)
    --python PATH              Path to python3 binary (default: python3)
    --help                     Show this help message

EXAMPLES:
    # Run all stability tests
    $0

    # Run tests with specific valkey binary
    $0 --valkey-server /path/to/valkey-server

    # Run specific test directly (bypassing this script)
    python3 testing/integration/stability_test.py StabilityTests.test_valkeyquery_stability_flat_with_backfill_coordinator

AVAILABLE TEST CASES (run via direct python call):
    - flat_with_backfill_coordinator
    - hnsw_with_backfill_no_coordinator
    - hnsw_no_backfill_no_coordinator
    - hnsw_with_backfill_coordinator_replica
    - hnsw_with_backfill_no_coordinator_replica
    - hnsw_with_backfill_coordinator_repl_diskless_disabled
    - hnsw_with_backfill_no_coordinator_repl_diskless_disabled
    - text_with_backfill_coordinator
    - text_with_backfill_no_coordinator
    - text_no_backfill_no_coordinator
    - tag_with_backfill_coordinator
    - tag_with_backfill_no_coordinator
    - tag_no_backfill_no_coordinator
    - numeric_with_backfill_coordinator
    - numeric_with_backfill_no_coordinator
    - numeric_no_backfill_no_coordinator

EOF
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================
main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --valkey-server)
                VALKEY_SERVER_PATH="$2"
                shift 2
                ;;
            --valkey-cli)
                VALKEY_CLI_PATH="$2"
                shift 2
                ;;
            --valkey-search)
                VALKEY_SEARCH_PATH="$2"
                shift 2
                ;;
            --memtier)
                MEMTIER_PATH="$2"
                shift 2
                ;;
            --python)
                PYTHON_BIN="$2"
                shift 2
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Set trap for cleanup
    trap cleanup EXIT
    
    # Check prerequisites
    check_prerequisites
    
    # Setup test environment
    setup_test_environment
    
    # Run stability tests
    run_stability_tests
    
    local exit_code=$?
    
    if [[ ${exit_code} -eq 0 ]]; then
        echo ""
        echo "✓ All stability tests completed successfully!"
        echo "Results saved to: ${TEST_UNDECLARED_OUTPUTS_DIR}"
    else
        echo ""
        echo "✗ Stability tests failed!"
        echo "Check logs in: ${TEST_UNDECLARED_OUTPUTS_DIR}"
    fi
    
    return ${exit_code}
}

# Execute main function
main "$@"