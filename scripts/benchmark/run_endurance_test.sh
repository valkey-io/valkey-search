#!/bin/bash
# ============================================================================
# Endurance Test Runner Script
# ============================================================================
# This script runs inside the Docker container and executes the complete
# endurance test workflow including server setup, TLS configuration,
# benchmarking with memtier, and result processing.
# ============================================================================

set -Eeuo pipefail

# Print configuration
echo "=== Endurance Test Configuration ==="
echo "Server Type: ${SERVER_TYPE}"
echo "Branch: ${BRANCH_VERSION}"
echo "TLS Enabled: ${ENABLE_TLS}"
echo "Duration: ${TEST_DURATION}s"
echo "Threads: ${THREADS}, Clients: ${CLIENTS}"
echo "Pipeline Depth: ${PIPELINE_DEPTH}"
echo "Data Size: ${DATA_SIZE} bytes"
echo "Workload: ${WORKLOAD_TYPE}"
echo "Keyspace: ${KEYSPACE_SIZE} keys"
echo ""

# Setup paths
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RUN_RESULTS_DIR="/workspace/results/${SERVER_TYPE}/${TIMESTAMP}"
TLS_DIR="/workspace/tls"
mkdir -p "${RUN_RESULTS_DIR}"
# TLS_DIR is pre-created and mounted by the workflow

# ============================================================================
# FUNCTION: Generate TLS Certificates
# ============================================================================
generate_tls_certificates() {
  local tls_dir="$1"
  
  echo "=== Generating TLS Certificates ==="
  cd "$tls_dir"
  
  # CA
  openssl genrsa -out ca-key.pem 4096 2>/dev/null
  openssl req -new -x509 -days 3650 -key ca-key.pem -out ca-cert.pem \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=Test CA" 2>/dev/null
  
  # Server cert
  openssl genrsa -out server-key.pem 4096 2>/dev/null
  openssl req -new -key server-key.pem -out server.csr \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost" 2>/dev/null
  openssl x509 -req -days 3650 -in server.csr \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out server-cert.pem \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") 2>/dev/null
  
  # Client cert
  openssl genrsa -out client-key.pem 4096 2>/dev/null
  openssl req -new -key client-key.pem -out client.csr \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=client" 2>/dev/null
  openssl x509 -req -days 3650 -in client.csr \
    -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem 2>/dev/null
  
  chmod 600 *.pem
  
  echo "TLS certificates generated successfully"
}

# ============================================================================
# FUNCTION: Build Server
# ============================================================================
build_server() {
  echo "=== Building ${SERVER_TYPE} ${BRANCH_VERSION} ==="
  
  cd /workspace
  
  if [[ "${SERVER_TYPE}" == "valkey" ]]; then
    if [[ ! -d "valkey" ]]; then
      git clone https://github.com/valkey-io/valkey.git
    fi
    cd valkey
    git fetch
    git checkout "${BRANCH_VERSION}"
    make distclean 2>/dev/null || true
    make BUILD_TLS=yes -j$(nproc)
    SERVER_BIN="$(pwd)/src/valkey-server"
    SERVER_CLI="$(pwd)/src/valkey-cli"
  else
    if [[ ! -d "redis" ]]; then
      git clone https://github.com/redis/redis.git
    fi
    cd redis
    git fetch
    git checkout "${BRANCH_VERSION}"
    make distclean 2>/dev/null || true
    make BUILD_TLS=yes -j$(nproc)
    SERVER_BIN="$(pwd)/src/redis-server"
    SERVER_CLI="$(pwd)/src/redis-cli"
  fi
  
  echo "Server built: ${SERVER_BIN}"
  "${SERVER_BIN}" --version
}

# ============================================================================
# FUNCTION: Start Server
# ============================================================================
start_server() {
  echo "=== Starting Server ==="
  
  local server_log="${RUN_RESULTS_DIR}/server.log"
  local config_file="/workspace/server.conf"
  
  # Base configuration
  cat > "${config_file}" << EOF
bind 127.0.0.1
protected-mode yes
port 6379
daemonize yes
logfile ${server_log}
loglevel notice
save ""
EOF
  
  # Add TLS configuration if enabled
  if [[ "${ENABLE_TLS}" == "true" ]]; then
    cat >> "${config_file}" << EOF
tls-port 6380
tls-cert-file ${TLS_DIR}/server-cert.pem
tls-key-file ${TLS_DIR}/server-key.pem
tls-ca-cert-file ${TLS_DIR}/ca-cert.pem
tls-auth-clients optional
tls-protocols "TLSv1.2 TLSv1.3"
EOF
    BENCHMARK_PORT=6380
  else
    BENCHMARK_PORT=6379
  fi
  
  # Start server
  "${SERVER_BIN}" "${config_file}"
  
  # Health check
  echo "Waiting for server to be ready..."
  for i in {1..30}; do
    if [[ "${ENABLE_TLS}" == "true" ]]; then
      if timeout 2 "${SERVER_CLI}" --tls --cacert "${TLS_DIR}/ca-cert.pem" \
         -p "${BENCHMARK_PORT}" PING &>/dev/null; then
        echo "Server is ready on port ${BENCHMARK_PORT} (TLS)"
        return 0
      fi
    else
      if timeout 2 "${SERVER_CLI}" -p "${BENCHMARK_PORT}" PING &>/dev/null; then
        echo "Server is ready on port ${BENCHMARK_PORT}"
        return 0
      fi
    fi
    sleep 1
  done
  
  echo "ERROR: Server failed to start"
  return 1
}

# ============================================================================
# FUNCTION: Run Benchmark
# ============================================================================
run_benchmark() {
  echo "=== Running Endurance Test with memtier_benchmark ==="
  
  # Build command array
  local cmd=(
    memtier_benchmark
    --server=127.0.0.1
    --port="${BENCHMARK_PORT}"
    --threads="${THREADS}"
    --clients="${CLIENTS}"
    --pipeline="${PIPELINE_DEPTH}"
    --data-size="${DATA_SIZE}"
    --key-pattern=R:R
    --key-maximum="${KEYSPACE_SIZE}"
    --hide-histogram
    --json-out-file="${RUN_RESULTS_DIR}/results.json"
    --out-file="${RUN_RESULTS_DIR}/results.txt"
  )
  
  # Add TLS options
  if [[ "${ENABLE_TLS}" == "true" ]]; then
    cmd+=(
      --tls
      --cert="${TLS_DIR}/client-cert.pem"
      --key="${TLS_DIR}/client-key.pem"
      --cacert="${TLS_DIR}/ca-cert.pem"
    )
  fi
  
  # Add duration or request count
  if (( TEST_DURATION > 0 )); then
    cmd+=(--test-time="${TEST_DURATION}")
  else
    cmd+=(--requests="${REQUEST_COUNT}")
  fi
  
  # Configure workload
  case "${WORKLOAD_TYPE}" in
    read_only) cmd+=(--ratio=0:1) ;;
    write_only) cmd+=(--ratio=1:0) ;;
    mixed) cmd+=(--ratio=1:1) ;;
  esac
  
  echo "Executing: ${cmd[*]}"
  echo ""
  
  # Run benchmark
  "${cmd[@]}"
  
  echo ""
  echo "Benchmark completed successfully"
}

# ============================================================================
# FUNCTION: Process Results
# ============================================================================
process_results() {
  echo "=== Processing Results ==="
  
  local json_file="${RUN_RESULTS_DIR}/results.json"
  local csv_file="${RUN_RESULTS_DIR}/summary.csv"
  
  if [[ ! -f "${json_file}" ]]; then
    echo "ERROR: Results file not found: ${json_file}"
    return 1
  fi
  
  # Generate CSV summary
  jq -r '
    ["timestamp","ops_sec","hits_sec","misses_sec","latency_avg_ms","latency_p50_ms","latency_p99_ms","latency_p99_9_ms","bandwidth_kb_sec"],
    (.ALL_STATS | [
      (now | todate),
      .Totals.Ops,
      .Totals.Hits,
      .Totals.Misses,
      .Totals.Latency,
      (.Percentiles."50.000" // 0),
      (.Percentiles."99.000" // 0),
      (.Percentiles."99.900" // 0),
      .Totals.KB_sec
    ])
    | @csv
  ' "${json_file}" > "${csv_file}"
  
  echo "Results summary:"
  cat "${csv_file}"
  echo ""
  
  # Save metadata
  cat > "${RUN_RESULTS_DIR}/metadata.json" << EOF
{
  "test_info": {
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "server_type": "${SERVER_TYPE}",
    "branch": "${BRANCH_VERSION}"
  },
  "configuration": {
    "tls_enabled": ${ENABLE_TLS},
    "test_duration": ${TEST_DURATION},
    "request_count": ${REQUEST_COUNT},
    "threads": ${THREADS},
    "clients": ${CLIENTS},
    "pipeline_depth": ${PIPELINE_DEPTH},
    "data_size": ${DATA_SIZE},
    "workload_type": "${WORKLOAD_TYPE}",
    "keyspace_size": ${KEYSPACE_SIZE}
  },
  "system": {
    "cpu_cores": $(nproc),
    "memtier_version": "$(memtier_benchmark --version 2>&1 | head -1 || echo 'unknown')"
  }
}
EOF
  
  echo "Results processed and saved to: ${RUN_RESULTS_DIR}"
}

# ============================================================================
# FUNCTION: Cleanup
# ============================================================================
cleanup() {
  echo "=== Cleanup ==="
  
  # Stop server
  if [[ -n "${SERVER_BIN:-}" ]]; then
    pkill -f "$(basename "${SERVER_BIN}")" 2>/dev/null || true
  fi
  
  # Clean up TLS files (but not the mounted directory itself)
  if [[ "${ENABLE_TLS}" == "true" && -d "${TLS_DIR}" ]]; then
    rm -f "${TLS_DIR}"/*.pem "${TLS_DIR}"/*.csr "${TLS_DIR}"/*.srl 2>/dev/null || true
  fi
  
  echo "Cleanup completed"
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================
main() {
  # Set trap for cleanup
  trap cleanup EXIT
  
  # Generate TLS certificates if needed
  if [[ "${ENABLE_TLS}" == "true" ]]; then
    generate_tls_certificates "${TLS_DIR}"
  fi
  
  # Build server
  build_server
  
  # Start server
  start_server
  
  # Run benchmark
  run_benchmark
  
  # Process results
  process_results
  
  echo ""
  echo "=== Endurance Test Completed Successfully ==="
}

# Execute main function
main
