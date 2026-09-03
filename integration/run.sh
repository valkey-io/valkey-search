#!/bin/bash -e

# Find the rood folder
ROOT_DIR=$(readlink -f $(readlink -f $(dirname $0))/..)
export ROOT_DIR

. ${ROOT_DIR}/scripts/common.rc

LOG_NOTICE "Root directory is: ${ROOT_DIR}"

function cleanup() {
  if [ -n "${LOGS_DIR:-}" ]; then
    pkill -9 -f "valkey-server.*${LOGS_DIR}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

function print_usage() {
  cat <<EOF
Usage: run.sh [options...]

    --debug                 Run integration tests in debug mode.
    --asan                  When passed, the integration will load the module under .build-release-asan/ | .build-debug-asan/
    --tsan                  When passed, the integration will load the module under .build-release-tsan/ | .build-debug-tsan/
    --capture               Disable pytest output capture (shows print statements in real-time).
    --parallel[=N] | -j[=N] Run tests in parallel with N workers (default: all CPU cores).
    --help | -h             Print this help message and exit.

EOF
}

function check_existence() {
  if [ ! -f "$2" ]; then
    LOG_ERROR "Could not locate file '$1': $2"
    exit 1
  fi
}

## Parse command line arguments
BUILD_CONFIG="release"
PARALLEL_WORKERS="${PARALLEL_WORKERS:=auto}"
if ! is_valid_parallel_workers "${PARALLEL_WORKERS}"; then
  LOG_ERROR "Invalid PARALLEL_WORKERS environment variable: '${PARALLEL_WORKERS}'. Supported values: 0, auto, logical, or positive integer."
  exit 1
fi
while [ $# -gt 0 ]; do
  arg=$1
  case $arg in
  --debug)
    shift || true
    BUILD_CONFIG="debug"
    LOG_INFO "Testing in debug mode"
    ;;
  --test-errors-stdout)
    shift || true
    ;;
  --asan)
    shift || true
    SAN_SUFFIX="-asan"
    export SAN_BUILD="address"
    LOG_INFO "Assuming ASan build"
    ;;
  --tsan)
    shift || true
    SAN_SUFFIX="-tsan"
    export SAN_BUILD="thread"
    LOG_INFO "Assuming TSan build"
    ;;
  --capture)
    shift || true
    export PYTEST_CAPTURE_DISABLED=1
    LOG_INFO "pytest capture mode will be disabled"
    ;;
  --parallel | -j)
    shift || true
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
      if is_valid_parallel_workers "$1"; then
        PARALLEL_WORKERS="$1"
        shift || true
      else
        LOG_ERROR "Invalid value for --parallel: '$1'. Supported values: 0, auto, logical, or positive integer."
        exit 1
      fi
    else
      PARALLEL_WORKERS="auto"
    fi
    ;;
  --parallel=* | -j=*)
    PARALLEL_WORKERS="${arg#*=}"
    shift || true
    if ! is_valid_parallel_workers "${PARALLEL_WORKERS}"; then
      LOG_ERROR "Invalid value for --parallel: '${PARALLEL_WORKERS}'. Supported values: 0, auto, logical, or positive integer."
      exit 1
    fi
    ;;
  -k | --filter)
    shift || true
    if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
      TEST_PATTERN="$1"
      shift || true
    else
      LOG_ERROR "Option $arg requires an argument."
      exit 1
    fi
    ;;
  --help | -h)
    print_usage
    exit 0
    ;;
  *)
    print_usage
    exit 1
    ;;
  esac
done

# If user did not pass --asan | --tsan, honor the environment variable
# SAN_BUILD.
if [ -z "${SAN_SUFFIX}" ] && [ ! -z "${SAN_BUILD}" ]; then
  if [[ "${SAN_BUILD}" == "address" ]]; then
    SAN_SUFFIX="-asan"
  elif [[ "${SAN_BUILD}" == "thread" ]]; then
    SAN_SUFFIX="-tsan"
  fi
fi

# Early sanity check for required external CLI binaries
missing=()
if ! command -v python3 &>/dev/null; then
  missing+=("python3")
fi
if ! command -v script &>/dev/null; then
  missing+=("script")
fi
if ! command -v git &>/dev/null; then
  missing+=("git")
fi
if [[ ${#missing[@]} -gt 0 ]]; then
  echo "" >&2
  printf "${RED}ERROR: Missing required CLI binaries for integration tests:${RESET}\n" >&2
  for tool in "${missing[@]}"; do
    printf "  ${RED}- %s${RESET}\n" "${tool}" >&2
  done
  echo "" >&2
  printf "${YELLOW}Note: It is strongly recommended to run within the repository devcontainer where all dependencies are pre-installed:${RESET}\n" >&2
  printf "    ${GREEN}.devcontainer/run_in_docker.sh ./build.sh --run-integration-tests${RESET}\n\n" >&2
  exit 1
fi

BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}${BUILD_DIR_SUFFIX:-}${SAN_SUFFIX}
WD=${BUILD_DIR}/integration
export LOGS_DIR=${WD}/.valkey-test-framework

# Check for user provided module path
MODULE_PATH="${MODULE_PATH:=}"
if [ -z "${MODULE_PATH}" ]; then
  MODULE_PATH=${BUILD_DIR}/libsearch.${MODULE_EXT}
fi
check_existence "MODULE_PATH" "${MODULE_PATH}"

setup_valkey_server
check_existence "VALKEY_SERVER_PATH" "${VALKEY_SERVER_PATH}"

export VALKEY_SERVER_PATH
export MODULE_PATH
print_environment_var "VALKEY_SERVER_PATH" "${VALKEY_SERVER_PATH}"
print_environment_var "MODULE_PATH" "${MODULE_PATH}"

mkdir -p ${WD}
LOG_INFO "Working directory is set to: ${WD}"

function setup_python() {
  if [ -z "${PYTHON_PATH}" ]; then
    LOG_INFO "Setting python env at: ${WD}/env"
    if [ ! -d ${WD}/env ]; then
      python3 -m venv ${WD}/env
    fi
    source ${WD}/env/bin/activate
    export PYTHON_PATH=${WD}/env/bin/python3
    export PIP_PATH=${WD}/env/bin/pip3
  fi
}

function zap() {
  echo "Zapping $1..."
  pids=$(ps -ef | grep $1 | grep -v grep | awk '{print $2;}')
  for pid in $pids; do
    kill -9 $pid
  done
}

# Check for user provided JSON module path
JSON_MODULE_PATH="${JSON_MODULE_PATH:=}"
if [ -z "${JSON_MODULE_PATH}" ]; then
  setup_json_module
  JSON_MODULE_PATH=${VALKEY_JSON_PATH}
fi
LOG_INFO "JSON_MODULE_PATH => ${JSON_MODULE_PATH}"

setup_python
install_test_framework

# Export variables required by the test framework
export MODULE_PATH=${MODULE_PATH}
export VALKEY_SERVER_PATH=${VALKEY_SERVER_PATH}
export JSON_MODULE_PATH=${JSON_MODULE_PATH}
export SKIPLOGCLEAN=1

FILTER_ARGS=()
if [ ! -z "${TEST_PATTERN}" ]; then
  FILTER_ARGS=(-k "${TEST_PATTERN}")
  LOG_INFO "TEST_PATTERN is set to: '${TEST_PATTERN}'"
else
  LOG_INFO "TEST_PATTERN is not set. Running all integration tests."
fi

RUN_SUCCESS=0
export LOGS_DIR=${WD}/.valkey-test-framework
print_environment_var "LOGS_DIR" "${LOGS_DIR}"

rm -fr ${LOGS_DIR}
mkdir -p ${LOGS_DIR}

PYTEST_OUTPUT_LOG=${LOGS_DIR}/pytest_output.log

function run_pytest() {
  if [ -n "${LOGS_DIR:-}" ]; then
    pkill -9 -f "valkey-server.*${LOGS_DIR}" 2>/dev/null || true
  fi
  
  # Check if PYTEST_CAPTURE_DISABLED is set
  CAPTURE_ARG="--capture=sys"
  if [[ "${PYTEST_CAPTURE_DISABLED}" ]]; then
    CAPTURE_ARG="--capture=no"
    LOG_INFO "pytest capture mode is disabled"
  fi
  
  PARALLEL_WORKERS="${PARALLEL_WORKERS:=auto}"
  XDIST_ARGS=""
  if [ -n "${PARALLEL_WORKERS}" ] && [ "${PARALLEL_WORKERS}" != "0" ] && [ "${PARALLEL_WORKERS}" != "1" ]; then
    if [ "${PARALLEL_WORKERS}" == "auto" ]; then
      PARALLEL_WORKERS=$(num_proc)
      if [ ${PARALLEL_WORKERS} -gt 32 ]; then
        PARALLEL_WORKERS=32
      elif [ ${PARALLEL_WORKERS} -lt 1 ]; then
        PARALLEL_WORKERS=1
      fi
    fi
    if [ "${PARALLEL_WORKERS}" != "1" ]; then
      XDIST_ARGS="-n ${PARALLEL_WORKERS} --dist=loadscope"
      LOG_INFO "Running integration tests in parallel with ${PARALLEL_WORKERS} workers"
    fi
  fi

  PYTEST_CMD=("${PYTHON_PATH}" -m pytest)
  if [ ${#FILTER_ARGS[@]} -gt 0 ]; then
    PYTEST_CMD+=("${FILTER_ARGS[@]}")
  fi
  PYTEST_CMD+=("${CAPTURE_ARG}")
  if [ -n "${XDIST_ARGS}" ]; then
    # shellcheck disable=SC2206
    XDIST_ARRAY=(${XDIST_ARGS})
    PYTEST_CMD+=("${XDIST_ARRAY[@]}")
  fi
  PYTEST_CMD+=(--cache-clear -v "${ROOT_DIR}/integration/")

  LOG_INFO "Running: ${PYTEST_CMD[*]}"
  # Capture pytest output to check for sanitizer errors
  if [[ "$(uname -s)" == "Darwin" ]]; then
    script -q "${PYTEST_OUTPUT_LOG}" "${PYTEST_CMD[@]}"
  else
    script -q -e -c "$(printf '%q ' "${PYTEST_CMD[@]}")" "${PYTEST_OUTPUT_LOG}"
  fi
  RUN_SUCCESS=$?
}

function run_with_retries() {
  counter=1
  retries=${INTEG_RETRIES:-1}
  if ((retries == 1)); then
    # Avoid the clutter and run it once.
    run_pytest
  else
    while ((counter <= retries)); do
      LOG_INFO "Running tests. Attempt number: ${counter}"
      set +e
      run_pytest
      set -e
      if [[ "${RUN_SUCCESS}" == "0" ]]; then
        LOG_INFO "Success!"
        return
      fi
      ((counter++))
      LOG_NOTICE "Retrying..."
    done
    LOG_ERROR "Retries exhausted"
    exit 1
  fi
}

run_with_retries

if [[ "${SAN_BUILD}" != "no" ]]; then
  printf "Checking for errors...\n"
  # Terminate valkey-server so the logs will be flushed
  if [ -n "${LOGS_DIR:-}" ]; then
    pkill -f "valkey-server.*${LOGS_DIR}" 2>/dev/null || true
  fi
  # Wait for 3 seconds making sure the processes terminated
  sleep 3
  # And now we can check the logs
  logfiles=$(find ${LOGS_DIR} -name "*.log")
  check_for_san_errors "${logfiles}"
  # Check pytest output for sanitizer errors
  check_for_san_errors "${PYTEST_OUTPUT_LOG}"
fi
