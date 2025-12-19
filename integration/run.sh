#!/bin/bash -e

# Find the rood folder
ROOT_DIR=$(readlink -f $(readlink -f $(dirname $0))/..)

. ${ROOT_DIR}/scripts/common.rc
setup_valkey_server

LOG_NOTICE "Root directory is: ${ROOT_DIR}"

function print_usage() {
  cat <<EOF
Usage: run.sh [options...]

    --debug                 Run integration tests in debug mode.
    --asan                  When passed, the integration will load the module under .build-release-asan/ | .build-debug-asan/
    --tsan                  When passed, the integration will load the module under .build-release-tsan/ | .build-debug-tsan/
    --capture               Disable pytest output capture (shows print statements in real-time).
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
while [ $# -gt 0 ]; do
  arg=$1
  case $arg in
  --debug)
    shift || true
    BUILD_CONFIG="debug"
    LOG_INFO "Testing in debug mode"
    ;;
  --asan)
    shift || true
    SAN_SUFFIX="-asan"
    LOG_INFO "Assuming ASan build"
    ;;
  --tsan)
    shift || true
    SAN_SUFFIX="-tsan"
    LOG_INFO "Assuming TSan build"
    ;;
  --capture)
    shift || true
    export PYTEST_CAPTURE_DISABLED=1
    LOG_INFO "pytest capture mode will be disabled"
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

BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}${SAN_SUFFIX}
WD=${BUILD_DIR}/integration

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

FILTER_ARGS=""
if [ ! -z "${TEST_PATTERN}" ]; then
  FILTER_ARGS=" -k ${TEST_PATTERN}"
  LOG_INFO "TEST_PATTERN is set to: '${TEST_PATTERN}'"
else
  LOG_INFO "TEST_PATTERN is not set. Running all integration tests."
fi

RUN_SUCCESS=0
export LOGS_DIR=${WD}/.valkey-test-framework
print_environment_var "LOGS_DIR" "${LOGS_DIR}"

rm -fr ${LOGS_DIR}
mkdir -p ${LOGS_DIR}

function run_pytest() {
  zap valkey-server
  
  # Check if PYTEST_CAPTURE_DISABLED is set
  CAPTURE_ARG="--capture=sys"
  if [[ "${PYTEST_CAPTURE_DISABLED}" ]]; then
    CAPTURE_ARG="--capture=no"
    LOG_INFO "pytest capture mode is disabled"
  fi
  
  LOG_INFO "Running: ${PYTHON_PATH} -m pytest ${FILTER_ARGS} ${CAPTURE_ARG} --cache-clear -v ${ROOT_DIR}/integration/"
  ${PYTHON_PATH} -m pytest ${FILTER_ARGS} ${CAPTURE_ARG} --cache-clear -v ${ROOT_DIR}/integration/
  RUN_SUCCESS=$?
}

function run_with_retries() {
  counter=1
  retries=${INTEG_RETRIES}
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
  pkill valkey-server || true
  # Wait for 3 seconds making sure the processes terminated
  sleep 3
  # And now we can check the logs
  logfiles=$(find ${LOGS_DIR} -name "*.log")
  check_for_san_errors "${logfiles}"
fi
