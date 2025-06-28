#!/bin/bash -e

# Find the rood folder
ROOT_DIR=$(readlink -f $(readlink -f $(dirname $0))/..)

. ${ROOT_DIR}/scripts/common.rc

LOG_NOTICE "Root directory is: ${ROOT_DIR}"

function print_usage() {
cat<<EOF
Usage: run.sh [options...]

    --debug                 Run integration tests in debug mode.
    --asan                  When passed, the integration will load the module under .build-release-asan/ | .build-debug-asan/
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
while [ $# -gt 0 ]
do
    arg=$1
    case $arg in
    --debug)
        shift || true
        BUILD_CONFIG="debug"
        LOG_INFO "Testing in debug mode"
        ;;
    --asan)
        shift || true
        ASAN_SUFFIX="-asan"
        LOG_INFO "Assuming ASan build"
        ;;
    --help|-h)
        print_usage
        exit 0
        ;;
    *)
        print_usage
        exit 1
        ;;
    esac
done

BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}${ASAN_SUFFIX}
WD=${BUILD_DIR}/integration

# Check for user provided module path
MODULE_PATH="${MODULE_PATH:=}"
if [ -z "${MODULE_PATH}" ]; then
    MODULE_PATH=${BUILD_DIR}/libsearch.${MODULE_EXT}
fi
check_existence "MODULE_PATH" "${MODULE_PATH}"

# Check for valkey-server
VALKEY_SERVER_PATH="${VALKEY_SERVER_PATH:=}"
if [ -z "${VALKEY_SERVER_PATH}" ]; then
    VALKEY_SERVER_PATH=$(which valkey-server||true)
    if [ -z "${VALKEY_SERVER_PATH}" ]; then
        LOG_ERROR "Could not find valkey-server in PATH nor in VALKEY_SERVER_PATH environment variable"
        exit 1
    fi
fi
check_existence "VALKEY_SERVER_PATH" "${VALKEY_SERVER_PATH}"

LOG_INFO "VALKEY_SERVER_PATH => ${VALKEY_SERVER_PATH}"

LOG_INFO "MODULE_PATH => ${MODULE_PATH}"
mkdir -p ${WD}
LOG_INFO "Working directory is set to: ${WD}"

function install_test_framework() {
    local test_framework_url="https://github.com/valkey-io/valkey-test-framework"
    local test_framework_path="${WD}/valkey-test-framework"

    LOG_INFO "PIP_PATH => ${PIP_PATH}"
    if [ -d "${test_framework_path}" ]; then
        LOG_INFO "valkey-test-framework found: ${test_framework_path}"
    else
        LOG_INFO "Cloning valkey-test-framework into ${test_framework_path}"
        git clone "${test_framework_url}" ${test_framework_path}
        pushd ${test_framework_path}
        local requirements_txt=${test_framework_path}/requirements.txt
        if [ -f ${requirements_txt} ]; then
            LOG_INFO "Installing requirements file"
            ${PIP_PATH} install -r ${requirements_txt}
        fi
        popd
        ln -sf ${test_framework_path}/src ${WD}/valkeytestframework
    fi
}

install_test_framework

# Export variables required by the test framework
export MODULE_PATH=${MODULE_PATH}
export VALKEY_SERVER_PATH=${VALKEY_SERVER_PATH}
export PYTHONPATH=${WD}/valkeytestframework:${WD}

FILTER_ARGS=""
if [ ! -z "${TEST_PATTERN}" ]; then
    FILTER_ARGS=" -k ${TEST_PATTERN}"
    LOG_INFO "TEST_PATTERN is set to: '${TEST_PATTERN}'"
else
    LOG_INFO "TEST_PATTERN is not set. Running all integration tests."
fi
LOG_INFO "Running: python3 -m pytest ${FILTER_ARGS} --capture=sys --cache-clear -v ${ROOT_DIR}/integration/"
python3 -m pytest ${FILTER_ARGS} --capture=sys --cache-clear -v ${ROOT_DIR}/integration/
