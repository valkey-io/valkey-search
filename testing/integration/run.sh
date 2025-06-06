#!/bin/bash -e

ROOT_DIR=$(readlink -f $(dirname $0))
BUILD_CONFIG=release
TEST=all
CLEAN="no"
VALKEY_VERSION="8.1.1"
VALKEY_JSON_VERSION="1.0.0"
MODULE_ROOT=${ROOT_DIR}/../..
DUMP_TEST_ERRORS_STDOUT="no"

# List of pytest-dependent test files
PYTEST_TESTS=(
    "compatibility"
)

# List of standalone test programs
STANDALONE_TESTS=(
    "stability_test"
    "vector_search_integration_test"
)

# Constants
BOLD_PINK='\e[35;1m'
RESET='\e[0m'
GREEN='\e[32;1m'
RED='\e[31;1m'
BLUE='\e[34;1m'

echo "Root directory: ${ROOT_DIR}"

function print_usage() {
cat<<EOF
Usage: test.sh [options...]

    --help | -h              Print this help message and exit.
    --clean                  Clean the current build configuration.
    --debug                  Build for debug version.
    --test                   Specify the test name or 'all'. Default all.
    --test-errors-stdout     When a test fails, dump the captured tests output to stdout.

EOF
}

## Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)
        shift || true
        CLEAN="yes"
        ;;
    --test)
        TEST="$2"
        shift 2
        ;;
   --debug)
        shift || true
        BUILD_CONFIG="debug"
        echo "Building in Debug mode"
        ;;
    --test-errors-stdout)
        shift || true
        DUMP_TEST_ERRORS_STDOUT="yes"
        ;;
    --help|-h)
        print_usage
        exit 0
        ;;
    *)
        printf "\n${RED}Unknown argument: $1${RESET}\n\n" >&2
        print_usage
        exit 1
        ;;
    esac
done

if [[ "${TEST}" != "all" ]]; then
    if [[ ! " ${PYTEST_TESTS[@]} ${STANDALONE_TESTS[@]} " =~ " ${TEST} " ]]; then
        printf "\n${RED}Invalid test value: ${TEST}${RESET}\n\n" >&2
        print_usage
        exit 1
    fi
fi

function is_cmake_required() {
    if [ ! -f ${BUILD_DIR}/CTestTestfile.cmake ]; then
        echo "yes"
        return
    fi
    local build_file_lastmodified=$(stat --printf "%Y" ${ROOT_DIR}/CMakeLists.txt)
    local cmake_cache_modified=$(stat --printf "%Y" ${BUILD_DIR}/CTestTestfile.cmake)
    if [ ${build_file_lastmodified} -gt ${cmake_cache_modified} ]; then
        echo "yes"
        return
    fi
    echo "no"
}


function is_build_required() {
    if [ ! -f ${1} ]; then
        echo "yes"
        return
    fi
    echo "no"
}


function configure() {

    printf "Checking if cmake configure is required..."
    RUN_CMAKE=$(is_cmake_required)
    printf "${GREEN}${RUN_CMAKE}${RESET}\n"

    local BUILD_TYPE=$(echo ${BUILD_CONFIG^})

    if [[ "${RUN_CMAKE}" == "yes" ]]; then
        printf "${BOLD_PINK}Running cmake...${RESET}\n"
        mkdir -p ${BUILD_DIR}
        cd $_
        cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE}
        cd ${ROOT_DIR}
    fi

    printf "Checking if valkey-server build is required..."
    BUILD_SERVER=$(is_build_required ${VALKEY_SERVER_PATH})
    printf "${GREEN}${BUILD_SERVER}${RESET}\n"
    if [[ "${BUILD_SERVER}" == "yes" ]]; then
        printf "${BOLD_PINK}Building valkey-server...${RESET}\n"
        if [ ! -d ${VALKEY_SERVER_DIR} ]; then
            git clone --branch ${VALKEY_VERSION} --single-branch https://github.com/valkey-io/valkey.git ${VALKEY_SERVER_DIR}
        fi
        mkdir -p ${VALKEY_SERVER_BUILD_DIR}
        cd ${VALKEY_SERVER_BUILD_DIR}
        cmake -DCMAKE_BUILD_TYPE=Release .. -GNinja
        ninja
        cd ${ROOT_DIR}
    fi

    printf "Checking if valkey-json build is required..."
    BUILD_JSON=$(is_build_required ${VALKEY_JSON_PATH})
    printf "${GREEN}${BUILD_JSON}${RESET}\n"
     if [[ "${BUILD_JSON}" == "yes" ]]; then
        printf "${BOLD_PINK}Building valkey-json...${RESET}\n"

        rm -rf ${VALKEY_JSON_DIR}
        git clone --branch ${VALKEY_JSON_VERSION} --single-branch https://github.com/valkey-io/valkey-json.git ${VALKEY_JSON_DIR}
        cd ${VALKEY_JSON_DIR}
        set +e
        SERVER_VERSION=$VALKEY_VERSION ./build.sh
        set -e
        #mkdir build
        #cd build
        #cmake .. -DVALKEY_VERSION=${VALKEY_VERSION}
        #make -j$(nproc)
        cd ${ROOT_DIR}
    fi

    printf "${BOLD_PINK}Building ValkeySearch ...${RESET}\n"
    if [[ "${BUILD_CONFIG}" == "debug" ]]; then
        ${MODULE_ROOT}/./build.sh --debug
    else
        ${MODULE_ROOT}/./build.sh
    fi
    printf "Building ValkeySearch done!\n"
}

function build() {
    cd ${BUILD_DIR}
    source venv/bin/activate
    make
}

BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}
VALKEY_SERVER_DIR=${BUILD_DIR}/valkey-${VALKEY_VERSION}
VALKEY_SERVER_BUILD_DIR=${VALKEY_SERVER_DIR}/.build-release
VALKEY_SERVER_PATH=${VALKEY_SERVER_BUILD_DIR}/bin/valkey-server
VALKEY_JSON_DIR=${BUILD_DIR}/valkey-json-${VALKEY_JSON_VERSION}
VALKEY_JSON_PATH=${VALKEY_JSON_DIR}/build/src/libjson.so
echo " VALKEY_SERVER_DIR is set to ${VALKEY_SERVER_DIR}"

if [[ "${CLEAN}" == "yes" ]]; then
    rm -rf ${BUILD_DIR}
    exit 0
fi

cleanup() {
  printf "Cleanup before exit..."
  pkill -9 valkey-server || true
  deactivate >/dev/null 2>&1
  cd ${ROOT_DIR}
  printf "${GREEN}done${RESET}\n"
}

# Ensure cleanup runs on exit
trap cleanup EXIT

configure
build

if ! command -v memtier_benchmark &> /dev/null; then
    printf "\n${RED}Error: memtier_benchmark is not installed or not in PATH.${RESET}\n\n" >&2
    exit 1
fi
export VALKEY_SERVER_PATH="$VALKEY_SERVER_PATH"
export VALKEY_CLI_PATH=${VALKEY_SERVER_BUILD_DIR}/bin/valkey-cli
export MEMTIER_PATH=memtier_benchmark
export VALKEY_SEARCH_PATH=${MODULE_ROOT}/.build-${BUILD_CONFIG}/libsearch.so
export VALKEY_JSON_PATH="${VALKEY_JSON_PATH}"
export TEST_UNDECLARED_OUTPUTS_DIR="$BUILD_DIR/output"
rm -rf $TEST_UNDECLARED_OUTPUTS_DIR
mkdir -p $TEST_UNDECLARED_OUTPUTS_DIR
export TEST_TMPDIR="$BUILD_DIR/tmp"
rm -rf $TEST_TMPDIR

mkdir -p $TEST_TMPDIR
pkill -9 valkey-server || true

run_tests() {
    if [[ "${TEST}" == "all" ]]; then
        for test in "${PYTEST_TESTS[@]}" "${STANDALONE_TESTS[@]}"; do
            run_single_test "$test"
        done
    else
        run_single_test "${TEST}"
    fi
}

run_single_test() {
    local test_name="$1"
    local test_file="${test_name}.py"
    if [[ " ${PYTEST_TESTS[@]} " =~ " ${test_name} " ]]; then
        printf "${BOLD_PINK}Running pytest for ${test_file}...${RESET}\n"
        pytest "${test_file}"
    elif [[ " ${STANDALONE_TESTS[@]} " =~ " ${test_name} " ]]; then
        printf "${BOLD_PINK}Running standalone test ${test_file}...${RESET}\n"
        python3 "${test_file}"
    else
        printf "${RED}Unknown test: ${test_name}${RESET}\n"
        exit 1
    fi
}

if [[ "${DUMP_TEST_ERRORS_STDOUT}" == "yes" ]]; then
    run_tests
else
    run_tests
fi
