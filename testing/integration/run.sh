#!/bin/bash -e

ROOT_DIR=$(readlink -f $(dirname $0))
WORKSPACE_HOME=$(readlink -f ${ROOT_DIR}/../..)

BUILD_CONFIG=release
TEST=all
CLEAN="no"
VALKEY_VERSION="8.1.1"
VALKEY_JSON_VERSION="unstable"
MODULE_ROOT=$(readlink -f ${ROOT_DIR}/../..)
DUMP_TEST_ERRORS_STDOUT="no"

# Constants
BOLD_PINK='\e[35;1m'
RESET='\e[0m'
GREEN='\e[32;1m'
RED='\e[31;1m'
BLUE='\e[34;1m'
GRAY='\e[90;1m'

echo "Root directory: ${ROOT_DIR}"
echo "WORKSPACE_HOME directory: ${WORKSPACE_HOME}"

function print_usage() {
cat<<EOF
Usage: test.sh [options...]

    --help | -h              Print this help message and exit.
    --clean                  Clean the current build configuration.
    --debug                  Build for debug version.
    --test                   Specify the test name [stability|vector_search_integration]. Default all.
    --test-errors-stdout     When a test fails, dump the captured tests output to stdout.
    --asan                   Build the ASan version of the module.
    --tsan                   Build the TSan version of the module.

EOF
}
SAN_BUILD="no"
san_suffix=""
## Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)
        shift || true
        CLEAN="yes"
        ;;
    --asan)
        shift || true
        SAN_BUILD="address"
        san_suffix="-asan"
        ;;
    --tsan)
        shift || true
        SAN_BUILD="thread"
        san_suffix="-tsan"
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

export SAN_BUILD

# Source the common.rc after we setup our environment variables
. ${WORKSPACE_HOME}/scripts/common.rc

if [[ ! "${TEST}" == "stability" ]] && [[ ! "${TEST}" == "vector_search_integration" ]] && [[ ! "${TEST}" == "all" ]]; then
    printf "\n${RED}Invalid test value: ${TEST}${RESET}\n\n" >&2
    print_usage
    exit 1
fi


if [[ "${SAN_BUILD}" != "no" ]]; then
    TEST="vector_search_integration" # for now, we only support this test with sanitizer
    printf "${GREEN}Running integration tests with ${SAN_BUILD} sanitizer support${RESET}\n"
fi

function is_cmake_required() {
    if [ ! -f ${BUILD_DIR}/CTestTestfile.cmake ]; then
        echo "yes"
        return
    fi
    local build_file_lastmodified=$(get_file_last_modified ${ROOT_DIR}/CMakeLists.txt)
    local cmake_cache_modified=$(get_file_last_modified ${BUILD_DIR}/CTestTestfile.cmake)
    if [ ${build_file_lastmodified} -gt ${cmake_cache_modified} ]; then
        echo "yes"
        return
    fi
    echo "no"
}

function configure() {
    printf "Checking if cmake configure is required..."
    RUN_CMAKE=$(is_cmake_required)
    printf "${GREEN}${RUN_CMAKE}${RESET}\n"

    local BUILD_TYPE=$(capitalize_string ${BUILD_CONFIG})

    if [[ "${RUN_CMAKE}" == "yes" ]]; then
        printf "${BOLD_PINK}Running cmake...${RESET}\n"
        mkdir -p ${BUILD_DIR}
        cd $_
        cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE}
        cd ${ROOT_DIR}
    fi
  
    setup_valkey_server
    setup_json_module

    # If the binary is already there, do not rebuild it
    printf "Checking for ${VALKEY_SEARCH_PATH}"
    if [ ! -f "${VALKEY_SEARCH_PATH}" ]; then
        printf "... ${RED}not found${RESET}\n"
        printf "\n${RED} Please build ${VALKEY_SEARCH_PATH} and try again${RESET}\n\n";
        exit 1
    else
        printf "... ${GREEN}found${RESET}\n"
    fi
}

function build() {
    cd ${BUILD_DIR}
    source venv/bin/activate
    make
}

BUILD_DIR_BASENAME=.build-${BUILD_CONFIG}${san_suffix}
BUILD_DIR=${ROOT_DIR}/${BUILD_DIR_BASENAME}
VALKEY_SEARCH_PATH=${MODULE_ROOT}/${BUILD_DIR_BASENAME}/libsearch.${MODULE_EXT}

if [[ "${CLEAN}" == "yes" ]]; then
    rm -rf ${BUILD_DIR}
    exit 0
fi

function cleanup() {
    local exit_code=$1
    printf "Cleanup before exit..."
    pkill valkey-server || true
    deactivate >/dev/null 2>&1 || true
    cd ${ROOT_DIR}
    printf "${GREEN}done${RESET}\n"
    if [[ $exit_code -ne 0 ]]; then
        printf "${RED}Script exit with error code ${exit_code}${RESET}\n"
    else
        printf "${GREEN}Script completed successfully${RESET}\n"
    fi
}

# Ensure cleanup runs on exit
trap 'exit_code=$?; cleanup ${exit_code}; exit $exit_code' EXIT

configure
build

if ! command -v memtier_benchmark &> /dev/null; then
    printf "\n${RED}Error: memtier_benchmark is not installed or not in PATH.${RESET}\n\n" >&2
    exit 1
fi

export MEMTIER_PATH=memtier_benchmark
export VALKEY_SEARCH_PATH=${VALKEY_SEARCH_PATH}
export TEST_UNDECLARED_OUTPUTS_DIR="$BUILD_DIR/output"
if [[ "${SAN_BUILD}" != "no" ]]; then
    export ASAN_OPTIONS="detect_odr_violation=0:detect_leaks=1:halt_on_error=1"
    if [[ "${SAN_BUILD}" == "address" ]]; then
        export LSAN_OPTIONS="suppressions=${MODULE_ROOT}/ci/asan.supp"
    else
        export LSAN_OPTIONS="suppressions=${MODULE_ROOT}/ci/tsan.supp"
    fi
    LOG_NOTICE "Using LSAN_OPTIONS=${LSAN_OPTIONS}"
fi

rm -rf $TEST_UNDECLARED_OUTPUTS_DIR
mkdir -p $TEST_UNDECLARED_OUTPUTS_DIR
export TEST_TMPDIR="$BUILD_DIR/tmp"
rm -rf $TEST_TMPDIR

print_environment_var VALKEY_SERVER_PATH ${VALKEY_SERVER_PATH}
print_environment_var VALKEY_CLI_PATH ${VALKEY_CLI_PATH}
print_environment_var MEMTIER_PATH ${MEMTIER_PATH}
print_environment_var VALKEY_SEARCH_PATH ${VALKEY_SEARCH_PATH}
print_environment_var VALKEY_JSON_PATH ${VALKEY_JSON_PATH}
print_environment_var TEST_UNDECLARED_OUTPUTS_DIR ${TEST_UNDECLARED_OUTPUTS_DIR}
print_environment_var TEST_TMPDIR ${TEST_TMPDIR}

mkdir -p $TEST_TMPDIR
pkill -9 valkey-server || true

ALL_FILES="vector_search_integration_test.py stability_test.py"

if [[ "${TEST}" == "all" ]]; then
    for file in $ALL_FILES; do
        python3 ${ROOT_DIR}/${file}
    done
else
    python3 ${ROOT_DIR}/${TEST}_test.py
fi

printf "Checking for errors...\n"
if [[ "${SAN_BUILD}" != "no" ]]; then
    # Terminate valkey-server so the logs will be flushed
    pkill valkey-server || true
    # Wait for 3 seconds making sure the processes terminated
    sleep 3
    # And now we can check the logs
    check_for_san_errors "$(ls ${TEST_UNDECLARED_OUTPUTS_DIR}/*_stdout.txt | grep -v valkey_cli_stdout)"
fi
