#!/bin/bash -e

ROOT_DIR=$(readlink -f $(dirname $0))
WORKSPACE_HOME=$(readlink -f ${ROOT_DIR}/../..)

BUILD_CONFIG=release
# TEST=all
TEST=vector_search_integration
CLEAN="no"
VALKEY_VERSION="9.1"
VALKEY_JSON_VERSION="unstable"
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
    --parallel[=N] | -j[=N]  Run tests in parallel with N workers (default: all CPU cores).

EOF
}
SAN_BUILD="no"
san_suffix=""
function is_valid_parallel_workers() {
    local val="$1"
    [[ "$val" == "auto" || "$val" == "logical" || "$val" =~ ^[0-9]+$ ]]
}

PARALLEL_WORKERS="${PARALLEL_WORKERS:=auto}"
if ! is_valid_parallel_workers "${PARALLEL_WORKERS}"; then
    printf "\n${RED}ERROR: Invalid PARALLEL_WORKERS environment variable: '${PARALLEL_WORKERS}'. Supported values: 0, auto, logical, or positive integer.${RESET}\n\n" >&2
    exit 1
fi
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
    --parallel | -j)
        shift || true
        if [[ $# -gt 0 && ! "$1" =~ ^- ]]; then
            if is_valid_parallel_workers "$1"; then
                PARALLEL_WORKERS="$1"
                shift || true
            else
                printf "\n${RED}ERROR: Invalid value for --parallel: '$1'. Supported values: 0, auto, logical, or positive integer.${RESET}\n\n" >&2
                exit 1
            fi
        else
            PARALLEL_WORKERS="auto"
        fi
        ;;
    --parallel=* | -j=*)
        PARALLEL_WORKERS="${1#*=}"
        shift || true
        if ! is_valid_parallel_workers "${PARALLEL_WORKERS}"; then
            printf "\n${RED}ERROR: Invalid value for --parallel: '${PARALLEL_WORKERS}'. Supported values: 0, auto, logical, or positive integer.${RESET}\n\n" >&2
            exit 1
        fi
        ;;
    --test)
        if [[ $# -lt 2 || "$2" =~ ^- ]]; then
            printf "\n${RED}ERROR: Option --test requires an argument.${RESET}\n\n" >&2
            exit 1
        fi
        TEST="$2"
        shift 2
        ;;
    --test=*)
        TEST="${1#*=}"
        shift || true
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
    local req_file_lastmodified=$(get_file_last_modified ${ROOT_DIR}/requirements.txt)
    if [ ${req_file_lastmodified} -gt ${cmake_cache_modified} ]; then
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
    if ! python3 -c "import pytest, xdist" 2>/dev/null; then
        pip install -r ${ROOT_DIR}/requirements.txt
    fi
    make
}

if [ -n "${san_suffix}" ]; then
    BUILD_DIR_BASENAME=.build-${BUILD_CONFIG}${san_suffix}
else
    BUILD_DIR_BASENAME=.build-${BUILD_CONFIG}${BUILD_DIR_SUFFIX}
fi
BUILD_DIR=${ROOT_DIR}/${BUILD_DIR_BASENAME}
export TEST_TMPDIR="$BUILD_DIR/tmp"
VALKEY_SEARCH_PATH=${MODULE_ROOT}/${BUILD_DIR_BASENAME}/libsearch.${MODULE_EXT}

if [[ "${CLEAN}" == "yes" ]]; then
    rm -rf ${BUILD_DIR}
    exit 0
fi

function cleanup() {
    local exit_code=$1
    printf "Cleanup before exit..."
    if [ -n "${TEST_TMPDIR:-}" ]; then
        pkill -9 -f "valkey-server.*${TEST_TMPDIR}" 2>/dev/null || true
    fi
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
trap 'exit_code=$?; cleanup ${exit_code}; exit $exit_code' EXIT INT TERM

# Early sanity check for required external CLI binaries
missing=()
if ! command -v memtier_benchmark &>/dev/null; then
    missing+=("memtier_benchmark")
fi
if ! command -v python3 &>/dev/null; then
    missing+=("python3")
fi
if ! command -v cmake &>/dev/null; then
    missing+=("cmake")
fi
if [[ ${#missing[@]} -gt 0 ]]; then
    printf "\n${RED}ERROR: Missing required CLI binaries for integration tests:${RESET}\n" >&2
    for tool in "${missing[@]}"; do
        printf "  ${RED}- %s${RESET}\n" "${tool}" >&2
    done
    printf "\n${YELLOW}Note: It is strongly recommended to run within the repository devcontainer where all dependencies are pre-installed:${RESET}\n" >&2
    printf "    ${GREEN}.devcontainer/run_in_docker.sh ./build.sh --run-integration-tests${RESET}\n\n" >&2
    exit 1
fi

configure
build

export MEMTIER_PATH=memtier_benchmark
export VALKEY_SEARCH_PATH=${VALKEY_SEARCH_PATH}
export TEST_UNDECLARED_OUTPUTS_DIR="$BUILD_DIR/output"


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
if [ -n "${TEST_TMPDIR:-}" ]; then
    pkill -9 -f "valkey-server.*${TEST_TMPDIR}" 2>/dev/null || true
fi

# Ensure virtual environment is active
if [ -f "${BUILD_DIR}/venv/bin/activate" ]; then
    source "${BUILD_DIR}/venv/bin/activate"
fi

PARALLEL_WORKERS="${PARALLEL_WORKERS:=auto}"
if [ -n "${PARALLEL_WORKERS}" ] && [ "${PARALLEL_WORKERS}" != "0" ] && [ "${PARALLEL_WORKERS}" != "1" ]; then
    if [ "${PARALLEL_WORKERS}" == "auto" ]; then
        PARALLEL_WORKERS=$(num_proc)
        if [ ${PARALLEL_WORKERS} -gt 32 ]; then
            PARALLEL_WORKERS=32
        elif [ ${PARALLEL_WORKERS} -lt 1 ]; then
            PARALLEL_WORKERS=1
        fi
    fi
fi

ALL_FILES="vector_search_integration_test.py stability_test.py"

if [ -n "${PARALLEL_WORKERS}" ] && [ "${PARALLEL_WORKERS}" != "0" ] && [ "${PARALLEL_WORKERS}" != "1" ]; then
    XDIST_ARGS="-n ${PARALLEL_WORKERS} --dist=load"
    echo "Running integration tests in parallel with ${PARALLEL_WORKERS} workers"
    if [[ "${TEST}" == "all" ]]; then
        python3 -m pytest ${XDIST_ARGS} -v ${ROOT_DIR}/vector_search_integration_test.py ${ROOT_DIR}/stability_test.py
    else
        python3 -m pytest ${XDIST_ARGS} -v ${ROOT_DIR}/${TEST}_test.py
    fi
else
    if [[ "${TEST}" == "all" ]]; then
        for file in $ALL_FILES; do
            python3 ${ROOT_DIR}/${file}
        done
    else
        python3 ${ROOT_DIR}/${TEST}_test.py
    fi
fi

if [[ "${SAN_BUILD}" != "no" ]]; then
    printf "Checking for errors...\n"
    # Terminate valkey-server so the logs will be flushed
    if [ -n "${TEST_TMPDIR:-}" ]; then
        pkill -f "valkey-server.*${TEST_TMPDIR}" 2>/dev/null || true
    fi
    # Wait for 3 seconds making sure the processes terminated
    sleep 3
    # And now we can check the logs
    check_for_san_errors "$(find ${TEST_UNDECLARED_OUTPUTS_DIR} -name "*_stdout.txt" | grep -v valkey_cli_stdout)"
fi
