#!/bin/bash -e

ROOT_DIR=$(readlink -f $(dirname $0))
BUILD_CONFIG=release
TEST=all
CLEAN="no"
VALKEY_VERSION="8.1.1"
VALKEY_JSON_VERSION="1.0.0"
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

STANDALONE_TESTS=(
    "vector_search_integration"
    "stability"
)
PYTEST_TESTS=(
    "compatibility"
)

function print_usage() {
cat<<EOF
Usage: test.sh [options...]

    --help | -h              Print this help message and exit.
    --clean                  Clean the current build configuration.
    --debug                  Build for debug version.
    --test                   Specify the test name. Default all.
                             Standalone tests: ${STANDALONE_TESTS[*]}
                             Pytest tests: ${PYTEST_TESTS[*]}
    --test-errors-stdout     When a test fails, dump the captured tests output to stdout.
    --asan                   Build the ASan version of the module.

EOF
}

## Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --clean)
        shift || true
        CLEAN="yes"
        ;;
    --asan)
        shift || true
        ASAN_BUILD="yes"
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

if [[ ! "${TEST}" == "all" ]] && [[ ! " ${STANDALONE_TESTS[*]} ${PYTEST_TESTS[*]} " =~ " ${TEST} " ]]; then
    printf "\n${RED}Invalid test value: ${TEST}${RESET}\n\n" >&2
    print_usage
    exit 1
fi

asan_suffix=""
if [[ "${ASAN_BUILD}" == "yes" ]]; then
    asan_suffix="-asan"
    TEST="vector_search_integration" # for now, we only support this test with ASan
    printf "${GREEN}Running integration tests with ASan support${RESET}\n"
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

        VALKEY_CMAKE_EXTRA_ARGS=""
        if [[ "${ASAN_BUILD}" == "yes" ]]; then
            VALKEY_CMAKE_EXTRA_ARGS="-DBUILD_SANITIZER=address"
        fi

        printf "${BOLD_PINK}Running valkey-server cmake:${RESET}cmake -DCMAKE_BUILD_TYPE=Release .. -GNinja ${VALKEY_CMAKE_EXTRA_ARGS}\n"
        cmake -DCMAKE_BUILD_TYPE=Release .. -GNinja ${VALKEY_CMAKE_EXTRA_ARGS}
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
        cd ${ROOT_DIR}
    fi

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

BUILD_DIR_BASENAME=.build-${BUILD_CONFIG}${asan_suffix}
BUILD_DIR=${ROOT_DIR}/${BUILD_DIR_BASENAME}
VALKEY_SERVER_DIR=${BUILD_DIR}/valkey-${VALKEY_VERSION}
VALKEY_SERVER_BUILD_DIR=${VALKEY_SERVER_DIR}/.build-release
VALKEY_SERVER_PATH=${VALKEY_SERVER_BUILD_DIR}/bin/valkey-server
VALKEY_JSON_DIR=${BUILD_DIR}/valkey-json-${VALKEY_JSON_VERSION}
VALKEY_JSON_PATH=${VALKEY_JSON_DIR}/build/src/libjson.so
VALKEY_SEARCH_PATH=${MODULE_ROOT}/${BUILD_DIR_BASENAME}/libsearch.so

echo " VALKEY_SERVER_DIR is set to ${VALKEY_SERVER_DIR}"

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

function print_environment_var() {
    local varname=$1
    local value=$2
    printf "${GRAY}${varname}${RESET} => ${GREEN}${value}${RESET}\n"
}

# Loop over valkey log files and search for "AddressSanitizer" lines
function check_for_asan_errors() {
    valkey_logs=$(ls ${TEST_UNDECLARED_OUTPUTS_DIR}/*_stdout.txt | grep -v valkey_cli_stdout)
    local exit_with_error=0
    local files_to_dump=""
    for logfile in ${valkey_logs}; do
        printf "Checking log file ${logfile} for ASan errors"
        local errors_count=$(cat ${logfile}| grep -w AddressSanitizer|wc -l)
        if [[ ${errors_count} -eq 0 ]]; then
            printf "... ${GREEN}ok${RESET}\n"
        else
            printf "... ${RED}found errors!${RESET}\n"
            exit_with_error=1
            files_to_dump="${files_to_dump} ${logfile}"
        fi
    done

    if [[ ${exit_with_error} -ne 0 ]]; then
        printf "\n\nDumping log files with errors\n\n"
        for file in ${files_to_dump}; do
            cat $file
            printf "\n\n -------------------------------- \n\n"
        done
        exit 1
    fi
}

export VALKEY_SERVER_PATH="$VALKEY_SERVER_PATH"
export VALKEY_CLI_PATH=${VALKEY_SERVER_BUILD_DIR}/bin/valkey-cli
export MEMTIER_PATH=memtier_benchmark
export VALKEY_SEARCH_PATH=${VALKEY_SEARCH_PATH}
export VALKEY_JSON_PATH="${VALKEY_JSON_PATH}"
export TEST_UNDECLARED_OUTPUTS_DIR="$BUILD_DIR/output"
if [[ "${ASAN_BUILD}" == "yes" ]]; then
    export ASAN_OPTIONS="detect_odr_violation=0:detect_leaks=1:halt_on_error=1"
    export LSAN_OPTIONS="suppressions=${MODULE_ROOT}/ci/asan.supp"
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


if [[ "${TEST}" == "all" ]]; then
    for test in "${STANDALONE_TESTS[@]}"; do
        python3 ${ROOT_DIR}/${test}_test.py
    done
    if [ ${#PYTEST_TESTS[@]} -gt 0 ]; then
        pytest ${ROOT_DIR} -v $(printf "%s_test.py " "${PYTEST_TESTS[@]}")
    fi
else
    if [[ " ${STANDALONE_TESTS[*]} " =~ " ${TEST} " ]]; then
        python3 ${ROOT_DIR}/${TEST}_test.py
    elif [[ " ${PYTEST_TESTS[*]} " =~ " ${TEST} " ]]; then
        pytest ${ROOT_DIR}/${TEST}_test.py -v
    else
        echo "Error: Test '$TEST' not found in either STANDALONE_TESTS or PYTEST_TESTS"
        exit 1
    fi
fi

printf "Checking for errors...\n"
if [[ "${ASAN_BUILD}" == "yes" ]]; then
    # Terminate valkey-server so the logs will be flushed
    pkill valkey-server || true
    # Wait for 3 seconds making sure the processes terminated
    sleep 3
    # And now we can check the logs
    check_for_asan_errors
fi
