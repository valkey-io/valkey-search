#!/bin/bash -e

BUILD_CONFIG=release
RUN_CMAKE="no"
ROOT_DIR=$(readlink -f $(dirname $0))
VERBOSE_ARGS=""
CMAKE_TARGET=""
RUN_TEST=""

# Constants
BOLD_PINK='\e[35;1m'
RESET='\e[0m'
GRAY='\e[38:5:243;1m'
GREEN='\e[32;1m'
RED='\e[31;1m'
BLUE='\e[34;1m'

echo "Root directory: ${ROOT_DIR}"

function print_usage() {
cat<<EOF
Usage: build.sh [options...]

    --help | -h         Print this help message and exit
    --configure         Run cmake stage (aka configure stage)
    --verbose | -v      Run verbose build
    --debug             Build for debug version
    --clean             Clean the current build configuration (debug or release)
    --run-tests         Run all tests. Optionally, pass a test name to run: "--run-tests=<test-name>"

Example usage:

    # Build the release configuration, run cmake if needed
    build.sh

    # Force run cmake and build the debug configuration
    build.sh --configure --debug

EOF
}

function configure() {
    printf "${BOLD_PINK}Running cmake...${RESET}\n"
    mkdir -p ${ROOT_DIR}/.build-${BUILD_CONFIG}
    cd $_
    local BUILD_TYPE=$(echo ${BUILD_CONFIG^})
    rm -f CMakeCache.txt
    cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_TESTS=ON -Wno-dev -GNinja
    cd ${ROOT_DIR}
}

function build() {
    # If the build folder does not exist, run cmake
    if [ ! -d "${ROOT_DIR}/.build-${BUILD_CONFIG}" ]; then
        configure
    fi
    printf "${BOLD_PINK}Building${RESET}\n"
    cd ${ROOT_DIR}/.build-${BUILD_CONFIG}
    ninja ${VERBOSE_ARGS} ${CMAKE_TARGET}
    cd ${ROOT_DIR}
}

## Parse command line arguments
while [ $# -gt 0 ]
do
    arg=$1
    case $arg in
    --clean)
        shift || true
        CMAKE_TARGET="clean"
        echo "Will run 'make clean'"
        ;;
    --debug)
        shift || true
        BUILD_CONFIG="debug"
        echo "Building in Debug mode"
        ;;
    --configure)
        shift || true
        RUN_CMAKE="yes"
        echo "Running cmake: true"
        ;;
    --run-tests)
        RUN_TEST="all"
        shift || true
        echo "Running all tests"
        ;;
    --run-tests=*)
        RUN_TEST=${1#*=}
        shift || true
        echo "Running test ${RUN_TEST}"
        ;;
    --verbose|-v)
        shift || true
        VERBOSE_ARGS="VERBOSE=1"
        echo "Verbose build: true"
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



function print_test_prefix() {
    printf "${BOLD_PINK}Running:${RESET} $1"
}

function print_test_ok() {
    printf "${GRAY}...${GREEN}ok${RESET}\n"
}

function print_test_summary() {
    printf "${BLUE}Test output can be found here:${RESET} ${TEST_OUTPUT_FILE}\n"
}

function print_test_error_and_exit() {
    printf "${GRAY}...${RED}failed${RESET}\n"
    print_test_summary
    exit 1
}

function check_tool() {
    local tool_name=$1
    local message=$2
    printf "Checking for ${tool_name}${GRAY}...${RESET}"
    command -v ${tool_name} > /dev/null || \
        (printf "${RED}failed${RESET}.\n${RED}ERROR${RESET} - could not locate tool '${tool_name}'. ${message}\n" && exit 1)
    printf "${GREEN}ok${RESET}\n"
}

function check_tools() {
    local tools="cmake g++ gcc ninja"
    for tool in $tools; do
        check_tool ${tool}
    done
}

check_tools

START_TIME=`date +%s`
if [[ "${RUN_CMAKE}" == "yes" ]]; then
    configure
fi
build
END_TIME=`date +%s`
BUILD_RUNTIME=$((END_TIME - START_TIME))

START_TIME=`date +%s`
TESTS_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}/tests
TEST_OUTPUT_FILE=${ROOT_DIR}/.build-${BUILD_CONFIG}/tests.out

if [[ "${RUN_TEST}" == "all" ]]; then
    rm -f ${TEST_OUTPUT_FILE}
    TESTS=$(ls ${TESTS_DIR}/*_test)
    for test in $TESTS; do
        echo "==> Running executable: ${test}" >> ${TEST_OUTPUT_FILE}
        echo "" >> ${TEST_OUTPUT_FILE}
        print_test_prefix "${test}"
        ${test} >> ${TEST_OUTPUT_FILE} 2>&1 || print_test_error_and_exit
        print_test_ok
    done
    print_test_summary
elif [ ! -z "${RUN_TEST}" ]; then
    rm -f ${TEST_OUTPUT_FILE}
    echo "==> Running executable: ${TESTS_DIR}/${RUN_TEST}" >> ${TEST_OUTPUT_FILE}
    echo "" >> ${TEST_OUTPUT_FILE}
    print_test_prefix "${TESTS_DIR}/${RUN_TEST}"
    ${TESTS_DIR}/${RUN_TEST} >> ${TEST_OUTPUT_FILE} 2>&1 || print_test_error_and_exit
    print_test_ok
    print_test_summary
fi
END_TIME=`date +%s`
TEST_RUNTIME=$((END_TIME - START_TIME))
