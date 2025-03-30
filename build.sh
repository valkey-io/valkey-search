#!/bin/bash -e

BUILD_CONFIG=release
RUN_CMAKE="no"
ROOT_DIR=$(readlink -f $(dirname $0))
VERBOSE_ARGS=""
CMAKE_TARGET=""
RUN_TEST=""
RUN_BUILD="yes"
DUMP_TEST_ERRORS_STDOUT="no"
NINJA_TOOL="ninja"
INTEGRETION_TEST="no"

# Constants
BOLD_PINK='\e[35;1m'
RESET='\e[0m'
GREEN='\e[32;1m'
RED='\e[31;1m'
BLUE='\e[34;1m'

echo "Root directory: ${ROOT_DIR}"

function print_usage() {
cat<<EOF
Usage: build.sh [options...]

    --help | -h               Print this help message and exit.
    --configure               Run cmake stage (aka configure stage).
    --verbose | -v            Run verbose build.
    --debug                   Build for debug version.
    --clean                   Clean the current build configuration (debug or release).
    --run-tests               Run all tests. Optionally, pass a test name to run: "--run-tests=<test-name>".
    --no-build                By default, build.sh always triggers a build. This option disables this behavior.
    --test-errors-stdout      When a test fails, dump the captured tests output to stdout.
    --run-integration-tests   Run integration tests.

Example usage:

    # Build the release configuration, run cmake if needed
    build.sh

    # Force run cmake and build the debug configuration
    build.sh --configure --debug

EOF
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
    --no-build)
        shift || true
        RUN_BUILD="no"
        echo "Running build: no"
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
    --run-integration-tests)
        INTEGRETION_TEST="yes"
        shift || true
        echo "Running integration tests"
        ;;
    --test-errors-stdout)
        DUMP_TEST_ERRORS_STDOUT="yes"
        shift || true
        echo "Write test errors to stdout on failure"
        ;;
    --verbose|-v)
        shift || true
        VERBOSE_ARGS="-v"
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

function configure() {
    printf "${BOLD_PINK}Running cmake...${RESET}\n"
    mkdir -p ${BUILD_DIR}
    cd $_
    local BUILD_TYPE=$(echo ${BUILD_CONFIG^})
    rm -f CMakeCache.txt
    cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_TESTS=ON -Wno-dev -GNinja
    cd ${ROOT_DIR}
}

function build() {
    printf "${BOLD_PINK}Building${RESET}\n"
    if [ -d ${BUILD_DIR} ]; then
        cd ${BUILD_DIR}
        ${NINJA_TOOL} ${VERBOSE_ARGS} ${CMAKE_TARGET}
        cd ${ROOT_DIR}

        printf "\n${GREEN}Build Successful!${RESET}\n\n"
        printf "${BOLD_PINK}Module path:${RESET} .build-${BUILD_CONFIG}/libsearch.so\n\n"
        printf "You may want to run the unit tests by executing:\n"
        printf "    ./build.sh --run-tests\n\n"
        printf "To load the module, execute the following command:\n"
        printf "    valkey-server --loadmodule %s/.build-${BUILD_CONFIG}/libsearch.so\n\n" "$PWD"
    fi
}

function print_test_prefix() {
    printf "${BOLD_PINK}Running:${RESET} $1"
}

function print_test_ok() {
    printf " ... ${GREEN}ok${RESET}\n"
}

function print_test_summary() {
    printf "${BLUE}Test output can be found here:${RESET} ${TEST_OUTPUT_FILE}\n"
}

function print_test_error_and_exit() {
    printf " ... ${RED}failed${RESET}\n"
    if [[ "${DUMP_TEST_ERRORS_STDOUT}" == "yes" ]]; then
        cat ${TEST_OUTPUT_FILE}
    fi
    print_test_summary
    exit 1
}

function check_tool() {
    local tool_name=$1
    local message=$2
    printf "Checking for ${tool_name}..."
    command -v ${tool_name} > /dev/null || \
        (printf "${RED}failed${RESET}.\n${RED}ERROR${RESET} - could not locate tool '${tool_name}'. ${message}\n" && exit 1)
    printf "${GREEN}ok${RESET}\n"
}

function check_tools() {
    local tools="cmake g++ gcc"
    for tool in $tools; do
        check_tool ${tool}
    done

    # Check for ninja. On RedHat based Linux, it is called ninja-build, while on Debian based Linux, it is simply ninja
    # Ubuntu / Mint et al will report "ID_LIKE=debian"
    local debian_output=$(cat /etc/*-release|grep -i debian|wc -l)
    if [ ${debian_output} -gt 0 ]; then
        NINJA_TOOL="ninja"
    else
        NINJA_TOOL="ninja-build"
    fi
    check_tool ${NINJA_TOOL}
}

# If any of the CMake files is newer than our "build.ninja" file, force "cmake" before building
function is_configure_required() {
    local ninja_build_file=${BUILD_DIR}/build.ninja
    if [[ "${RUN_CMAKE}" == "yes" ]]; then
        # User asked for configure
        echo "yes"
        return
    fi

    if [ ! -f ${ninja_build_file} ] || [ ! -f ${BUILD_DIR}/CMakeCache.txt ]; then
        # No ninja build file
        echo "yes"
        return
    fi
    local build_file_lastmodified=$(stat --printf "%Y" ${ninja_build_file})
    local cmake_files=$(find ${ROOT_DIR} -name "CMakeLists.txt" -o -name "*.cmake"| grep -v ".build-release" | grep -v ".build-debug")
    for cmake_file in ${cmake_files}; do
        local cmake_file_modified=$(stat --printf "%Y" ${cmake_file})
        if [ ${cmake_file_modified} -gt ${build_file_lastmodified} ]; then
            echo "yes"
            return
        fi
    done
    echo "no"
}


cleanup() {
  cd ${ROOT_DIR}
}

# Ensure cleanup runs on exit
trap cleanup EXIT

if [[ "${INTEGRETION_TEST}" == "yes" ]]; then
    cd testing/integration
    params=""
    if [[ "${DUMP_TEST_ERRORS_STDOUT}" == "yes" ]]; then
        params=" --test-errors-stdout"
    fi
    if [[ "${BUILD_CONFIG}" == "debug" ]]; then
        params="${params} --debug"
    fi
    ./run.sh ${params}
    exit 0
fi


BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}
TESTS_DIR=${BUILD_DIR}/tests
TEST_OUTPUT_FILE=${BUILD_DIR}/tests.out

printf "Checking if configure is required..."
FORCE_CMAKE=$(is_configure_required)
printf "${GREEN}${FORCE_CMAKE}${RESET}\n"
check_tools

START_TIME=`date +%s`
if [[ "${RUN_CMAKE}" == "yes" ]] || [[ "${FORCE_CMAKE}" == "yes" ]]; then
    configure
fi

if [[ "${RUN_BUILD}" == "yes" ]]; then
    build
fi

END_TIME=`date +%s`
BUILD_RUNTIME=$((END_TIME - START_TIME))

START_TIME=`date +%s`

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
