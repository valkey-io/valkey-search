#!/bin/bash -e

BUILD_CONFIG=release
RUN_CMAKE="no"
ROOT_DIR=$(readlink -f $(dirname $0))
VERBOSE_ARGS=""
CMAKE_TARGET=""
RUN_TEST=""

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
    mkdir -p ${ROOT_DIR}/.build-${BUILD_CONFIG}
    cd $_
    local BUILD_TYPE=$(echo ${BUILD_CONFIG^})
    rm -f CMakeCache.txt
    cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_TESTS=ON -Wno-dev
    cd ${ROOT_DIR}
}

function build() {
    # If the build folder does not exist, run cmake
    if [ ! -d "${ROOT_DIR}/.build-${BUILD_CONFIG}" ]; then
        configure
    fi
    cd ${ROOT_DIR}/.build-${BUILD_CONFIG}
    make -j$(nproc) ${VERBOSE_ARGS} ${CMAKE_TARGET}
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

function PRINT_TEST_NAME() {
    echo -e "\e[35;1m-- Running: $1 \e[0m"
}

START_TIME=`date +%s`
if [[ "${RUN_CMAKE}" == "yes" ]]; then
    configure
fi
build
END_TIME=`date +%s`
BUILD_RUNTIME=$((END_TIME - START_TIME))

START_TIME=`date +%s`
TESTS_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}/tests
if [[ "${RUN_TEST}" == "all" ]]; then
    TESTS=$(ls ${TESTS_DIR}/*_test)
    for test in $TESTS; do
        PRINT_TEST_NAME "${test}"
        ${test} --gtest_color=yes
    done
elif [ ! -z "${RUN_TEST}" ]; then
    PRINT_TEST_NAME "${TESTS_DIR}/${RUN_TEST}"
    ${TESTS_DIR}/${RUN_TEST} --gtest_color=yes
fi
END_TIME=`date +%s`
TEST_RUNTIME=$((END_TIME - START_TIME))

echo -e "\e[38:5:243;1m== Build time: ${BUILD_RUNTIME} seconds, Tests time: ${TEST_RUNTIME} seconds ==\e[0m "
