#!/bin/bash -e

BUILD_CONFIG=release
RUN_CMAKE="no"
ROOT_DIR=$(readlink -f $(dirname $0))
VERBOSE_ARGS=""
CMAKE_TARGET=""
CMAKE_EXTRA_ARGS=""
FORMAT="no"
RUN_TEST=""
RUN_BUILD="yes"
DUMP_TEST_ERRORS_STDOUT="no"
INTEGRATION_TEST="no"
SAN_BUILD="no"
ARGV=$@
EXIT_CODE=0
INTEG_RETRIES=1
JOBS=""
CMAKE_GENERATOR=${CMAKE_GENERATOR:-"Ninja"}

echo "Root directory: ${ROOT_DIR}"

function print_usage() {
    cat <<EOF
Usage: build.sh [options...]

    --help | -h                       Print this help message and exit.
    --configure                       Run cmake stage (aka configure stage).
    --verbose | -v                    Run verbose build.
    --debug                           Build for debug version.
    --clean                           Clean the current build configuration (debug or release).
    --format                          Applies clang-format. (Run in dev container environment to ensure correct clang-format version)
    --run-tests                       Run all tests. Optionally, pass a test name to run: "--run-tests=<test-name>".
    --no-build                        By default, build.sh always triggers a build. This option disables this behavior.
    --test-errors-stdout              When a test fails, dump the captured tests output to stdout.
    --run-integration-tests[=pattern] Run integration tests.
    --use-system-modules              Use system's installed gRPC, Protobuf & Abseil dependencies.
    --asan                            Build with address sanitizer enabled.
    --tsan                            Build with thread sanitizer enabled.
    --retries=N                       Attempt to run integration tests N times. Default is 1.
    --jobs=N                          Limit the build workers to N. Default: use all available cores.

Example usage:

    # Build the release configuration, run cmake if needed
    build.sh

    # Force run cmake and build the debug configuration
    build.sh --configure --debug

EOF
}

## Parse command line arguments
while [ $# -gt 0 ]; do
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
    --format)
        FORMAT="yes"
        shift || true
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
    --unittest-output=*)
        UNITTEST_OUTPUT="${arg#*=}"
        shift || true
        # echo "Unit Test Output Directory: ${UNITTEST_OUTPUT} ** not yet implemented **"
        # Not currently implemented in build.sh, but used by upstream build_ubuntu.sh
        ;;
    --run-integration-tests)
        INTEGRATION_TEST="yes"
        shift || true
        echo "Running integration tests (all)"
        ;;
    --integration-output=*)
        INTEGRATION_OUTPUT="${arg#*=}"
        shift || true
        # echo "Integration Test Output Directory: ${INTEGRATION_OUTPUT} ** not yet implemented **"
        # Not currently implemented in build.sh, but used by upstream build_ubuntu.sh
        ;;
    --run-integration-tests=*)
        INTEGRATION_TEST="yes"
        TEST_PATTERN=${1#*=}
        shift || true
        echo "Running integration tests with pattern=${TEST_PATTERN}"
        ;;
    --retries=*)
        INTEG_RETRIES=${1#*=}
        shift || true
        ;;
    --test-errors-stdout)
        DUMP_TEST_ERRORS_STDOUT="yes"
        shift || true
        echo "Write test errors to stdout on failure"
        ;;
    --use-system-modules)
        CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DWITH_SUBMODULES_SYSTEM=ON"
        shift || true
        echo "Using extra cmake arguments: ${CMAKE_EXTRA_ARGS}"
        ;;
    --asan)
        CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DSAN_BUILD=address"
        SAN_BUILD="address"
        shift || true
        echo "Using extra cmake arguments: ${CMAKE_EXTRA_ARGS}"
        ;;
    --tsan)
        CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DSAN_BUILD=thread"
        SAN_BUILD="thread"
        shift || true
        echo "Using extra cmake arguments: ${CMAKE_EXTRA_ARGS}"
        ;;
    --jobs=*)
        JOBS=${1#*=}
        shift || true
        ;;
    --verbose | -v)
        shift || true
        VERBOSE_ARGS="-v"
        echo "Verbose build: true"
        ;;
    --help | -h)
        print_usage
        exit 0
        ;;
    *)
        echo "Unknown argument: ${arg}"
        print_usage
        exit 1
        ;;
    esac
done

# Import our functions, needs to be done after parsing the command line arguments
export SAN_BUILD
export ROOT_DIR
. "${ROOT_DIR}/scripts/common.rc"

if [[ "${CMAKE_GENERATOR}" == "Ninja" ]]; then
  BUILD_TOOL="ninja"
else
  BUILD_TOOL="make -j$(num_proc)"
fi

function configure() {
    printf "${BOLD_PINK}Running cmake...${RESET}\n"
    printf "Generating ${GREEN}${CMAKE_GENERATOR}${RESET} build files\n"
    mkdir -p ${BUILD_DIR}
    cd $_
    local BUILD_TYPE=$(capitalize_string ${BUILD_CONFIG})
    rm -f CMakeCache.txt
    printf "Running: cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_TESTS=ON -Wno-dev -G"${CMAKE_GENERATOR}" ${CMAKE_EXTRA_ARGS}\n"
    cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_TESTS=ON -Wno-dev -G"${CMAKE_GENERATOR}" ${CMAKE_EXTRA_ARGS}
    cd ${ROOT_DIR}
}

function build() {
    printf "${BOLD_PINK}Building${RESET}\n"
    if [ -d "${BUILD_DIR}" ]; then
        cd "${BUILD_DIR}"
        if [ -z "${JOBS}" ]; then
            ${BUILD_TOOL} ${VERBOSE_ARGS} ${CMAKE_TARGET}
        else
            ${BUILD_TOOL} -j ${JOBS} ${VERBOSE_ARGS} ${CMAKE_TARGET}
        fi
        cd "${ROOT_DIR}"

        printf "\n${GREEN}Build Successful!${RESET}\n\n"
        printf "${BOLD_PINK}Module path:${RESET} ${BUILD_DIR}/libsearch.${MODULE_EXT}\n\n"

        if [ -z "${RUN_TEST}" ]; then
            printf "You may want to run the unit tests by executing:\n"
            printf "    ./build.sh ${ARGV} --run-tests\n\n"
        fi

        printf "To load the module, execute the following command:\n"
        printf "    valkey-server --loadmodule ${BUILD_DIR}/libsearch.${MODULE_EXT}\n\n"
    fi
}

function format() {
    cd "${ROOT_DIR}"
    printf "Formatting...\n"
    find src testing vmsdk/src vmsdk/testing -name "*.h" -o -name "*.cc" | xargs clang-format -i
    printf "Applied clang-format\n"
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
        cat "${TEST_OUTPUT_FILE}"
        # To avoid dumping the content over and over again,
        # clear the file
        cp /dev/null "${TEST_OUTPUT_FILE}"
    fi

    # When running tests with sanitizer enabled, do not terminate the execution after the first failure continue
    # running the remainder of the tests
    if [[ "${SAN_BUILD}" == "no" ]]; then
        print_test_summary
        exit 1
    else
        # Make sure to exit the script with an error
        EXIT_CODE=1
    fi
}

function check_tool() {
    local tool_name=$1
    local message=$2
    printf "Checking for ${tool_name}..."
    command -v "${tool_name}" >/dev/null ||
        (printf "${RED}failed${RESET}.\n${RED}ERROR${RESET} - could not locate tool '${tool_name}'. ${message}\n" && exit 1)
    printf "${GREEN}ok${RESET}\n"
}

function check_tools() {
    local tools="cmake g++ gcc"
    for tool in $tools; do
        check_tool ${tool}
    done

    os_name=$(uname -s)
    if [[ "${BUILD_TOOL}" =~ make ]]; then
        BUILDER_TOOL="make"
    elif [[ "${os_name}" == "Darwin" ]]; then
        # ninja is can be installed via "brew"
        BUILDER_TOOL="ninja"
    else
        # Check for ninja. On RedHat based Linux, it is called ninja-build, while on Debian based Linux, it is simply ninja
        # Ubuntu / Mint et al will report "ID_LIKE=debian"
        local debian_output=$(cat /etc/*-release | grep -i debian | wc -l)
        if [ ${debian_output} -gt 0 ]; then
            BUILDER_TOOL="ninja"
        else
            BUILDER_TOOL="ninja-build"
        fi
    fi
    check_tool ${BUILD_TOOL}
}

# If any of the CMake files is newer than our "build.ninja" file, force "cmake" before building
function is_configure_required() {
    if [[ "${BUILD_TOOL}" == "ninja" ]]; then
      local top_level_build_file=${BUILD_DIR}/build.ninja
    else
      local top_level_build_file=${BUILD_DIR}/Makefile
    fi

    if [[ "${RUN_CMAKE}" == "yes" ]]; then
        # User asked for configure
        echo "yes"
        return
    fi

    if [ ! -f "${top_level_build_file}" ] || [ ! -f "${BUILD_DIR}/CMakeCache.txt" ]; then
        echo "yes"
        return
    fi

    local build_file_lastmodified=$(get_file_last_modified "${top_level_build_file}")
    local IFS=$'\n'
    local cmake_files=$(find "${ROOT_DIR}" -name "CMakeLists.txt" -o -name "*.cmake" | grep -v ".build-release" | grep -v ".build-debug")
    for cmake_file in $cmake_files; do
        local cmake_file_modified=$(get_file_last_modified "${cmake_file}")
        if [ "${cmake_file_modified}" -gt "${build_file_lastmodified}" ]; then
            echo "yes"
            return
        fi
    done
    echo "no"
}

cleanup() {
    cd "${ROOT_DIR}"
}

# Ensure cleanup runs on exit
trap cleanup EXIT
export CMAKE_POLICY_VERSION_MINIMUM=3.5

if [[ "${FORMAT}" == "yes" ]]; then
    format
fi

BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}
if [[ "${SAN_BUILD}" != "no" ]]; then
    printf "${BOLD_PINK}${SAN_BUILD} sanitizer build is enabled${RESET}\n"
    if [[ "${SAN_BUILD}" == "address" ]]; then
        BUILD_DIR=${BUILD_DIR}-asan
    else
        BUILD_DIR=${BUILD_DIR}-tsan
        export TSAN_OPTIONS="suppressions=${ROOT_DIR}/ci/tsan.supp"
    fi
fi

TESTS_DIR=${BUILD_DIR}/tests
TEST_OUTPUT_FILE=${BUILD_DIR}/tests.out

printf "Checking if configure is required..."

FORCE_CMAKE=$(is_configure_required)
printf "${GREEN}${FORCE_CMAKE}${RESET}\n"
check_tools

if [[ "${CMAKE_GENERATOR}" == "Ninja" ]]; then
  BUILD_TOOL="${NINJA_TOOL}"
else
  BUILD_TOOL="make -j$(num_proc)"
fi

START_TIME=$(date +%s)
if [[ "${RUN_CMAKE}" == "yes" ]] || [[ "${FORCE_CMAKE}" == "yes" ]]; then
    configure
fi

if [[ "${RUN_BUILD}" == "yes" ]]; then
    build
fi

END_TIME=$(date +%s)
BUILD_RUNTIME=$((END_TIME - START_TIME))

START_TIME=$(date +%s)

if [[ "${SAN_BUILD}" != "no" ]]; then
    export ASAN_OPTIONS="detect_odr_violation=0"
fi

if [[ "${RUN_TEST}" == "all" ]]; then
    rm -f "${TEST_OUTPUT_FILE}"
    find "${TESTS_DIR}" -name "*_test" -type f | while read -r test; do
        echo "==> Running executable: ${test}" >> "${TEST_OUTPUT_FILE}"
        echo "" >> "${TEST_OUTPUT_FILE}"
        print_test_prefix "${test}"
        ("${test}" >> "${TEST_OUTPUT_FILE}" 2>&1 && print_test_ok) || print_test_error_and_exit
    done
    print_test_summary
elif [ ! -z "${RUN_TEST}" ]; then
    rm -f "${TEST_OUTPUT_FILE}"
    echo "==> Running executable: ${TESTS_DIR}/${RUN_TEST}" >> "${TEST_OUTPUT_FILE}"
    echo "" >> "${TEST_OUTPUT_FILE}"
    print_test_prefix "${TESTS_DIR}/${RUN_TEST}"
    ("${TESTS_DIR}/${RUN_TEST}" && print_test_ok) || print_test_error_and_exit
    print_test_summary
elif [[ "${INTEGRATION_TEST}" == "yes" ]]; then
    if [ ! -z "${TEST_PATTERN}" ]; then
        echo ""
        LOG_WARNING " ** TEST_PATTERN is found, skipping Abseil based integration tests **"
        echo ""
    else
        # Abseil based tests do not support filtering tests based on "-k" flag
        # so when the TEST_PATTERN env variable is found, skip Abseil based tests
        pushd testing/integration >/dev/null
        params=""
        if [[ "${DUMP_TEST_ERRORS_STDOUT}" == "yes" ]]; then
            params=" --test-errors-stdout"
        fi
        if [[ "${BUILD_CONFIG}" == "debug" ]]; then
            params="${params} --debug"
        fi

        if [[ "${SAN_BUILD}" == "address" ]]; then
            params="${params} --asan"
        fi
        if [[ "${SAN_BUILD}" == "thread" ]]; then
            params="${params} --tsan"
        fi
        ./run.sh ${params}
        popd >/dev/null
    fi

    # Run OSS integration tests
    pushd integration >/dev/null
    if [[ "${TEST_PATTERN}" == "oss" ]]; then
        TEST_PATTERN=""
    fi
    export TEST_PATTERN=${TEST_PATTERN}
    export INTEG_RETRIES=${INTEG_RETRIES}
    # Run will run ASan or normal tests based on the environment variable SAN_BUILD
    ./run.sh
    popd >/dev/null
fi

END_TIME=$(date +%s)
TEST_RUNTIME=$((END_TIME - START_TIME))
exit ${EXIT_CODE}
