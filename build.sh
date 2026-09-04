#!/bin/bash -e

BUILD_CONFIG=release
RUN_CMAKE="no"
ROOT_DIR=$(readlink -f $(dirname $0))
VERBOSE_ARGS=""
CMAKE_TARGET=""
CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS:-}"
FORMAT="no"
RUN_TEST=""
RUN_BUILD="yes"
DUMP_TEST_ERRORS_STDOUT="no"
INTEGRATION_TEST="no"
SAN_BUILD="no"
USE_SYSTEM_MODULES="auto"
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
    --test-verbose                    Enable verbose test output (sets TEST_VERBOSE=1).
    --no-build                        By default, build.sh always triggers a build. This option disables this behavior.
    --test-errors-stdout              When a test fails, dump the captured tests output to stdout.
    --run-integration-tests[=pattern] Run integration tests.
    --no-system-modules               Disable system dependencies and force building from submodules.
    --asan                            Build with address sanitizer enabled.
    --tsan                            Build with thread sanitizer enabled.
    --retries=N                       Attempt to run integration tests N times. Default is 1.
    --jobs=N                          Limit the build workers to N. Default: use all available cores.

Example usage:

    # Build the release configuration, run cmake if needed
    ./build.sh

    # Force run cmake and build the debug configuration
    ./build.sh --configure --debug

    # Build debug version and run all unit tests
    ./build.sh --debug --run-tests

    # Run a specific unit test suite
    ./build.sh --debug --run-tests=query_test

    # Run unit tests matching a regex pattern
    ./build.sh --debug --run-tests="index.*"

    # Run unit tests with verbose debug traces enabled
    ./build.sh --debug --run-tests --test-verbose

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
    --no-system-modules)
        USE_SYSTEM_MODULES="no"
        shift || true
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
    --test-verbose)
        export TEST_VERBOSE=1
        shift || true
        echo "Verbose test output: true"
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

if [[ "${USE_SYSTEM_MODULES}" != "no" ]]; then
    san_suffix=""
    if [[ "${SAN_BUILD}" == "address" ]]; then
        san_suffix="-asan"
    elif [[ "${SAN_BUILD}" == "thread" ]]; then
        san_suffix="-tsan"
    fi
    DEPS_DIR="/opt/valkey-search-deps${san_suffix}"
    if [ -d "${DEPS_DIR}" ] && [ -f "${DEPS_DIR}/bin/grpc_cpp_plugin" ]; then
        CMAKE_DIR="${DEPS_DIR}/lib/cmake"
        export CMAKE_PREFIX_PATH="${CMAKE_DIR}/protobuf:${CMAKE_DIR}/absl:${CMAKE_DIR}/grpc:${CMAKE_DIR}/GTest:${CMAKE_DIR}/utf8_range:${CMAKE_DIR}/benchmark:${DEPS_DIR}${CMAKE_PREFIX_PATH:+:${CMAKE_PREFIX_PATH}}"
        if [[ "${CMAKE_EXTRA_ARGS}" != *"-DWITH_SUBMODULES_SYSTEM"* ]]; then
            CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DWITH_SUBMODULES_SYSTEM=ON"
        fi
        echo "Auto-detected system dependencies from ${DEPS_DIR}"
    fi
fi

# Import our functions, needs to be done after parsing the command line arguments
export SAN_BUILD
export ROOT_DIR
. "${ROOT_DIR}/scripts/common.rc"

if [[ "${CMAKE_GENERATOR}" == "Ninja" ]]; then
  BUILD_TOOL="ninja"
else
  BUILD_TOOL="make -j$(num_proc)"
fi

function build_icu_if_needed() {
    printf "${BOLD_PINK}Checking ICU dependencies...${RESET}\n"
    
    local ICU_SOURCE_DIR="${ROOT_DIR}/third_party/icu/source"
    local ICU_BUILD_DIR="${BUILD_DIR}/icu"
    
    # ICU source is committed to repository - no download needed
    if [ ! -d "${ICU_SOURCE_DIR}" ]; then
        printf "${RED}ERROR: ICU source not found in third_party/icu/source${RESET}\n"
        printf "ICU source should be committed to the repository.\n"
        exit 1
    fi
    
    # Check if ICU static libraries exist in build directory (clean approach)
    if [ -f "${ICU_BUILD_DIR}/install/lib/libicudata.a" ] && \
       [ -f "${ICU_BUILD_DIR}/install/lib/libicui18n.a" ] && \
       [ -f "${ICU_BUILD_DIR}/install/lib/libicuuc.a" ]; then
        printf "${GREEN}ICU static libraries found in build directory${RESET}\n"
        return 0
    fi
    
    printf "${BOLD_PINK}Building ICU with static data packaging...${RESET}\n"
    
    # Create clean build directory (out-of-tree build)
    rm -rf "${ICU_BUILD_DIR}"
    mkdir -p "${ICU_BUILD_DIR}"
    cd "${ICU_BUILD_DIR}"
    
    printf "Configuring ICU for static linking with embedded data...\n"
    
    local ICU_LOG="${ICU_BUILD_DIR}/icu-build.log"

    # Configure with static data packaging (output saved to log file)
    if ! "${ICU_SOURCE_DIR}/configure" \
        --enable-static \
        --disable-shared \
        --with-data-packaging=static \
        --disable-extras \
        --disable-icuio \
        --disable-layout \
        --disable-tests \
        --disable-samples \
        --enable-tools \
        --prefix="${ICU_BUILD_DIR}/install" \
        CFLAGS="-O2 -fPIC" \
        CXXFLAGS="-O2 -fPIC" > "${ICU_LOG}" 2>&1; then
        printf "${RED}ICU configure failed. See ${ICU_LOG}${RESET}\n"
        tail -30 "${ICU_LOG}"
        exit 1
    fi
    
    printf "Building ICU static libraries...\n"
    
    # Build with static data mode (output saved to log file)
    if ! make PKGDATA_MODE=static -j$(num_proc) >> "${ICU_LOG}" 2>&1; then
        printf "${RED}ICU build failed. See ${ICU_LOG}${RESET}\n"
        tail -50 "${ICU_LOG}"
        exit 1
    fi
    
    printf "Installing ICU libraries...\n"
    
    # Install to build directory
    if ! make install PKGDATA_MODE=static >> "${ICU_LOG}" 2>&1; then
        printf "${RED}ICU install failed. See ${ICU_LOG}${RESET}\n"
        tail -30 "${ICU_LOG}"
        exit 1
    fi
    
    cd "${ROOT_DIR}"
    
    # Verify libraries were created in build directory
    if [ -f "${ICU_BUILD_DIR}/install/lib/libicudata.a" ] && \
       [ -f "${ICU_BUILD_DIR}/install/lib/libicui18n.a" ] && \
       [ -f "${ICU_BUILD_DIR}/install/lib/libicuuc.a" ]; then
        printf "${GREEN}SUCCESS: ICU static libraries built successfully${RESET}\n"
        printf "Libraries created in build directory:\n"
        ls -lh "${ICU_BUILD_DIR}/install/lib/"*.a
        printf "Source directory kept clean\n"
    else
        printf "${RED}ERROR: ICU static libraries not found after build${RESET}\n"
        exit 1
    fi
}


function configure() {
    printf "${BOLD_PINK}Running cmake...${RESET}\n"
    printf "Generating ${GREEN}${CMAKE_GENERATOR}${RESET} build files\n"
    mkdir -p ${BUILD_DIR}
    cd $_
    local BUILD_TYPE=$(capitalize_string ${BUILD_CONFIG})
    rm -f CMakeCache.txt
    printf "Running: cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DBUILD_UNIT_TESTS=ON -Wno-dev -G"${CMAKE_GENERATOR}" ${CMAKE_EXTRA_ARGS}\n"
    cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DBUILD_UNIT_TESTS=ON -Wno-dev -G"${CMAKE_GENERATOR}" ${CMAKE_EXTRA_ARGS}
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
    find src testing vmsdk/src vmsdk/testing -name "*.h" -o -name "*.cc" | grep -v '^src/indexes/text/rax/' | xargs clang-format -i
    printf "Applied clang-format\n"
}

function check_tool() {
    local tool_name=$1
    local message=$2
    printf "Checking for ${tool_name}..."
    command -v "${tool_name}" >/dev/null ||
        (printf "${RED}failed${RESET}.\n${RED}ERROR${RESET} - could not locate tool '${tool_name}'. ${message}\n" && exit 1)
    printf "${GREEN}ok${RESET}\n"
}

function determine_ninja() {
    local os_name=$(uname -s)
    if [[ "${os_name}" == "Darwin" ]]; then
        # ninja is can be installed via "brew"
        echo "ninja"
    else
        # Check for ninja. On RedHat based Linux, it is called ninja-build, while on Debian based Linux, it is simply ninja
        # Ubuntu / Mint et al will report "ID_LIKE=debian"
        local debian_output=$(cat /etc/*-release | grep -i debian | wc -l)
        if [ ${debian_output} -gt 0 ]; then
            echo "ninja"
        else
            echo "ninja-build"
        fi
    fi
}

function check_tools() {
    local tools="cmake g++ gcc"
    for tool in $tools; do
        check_tool ${tool}
    done

    local build_tool=$(determine_ninja)
    if [[ "${BUILD_TOOL}" =~ make ]]; then
        build_tool="make"
    fi
    check_tool ${build_tool}
}

function check_and_clean_on_branch_change() {
    local current_branch=$(git -C "${ROOT_DIR}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
    local branch_stamp="${BUILD_DIR}/.last_build_branch"
    if [ -d "${BUILD_DIR}" ] && [ -f "${branch_stamp}" ] && [ "$(cat "${branch_stamp}")" != "${current_branch}" ]; then
        printf "${BOLD_PINK}Notice: Branch changed from $(cat "${branch_stamp}") to ${current_branch}. Cleaning stale protobuf artifacts...${RESET}\n"
        rm -f "${BUILD_DIR}"/src/*.pb.* 2>/dev/null || true
    fi
    mkdir -p "${BUILD_DIR}"
    echo "${current_branch}" > "${branch_stamp}"
}

# If any of the CMake files is newer than our "build.ninja" file, force "cmake" before building
function is_configure_required() {
    if [[ "${BUILD_TOOL}" =~ ninja ]]; then
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
    local cmake_files=$(find "${ROOT_DIR}" -name "CMakeLists.txt" -o -name "*.cmake" | grep -v "\.build-")
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

if [[ "${CMAKE_GENERATOR}" == "Ninja" ]]; then
  # Using Ninja (the default)
  BUILD_TOOL=$(determine_ninja)
else
  # Using Makefile
  BUILD_TOOL="make -j$(num_proc)"
fi

# Ensure cleanup runs on exit
trap cleanup EXIT
export CMAKE_POLICY_VERSION_MINIMUM=3.5

if [[ "${FORMAT}" == "yes" ]]; then
    format
fi

BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}${BUILD_DIR_SUFFIX:-}
if [[ "${SAN_BUILD}" != "no" ]]; then
    printf "${BOLD_PINK}${SAN_BUILD} sanitizer build is enabled${RESET}\n"
    if [[ "${SAN_BUILD}" == "address" ]]; then
        BUILD_DIR=${BUILD_DIR}-asan
    else
        BUILD_DIR=${BUILD_DIR}-tsan
    fi
fi

check_and_clean_on_branch_change

printf "Checking if configure is required..."

FORCE_CMAKE=$(is_configure_required)
printf "${GREEN}${FORCE_CMAKE}${RESET}\n"
check_tools

START_TIME=$(date +%s)

# Build ICU dependencies before configuring cmake
build_icu_if_needed

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

if [ -n "${RUN_TEST}" ]; then
    test_filter=""
    if [[ "${RUN_TEST}" != "all" ]]; then
        test_filter="-R ${RUN_TEST}"
    fi
    test_jobs=${JOBS:-$(num_proc)}
    printf "${BOLD_PINK}Running unit tests (-j ${test_jobs})...${RESET}\n"
    set -o pipefail
    if ! GTEST_COLOR=yes CLICOLOR_FORCE=1 ctest --test-dir "${BUILD_DIR}" ${test_filter} -j ${test_jobs} --output-on-failure 2>&1 | tee "${BUILD_DIR}/tests.out"; then
        EXIT_CODE=1
        if [ -f "${BUILD_DIR}/Testing/Temporary/LastTest.log" ]; then
            sed -i -r "s/\x1B\[[0-9;]*[a-zA-Z]//g" "${BUILD_DIR}/Testing/Temporary/LastTest.log"
            printf "\n${RED}======================= FAILED TEST DETAILS =======================${RESET}\n"
            grep -E -B 2 -A 25 -i "(: Failure|==[0-9]+==ERROR: |SUMMARY: .*Sanitizer|\[  FAILED  \]|Check failed:|Assertion \`.*\' failed)" "${BUILD_DIR}/Testing/Temporary/LastTest.log" || true
            printf "${RED}===================================================================${RESET}\n"
        fi
    elif [ -f "${BUILD_DIR}/Testing/Temporary/LastTest.log" ]; then
        sed -i -r "s/\x1B\[[0-9;]*[a-zA-Z]//g" "${BUILD_DIR}/Testing/Temporary/LastTest.log"
    fi
    if [ -f "${BUILD_DIR}/tests.out" ]; then
        sed -i -r "s/\x1B\[[0-9;]*[a-zA-Z]//g" "${BUILD_DIR}/tests.out"
    fi
    printf "\n${BLUE}Test output can be found in:${RESET} ${BUILD_DIR}/tests.out\n"
    printf "${BLUE}Detailed CTest logs can be found in:${RESET} ${BUILD_DIR}/Testing/Temporary/LastTest.log\n\n"
elif [[ "${INTEGRATION_TEST}" == "yes" ]]; then
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

    if [ ! -z "${TEST_PATTERN}" ]; then
        echo ""
        LOG_WARNING " ** TEST_PATTERN is found, skipping Abseil based integration tests **"
        echo ""
    else
        # Abseil based tests do not support filtering tests based on "-k" flag
        # so when the TEST_PATTERN env variable is found, skip Abseil based tests
        pushd testing/integration >/dev/null
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
    export MODULE_PATH=${BUILD_DIR}/libsearch.${MODULE_EXT}
    # Run will run ASan or normal tests based on the environment variable SAN_BUILD
    ./run.sh ${params} || EXIT_CODE=1
    popd >/dev/null
fi

END_TIME=$(date +%s)
TEST_RUNTIME=$((END_TIME - START_TIME))
exit ${EXIT_CODE}
