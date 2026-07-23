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
SAN_COMPILE_FLAGS=""
SAN_LINKER_FLAGS=""
BUILD_DIR_ARG=""
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
    --build-dir[=PATH]                Use an alternate build directory.
    --run-tests                       Run all unit tests via CTest. Optionally, pass a ctest -R regex to
                                       filter: "--run-tests=<pattern>" (e.g. "SchemaManagerTest" or
                                       "SchemaManagerTest.SomeCase").
    --no-build                        By default, build.sh always triggers a build. This option disables this behavior.
    --test-errors-stdout              When a test fails, dump the captured tests output to stdout (ctest --output-on-failure).
    --run-integration-tests[=pattern] Run integration tests.
    --use-system-modules              Use system's installed gRPC, Protobuf & Abseil dependencies.
    --vendored                        Statically link gRPC, Protobuf & Abseil (VALKEY_VENDORED_DEPS=ON).
    --asan                            Build with address sanitizer enabled.
    --tsan                            Build with thread sanitizer enabled.
    --retries=N                       Attempt to run integration tests N times. Default is 1.
    --jobs=N                          Limit the build workers and CTest's parallel test jobs to N. Default: use all available cores for the build, run tests serially.

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
    --build-dir)
        shift || true
        if [ $# -eq 0 ]; then
            echo "Missing value for --build-dir"
            print_usage
            exit 1
        fi
        BUILD_DIR_ARG=$1
        shift || true
        echo "Using build directory ${BUILD_DIR_ARG}"
        ;;
    --build-dir=*)
        BUILD_DIR_ARG=${arg#*=}
        shift || true
        echo "Using build directory ${BUILD_DIR_ARG}"
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
    --vendored)
        CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DVALKEY_VENDORED_DEPS=ON"
        shift || true
        echo "Using extra cmake arguments: ${CMAKE_EXTRA_ARGS}"
        ;;
    --asan)
        CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DSAN_BUILD=address"
        SAN_BUILD="address"
        SAN_COMPILE_FLAGS="-O1 -fno-omit-frame-pointer -fsanitize=address -fno-lto -DSAN_BUILD=address"
        SAN_LINKER_FLAGS="-fsanitize=address"
        shift || true
        echo "Using extra cmake arguments: ${CMAKE_EXTRA_ARGS}"
        ;;
    --tsan)
        CMAKE_EXTRA_ARGS="${CMAKE_EXTRA_ARGS} -DSAN_BUILD=thread"
        SAN_BUILD="thread"
        SAN_COMPILE_FLAGS="-O1 -fno-omit-frame-pointer -fsanitize=thread -fno-lto -DSAN_BUILD=thread"
        SAN_LINKER_FLAGS="-fsanitize=thread"
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

# Compute the compiler/linker flags valkey-search is built with. These used to
# live in cmake/Modules/valkey_search.cmake as a set of per-target CMake
# functions; they now live here so build.sh can pass them to cmake as plain
# -DCMAKE_*_FLAGS arguments instead of CMakeLists.txt having to know about
# build configs/architectures/sanitizers.
#
# Debug-vs-Release selection is left entirely to CMake's own per-config flag
# variables (CMAKE_*_FLAGS_RELEASE / CMAKE_*_FLAGS_DEBUG, which CMake merges
# with the plain CMAKE_*_FLAGS based on CMAKE_BUILD_TYPE) rather than branched
# on here: we hand cmake static values for both configs and let it pick.
# Architecture/OS are still detected here via `case`, since that's platform
# detection rather than a build-configuration choice.
function compute_build_flags() {
    local arch_simd_flags=""
    case "$(uname -m)" in
    x86_64)
        arch_simd_flags="-mcx16 -msse4.2 -mpclmul -mavx -mavx2 -maes -mfma -mprfchw"
        ;;
    esac

    local openmp_flags="-fopenmp"
    case "${UNAME_S}" in
    Darwin)
        openmp_flags=""
        ;;
    esac

    # Always-on flags, merged by CMake with the _RELEASE/_DEBUG variants below.
    # SAN_COMPILE_FLAGS/SAN_LINKER_FLAGS are set directly at the --asan/--tsan
    # argument-parsing sites (empty when no sanitizer was requested).
    local common_flags="-falign-functions=5 -fmath-errno -ffp-contract=off -fno-rounding-math ${arch_simd_flags} -mtune=generic -gdwarf-5 -gz=zlib -ffast-math -funroll-loops -ftree-vectorize ${openmp_flags} -flax-vector-conversions -Wno-unknown-pragmas -Wno-sign-compare -Wno-uninitialized -DTESTING_TMP_DISABLED ${SAN_COMPILE_FLAGS}"

    VALKEY_SEARCH_C_FLAGS="${common_flags}"
    VALKEY_SEARCH_CXX_FLAGS="${common_flags}"

    # Passing -DCMAKE_{C,CXX}_FLAGS_RELEASE on the cmake command line replaces
    # CMake's own default for that cache variable (normally "-O3 -DNDEBUG")
    # rather than appending to it, so we have to reproduce it explicitly here.
    VALKEY_SEARCH_C_FLAGS_RELEASE="-O3 -DNDEBUG -ffile-prefix-map=${ROOT_DIR}= -ffat-lto-objects"
    VALKEY_SEARCH_CXX_FLAGS_RELEASE="${VALKEY_SEARCH_C_FLAGS_RELEASE}"

    VALKEY_SEARCH_C_FLAGS_DEBUG="-O0 -fno-omit-frame-pointer -fno-lto"
    VALKEY_SEARCH_CXX_FLAGS_DEBUG="${VALKEY_SEARCH_C_FLAGS_DEBUG}"

    VALKEY_SEARCH_SHARED_LINKER_FLAGS="${SAN_LINKER_FLAGS}"
    VALKEY_SEARCH_MODULE_LINKER_FLAGS="${SAN_LINKER_FLAGS}"

    # Emit fat LTO objects at compile time (above) and defer the actual LTO
    # optimization to link time, only for Release builds.
    VALKEY_SEARCH_SHARED_LINKER_FLAGS_RELEASE="-flto"
    VALKEY_SEARCH_MODULE_LINKER_FLAGS_RELEASE="-flto"
}

function configure() {
    printf "${BOLD_PINK}Running cmake...${RESET}\n"
    printf "Generating ${GREEN}${CMAKE_GENERATOR}${RESET} build files\n"
    local BUILD_TYPE=$(capitalize_string ${BUILD_CONFIG})
    mkdir -p "${BUILD_DIR}"
    rm -f "${BUILD_DIR}/CMakeCache.txt"
    compute_build_flags
    printf "Running: cmake -S %s -B %s -DCMAKE_BUILD_TYPE=%s -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DBUILD_UNIT_TESTS=ON -DCMAKE_C_FLAGS=\"%s\" -DCMAKE_CXX_FLAGS=\"%s\" -DCMAKE_C_FLAGS_RELEASE=\"%s\" -DCMAKE_CXX_FLAGS_RELEASE=\"%s\" -DCMAKE_C_FLAGS_DEBUG=\"%s\" -DCMAKE_CXX_FLAGS_DEBUG=\"%s\" -DCMAKE_SHARED_LINKER_FLAGS=\"%s\" -DCMAKE_MODULE_LINKER_FLAGS=\"%s\" -DCMAKE_SHARED_LINKER_FLAGS_RELEASE=\"%s\" -DCMAKE_MODULE_LINKER_FLAGS_RELEASE=\"%s\" -Wno-dev -G\"%s\" %s\n" \
        "${ROOT_DIR}" "${BUILD_DIR}" "${BUILD_TYPE}" \
        "${VALKEY_SEARCH_C_FLAGS}" "${VALKEY_SEARCH_CXX_FLAGS}" \
        "${VALKEY_SEARCH_C_FLAGS_RELEASE}" "${VALKEY_SEARCH_CXX_FLAGS_RELEASE}" \
        "${VALKEY_SEARCH_C_FLAGS_DEBUG}" "${VALKEY_SEARCH_CXX_FLAGS_DEBUG}" \
        "${VALKEY_SEARCH_SHARED_LINKER_FLAGS}" "${VALKEY_SEARCH_MODULE_LINKER_FLAGS}" \
        "${VALKEY_SEARCH_SHARED_LINKER_FLAGS_RELEASE}" "${VALKEY_SEARCH_MODULE_LINKER_FLAGS_RELEASE}" \
        "${CMAKE_GENERATOR}" "${CMAKE_EXTRA_ARGS}"
    cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DBUILD_UNIT_TESTS=ON \
        -DCMAKE_C_FLAGS="${VALKEY_SEARCH_C_FLAGS}" \
        -DCMAKE_CXX_FLAGS="${VALKEY_SEARCH_CXX_FLAGS}" \
        -DCMAKE_C_FLAGS_RELEASE="${VALKEY_SEARCH_C_FLAGS_RELEASE}" \
        -DCMAKE_CXX_FLAGS_RELEASE="${VALKEY_SEARCH_CXX_FLAGS_RELEASE}" \
        -DCMAKE_C_FLAGS_DEBUG="${VALKEY_SEARCH_C_FLAGS_DEBUG}" \
        -DCMAKE_CXX_FLAGS_DEBUG="${VALKEY_SEARCH_CXX_FLAGS_DEBUG}" \
        -DCMAKE_SHARED_LINKER_FLAGS="${VALKEY_SEARCH_SHARED_LINKER_FLAGS}" \
        -DCMAKE_MODULE_LINKER_FLAGS="${VALKEY_SEARCH_MODULE_LINKER_FLAGS}" \
        -DCMAKE_SHARED_LINKER_FLAGS_RELEASE="${VALKEY_SEARCH_SHARED_LINKER_FLAGS_RELEASE}" \
        -DCMAKE_MODULE_LINKER_FLAGS_RELEASE="${VALKEY_SEARCH_MODULE_LINKER_FLAGS_RELEASE}" \
        -Wno-dev -G"${CMAKE_GENERATOR}" ${CMAKE_EXTRA_ARGS}
}

function build() {
    printf "${BOLD_PINK}Building${RESET}\n"
    if [ -d "${BUILD_DIR}" ]; then
        local build_args=()
        if [ -n "${JOBS}" ]; then
            build_args+=(--parallel "${JOBS}")
        fi
        if [ -n "${VERBOSE_ARGS}" ]; then
            build_args+=(--verbose)
        fi
        if [ -n "${CMAKE_TARGET}" ]; then
            build_args+=(--target "${CMAKE_TARGET}")
        fi
        cmake --build "${BUILD_DIR}" "${build_args[@]}"

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

function print_test_summary() {
    printf "${BLUE}CTest JUnit report:${RESET} ${BUILD_DIR}/test-results.xml\n"
    printf "${BLUE}Full test output:${RESET} ${BUILD_DIR}/Testing/Temporary/LastTest.log\n"
}

# Runs the unit tests registered with CTest (via gtest_discover_tests). $1,
# if non-empty, is passed to ctest's "-R" regex test filter.
function run_unit_tests() {
    local filter="$1"
    printf "${BOLD_PINK}Running unit tests (via CTest)${RESET}\n"

    local ctest_args=(--output-junit "test-results.xml")
    if [[ "${DUMP_TEST_ERRORS_STDOUT}" == "yes" ]]; then
        ctest_args+=(--output-on-failure)
    fi
    if [ -n "${JOBS}" ]; then
        ctest_args+=(--parallel "${JOBS}")
    fi
    if [ -n "${filter}" ]; then
        ctest_args+=(-R "${filter}")
    fi

    # CTest already runs every registered test and reports pass/fail for each
    # one (regardless of sanitizer build), so just propagate its exit code.
    # Run in a subshell so we don't disturb the caller's working directory.
    (cd "${BUILD_DIR}" && ctest "${ctest_args[@]}") || EXIT_CODE=1
    print_test_summary
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
    local tools="cmake ctest g++ gcc"
    for tool in $tools; do
        check_tool ${tool}
    done

    local build_tool=$(determine_ninja)
    if [[ "${BUILD_TOOL}" =~ make ]]; then
        build_tool="make"
    fi
    check_tool ${build_tool}
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
    local cmake_files=$(find "${ROOT_DIR}" \
        \( -path "${BUILD_DIR}" -o -path "${BUILD_DIR}/*" \) -prune -o \
        \( -name "CMakeLists.txt" -o -name "*.cmake" \) -print)
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

if [[ "${SAN_BUILD}" != "no" ]]; then
    printf "${BOLD_PINK}${SAN_BUILD} sanitizer build is enabled${RESET}\n"
    # Needed before the build step, not just the test-run step: CMake's default
    # gtest_discover_tests() runs each test binary's --gtest_list_tests as part
    # of the build itself, which would otherwise hit an ASan ODR-violation
    # false positive.
    export ASAN_OPTIONS="detect_odr_violation=0"
fi

if [ -n "${BUILD_DIR_ARG}" ]; then
    case "${BUILD_DIR_ARG}" in
        /*) BUILD_DIR="${BUILD_DIR_ARG}" ;;
        *) BUILD_DIR="$(pwd)/${BUILD_DIR_ARG}" ;;
    esac
else
    BUILD_DIR=${ROOT_DIR}/.build-${BUILD_CONFIG}
    if [[ "${SAN_BUILD}" == "address" ]]; then
        BUILD_DIR=${BUILD_DIR}-asan
    elif [[ "${SAN_BUILD}" == "thread" ]]; then
        BUILD_DIR=${BUILD_DIR}-tsan
    fi
fi

printf "Checking if configure is required..."

FORCE_CMAKE=$(is_configure_required)
printf "${GREEN}${FORCE_CMAKE}${RESET}\n"
check_tools

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

if [[ "${RUN_TEST}" == "all" ]]; then
    run_unit_tests ""
elif [ ! -z "${RUN_TEST}" ]; then
    run_unit_tests "${RUN_TEST}"
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
    ./run.sh || EXIT_CODE=1
    popd >/dev/null
fi

END_TIME=$(date +%s)
TEST_RUNTIME=$((END_TIME - START_TIME))
exit ${EXIT_CODE}
