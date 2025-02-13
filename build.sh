#!/bin/bash -e

BUILD_CONFIG=debug
RUN_CMAKE="no"
ROOT_DIR=$(readlink -f $(dirname $0))
VERBOSE_ARGS=""
CMAKE_TARGET=""

echo "Root directory: ${ROOT_DIR}"

function print_usage() {
cat<<EOF
Usage: build.sh [options...]

    --help | -h         Print this help message and exit
    --cmake             Run cmake stage
    --verbose | -v      Run verbose build
    --release           Build for release
    --clean             Clean the current build configuration (debug or release)

Example usage:
    # run CMake for debug build & build
    build.sh --configure

    # run CMake for release build & build
    build.sh --release --configure
EOF
}

function configure() {
    mkdir -p ${ROOT_DIR}/build-${BUILD_CONFIG}
    cd $_
    local BUILD_TYPE=$(echo ${BUILD_CONFIG^})
    cmake .. -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DBUILD_TESTS=ON
    cd ${ROOT_DIR}
}

function build() {
    # If the build folder does not exist, run cmake
    if [ ! -d "${ROOT_DIR}/build-${BUILD_CONFIG}" ]; then
        configure
    fi
    cd ${ROOT_DIR}/build-${BUILD_CONFIG}
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
    --release)
        shift || true
        BUILD_CONFIG="release"
        echo "Building in Release mode"
        ;;
    --cmake)
        shift || true
        RUN_CMAKE="yes"
        echo "Running cmake: true"
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

if [[ "${RUN_CMAKE}" == "yes" ]]; then
    configure
fi
build
