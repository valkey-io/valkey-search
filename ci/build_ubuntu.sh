#!/bin/bash -e

CI_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$(readlink -f ${CI_DIR}/..)
BUILD_SH_ARGS=$@

# Constants
RESET='\e[0m'
GREEN='\e[32;1m'
RED='\e[31;1m'

function LOG_INFO() {
    printf "${GREEN}INFO ${RESET} $1\n"
}

function LOG_ERROR() {
    printf "${RED}ERROR${RESET} $1\n"
}

function get_deb_suffix() {
    local ARCH=""
    local uname_m=$(uname -m)
    if [[ "${uname_m}" == "x86_64" ]]; then
        ARCH="amd64"
    else
        ARCH="arm64"
    fi

    if [ ! -f /usr/bin/lsb_release ]; then
        DISTRO="linux"
    else
        CODENAME=$(lsb_release -c|cut -d":" -f2)
        DISTRO_ID=$(lsb_release -i|cut -d":" -f2)
        CODENAME=${CODENAME#[$'\r\t\n ']}
        DISTRO_ID=${DISTRO_ID#[$'\r\t\n ']}
        DISTRO=${DISTRO_ID}-${CODENAME}
        DISTRO=$(echo "$DISTRO" | tr '[:upper:]' '[:lower:]')
    fi
    echo valkey-search-deps-${DISTRO}-${ARCH}.deb
}

# Prepare the environment before getting started
function prepare_env() {
    if [ ! -d /opt/valkey-search-deps/ ]; then
        local deb_package=$(get_deb_suffix)
        LOG_INFO "Installing ${ROOT_DIR}/debs/${deb_package}"
        sudo dpkg -i ${ROOT_DIR}/debs/${deb_package}
    fi
}

function cleanup() {
    # This method is called just before the script exits
    local exit_code=$?
    LOG_INFO "Cleaning up before exit"

    if [[ $exit_code -ne 0 ]]; then
        LOG_ERROR "Script ended with error code ${exit_code}"
    else
        LOG_INFO "Script completed successfully"
    fi
}

function build_and_run_tests() {
    local DEPS_DIR=/opt/valkey-search-deps
    local CMAKE_DIR=${DEPS_DIR}/lib/cmake
    # Let CMake find <Package>-config.cmake files by updating the CMAKE_PREFIX_PATH variable
    export CMAKE_PREFIX_PATH=${CMAKE_DIR}/protobuf:${CMAKE_DIR}/absl:${CMAKE_DIR}/grpc:${CMAKE_DIR}/GTest:${CMAKE_DIR}/utf8_range:${DEPS_DIR}
    (cd ${ROOT_DIR} && ./build.sh --use-system-modules --test-errors-stdout ${BUILD_SH_ARGS})
}

# Write a success or error message on exit
trap cleanup EXIT

cd ${CI_DIR}

prepare_env
build_and_run_tests
