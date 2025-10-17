#!/bin/bash -e

CI_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$(readlink -f ${CI_DIR}/..)
BUILD_SH_ARGS=$@
WGET="wget -q"
HOSTADDR="https://github.com/valkey-io/valkey-search/releases/download/1.0.0-rc1"

# Constants
RESET='\e[0m'
GREEN='\e[32;1m'
RED='\e[31;1m'

san_suffix=""
## Search for --asan/--tsan
while [ $# -gt 0 ]
do
    arg=$1
    case $arg in
    --asan)
        SAN_BUILD="address"
        shift || true
        san_suffix="-asan"
        echo "Building with ASAN enabled"
        ;;
    --tsan)
        SAN_BUILD="thread"
        shift || true
        san_suffix="-tsan"
        echo "Building with TSAN enabled"
        ;;
    *)
        shift || true
        ;;
    esac
done


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
    echo valkey-search-deps-${DISTRO}${san_suffix}-${ARCH}.deb
}

function download_deb() {
    local deb_package=$1
    LOG_INFO "Downloading ${HOSTADDR}/${deb_package}"
    ${WGET} ${HOSTADDR}/${deb_package} -O ${ROOT_DIR}/debs/${deb_package}
}

# Prepare the environment before getting started
function prepare_env() {
    local deb_package=$(get_deb_suffix)
    if [ ! -d /opt/valkey-search-deps${san_suffix}/ ]; then
        # Fetch the deb from github
        download_deb ${deb_package}
        LOG_INFO "Installing ${ROOT_DIR}/debs/${deb_package}"
        sudo dpkg -i ${ROOT_DIR}/debs/${deb_package}
    else
        LOG_INFO "Debian file: '${deb_package}' is already installed"
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
    local DEPS_DIR=/opt/valkey-search-deps${san_suffix}
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
echo It ran stuff > /workspace/test-results/x.y