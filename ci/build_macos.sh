#!/bin/bash -e
# Generated with Claude Code (https://claude.com/claude-code)

CI_DIR=$(readlink -f $(dirname $0))
ROOT_DIR=$(readlink -f ${CI_DIR}/..)
BUILD_SH_ARGS=$@

DEPS_DIR="${VALKEY_SEARCH_DEPS_DIR:-$HOME/valkey-search-deps}"
HIGHWAYHASH_REF="f8381f3331d9c56a9792f9b4a35f61c41108c39e"  # google/highwayhash master HEAD as of 2026-07-13

RESET='\e[0m'
GREEN='\e[32;1m'
RED='\e[31;1m'

function LOG_INFO() {
    printf "${GREEN}INFO ${RESET} $1\n"
}

function LOG_ERROR() {
    printf "${RED}ERROR${RESET} $1\n"
}

function install_brew_deps() {
    LOG_INFO "Installing Homebrew dependencies"
    brew install grpc protobuf abseil icu4c googletest google-benchmark
}

function build_highwayhash() {
    if [ -f "${DEPS_DIR}/include/highwayhash/highwayhash.h" ]; then
        LOG_INFO "highwayhash already installed at ${DEPS_DIR}, skipping"
        return
    fi
    LOG_INFO "Building highwayhash from source into ${DEPS_DIR}"
    local src_dir=$(mktemp -d)
    git clone https://github.com/google/highwayhash.git "${src_dir}"
    git -C "${src_dir}" checkout "${HIGHWAYHASH_REF}"
    cmake -S "${src_dir}" -B "${src_dir}/build" -GNinja -DCMAKE_BUILD_TYPE=Release
    cmake --build "${src_dir}/build"
    mkdir -p "${DEPS_DIR}/lib" "${DEPS_DIR}/include"
    cp "${src_dir}/build/libhighwayhash.a" "${DEPS_DIR}/lib/"
    cp -r "${src_dir}/highwayhash" "${DEPS_DIR}/include/"
    rm -rf "${src_dir}"
}

function prepare_env() {
    install_brew_deps
    build_highwayhash
}

function build_and_run() {
    export CMAKE_PREFIX_PATH="$(brew --prefix):$(brew --prefix icu4c):${DEPS_DIR}"
    LOG_INFO "CMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}"
    (cd ${ROOT_DIR} && ./build.sh ${BUILD_SH_ARGS})
}

cd ${CI_DIR}
prepare_env
build_and_run
