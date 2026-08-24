#!/bin/bash -e

# install_deps.sh
#
# Installs the system packages and C++ toolchain (GCC >= 12) required to build
# valkey-search from source. This is the single, maintained source of truth for
# "pre-build installation activities" — it is invoked by CI (the PR search
# benchmark workflow) and can be run directly by any developer preparing a fresh
# machine:
#
#     ./ci/install_deps.sh
#     ./build.sh
#
# The script is install-only: it does not build the module. After it completes,
# `./build.sh` is expected to succeed. It is idempotent and safe to re-run.
#
# NOTE: Only Amazon Linux 2023 is supported today. 

# Minimum GCC major version required to build valkey-search.
GCC_MIN_VERSION=12

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

# Install the compiler toolchain and development headers needed to build
# valkey-search (and the valkey-server / valkey-benchmark it is exercised
# against) on Amazon Linux 2023.
function install_amazon_linux() {
    LOG_INFO "Installing build dependencies via dnf..."
    sudo dnf groupinstall "Development Tools" -y
    sudo dnf install -y \
        gcc gcc-c++ cmake \
        ninja-build \
        python3-devel \
        openssl-devel \
        systemd-devel \
        bzip2-devel \
        libffi-devel \
        perf

    ensure_gcc_amazon_linux
}

# valkey-search requires GCC >= 12
function ensure_gcc_amazon_linux() {
    local current_version=0
    if /usr/local/bin/gcc --version >/dev/null 2>&1; then
        current_version=$(/usr/local/bin/gcc -dumpversion | cut -d. -f1)
    fi

    if [[ "${current_version}" -ge "${GCC_MIN_VERSION}" ]]; then
        LOG_INFO "✓ GCC ${current_version} already available at /usr/local/bin/gcc (>= ${GCC_MIN_VERSION})"
    else
        LOG_INFO "GCC >= ${GCC_MIN_VERSION} not found (current: ${current_version}) — installing GCC 14 from AL2023 repos..."
        sudo dnf install -y gcc14 gcc14-c++

        # Create symlinks in /usr/local/bin so builds pick up GCC 14 by default.
        sudo ln -sf /usr/bin/gcc14-gcc /usr/local/bin/gcc
        sudo ln -sf /usr/bin/gcc14-g++ /usr/local/bin/g++
        sudo ln -sf /usr/bin/gcc14-gcc /usr/local/bin/cc
        sudo ln -sf /usr/bin/gcc14-g++ /usr/local/bin/c++
        LOG_INFO "✓ GCC 14 installed and symlinked into /usr/local/bin"
    fi

    /usr/local/bin/gcc --version
    /usr/local/bin/g++ --version
}

# Detect the distro and dispatch. Extend with additional cases (e.g. ubuntu)
# as more build environments are supported.
if [ -f /etc/os-release ]; then
    . /etc/os-release
else
    LOG_ERROR "Cannot detect OS: /etc/os-release not found"
    exit 1
fi

case "${ID}" in
    amzn)
        # ID=amzn covers both Amazon Linux 2 (VERSION_ID=2) and AL2023
        # (VERSION_ID=2023). Only AL2023 is supported; reject anything else
        # before we start installing AL2023-only packages (e.g. gcc14).
        if [[ "${VERSION_ID}" != "2023" ]]; then
            LOG_ERROR "Unsupported Amazon Linux version '${VERSION_ID}'. Only Amazon Linux 2023 is currently supported."
            exit 1
        fi
        install_amazon_linux
        ;;
    *)
        LOG_ERROR "Unsupported distro '${ID}'. Only Amazon Linux 2023 (amzn) is currently supported."
        LOG_ERROR "Add a case for '${ID}' in ci/install_deps.sh to support it."
        exit 1
        ;;
esac

LOG_INFO "Build dependencies installed. You can now run ./build.sh"
