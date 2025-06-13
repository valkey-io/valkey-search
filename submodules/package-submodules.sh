#!/bin/bash -e

## Search for --asan
while [ $# -gt 0 ]
do
    arg=$1
    case $arg in
    --asan)
        ASAN_BUILD="yes"
        shift || true
        ;;
    *)
        shift || true
        ;;
    esac
done

if [[ "${ASAN_BUILD}" == "yes" ]]; then
    asan_suffix="-asan"
    echo "Building with ASAN enabled"
fi


ROOT_DIR=$(readlink -f $(dirname $0)/..)
BUILD_DIR=$(readlink -f $(dirname $0))/.build-release${asan_suffix}
SRC_DIR=${BUILD_DIR}/.src
INSTALL_DIR=${BUILD_DIR}/install

mkdir -p ${SRC_DIR}
mkdir -p ${BUILD_DIR}
mkdir -p ${INSTALL_DIR}

cd ${BUILD_DIR}

function clone_repo() {
    local GIT_URL=$1
    local MODULE_NAME=$(basename ${GIT_URL})
    local GIT_BRANCH=$2
    cd ${SRC_DIR}
    if [ -d "${SRC_DIR}/${MODULE_NAME}" ]; then
        rm -fr "${SRC_DIR}/${MODULE_NAME}"
    fi
    git clone --branch=${GIT_BRANCH} \
        ${GIT_URL} ${MODULE_NAME}    \
        --recurse-submodules         \
        --shallow-submodules         \
        --depth=1                    \
        --single-branch
}

function build_submodule() {
    local MODULE_NAME=$1
    local MODULE_CMAKE_ARGS=$2
    local INSTALL=$3
    local CMAKE_BASE_ARGS="-Wno-dev -DCMAKE_BUILD_TYPE=Release -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX=${INSTALL_DIR} -DCMAKE_CXX_STANDARD=20"
    mkdir -p ${SRC_DIR}/${MODULE_NAME}/.build-release
    cd $_
    export CXXFLAGS="-Wno-missing-requires -Wno-attributes -Wno-deprecated -Wno-return-type -Wno-stringop-overflow -Wno-deprecated-declarations"
    if [[ "${ASAN_BUILD}" == "1" ]]; then
        echo "Passing ASAN flags"
        export CXXFLAGS="-fsanitize=address ${CXXFLAGS}"
        export CFLAGS="-fsanitize=address ${CFLAGS}"
        export LDFLAGS="-fsanitize=address ${LDFLAGS}"
    fi
    cmake .. -GNinja ${CMAKE_BASE_ARGS} ${MODULE_CMAKE_ARGS}
    ninja ${INSTALL}
}

# Build and install dependencies

if [ ! -f "${INSTALL_DIR}/include/absl/base/options.h" ]; then
    clone_repo "https://github.com/grpc/grpc" "v1.70.1"
    build_submodule "grpc" "-DSKIP_INSTALL_ALL=ON -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_SSL_PROVIDER=package" "install"
fi

if [ ! -f "${INSTALL_DIR}/include/gtest/gtest.h" ]; then
    clone_repo "https://github.com/google/googletest" "main"
    build_submodule "googletest" "" "install"
fi

if [ ! -f "${INSTALL_DIR}/include/highwayhash/highwayhash.h" ]; then
    clone_repo "https://github.com/google/highwayhash" "master"
    build_submodule "highwayhash" "" ""

    # Manually install highwayhash
    cp ${SRC_DIR}/highwayhash/.build-release/libhighwayhash.a ${INSTALL_DIR}/lib
    cp -fr ${SRC_DIR}/highwayhash/highwayhash ${INSTALL_DIR}/include
fi

# Pack everything into a deb file

function get_arch_spec() {
    local uname_m=$(uname -m)
    if [[ "${uname_m}" == "x86_64" ]]; then
        echo "amd64"
    else
        echo "arm64"
    fi
}

function get_deb_suffix() {
    local arch=$(get_arch_spec)
    if [ ! -f /usr/bin/lsb_release ]; then
        distro="linux"
    else
        codename=$(lsb_release -c|cut -d":" -f2)
        distro_id=$(lsb_release -i|cut -d":" -f2)
        codename=${codename#[$'\r\t\n ']}
        distro_id=${distro_id#[$'\r\t\n ']}
        distro=${distro_id}-${codename}
        distro=$(echo "$distro" | tr '[:upper:]' '[:lower:]')
    fi

    echo valkey-search-deps-${distro}${asan_suffix}-${arch}
}

ARCH=$(get_arch_spec)
DEB_NAME=$(get_deb_suffix)
DEB_ROOT=${BUILD_DIR}/${DEB_NAME}
rm -fr ${DEB_ROOT}
mkdir -p ${DEB_ROOT}/DEBIAN
mkdir -p ${DEB_ROOT}/opt/valkey-search-deps${asan_suffix}
cp -fr ${INSTALL_DIR}/* ${DEB_ROOT}/opt/valkey-search-deps${asan_suffix}
cat<<EOF > ${DEB_ROOT}/DEBIAN/control
Package: ${DEB_NAME}
Version: 1.0
Maintainer: eifrah@amazon.com
Architecture: ${ARCH}
Description: dependencies for building valkey-search
EOF

cd ${BUILD_DIR}
dpkg-deb --build ${DEB_NAME}

mkdir -p ${ROOT_DIR}/debs
mv ${BUILD_DIR}/${DEB_NAME}.deb ${ROOT_DIR}/debs
echo ""
echo ""
echo "Debian file generated: ${ROOT_DIR}/debs/${DEB_NAME}.deb"
echo ""
echo ""
