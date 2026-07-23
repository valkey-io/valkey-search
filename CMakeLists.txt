cmake_minimum_required(VERSION 3.16)

project(
    ValkeySearch
    VERSION 1.0.0
    LANGUAGES C CXX
)

option(BUILD_UNIT_TESTS "Build valkey-search unit tests" ON)
option(VALKEY_VENDORED_DEPS "Use static vendored dependencies" OFF)

# In vendored (static) dependency mode, every test executable statically
# links the same copies of protobuf/gRPC/absl/ICU, which multiplies link
# time and disk usage by the number of test executables. Default to a
# single combined test executable there to avoid that cost.
option(UNIT_TESTS_SINGLE_EXE "Build all unit tests into a single executable instead of one per suite" ${VALKEY_VENDORED_DEPS})

set(SAN_BUILD "OFF" CACHE STRING "Build with sanitizers (OFF, address, thread, memory, undefined)")

# Sanitizer builds pass -fno-lto, but CMake's native INTERPROCEDURAL_OPTIMIZATION
# target property (used by third_party/snowball, and potentially other vendored
# components that opt into LTO for performance) doesn't know that:
# check_ipo_supported() only probes generic toolchain LTO support, so it stays
# "enabled" even under -fno-lto. The result: CMake correctly notices -fno-lto
# and skips auto-adding -flto to whatever links against an IPO-enabled target,
# but that target still emits slim (LTO-bitcode-only, no real-code fallback)
# objects -- which nothing without -flto can then link, failing with "plugin
# needed to handle lto object". Disabling the default here means any component
# that wants IPO must check this variable rather than deciding for itself.
if (SAN_BUILD)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION OFF)
endif ()

# this is a built-in CMake variable.. but let's prefer SHARED_LIBS unless specifically disabled..
set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries by default")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# @TODO: these are only here for testing purposes.. should be removed.
add_compile_options(-Wno-defaulted-function-deleted)
add_compile_options(-Wno-deprecated-declarations)
add_compile_options(-Wno-unknown-pragmas)
add_compile_definitions(-DTESTING_TMP_DISABLED)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# Shared root for generated protobuf/grpc sources consumed across subdirectories.
set(VALKEY_PROTOGEN_BINARY_DIR "${CMAKE_BINARY_DIR}/protogen")

if (VALKEY_VENDORED_DEPS)
    set(gRPC_USE_STATIC_LIBS ON)
    set(Protobuf_USE_STATIC_LIBS ON)
    set(absl_USE_STATIC_LIBS ON)
    set(ICU_USE_STATIC_LIBS ON)
    add_compile_definitions(U_STATIC_IMPLEMENTATION)
endif()

# Find Abseil, gRPC, etc
# for some reason, if we're using vendored dependencies, the
# search order for gRPC and Protobuf need to be reversed

# grpc depends on protobuf (not the other way around), but some versions of gRPC
# expect protobuf to already be installed, while some versions of protobuf complain
# if gRPC is already found as a package. To solve this, we first search for gRPC.
# if it's found then we assume protobuf is also found; otherwise we assume that
# gRPC requires protobuf and we try that route instead.

# use CONFIG to find the CMake files from gRPC rather than the built-in
# cmake "MODULE" mode which uses CMake's own helpers (which might be stale).
# We can set 'CMAKE_FIND_PACKAGE_PREFER_CONFIG' .. but this doesn't have
# similar behavior..

find_package(gRPC CONFIG QUIET)

if (NOT gRPC_FOUND)
    find_package(Protobuf CONFIG REQUIRED)
    find_package(gRPC CONFIG REQUIRED)
endif()

find_package(absl REQUIRED)
find_package(ICU REQUIRED COMPONENTS uc i18n data)


find_package(highwayhash REQUIRED)

include(${CMAKE_SOURCE_DIR}/protogen/protogen.cmake)
protogen_setup(
    ${VALKEY_PROTOGEN_BINARY_DIR}
    src/coordinator/coordinator
    src/rdb_section
    src/index_schema
    third_party/hnswlib/index
)

if (NOT TARGET highwayhash)
    add_library(highwayhash UNKNOWN IMPORTED)
    set_target_properties(highwayhash PROPERTIES
        IMPORTED_LOCATION "${HIGHWAYHASH_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${HIGHWAYHASH_INCLUDE_DIR}")
endif()

if (NOT TARGET highwayhash::highwayhash)
    add_library(highwayhash::highwayhash ALIAS highwayhash)
endif()

if (BUILD_UNIT_TESTS)
    find_package(GTest REQUIRED)
    find_package(benchmark REQUIRED)
    enable_testing()
    include(GoogleTest)
endif()

add_subdirectory(vmsdk)
add_subdirectory(third_party)
add_subdirectory(src)

add_dependencies(coordinator protogen)

if (BUILD_UNIT_TESTS)
    add_compile_definitions(VALKEYSEARCH_BUILD_TESTS)
    add_subdirectory(testing)
endif()

# Create a symbolic link to the root directory for compile_commands.json
execute_process(
        COMMAND
        ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_SOURCE_DIR}/compile_commands.json)
