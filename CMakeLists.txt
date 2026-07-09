cmake_minimum_required(VERSION 3.16)

project(
    ValkeySearch
    VERSION 1.0.0
    LANGUAGES C CXX
)

option(BUILD_UNIT_TESTS "Build valkey-search unit tests" ON)
option(VALKEY_VENDORED_DEPS "Use static vendored dependencies" OFF)
set(SAN_BUILD "OFF" CACHE STRING "Build with sanitizers (OFF, address, thread, memory, undefined)")

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

find_package(gRPC QUIET)
if (NOT gRPC_FOUND)
    find_package(Protobuf REQUIRED)
    find_package(gRPC REQUIRED)
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
