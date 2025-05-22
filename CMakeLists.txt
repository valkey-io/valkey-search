cmake_minimum_required(VERSION 3.16)

# Project definition

project(ValkeySearch VERSION 1.0.0 LANGUAGES C CXX)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/Modules/")

# Options
option(BUILD_TESTS "Build valkey-search tests" ON)
option(WITH_SUBMODULES_SYSTEM "Use system submodules" OFF)
option(ASAN_BUILD "Enable address sanitizer build" OFF)

# Initialise submodules (gRPC, Protobuf, GTest & Abseil)
add_subdirectory(submodules)

if(ASAN_BUILD)
    message(STATUS "Address sanitizer is enabled")
endif()

message(STATUS "GRPC_CPP_PLUGIN_PATH is set to ${GRPC_CPP_PLUGIN_PATH}")
message(STATUS "protoc_EXE is set to ${protoc_EXE}")
message(STATUS "highwayhash include path: ${HIGHWAY_HASH_INCLUDE_PATH}")

set(CMAKE_CXX_STANDARD 20 REQUIRED)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR}/.deps/install/include)

include(valkey_search)

add_subdirectory(vmsdk)
add_subdirectory(third_party)
add_subdirectory(src)

if(BUILD_TESTS)
  message(STATUS "Building tests")
  add_subdirectory(testing)
endif()

# Create a symbolic link to the root directory for compile_commands.json
execute_process(
  COMMAND
    ${CMAKE_COMMAND} -E create_symlink
    ${CMAKE_BINARY_DIR}/compile_commands.json
    ${CMAKE_SOURCE_DIR}/compile_commands.json)
