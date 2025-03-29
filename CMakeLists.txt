cmake_minimum_required(VERSION 3.16)

# Project definition
project(ValkeySearch VERSION 99.99.99 LANGUAGES C CXX) # Version is 99.99.99 for main development branch

# Options
option(BUILD_TESTS "Build valkey-search tests" ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

set(CMAKE_CXX_STANDARD 20 REQUIRED)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR}/.deps/install/include)

add_subdirectory(submodules)
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
