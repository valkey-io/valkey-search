# VMSDK common test library - shared dependencies for all vmsdk test suites
add_library(vmsdk_testing_common INTERFACE)
target_include_directories(vmsdk_testing_common
                           INTERFACE ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vmsdk_testing_common INTERFACE vmsdklib)
target_link_libraries(vmsdk_testing_common INTERFACE valkey_module)
target_link_libraries(vmsdk_testing_common INTERFACE vmsdk_testing_infra)

# 1. VMSDK Core Test Suite - tests requiring vmsdklib + valkey_module +
# vmsdk_testing_infra
set(VMSDK_CORE_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/blocked_client_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/cluster_map_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/command_parser_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/concurrency_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/configuration_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/log_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/memory_allocation_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/module_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/module_type_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils_test.cc)

add_executable(vmsdk_core_test ${VMSDK_CORE_TEST_SOURCES})
target_include_directories(vmsdk_core_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vmsdk_core_test PRIVATE vmsdk_testing_common)
finalize_test_flags(vmsdk_core_test)

# 2. VMSDK Infra Test Suite - lightweight tests requiring only vmsdklib
set(VMSDK_INFRA_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/thread_pool_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/mrmw_mutex_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/status_macros_test.cc)

add_executable(vmsdk_infra_test ${VMSDK_INFRA_TEST_SOURCES})
target_include_directories(vmsdk_infra_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(vmsdk_infra_test PRIVATE vmsdk_testing_common)
finalize_test_flags(vmsdk_infra_test)

# Benchmark (conditional - excluded during sanitizer builds)
string(TOLOWER "$ENV{SAN_BUILD}" SAN_BUILD_LOWER)
if("${SAN_BUILD_LOWER}" STREQUAL "no")
  set(SRCS_SHARDED_ATOMIC_BENCH
      ${CMAKE_CURRENT_LIST_DIR}/sharded_atomic_benchmark.cc)
  add_executable(sharded_atomic_benchmark ${SRCS_SHARDED_ATOMIC_BENCH})
  target_include_directories(sharded_atomic_benchmark
                             PUBLIC ${CMAKE_CURRENT_LIST_DIR})
  target_link_libraries(sharded_atomic_benchmark PRIVATE vmsdk_testing_common)
  target_link_libraries(sharded_atomic_benchmark
                        PRIVATE benchmark::benchmark)
  target_link_libraries(sharded_atomic_benchmark
                        PRIVATE benchmark::benchmark_main)
  finalize_test_flags(sharded_atomic_benchmark)
endif()
