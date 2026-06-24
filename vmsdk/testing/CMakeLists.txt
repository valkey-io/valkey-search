add_library(testing_common INTERFACE)
target_link_libraries(testing_common INTERFACE vmsdk_testing_infra)
target_link_libraries(testing_common INTERFACE vmsdk_dso)
target_include_directories(testing_common INTERFACE .)

set(SRCS_BLOCKED_CLIENT_TEST blocked_client_test.cc)
add_executable(blocked_client_test ${SRCS_BLOCKED_CLIENT_TEST})
target_link_libraries(blocked_client_test PUBLIC testing_common)

set(SRCS_COMMAND_PARSER_TEST command_parser_test.cc)
add_executable(command_parser_test ${SRCS_COMMAND_PARSER_TEST})
target_link_libraries(command_parser_test PUBLIC testing_common)

set(SRCS_CONCURRENCY_TEST concurrency_test.cc)
add_executable(concurrency_test ${SRCS_CONCURRENCY_TEST})
target_include_directories(concurrency_test PUBLIC .)
target_link_libraries(concurrency_test PUBLIC testing_common)

set(SRCS_MODULE_TYPE_TEST module_type_test.cc)
add_executable(module_type_test ${SRCS_MODULE_TYPE_TEST})
target_include_directories(module_type_test PUBLIC .)
target_link_libraries(module_type_test PUBLIC testing_common)

set(SRCS_MODULE_TEST module_test.cc)
add_executable(module_test ${SRCS_MODULE_TEST})
target_include_directories(module_test PUBLIC .)
target_link_libraries(module_test PUBLIC testing_common)

set(SRCS_THREAD_POOL_TEST thread_pool_test.cc)
add_executable(thread_pool_test ${SRCS_THREAD_POOL_TEST})
target_include_directories(thread_pool_test PUBLIC .)
target_link_libraries(thread_pool_test PUBLIC testing_common)

set(SRCS_TIME_SLICED_MRMW_MUTEX_TEST
    mrmw_mutex_test.cc)
add_executable(time_sliced_mrmw_mutex_test ${SRCS_TIME_SLICED_MRMW_MUTEX_TEST})
target_include_directories(time_sliced_mrmw_mutex_test
                           PUBLIC .)
target_link_libraries(time_sliced_mrmw_mutex_test PUBLIC testing_common)

set(SRCS_STATUS_MACROS_TEST status_macros_test.cc)
add_executable(status_macros_test ${SRCS_STATUS_MACROS_TEST})
target_include_directories(status_macros_test PUBLIC .)
target_link_libraries(status_macros_test PUBLIC testing_common)

set(SRCS_MEMORY_ALLOCATION_TEST
    memory_allocation_test.cc)
add_executable(memory_allocation_test ${SRCS_MEMORY_ALLOCATION_TEST})
target_include_directories(memory_allocation_test
                           PUBLIC .)
target_link_libraries(memory_allocation_test PUBLIC testing_common)

set(SRCS_UTILS_TEST utils_test.cc)
add_executable(utils_test ${SRCS_UTILS_TEST})
target_include_directories(utils_test PUBLIC .)
target_link_libraries(utils_test PUBLIC testing_common)

set(SRCS_LOG_TEST log_test.cc)
add_executable(log_test ${SRCS_LOG_TEST})
target_include_directories(log_test PUBLIC .)
target_link_libraries(log_test PUBLIC testing_common)

set(SRCS_CONFIG_TEST configuration_test.cc)
add_executable(config_test ${SRCS_CONFIG_TEST})
target_include_directories(config_test PUBLIC .)
target_link_libraries(config_test PUBLIC testing_common)

set(SRCS_CLUSTER_MAP_TEST cluster_map_test.cc)
add_executable(cluster_map_test ${SRCS_CLUSTER_MAP_TEST})
target_include_directories(cluster_map_test PUBLIC .)
target_link_libraries(cluster_map_test PUBLIC testing_common)

if(NOT SAN_BUILD)
    set(SRCS_SHARDED_ATOMIC_BENCH sharded_atomic_benchmark.cc)
    add_executable(sharded_atomic_benchmark ${SRCS_SHARDED_ATOMIC_BENCH})
    target_include_directories(sharded_atomic_benchmark PUBLIC .)
    target_link_libraries(sharded_atomic_benchmark PUBLIC testing_common)
endif()
