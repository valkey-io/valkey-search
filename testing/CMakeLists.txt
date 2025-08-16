# Base common test library - used by all test suites
set(SRCS_COMMON ${CMAKE_CURRENT_LIST_DIR}/common.cc
                ${CMAKE_CURRENT_LIST_DIR}/common.h)

add_library(testing_common_base STATIC ${SRCS_COMMON})
valkey_search_target_update_san_flags(testing_common_base)

target_include_directories(testing_common_base PUBLIC ${HIGHWAY_HASH_INCLUDE_PATH})
target_include_directories(testing_common_base PUBLIC ${CMAKE_CURRENT_LIST_DIR})
# Core libraries used by most test suites
target_link_libraries(testing_common_base PUBLIC attribute_data_type)
target_link_libraries(testing_common_base PUBLIC index_schema)
target_link_libraries(testing_common_base PUBLIC keyspace_event_manager)
target_link_libraries(testing_common_base PUBLIC schema_manager)
target_link_libraries(testing_common_base PUBLIC valkey_search)
target_link_libraries(testing_common_base PUBLIC vector_externalizer)
target_link_libraries(testing_common_base PUBLIC metadata_manager)
target_link_libraries(testing_common_base PUBLIC vector_base)
target_link_libraries(testing_common_base PUBLIC vector_hnsw)
target_link_libraries(testing_common_base PUBLIC search)
target_link_libraries(testing_common_base PUBLIC string_interning)
target_link_libraries(testing_common_base PUBLIC index_schema_cc_proto)
target_link_libraries(testing_common_base PUBLIC blocked_client)
target_link_libraries(testing_common_base PUBLIC numeric)
target_link_libraries(testing_common_base PUBLIC tag)
target_link_libraries(testing_common_base PUBLIC vector_flat)
target_link_libraries(testing_common_base PUBLIC predicate)
target_link_libraries(testing_common_base PUBLIC log)
target_link_libraries(testing_common_base PUBLIC index_base)
target_link_libraries(testing_common_base PUBLIC filter_parser)
target_link_libraries(testing_common_base PUBLIC allocator)
target_link_libraries(testing_common_base PUBLIC intrusive_ref_count)
target_link_libraries(testing_common_base PUBLIC patricia_tree)
target_link_libraries(testing_common_base PUBLIC server_events)
target_link_libraries(testing_common_base PUBLIC rdb_serialization)
target_link_libraries(testing_common_base PUBLIC iostream)
target_link_libraries(testing_common_base PUBLIC managed_pointers)
target_link_libraries(testing_common_base PUBLIC thread_pool)
target_link_libraries(testing_common_base PUBLIC type_conversions)
target_link_libraries(testing_common_base PUBLIC status_macros)
target_link_libraries(testing_common_base PUBLIC module)
target_link_libraries(testing_common_base PUBLIC utils)
target_link_libraries(testing_common_base PUBLIC valkey_module)
target_link_libraries(testing_common_base PUBLIC vmsdk_testing_infra)

# Coordinator common library - used by tests that need coordinator functionality
add_library(testing_common_coordinator INTERFACE)
target_link_libraries(testing_common_coordinator INTERFACE coordinator_common)
target_link_libraries(testing_common_coordinator INTERFACE coordinator_cc_proto)
target_link_libraries(testing_common_coordinator INTERFACE util)
target_link_libraries(testing_common_coordinator INTERFACE client)
target_link_libraries(testing_common_coordinator INTERFACE client_pool)

# 1. Commands Test Suite - consolidates FT command related tests
set(COMMANDS_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/ft_create_parser_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_search_parser_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_search_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_dropindex_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_list_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_info_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/ft_create_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/filter_test.cc
)

add_executable(commands_test ${COMMANDS_TEST_SOURCES})
target_include_directories(commands_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(commands_test PRIVATE testing_common_base)
target_link_libraries(commands_test PRIVATE testing_common_coordinator)
target_link_libraries(commands_test PRIVATE ft_create_parser)
target_link_libraries(commands_test PRIVATE ft_search_parser)
target_link_libraries(commands_test PRIVATE commands)
target_link_libraries(commands_test PRIVATE latency_sampler)
finalize_test_flags(commands_test)

# 2. Indexes Test Suite - consolidates index-related tests
set(INDEXES_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/index_schema_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/numeric_index_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/posting_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/tag_index_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/vector_test.cc
)

add_executable(abc_indexes_test ${INDEXES_TEST_SOURCES})
target_include_directories(abc_indexes_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(abc_indexes_test PRIVATE testing_common_base)
target_link_libraries(abc_indexes_test PRIVATE text)
target_link_libraries(abc_indexes_test PRIVATE hnswlib_vmsdk)
finalize_test_flags(abc_indexes_test)

# 3. Core Test Suite - consolidates core functionality tests
set(CORE_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/valkey_search_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/schema_manager_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/keyspace_event_manager_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/server_events_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/attribute_data_type_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/multi_exec_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/vector_externalizer_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/acl_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/rdb_serialization_test.cc
)

add_executable(core_test ${CORE_TEST_SOURCES})
target_include_directories(core_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(core_test PRIVATE testing_common_base)
target_link_libraries(core_test PRIVATE testing_common_coordinator)
target_link_libraries(core_test PRIVATE metrics)
target_link_libraries(core_test PRIVATE commands)
target_link_libraries(core_test PRIVATE acl)
target_link_libraries(core_test PRIVATE concurrency)
finalize_test_flags(core_test)

# 4. Query Test Suite - consolidates query and search related tests
set(QUERY_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/search_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/query/fanout_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/query/response_generator_test.cc
)

add_executable(query_test ${QUERY_TEST_SOURCES})
target_include_directories(query_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(query_test PUBLIC ${CMAKE_CURRENT_LIST_DIR}/query)
target_link_libraries(query_test PRIVATE testing_common_base)
target_link_libraries(query_test PRIVATE testing_common_coordinator)
target_link_libraries(query_test PRIVATE fanout)
target_link_libraries(query_test PRIVATE response_generator)
target_link_libraries(query_test PRIVATE search_converter)
finalize_test_flags(query_test)

# 5. Coordinator Test Suite - consolidates coordinator related tests
set(COORDINATOR_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/coordinator/metadata_manager_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/coordinator/client_test.cc
)

add_executable(coordinator_test ${COORDINATOR_TEST_SOURCES})
target_include_directories(coordinator_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(coordinator_test PUBLIC ${CMAKE_CURRENT_LIST_DIR}/coordinator)
target_link_libraries(coordinator_test PRIVATE testing_common_base)
target_link_libraries(coordinator_test PRIVATE testing_common_coordinator)
finalize_test_flags(coordinator_test)

# Create coordinator_common interface library (used by coordinator tests)
set(SRCS_COORDINATOR_COMMON coordinator/common.h)

add_library(coordinator_common INTERFACE)
target_include_directories(coordinator_common INTERFACE ${CMAKE_CURRENT_LIST_DIR}/coordinator)
target_link_libraries(coordinator_common INTERFACE client)
target_link_libraries(coordinator_common INTERFACE client_pool)
target_link_libraries(coordinator_common INTERFACE server)

# 6. Utils Test Suite - consolidates utility tests
set(UTILS_TEST_SOURCES
    ${CMAKE_CURRENT_LIST_DIR}/utils/allocator_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/intrusive_list_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/intrusive_ref_count_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/lru_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/patricia_tree_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/segment_tree_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/utils/string_interning_test.cc
)

add_executable(valkey_utils_test ${UTILS_TEST_SOURCES})
target_include_directories(valkey_utils_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_include_directories(valkey_utils_test PUBLIC ${CMAKE_CURRENT_LIST_DIR}/utils)
target_link_libraries(valkey_utils_test PRIVATE testing_common_base)
target_link_libraries(valkey_utils_test PRIVATE intrusive_list)
target_link_libraries(valkey_utils_test PRIVATE lru)
target_link_libraries(valkey_utils_test PRIVATE segment_tree)
finalize_test_flags(valkey_utils_test)

# 7. Text Index Test Suite
add_executable(text_index_test 
    ${CMAKE_CURRENT_LIST_DIR}/radix_test.cc
    ${CMAKE_CURRENT_LIST_DIR}/text_index_schema_test.cc)
target_include_directories(text_index_test PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(text_index_test PRIVATE testing_common_base)
target_link_libraries(text_index_test PRIVATE text)
finalize_test_flags(text_index_test)
