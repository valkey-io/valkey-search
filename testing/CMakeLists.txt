# Base common test library - used by all test suites
set(SRCS_COMMON common.cc common.h)

add_library(testing_common_base SHARED ${SRCS_COMMON})
target_link_libraries(testing_common_base PUBLIC search_dso)
target_link_libraries(testing_common_base PUBLIC GTest::gtest_main GTest::gmock)
target_link_libraries(testing_common_base PUBLIC vmsdk_dso)
target_link_libraries(testing_common_base PUBLIC vmsdk_testing)

# 1. Commands Test Suite - consolidates FT command related tests
set(COMMANDS_TEST_SOURCES
    ft_aggregate_exec_test.cc
    ft_aggregate_parser_test.cc
    ft_create_parser_test.cc
    ft_search_parser_test.cc
    ft_search_test.cc
    ft_dropindex_test.cc
    ft_list_test.cc
    ft_info_test.cc
    ft_create_test.cc
    commands/ft_internal_update_test.cc
    filter_test.cc)

add_executable(commands_test ${COMMANDS_TEST_SOURCES})
target_include_directories(commands_test PUBLIC .)
target_link_libraries(commands_test PRIVATE testing_common_base)

# 1. Indexes Test Suite - consolidates index-related tests
set(INDEXES_TEST_SOURCES
    index_schema_test.cc
    lexer_test.cc
    numeric_index_test.cc
    posting_test.cc
    tag_index_test.cc
    text_test.cc
    vector_test.cc)

add_executable(indexes_test ${INDEXES_TEST_SOURCES})
target_include_directories(indexes_test PUBLIC .)
target_link_libraries(indexes_test PRIVATE testing_common_base)

# 1. Core Test Suite - consolidates core functionality tests
set(CORE_TEST_SOURCES
    valkey_search_test.cc
    schema_manager_test.cc
    keyspace_event_manager_test.cc
    server_events_test.cc
    attribute_data_type_test.cc
    multi_exec_test.cc
    vector_externalizer_test.cc
    acl_test.cc
    rdb_serialization_test.cc)

add_executable(core_test ${CORE_TEST_SOURCES})
target_include_directories(core_test PUBLIC .)
target_link_libraries(core_test PRIVATE testing_common_base)

# 1. Query Test Suite - consolidates query and search related tests
set(QUERY_TEST_SOURCES
    search_test.cc
    query/response_generator_test.cc)

add_executable(query_test ${QUERY_TEST_SOURCES})
target_include_directories(query_test PUBLIC .)
target_link_libraries(query_test PRIVATE testing_common_base)

# 1. Coordinator Test Suite - consolidates coordinator related tests
set(COORDINATOR_TEST_SOURCES
    coordinator/metadata_manager_test.cc
    coordinator/client_test.cc)

add_executable(coordinator_test ${COORDINATOR_TEST_SOURCES})
target_include_directories(coordinator_test PUBLIC .)
target_include_directories(coordinator_test
                           PUBLIC coordinator)
target_link_libraries(coordinator_test PRIVATE testing_common_base)

# 1. Utils Test Suite - consolidates utility tests
set(UTILS_TEST_SOURCES
    utils/allocator_test.cc
    utils/intrusive_list_test.cc
    utils/intrusive_ref_count_test.cc
    utils/lru_test.cc
    utils/patricia_tree_test.cc
    utils/numeric_btree_test.cc
    utils/string_interning_test.cc)

add_executable(valkey_utils_test ${UTILS_TEST_SOURCES})
target_include_directories(valkey_utils_test PUBLIC .)
target_include_directories(valkey_utils_test
                           PUBLIC utils)
target_link_libraries(valkey_utils_test PUBLIC testing_common_base)

# 7. Text Index Test Suite
add_executable(text_index_test
    flat_position_map_test.cc
    radix_test.cc
    rax_wrapper_test.cc
    text_index_schema_test.cc)
target_include_directories(text_index_test PUBLIC .)
target_link_libraries(text_index_test PRIVATE testing_common_base)