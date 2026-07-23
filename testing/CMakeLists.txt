# Base common test library - used by all test suites
set(SRCS_COMMON common.cc common.h)

if (VALKEY_VENDORED_DEPS)
    # when using vendored (static deps) mode, tests are not linked against the intermediate search_dso object
    # (which itself is also not built), because we end up with the same static deps being linked in twice.
    # I asked Claude to manually enumerate the dependencies of the _dso variants instead.
    include(object_library_helper)
    add_library(testing_common_objs OBJECT ${SRCS_COMMON})
    target_link_libraries(testing_common_objs PUBLIC search_base)
    target_link_libraries(testing_common_objs PUBLIC vmsdk)
    target_link_libraries(testing_common_objs PUBLIC vmsdk_testing)
    target_link_libraries(testing_common_objs PUBLIC GTest::gtest_main GTest::gmock)

    create_object_interface(testing_common_base search_base vmsdk testing_common_objs)
    target_link_libraries(testing_common_base INTERFACE gRPC::grpc++ protobuf::libprotobuf absl::base)
    target_link_libraries(testing_common_base INTERFACE ICU::uc ICU::i18n ICU::data)
    target_link_libraries(testing_common_base INTERFACE GTest::gtest_main GTest::gmock)
    target_link_libraries(testing_common_base INTERFACE vmsdk_testing)
else ()
    add_library(testing_common_base SHARED ${SRCS_COMMON})
    target_link_libraries(testing_common_base PUBLIC search_dso)
    target_link_libraries(testing_common_base PUBLIC vmsdk_dso)
    target_link_libraries(testing_common_base PUBLIC GTest::gtest_main GTest::gmock)
    target_link_libraries(testing_common_base PUBLIC vmsdk_testing)
endif ()

# Registers a test suite. Normally this creates its own test executable, but
# when UNIT_TESTS_SINGLE_EXE is set, all suites are instead accumulated and
# built into a single "unit_tests" executable (see bottom of this file) to
# cut down on redundant linking against the (potentially static) dependencies.
macro(add_test_suite name sources)
    if (UNIT_TESTS_SINGLE_EXE)
        list(APPEND TEST_ALL_SOURCES ${sources})
        if (ARGC GREATER 2)
            list(APPEND TEST_ALL_INCLUDE_DIRS ${ARGV2})
        endif ()
    else ()
        add_executable(${name} ${sources})
        target_include_directories(${name} PUBLIC . ${ARGV2})
        target_link_libraries(${name} PRIVATE testing_common_base)
        gtest_discover_tests(${name})
    endif ()
endmacro()

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

add_test_suite(commands_test "${COMMANDS_TEST_SOURCES}")

# 1. Indexes Test Suite - consolidates index-related tests
set(INDEXES_TEST_SOURCES
    index_schema_test.cc
    lexer_test.cc
    numeric_index_test.cc
    posting_test.cc
    tag_index_test.cc
    text_test.cc
    vector_test.cc)

add_test_suite(indexes_test "${INDEXES_TEST_SOURCES}")

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

add_test_suite(core_test "${CORE_TEST_SOURCES}")

# 1. Query Test Suite - consolidates query and search related tests
set(QUERY_TEST_SOURCES
    search_test.cc
    query/response_generator_test.cc)

add_test_suite(query_test "${QUERY_TEST_SOURCES}")

# 1. Coordinator Test Suite - consolidates coordinator related tests
set(COORDINATOR_TEST_SOURCES
    coordinator/metadata_manager_test.cc
    coordinator/client_test.cc)

add_test_suite(coordinator_test "${COORDINATOR_TEST_SOURCES}" coordinator)

# 1. Utils Test Suite - consolidates utility tests
set(UTILS_TEST_SOURCES
    utils/allocator_test.cc
    utils/intrusive_list_test.cc
    utils/intrusive_ref_count_test.cc
    utils/lru_test.cc
    utils/patricia_tree_test.cc
    utils/numeric_btree_test.cc
    utils/string_interning_test.cc)

add_test_suite(valkey_utils_test "${UTILS_TEST_SOURCES}" utils)

# 7. Text Index Test Suite
set(TEXT_INDEX_TEST_SOURCES
    flat_position_map_test.cc
    radix_test.cc
    rax_wrapper_test.cc
    text_index_schema_test.cc)

add_test_suite(text_index_test "${TEXT_INDEX_TEST_SOURCES}")

if (UNIT_TESTS_SINGLE_EXE)
    list(REMOVE_DUPLICATES TEST_ALL_INCLUDE_DIRS)

    add_executable(unit_tests ${TEST_ALL_SOURCES})
    target_include_directories(unit_tests PUBLIC . ${TEST_ALL_INCLUDE_DIRS})
    target_link_libraries(unit_tests PRIVATE testing_common_base)
    gtest_discover_tests(unit_tests)
endif ()
