add_executable(fuzz_query_parser fuzz_query_parser.cc)

target_link_libraries(fuzz_query_parser PRIVATE fuzz_common)

set_target_properties(fuzz_query_parser PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

if(UNIX AND NOT APPLE)
    target_link_libraries(fuzz_query_parser PRIVATE lib_to_add_end_group_flag)
endif()

if(VALKEY_SEARCH_IS_ARM)
    target_link_libraries(fuzz_query_parser PRIVATE pthread)
endif()

if(SAN_BUILD)
    target_link_options(fuzz_query_parser PRIVATE "-fsanitize=${SAN_BUILD}")
endif()
