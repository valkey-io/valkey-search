add_executable(fuzz_expr fuzz_expr.cc)

target_link_libraries(fuzz_expr PRIVATE fuzz_common)

set_target_properties(fuzz_expr PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tests"
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

if(UNIX AND NOT APPLE)
    target_link_libraries(fuzz_expr PRIVATE lib_to_add_end_group_flag)
endif()

if(VALKEY_SEARCH_IS_ARM)
    target_link_libraries(fuzz_expr PRIVATE pthread)
endif()

if(SAN_BUILD)
    target_link_options(fuzz_expr PRIVATE "-fsanitize=${SAN_BUILD}")
endif()
