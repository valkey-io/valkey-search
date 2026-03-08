add_library(fuzz_common STATIC fuzz_common.cc)

target_include_directories(fuzz_common PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
)

target_link_libraries(fuzz_common PUBLIC
    attribute_data_type
    commands
    ft_create_parser
    ft_search_parser
    index_schema
    keyspace_event_manager
    valkey_search
    search
    index_schema_cc_proto
    vmsdklib
    filter_parser
    predicate
    index_base
    string_interning
    allocator
    server_events
    rdb_serialization
    iostream
    valkey_module
    expr
)

if(UNIX AND NOT APPLE)
    target_link_options(fuzz_common PUBLIC "LINKER:--start-group")
    target_link_options(fuzz_common PUBLIC
                        "LINKER:--allow-multiple-definition")
endif()

foreach(__lib ${THIRD_PARTY_LIBS})
    target_link_libraries(fuzz_common PUBLIC ${__lib})
endforeach()

target_compile_options(fuzz_common PRIVATE -O1)
valkey_search_target_update_compile_flags(fuzz_common)

set_target_properties(fuzz_common PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)
