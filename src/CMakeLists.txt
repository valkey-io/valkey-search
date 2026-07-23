include (protobuf_helper)

set(SEARCH_SRCS
        acl.cc
        acl.h
        attribute.cc
        attribute.h
        attribute_data_type.cc
        attribute_data_type.h
        index_schema.cc
        index_schema.h
        keyspace_event_manager.cc
        keyspace_event_manager.h
        metrics.h
        module_loader.cc
        rdb_serialization.cc
        rdb_serialization.h
        schema_manager.cc
        schema_manager.h
        server_events.cc
        valkey_search.cc
        valkey_search.h
        valkey_search_options.cc
        vector_externalizer.cc
        vector_externalizer.h
        version.h)

# search_base is the collection of compiled objects that forms the final library/module
add_library(search_base OBJECT ${SEARCH_SRCS})
add_generated_proto_sources(search_base index_schema)
add_generated_proto_sources(search_base rdb_section)

# mydeps is the dependency bundle; it contains no objects, only include paths
add_library(mydeps INTERFACE)
target_include_directories(mydeps INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}) # src/
target_include_directories(mydeps INTERFACE ${CMAKE_SOURCE_DIR}) # top-level
target_include_directories(mydeps INTERFACE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(mydeps INTERFACE ${CMAKE_SOURCE_DIR}/vmsdk/include)
target_include_directories(mydeps INTERFACE ${VALKEY_PROTOGEN_BINARY_DIR})
target_include_directories(mydeps INTERFACE $<TARGET_PROPERTY:vmsdk,INTERFACE_INCLUDE_DIRECTORIES>)
target_include_directories(mydeps INTERFACE $<TARGET_PROPERTY:absl::base,INTERFACE_INCLUDE_DIRECTORIES>)

add_dependencies(mydeps protogen)

add_subdirectory(coordinator)
add_subdirectory(commands)
add_subdirectory(indexes)
add_subdirectory(expr)
add_subdirectory(utils)
add_subdirectory(query)

target_link_libraries(search_base PUBLIC gRPC::grpc++ protobuf::libprotobuf absl::base)

set (SEARCH_LIBS
        coordinator
        indexes
        expr
        query
        commands
        valkey_search::utils
        hnswlib
        hdrhistogram_c
        simsimd_c
        mydeps)

foreach (lib IN LISTS SEARCH_LIBS)
    target_link_libraries(search_base PUBLIC ${lib})
    link_target_objects(search_base ${lib})
endforeach()

add_library (search MODULE $<TARGET_OBJECTS:search_base>)
target_link_libraries(search PRIVATE search_base)
target_sources(search PRIVATE $<TARGET_OBJECTS:vmsdk>)
# testing/integration/run.sh and scripts/common.rc expect the module at a flat
# path, e.g. ${BUILD_DIR}/libsearch.so, not nested under src/.
set_target_properties(search PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

if (UNIX AND NOT APPLE)
    # Hide our malloc/free/new/delete override symbols (see
    # vmsdk/src/memory_allocation_overrides.h) so they don't interpose on
    # another module's (e.g. valkey-json's) allocator when both are loaded
    # into the same process. See integration/test_cross_module_compat.py.
    target_link_options(search PRIVATE
        "LINKER:--version-script=${CMAKE_SOURCE_DIR}/vmsdk/versionscript.lds")
endif ()

if (NOT VALKEY_VENDORED_DEPS)
    add_library (search_dso SHARED $<TARGET_OBJECTS:search_base>)
    target_link_libraries(search_dso PRIVATE search_base)

    target_link_libraries(search_dso PUBLIC
            vmsdk_dso
            gRPC::grpc++
            absl::base
            protobuf::libprotobuf)

    target_link_libraries(search_dso INTERFACE mydeps)
    target_link_libraries(search_dso INTERFACE ICU::uc ICU::i18n ICU::data)
endif ()