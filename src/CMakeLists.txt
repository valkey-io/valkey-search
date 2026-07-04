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

add_library(search_base OBJECT ${SEARCH_SRCS})
add_generated_proto_sources(search_base index_schema)
add_generated_proto_sources(search_base rdb_section)

add_library(mydeps INTERFACE)
target_include_directories(mydeps INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}) # src/
target_include_directories(mydeps INTERFACE ${CMAKE_SOURCE_DIR}) # top-level
target_include_directories(mydeps INTERFACE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(mydeps INTERFACE ${CMAKE_SOURCE_DIR}/vmsdk/include)
target_include_directories(mydeps INTERFACE ${VALKEY_PROTOGEN_BINARY_DIR})

target_link_libraries(mydeps INTERFACE vmsdk)
target_sources(mydeps INTERFACE $<TARGET_OBJECTS:vmsdk>)

add_dependencies(mydeps protogen)

# hack so we don't need to get every single subcomponent explicitly
# link against abseil (Which the entire application uses anyway)..
get_target_property(ABSL_INCLUDE_DIRS absl::base INTERFACE_INCLUDE_DIRECTORIES)
message(STATUS "Abseil include directories: ${ABSL_INCLUDE_DIRS}")
target_include_directories(mydeps INTERFACE ${ABSL_INCLUDE_DIRS})

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
add_library (search_dso SHARED $<TARGET_OBJECTS:search_base>)

target_link_libraries(search PUBLIC search_base)
target_link_libraries(search_dso PUBLIC search_base)