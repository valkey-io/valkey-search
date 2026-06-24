include (protobuf_helper)

add_library(coordinator OBJECT
        client.cc
        client.h
        client_pool.h
        grpc_suspender.cc
        grpc_suspender.h
        info_converter.cc
        info_converter.h
        metadata_manager.cc
        metadata_manager.h
        search_converter.cc
        search_converter.h
        server.cc
        server.h
        util.h)

add_generated_proto_sources(coordinator coordinator)
target_link_libraries(coordinator PUBLIC absl::base highwayhash)
target_link_libraries(coordinator PUBLIC mydeps)

target_include_directories(coordinator PRIVATE ${VALKEY_PROTOGEN_BINARY_DIR})
target_include_directories(coordinator PRIVATE ${CMAKE_SOURCE_DIR}/vmsdk/src)
set_source_files_properties(metadata_manager.cc PROPERTIES INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/src)

add_dependencies(coordinator protogen)